import _ from 'lodash/fp.js'
import {
  ChangeStreamInsertDocument,
  Collection,
  ObjectId,
  Db,
  ChangeStream,
} from 'mongodb'
import * as mongodb from 'mongodb'
import {
  Events,
  SyncOptions,
  ProcessRecord,
  ProcessRecords,
  ScanOptions,
  ChangeOptions,
  ChangeStreamOptions,
  JSONSchema,
  State,
  SimpleState,
  SortField,
} from './types.js'
import _debug from 'debug'
import type { Redis } from 'ioredis'
import { batchQueue, defer, Deferred, QueueOptions } from 'prom-utils'
import {
  generatePipelineFromOmit,
  getCollectionKey,
  omitFieldForUpdate,
  removeMetadata,
  safelyCheckNext,
  setDefaults,
  when,
  delayed,
} from './util.js'
import EventEmitter from 'eventemitter3'
import ms from 'ms'
import { fsm, StateTransitions } from 'simple-machines'

const debug = _debug('mongochangestream')

const keyPrefix = 'mongoChangeStream'

/**
 * Get Redis keys used for the collection.
 */
export const getKeys = (collection: Collection) => {
  const collectionKey = getCollectionKey(collection)
  const collectionPrefix = `${keyPrefix}:${collectionKey}`
  const scanCompletedKey = `${collectionPrefix}:initialScanCompletedOn`
  const lastScanIdKey = `${collectionPrefix}:lastScanId`
  const changeStreamTokenKey = `${collectionPrefix}:changeStreamToken`
  const schemaKey = `${collectionPrefix}:schema`
  const lastChangeProcessedAtKey = `${collectionPrefix}:lastChangeProcessedAt`
  const lastScanProcessedAtKey = `${collectionPrefix}:lastScanProcessedAt`
  const resyncKey = `${collectionPrefix}:resync`
  return {
    scanCompletedKey,
    lastScanIdKey,
    changeStreamTokenKey,
    schemaKey,
    lastChangeProcessedAtKey,
    lastScanProcessedAtKey,
    resyncKey,
  }
}

const stateTransitions: StateTransitions<State> = {
  stopped: ['starting'],
  starting: ['started'],
  started: ['stopping'],
  stopping: ['stopped'],
}

const simpleStateTransistions: StateTransitions<SimpleState> = {
  started: ['stopped'],
  stopped: ['started'],
}

export function initSync<ExtendedEvents extends EventEmitter.ValidEventTypes>(
  redis: Redis,
  collection: Collection,
  options: SyncOptions = {}
) {
  const keys = getKeys(collection)
  const { omit } = options
  const emitter = new EventEmitter<Events | ExtendedEvents>()
  const emit = (event: Events, data: object) => {
    emitter.emit(event, { type: event, ...data })
  }
  const emitStateChange = (change: object) => emit('stateChange', change)

  /** Detect if resync flag is set */
  const detectResync = (resyncCheckInterval = ms('1m')) => {
    let resyncTimer: NodeJS.Timer
    const state = fsm(simpleStateTransistions, 'stopped', {
      name: 'Resync',
      onStateChange: emitStateChange,
    })

    const shouldResync = () => redis.exists(keys.resyncKey)

    const start = () => {
      debug('Starting resync check')
      if (state.is('started')) {
        return
      }
      // Check periodically if the collection should be resynced
      resyncTimer = setInterval(async () => {
        if (await shouldResync()) {
          debug('Resync triggered')
          emit('resync', {})
        }
      }, resyncCheckInterval)
      state.change('started')
    }

    const stop = () => {
      debug('Stopping resync check')
      if (state.is('stopped')) {
        return
      }
      clearInterval(resyncTimer)
      state.change('stopped')
    }

    return { start, stop }
  }

  /**
   * Retrieve value from Redis and parse as int if possible
   */
  const getLastSyncedAt = (key: string) =>
    redis.get(key).then((x) => {
      if (x) {
        return parseInt(x, 10)
      }
    })

  async function runInitialScan<T = any>(
    processRecords: ProcessRecords,
    options: QueueOptions & ScanOptions<T> = {}
  ) {
    let deferred: Deferred
    let cursor: ReturnType<typeof collection.aggregate>
    const state = fsm(stateTransitions, 'stopped', {
      name: 'Initial scan',
      onStateChange: emitStateChange,
    })
    const defaultSortField: SortField<ObjectId> = {
      field: '_id',
      serialize: _.toString,
      deserialize: (x: string) => new ObjectId(x),
    }

    const sortField = options.sortField || defaultSortField

    /** Get the last id inserted */
    const getLastIdInserted = () =>
      collection
        .find()
        .project({ [sortField.field]: 1 })
        .sort({ [sortField.field]: -1 })
        .limit(1)
        .toArray()
        .then((x) => {
          return x[0]?.[sortField.field]
        })

    const getCursor = async () => {
      // Lookup last id successfully processed
      const lastIdProcessed = await redis.get(keys.lastScanIdKey)
      debug('Last id processed %s', lastIdProcessed)
      // Query collection
      const omitPipeline = omit ? [{ $project: setDefaults(omit, 0) }] : []
      const extendedPipeline = options.pipeline ?? []
      const pipeline = [
        // Skip ids already processed
        ...(lastIdProcessed
          ? [
              {
                $match: {
                  [sortField.field]: {
                    $gt: sortField.deserialize(lastIdProcessed),
                  },
                },
              },
            ]
          : []),
        { $sort: { [sortField.field]: 1 } },
        ...omitPipeline,
        ...extendedPipeline,
      ]
      debug('Initial scan pipeline %O', pipeline)
      return collection.aggregate(pipeline)
    }

    /**
     * Periodically check that records are being processed.
     */
    const healthChecker = () => {
      const { interval = ms('1m') } = options.healthCheck || {}
      let timer: NodeJS.Timer
      const state = fsm(simpleStateTransistions, 'stopped', {
        name: 'Initial scan health check',
        onStateChange: emitStateChange,
      })

      const runHealthCheck = async () => {
        debug('Checking health - initial scan')
        const lastHealthCheck = new Date().getTime() - interval
        const withinHealthCheck = (x?: number) => x && x > lastHealthCheck
        const lastSyncedAt = await getLastSyncedAt(keys.lastScanProcessedAtKey)
        debug('Last scan processed at %d', lastSyncedAt)
        // Records were not synced within the health check window
        if (!withinHealthCheck(lastSyncedAt) && !state.is('stopped')) {
          debug('Health check failed - initial scan')
          emit('healthCheckFail', { failureType: 'initialScan', lastSyncedAt })
        }
      }
      const start = () => {
        debug('Starting health check - initial scan')
        if (state.is('started')) {
          return
        }
        timer = setInterval(runHealthCheck, interval)
        state.change('started')
      }
      const stop = () => {
        debug('Stopping health check - initial scan')
        if (state.is('stopped')) {
          return
        }
        clearInterval(timer)
        state.change('stopped')
      }
      return { start, stop }
    }

    const healthCheck = healthChecker()

    /** Start the initial scan */
    const start = async () => {
      debug('Starting initial scan')
      // Nothing to do
      if (state.is('starting', 'started')) {
        debug('Initial scan already starting or started')
        return
      }
      if (state.is('stopping')) {
        // Wait until stopping -> stopped
        await state.waitForChange('stopped')
      }
      state.change('starting')
      deferred = defer()
      // Determine if initial scan has already completed
      const scanCompleted = await redis.get(keys.scanCompletedKey)
      // Scan already completed so return
      if (scanCompleted) {
        debug(`Initial scan previously completed on %s`, scanCompleted)
        // We're done
        state.change('stopped')
        return
      }
      // Start the health check
      if (options.healthCheck?.enabled) {
        healthCheck.start()
      }

      const _processRecords = async (records: ChangeStreamInsertDocument[]) => {
        // Process batch of records
        await processRecords(records)
        debug('Processed %d records', records.length)
        const lastDocument = records[records.length - 1].fullDocument
        // Record last id of the batch
        const lastId = _.get(sortField.field, lastDocument)
        debug('Last id %s', lastId)
        if (lastId) {
          await redis.mset(
            keys.lastScanIdKey,
            sortField.serialize(lastId),
            keys.lastScanProcessedAtKey,
            new Date().getTime()
          )
        }
      }
      // Create queue
      const queue = batchQueue(_processRecords, options)
      // Query collection
      cursor = await getCursor()
      // Change state
      state.change('started')
      // Take a snapshot of the last id inserted into the collection
      const lastIdInserted = await getLastIdInserted()
      debug('Last id inserted %s', lastIdInserted)

      const ns = { db: collection.dbName, coll: collection.collectionName }
      const nextChecker = safelyCheckNext(cursor)
      // Process documents
      while (await nextChecker.hasNext()) {
        const doc = await cursor.next()
        debug('Initial scan doc %O', doc)
        // Doc can be null if cursor is closed
        if (doc) {
          const changeStreamDoc = {
            fullDocument: doc,
            operationType: 'insert',
            ns,
          } as unknown as ChangeStreamInsertDocument
          await queue.enqueue(changeStreamDoc)
        }
      }
      // Flush the queue
      await queue.flush()
      // Emit
      if (nextChecker.errorExists()) {
        emit('cursorError', nextChecker.getLastError())
      }
      // Final id processed
      const finalIdProcessed = await redis.get(keys.lastScanIdKey)
      debug('Final id processed %s', finalIdProcessed)
      // Did we complete the initial scan?
      if (
        // No records in the collection
        !lastIdInserted ||
        // Final id processed was at least the last id inserted as of start
        (finalIdProcessed &&
          sortField.deserialize(finalIdProcessed) >= lastIdInserted)
      ) {
        debug('Completed initial scan')
        // Stop the health check
        healthCheck.stop()
        // Record scan complete
        await redis.set(keys.scanCompletedKey, new Date().toString())
        // Emit event
        emit('initialScanComplete', { lastId: finalIdProcessed })
      }
      // Resolve deferred
      deferred.done()
      debug('Exit initial scan')
    }

    /** Stop the initial scan */
    const stop = async () => {
      debug('Stopping initial scan')
      // Nothing to do
      if (state.is('stopping', 'stopped')) {
        debug('Initial scan already stopping or stopped')
        return
      }
      if (state.is('starting')) {
        // Wait until starting -> started
        await state.waitForChange('started')
      }
      state.change('stopping')
      // Stop the health check
      healthCheck.stop()
      // Close the cursor
      await cursor?.close()
      debug('MongoDB cursor closed')
      // Wait for start fn to finish
      await deferred?.promise
      state.change('stopped')
      debug('Stopped initial scan')
    }

    /** Restart the initial scan */
    const restart = async () => {
      debug('Restarting initial scan')
      await stop()
      start()
    }

    return { start, stop, restart }
  }

  const processChangeStream = async (
    processRecord: ProcessRecord,
    options: ChangeStreamOptions = {}
  ) => {
    let deferred: Deferred
    let changeStream: ChangeStream
    const state = fsm(stateTransitions, 'stopped', {
      name: 'Change stream',
      onStateChange: emitStateChange,
    })
    const defaultOptions = { fullDocument: 'updateLookup' }

    const healthCheck = {
      enabled: false,
      maxSyncDelay: ms('5m'),
      interval: ms('1m'),
      ...options.healthCheck,
    }

    const getChangeStream = async () => {
      const omitPipeline = omit ? generatePipelineFromOmit(omit) : []
      const extendedPipeline = options.pipeline ?? []
      const pipeline = [...omitPipeline, ...extendedPipeline]
      debug('Change stream pipeline %O', pipeline)
      // Lookup change stream token
      const token = await redis.get(keys.changeStreamTokenKey)
      const changeStreamOptions: mongodb.ChangeStreamOptions = token
        ? // Resume token found, so set change stream resume point
          { ...defaultOptions, resumeAfter: JSON.parse(token) }
        : defaultOptions
      return collection.watch(pipeline, changeStreamOptions)
    }

    const runHealthCheck = async (eventReceivedAt: number) => {
      debug('Checking health - change stream')
      const lastSyncedAt = await getLastSyncedAt(keys.lastChangeProcessedAtKey)
      debug('Event received at %d', eventReceivedAt)
      debug('Last change processed at %d', lastSyncedAt)
      // Change stream event not synced
      if (!lastSyncedAt || lastSyncedAt < eventReceivedAt) {
        debug('Health check failed - change stream')
        emit('healthCheckFail', {
          failureType: 'changeStream',
          eventReceivedAt,
          lastSyncedAt,
        })
      }
    }

    const healthChecker = delayed(runHealthCheck, healthCheck.maxSyncDelay)

    /** Start processing change stream */
    const start = async () => {
      debug('Starting change stream')
      // Nothing to do
      if (state.is('starting', 'started')) {
        debug('Change stream already starting or started')
        return
      }
      if (state.is('stopping')) {
        // Wait until stopping -> stopped
        await state.waitForChange('stopped')
      }
      state.change('starting')
      // New deferred
      deferred = defer()
      // Start the change stream
      changeStream = await getChangeStream()
      state.change('started')
      const nextChecker = safelyCheckNext(changeStream)
      // Consume change stream
      while (await nextChecker.hasNext()) {
        // Schedule health check
        if (healthCheck.enabled) {
          healthChecker(new Date().getTime())
        }
        let event = await changeStream.next()
        debug('Change stream event %O', event)
        // Get resume token
        const token = event?._id
        debug('token %o', token)
        // Omit nested fields that are not handled by $unset.
        // For example, if 'a' was omitted then 'a.b.c' should be omitted.
        if (event.operationType === 'update' && omit) {
          event = omitFieldForUpdate(omit)(event)
        }
        // Process record
        await processRecord(event)

        await redis.mset(
          keys.changeStreamTokenKey,
          JSON.stringify(token),
          keys.lastChangeProcessedAtKey,
          new Date().getTime()
        )
      }
      // Emit
      if (nextChecker.errorExists()) {
        emit('cursorError', nextChecker.getLastError())
      }
      deferred.done()
      debug('Exit change stream')
    }

    /** Stop processing change stream */
    const stop = async () => {
      debug('Stopping change stream')
      // Nothing to do
      if (state.is('stopping', 'stopped')) {
        debug('Change stream already stopping or stopped')
        return
      }
      if (state.is('starting')) {
        // Wait until starting -> started
        await state.waitForChange('started')
      }
      state.change('stopping')
      // Cancel health check
      healthChecker.cancel()
      // Close the change stream
      await changeStream?.close()
      debug('MongoDB change stream closed')
      // Wait for start fn to finish
      await deferred?.promise
      state.change('stopped')
      debug('Stopped change stream')
    }

    /** Restart change stream */
    const restart = async () => {
      debug('Restarting change stream')
      await stop()
      start()
    }

    return { start, stop, restart }
  }

  const reset = async () => {
    debug('Reset')
    await redis.del(...Object.values(keys))
  }

  const getCollectionSchema = async (db: Db): Promise<JSONSchema> => {
    const colls = await db
      .listCollections({ name: collection.collectionName })
      .toArray()
    return _.get('0.options.validator.$jsonSchema', colls) || {}
  }

  /**
   * Get cached collection schema
   */
  const getCachedCollectionSchema = () =>
    redis.get(keys.schemaKey).then((val: any) => val && JSON.parse(val))

  const detectSchemaChange = async (db: Db, options: ChangeOptions = {}) => {
    const interval = options.interval || ms('1m')
    const shouldRemoveMetadata = options.shouldRemoveMetadata
    const maybeRemoveMetadata = when(shouldRemoveMetadata, removeMetadata)
    const state = fsm(simpleStateTransistions, 'stopped', {
      name: 'Schema change',
      onStateChange: emitStateChange,
    })

    let timer: NodeJS.Timer
    // Check for a cached schema
    let previousSchema = await getCachedCollectionSchema().then((schema) => {
      if (schema) {
        return maybeRemoveMetadata(schema)
      }
    })
    if (!previousSchema) {
      const schema = await getCollectionSchema(db).then(maybeRemoveMetadata)
      // Persist schema
      await redis.setnx(keys.schemaKey, JSON.stringify(schema))
      previousSchema = schema
    }
    debug('Previous schema %O', previousSchema)
    // Check for a schema change
    const checkForSchemaChange = async () => {
      const currentSchema = await getCollectionSchema(db).then(
        maybeRemoveMetadata
      )
      // Schemas are no longer the same
      if (!_.isEqual(currentSchema, previousSchema)) {
        debug('Schema change detected %O', currentSchema)
        // Persist schema
        await redis.set(keys.schemaKey, JSON.stringify(currentSchema))
        // Emit change
        emit('schemaChange', { previousSchema, currentSchema })
        // Previous schema is now the current schema
        previousSchema = currentSchema
      }
    }
    const start = () => {
      debug('Starting polling for schema changes')
      if (state.is('started')) {
        return
      }
      // Perform an inital check
      checkForSchemaChange()
      // Check for schema changes every interval
      timer = setInterval(checkForSchemaChange, interval)
      state.change('started')
    }
    const stop = () => {
      debug('Stopping polling for schema changes')
      if (state.is('stopped')) {
        return
      }
      clearInterval(timer)
      state.change('stopped')
    }
    return { start, stop }
  }

  return {
    /**
     * Run initial collection scan. `options.batchSize` defaults to 500.
     * Sorting defaults to `_id`.
     *
     * Call `start` to start processing documents and `stop` to close
     * the cursor.
     */
    runInitialScan,
    /**
     * Process MongoDB change stream for the collection.
     * If omit is passed to `initSync` a pipeline stage that removes
     * those fields will be prepended to the `pipeline` argument.
     *
     * Call `start` to start processing events and `stop` to close
     * the change stream.
     */
    processChangeStream,
    /**
     * Delete all Redis keys for the collection.
     */
    reset,
    /**
     * Get the existing JSON schema for the collection.
     */
    getCollectionSchema,
    /**
     * Check for schema changes every interval and emit 'change' event if found.
     * Optionally, set interval and strip metadata (i.e., title and description)
     * from the JSON schema.
     *
     * Call `start` to start polling for schema changes and `stop` to clear
     * the timer.
     */
    detectSchemaChange,
    /**
     * Determine if the collection should be resynced by checking for the existence
     * of the resync key in Redis.
     */
    detectResync,
    /**
     * Redis keys used for the collection.
     */
    keys,
    /** Event emitter */
    emitter,
  }
}
