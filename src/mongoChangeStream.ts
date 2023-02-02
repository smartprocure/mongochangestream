import _ from 'lodash/fp.js'
import {
  ChangeStreamInsertDocument,
  Collection,
  ObjectId,
  Db,
  ChangeStream,
} from 'mongodb'
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

export const defaultSortField = {
  field: '_id',
  serialize: _.toString,
  deserialize: (x: string) => new ObjectId(x),
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

export const initSync = (
  redis: Redis,
  collection: Collection,
  options: SyncOptions = {}
) => {
  const keys = getKeys(collection)
  const { omit } = options
  const omitPipeline = omit ? generatePipelineFromOmit(omit) : []
  const emitter = new EventEmitter()
  const emit = (event: Events, data: object) => {
    emitter.emit(event, { type: event, ...data })
  }
  const emitStateChange = (change: object) => emit('stateChange', change)

  // Detect if resync flag is set
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

  /**
   * Get the timestamp of the last record updated
   */
  const getLastRecordUpdatedAt = (field: string): Promise<number | undefined> =>
    collection
      .find({})
      .sort({ [field]: -1 })
      .project({ [field]: 1 })
      .limit(1)
      .toArray()
      .then((x) => {
        if (x.length) {
          return x[0][field]?.getTime()
        }
      })

  const runInitialScan = async (
    processRecords: ProcessRecords,
    options: QueueOptions & ScanOptions = {}
  ) => {
    let deferred: Deferred
    let cursor: ReturnType<typeof collection.find>
    const state = fsm(stateTransitions, 'stopped', {
      name: 'Initial scan',
      onStateChange: emitStateChange,
    })

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
      const sortField = options.sortField || defaultSortField
      // Redis keys
      const { scanCompletedKey, lastScanIdKey } = keys
      // Determine if initial scan has already completed
      const scanCompleted = await redis.get(scanCompletedKey)
      // Scan already completed so return
      if (scanCompleted) {
        debug(`Initial scan previously completed on %s`, scanCompleted)
        // We're done
        deferred.done()
        state.change('started')
        return
      }
      // Start the health check
      if (options.healthCheck?.enabled) {
        healthCheck.start()
      }

      const _processRecords = async (records: ChangeStreamInsertDocument[]) => {
        // Process batch of records
        await processRecords(records)
        const lastDocument = records[records.length - 1].fullDocument
        // Record last id of the batch
        const lastId = _.get(sortField.field, lastDocument)
        if (lastId) {
          await redis.mset(
            lastScanIdKey,
            sortField.serialize(lastId),
            keys.lastScanProcessedAtKey,
            new Date().getTime()
          )
        }
      }
      // Lookup last id successfully processed
      const lastId = await redis.get(lastScanIdKey)
      debug('Last scan id %s', lastId)
      // Create queue
      const queue = batchQueue(_processRecords, options)
      // Query collection
      cursor = collection
        // Skip ids already processed
        .find(
          lastId
            ? { [sortField.field]: { $gt: sortField.deserialize(lastId) } }
            : {},
          omit ? { projection: setDefaults(omit, 0) } : {}
        )
        .sort({ [sortField.field]: 1 })
      // Change state
      state.change('started')
      const ns = { db: collection.dbName, coll: collection.collectionName }
      // Process documents
      while (await safelyCheckNext(cursor)) {
        const doc = await cursor.next()
        debug('Initial scan doc %O', doc)
        const changeStreamDoc = {
          fullDocument: doc,
          operationType: 'insert',
          ns,
        } as unknown as ChangeStreamInsertDocument
        await queue.enqueue(changeStreamDoc)
      }
      // Flush the queue
      await queue.flush()
      // Don't record scan complete if stopping
      if (!state.is('stopping')) {
        debug('Completed initial scan')
        // Stop the health check
        healthCheck.stop()
        // Record scan complete
        await redis.set(scanCompletedKey, new Date().toString())
      }
      deferred.done()
      debug('Exit initial scan')
    }

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

    const restart = async () => {
      debug('Restarting initial scan')
      await stop()
      start()
    }

    return { start, stop, restart }
  }

  const defaultOptions = { fullDocument: 'updateLookup' }

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
    const pipeline = options.pipeline || []

    /**
     * Periodically check that change stream events are being processed.
     * Only applies to records inserted into the collection.
     */
    const healthChecker = () => {
      const {
        field,
        interval = ms('1m'),
        maxSyncDelay = ms('5m'),
      } = options.healthCheck || {}
      let timer: NodeJS.Timer
      const state = fsm(simpleStateTransistions, 'stopped', {
        name: 'Change stream health check',
        onStateChange: emitStateChange,
      })

      const runHealthCheck = async () => {
        debug('Checking health - change stream')
        const [lastSyncedAt, lastRecordUpdatedAt] = await Promise.all([
          getLastSyncedAt(keys.lastChangeProcessedAtKey),
          // Typescript hack
          getLastRecordUpdatedAt(field as string),
        ])
        debug('Last change processed at %d', lastSyncedAt)
        debug('Last record created at %d', lastRecordUpdatedAt)
        // A record was updated but not synced within 5 seconds of being updated
        if (
          !state.is('stopped') &&
          lastRecordUpdatedAt &&
          lastSyncedAt &&
          lastRecordUpdatedAt - lastSyncedAt > maxSyncDelay
        ) {
          debug('Health check failed - change stream')
          emit('healthCheckFail', {
            failureType: 'changeStream',
            lastRecordUpdatedAt,
            lastSyncedAt,
          })
        }
      }
      const start = () => {
        debug('Starting health check - change stream')
        if (state.is('started')) {
          return
        }
        timer = setInterval(runHealthCheck, interval)
        state.change('started')
      }
      const stop = () => {
        debug('Stopping health check - change stream')
        if (state.is('stopped')) {
          return
        }
        clearInterval(timer)
        state.change('stopped')
      }
      return { start, stop }
    }

    const healthCheck = healthChecker()

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
      // Redis keys
      const { changeStreamTokenKey } = keys
      // Lookup change stream token
      const token = await redis.get(changeStreamTokenKey)
      const changeStreamOptions = token
        ? // Resume token found, so set change stream resume point
          { ...defaultOptions, resumeAfter: JSON.parse(token) }
        : defaultOptions
      // Start the change stream
      changeStream = collection.watch(
        [...omitPipeline, ...pipeline],
        changeStreamOptions
      )
      state.change('started')
      // Start the health check
      if (options.healthCheck?.enabled) {
        healthCheck.start()
      }
      // Consume change stream
      while (await safelyCheckNext(changeStream)) {
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
        // Update change stream token
        await redis.mset(
          changeStreamTokenKey,
          JSON.stringify(token),
          keys.lastChangeProcessedAtKey,
          new Date().getTime()
        )
      }
      deferred.done()
      debug('Exit change stream')
    }

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
      // Stop the health check
      healthCheck.stop()
      // Close the change stream
      await changeStream?.close()
      debug('MongoDB change stream closed')
      // Wait for start fn to finish
      await deferred?.promise
      state.change('stopped')
      debug('Stopped change stream')
    }

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
