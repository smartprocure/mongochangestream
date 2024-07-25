import _debug from 'debug'
import EventEmitter from 'eventemitter3'
import type { Redis } from 'ioredis'
import _ from 'lodash/fp.js'
import {
  type ChangeStream,
  type ChangeStreamDocument,
  type ChangeStreamInsertDocument,
  type Collection,
  type Db,
  type Document,
  ObjectId,
} from 'mongodb'
import * as mongodb from 'mongodb'
import ms from 'ms'
import { batchQueue, defer, type Deferred, type QueueOptions } from 'prom-utils'
import { fsm, type StateTransitions } from 'simple-machines'

import {
  ChangeOptions,
  ChangeStreamOptions,
  Events,
  JSONSchema,
  ProcessChangeStreamRecords,
  ProcessInitialScanRecords,
  ScanOptions,
  SimpleState,
  SortField,
  State,
  SyncOptions,
} from './types.js'
import {
  generatePipelineFromOmit,
  getCollectionKey,
  omitFieldForUpdate,
  removeUnusedFields,
  safelyCheckNext,
  setDefaults,
  when,
} from './util.js'

const debug = _debug('mongochangestream')

const keyPrefix = 'mongoChangeStream'

/**
 * Get Redis keys used for the collection.
 */
export const getKeys = (collection: Collection, options: SyncOptions) => {
  const collectionKey = getCollectionKey(collection)
  const uniqueId = options.uniqueId ? `:${options.uniqueId}` : ''
  const collectionPrefix = `${keyPrefix}:${collectionKey}${uniqueId}`
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
  starting: ['started', 'stopped'],
  started: ['stopping'],
  stopping: ['stopped'],
}

const simpleStateTransistions: StateTransitions<SimpleState> = {
  started: ['stopped'],
  stopped: ['started'],
}

const stateFields = ['get', 'is'] as const

export function initSync<ExtendedEvents extends EventEmitter.ValidEventTypes>(
  redis: Redis,
  collection: Collection,
  options: SyncOptions = {}
) {
  const keys = getKeys(collection, options)
  const { omit } = options
  const emitter = new EventEmitter<Events | ExtendedEvents>()
  const emit = (event: Events, data: object) => {
    emitter.emit(event, { type: event, ...data })
  }
  const emitStateChange = (change: object) => emit('stateChange', change)

  /** Detect if resync flag is set */
  const detectResync = (resyncCheckInterval = ms('1m')) => {
    let resyncTimer: NodeJS.Timeout
    const state = fsm(simpleStateTransistions, 'stopped', {
      name: 'detectResync',
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

  async function runInitialScan<T = any>(
    processRecords: ProcessInitialScanRecords,
    options: QueueOptions & ScanOptions<T> = {}
  ) {
    let deferred: Deferred
    let cursor: ReturnType<typeof collection.aggregate>
    const state = fsm(stateTransitions, 'stopped', {
      name: 'runInitialScan',
      onStateChange: emitStateChange,
    })
    const defaultSortField: SortField<ObjectId> = {
      field: '_id',
      serialize: _.toString,
      deserialize: (x: string) => new ObjectId(x),
      order: 'asc',
    }

    const sortField = options.sortField || defaultSortField

    const sortOrderToNum = { asc: 1, desc: -1 }
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
        {
          $sort: {
            [sortField.field]: sortOrderToNum[sortField.order ?? 'asc'],
          },
        },
        ...omitPipeline,
        ...extendedPipeline,
      ]
      debug('Initial scan pipeline %O', pipeline)
      return collection.aggregate(pipeline)
    }

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

      const ns = { db: collection.dbName, coll: collection.collectionName }
      const nextChecker = safelyCheckNext(cursor)
      let doc: Document | null
      // Process documents
      while ((doc = await nextChecker.getNext())) {
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
      // We are not stopping
      if (!state.is('stopping')) {
        // An error occurred getting next
        if (nextChecker.errorExists()) {
          emit('cursorError', {
            name: 'runInitialScan',
            error: nextChecker.getLastError(),
          })
        }
        // Exited cleanly from the loop so we're done
        else {
          debug('Completed initial scan')
          // Record scan complete
          await redis.set(keys.scanCompletedKey, new Date().toString())
          // Emit event
          emit('initialScanComplete', {})
        }
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

    return { start, stop, restart, state: _.pick(stateFields, state) }
  }

  const processChangeStream = async (
    processRecords: ProcessChangeStreamRecords,
    options: QueueOptions & ChangeStreamOptions = {}
  ) => {
    let deferred: Deferred
    let changeStream: ChangeStream
    const state = fsm(stateTransitions, 'stopped', {
      name: 'processChangeStream',
      onStateChange: emitStateChange,
    })
    const defaultOptions = { fullDocument: 'updateLookup' }
    const operationTypes = options.operationTypes
    debug('Operation types %o', operationTypes)

    /**
     * Get the change stream, resuming from a previous token if exists.
     */
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

      const _processRecords = async (records: ChangeStreamDocument[]) => {
        // Process batch of records
        await processRecords(records)
        debug('Processed %d records', records.length)
        const token = records[records.length - 1]._id
        debug('Token %s', token)
        if (token) {
          // Persist state
          await redis.mset(
            keys.changeStreamTokenKey,
            JSON.stringify(token),
            keys.lastChangeProcessedAtKey,
            new Date().getTime()
          )
        }
      }
      // New deferred
      deferred = defer()
      // Create queue
      const queue = batchQueue(_processRecords, {
        timeout: ms('30s'),
        ...options,
      })
      // Start the change stream
      changeStream = await getChangeStream()
      state.change('started')
      const nextChecker = safelyCheckNext(changeStream)
      let event: ChangeStreamDocument | null
      // Consume change stream
      while ((event = await nextChecker.getNext())) {
        debug('Change stream event %O', event)
        // Skip the event if the operation type is not one we care about
        if (operationTypes && !operationTypes.includes(event.operationType)) {
          debug('Skipping operation type: %s', event.operationType)
          continue
        }
        // Omit nested fields that are not handled by $unset.
        // For example, if 'a' was omitted then 'a.b.c' should be omitted.
        if (event.operationType === 'update' && omit) {
          event = omitFieldForUpdate(omit)(event) as ChangeStreamDocument
        }
        await queue.enqueue(event)
      }
      await queue.flush()
      // An error occurred getting next and we are not stopping
      if (nextChecker.errorExists() && !state.is('stopping')) {
        emit('cursorError', {
          name: 'processChangeStream',
          error: nextChecker.getLastError(),
        })
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

    return { start, stop, restart, state: _.pick(stateFields, state) }
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
    const shouldRemoveUnusedFields =
      options.shouldRemoveUnusedFields || options.shouldRemoveMetadata
    const maybeRemoveUnusedFields = when(
      shouldRemoveUnusedFields,
      removeUnusedFields
    )
    const state = fsm(simpleStateTransistions, 'stopped', {
      name: 'detectSchemaChange',
      onStateChange: emitStateChange,
    })

    let timer: NodeJS.Timeout
    // Check for a cached schema
    let previousSchema = await getCachedCollectionSchema().then((schema) => {
      if (schema) {
        return maybeRemoveUnusedFields(schema)
      }
    })
    if (!previousSchema) {
      const schema = await getCollectionSchema(db).then(maybeRemoveUnusedFields)
      // Persist schema
      await redis.setnx(keys.schemaKey, JSON.stringify(schema))
      previousSchema = schema
    }
    debug('Previous schema %O', previousSchema)
    // Check for a schema change
    const checkForSchemaChange = async () => {
      const currentSchema = await getCollectionSchema(db).then(
        maybeRemoveUnusedFields
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
