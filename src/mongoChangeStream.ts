import _ from 'lodash/fp.js'
import {
  ChangeStreamInsertDocument,
  Collection,
  ObjectId,
  Db,
  ChangeStream,
} from 'mongodb'
import toIterator from './changeStreamToIterator.js'
import {
  Events,
  SyncOptions,
  ProcessRecord,
  ProcessRecords,
  ScanOptions,
  ChangeOptions,
  ChangeStreamOptions,
  JSONSchema,
} from './types.js'
import _debug from 'debug'
import type { Redis } from 'ioredis'
import { batchQueue, defer, Deferred, QueueOptions } from 'prom-utils'
import {
  generatePipelineFromOmit,
  getCollectionKey,
  omitFieldForUpdate,
  removeMetadata,
  setDefaults,
  when,
} from './util.js'
import EventEmitter from 'eventemitter3'
import ms from 'ms'

const debug = _debug('mongochangestream')

const keyPrefix = 'mongoChangeStream'

/**
 * Get Redis keys used for the collection.
 */
const getKeys = (collection: Collection) => {
  const collectionKey = getCollectionKey(collection)
  const collectionPrefix = `${keyPrefix}:${collectionKey}`
  const scanCompletedKey = `${collectionPrefix}:initialScanCompletedOn`
  const lastScanIdKey = `${collectionPrefix}:lastScanId`
  const changeStreamTokenKey = `${collectionPrefix}:changeStreamToken`
  const schemaKey = `${collectionPrefix}:schema`
  return {
    scanCompletedKey,
    lastScanIdKey,
    changeStreamTokenKey,
    schemaKey,
  }
}

export const defaultSortField = {
  field: '_id',
  serialize: _.toString,
  deserialize: (x: string) => new ObjectId(x),
}

export const initSync = (
  redis: Redis,
  collection: Collection,
  options: SyncOptions = {}
) => {
  const keys = getKeys(collection)
  const omit = options.omit
  const omitPipeline = omit ? generatePipelineFromOmit(omit) : []

  const getLastSyncedAt = (key: string) =>
    redis.get(key).then((x) => {
      if (x) {
        return parseInt(x, 10)
      }
    })

  const getLastRecordCreatedAt = (): Promise<number> =>
    collection
      .find({})
      .sort({ _id: -1 })
      .project({ _id: 1 })
      .limit(1)
      .toArray()
      .then((x) => {
        if (x.length) {
          return x[0]._id.getTimestamp().getTime()
        }
      })

  const runInitialScan = async (
    processRecords: ProcessRecords,
    options: QueueOptions & ScanOptions = {}
  ) => {
    let deferred: Deferred
    let cursor: ReturnType<typeof collection.find>
    let aborted: boolean

    const maintainHealth = (healthCheckInterval = ms('1m')) => {
      let timer: NodeJS.Timer
      let stopped: boolean

      const getLastHealthCheck = () =>
        new Date().getTime() - healthCheckInterval

      const checkHealth = async () => {
        const lastSyncedAt = await getLastSyncedAt('lastScanProcessedAt')
        // A entire health check interval has passed
        if (!stopped && lastSyncedAt && lastSyncedAt < getLastHealthCheck()) {
          debug('Restarting initial scan')
          restart()
        }
      }
      const start = () => {
        debug('Starting initial scan health monitoring')
        stopped = false
        timer = setInterval(checkHealth, healthCheckInterval)
      }
      const stop = () => {
        debug('Stopping initial scan health monitoring')
        stopped = true
        clearInterval(timer)
      }
      return { start, stop }
    }

    const healthCheck = maintainHealth(options.healthCheckInterval)

    const start = async () => {
      debug('Starting initial scan')
      aborted = false
      const sortField = options.sortField || defaultSortField
      // Redis keys
      const { scanCompletedKey, lastScanIdKey } = keys
      // Determine if initial scan has already completed
      const scanCompleted = await redis.get(scanCompletedKey)
      // Scan already completed so return
      if (scanCompleted) {
        debug(`Initial scan previously completed on %s`, scanCompleted)
        // Exit
        return
      }
      if (options.maintainHealth) {
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
            'lastScanProcessedAt',
            new Date().getTime()
          )
        }
        deferred.done()
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
      const ns = { db: collection.dbName, coll: collection.collectionName }
      // Process documents
      for await (const doc of cursor) {
        debug('Initial scan doc %O', doc)
        deferred = defer()
        const changeStreamDoc = {
          fullDocument: doc,
          operationType: 'insert',
          ns,
        } as unknown as ChangeStreamInsertDocument
        await queue.enqueue(changeStreamDoc)
      }
      // Flush the queue
      await queue.flush()
      // Don't record scan complete if aborted
      if (!aborted) {
        debug('Completed initial scan')
        // Stop the health check
        healthCheck.stop()
        // Record scan complete
        await redis.set(scanCompletedKey, new Date().toString())
      }
    }

    const stop = async () => {
      debug('Stopping initial scan')
      aborted = true
      // Stop the health check
      healthCheck.stop()
      // Close the cursor
      await cursor?.close()
      // Wait for the queue to be flushed
      await deferred?.promise
    }

    const restart = async () => {
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
    const pipeline = options.pipeline || []

    const maintainHealth = (healthCheckInterval = ms('1m')) => {
      let timer: NodeJS.Timer
      let stopped: boolean
      let lastRecordCreatedAt: number | undefined

      const getLastHealthCheck = () =>
        new Date().getTime() - healthCheckInterval

      const checkHealth = async () => {
        if (!lastRecordCreatedAt) {
          const time = await getLastRecordCreatedAt()
          // Record was created within the health check interval
          if (time > getLastHealthCheck()) {
            lastRecordCreatedAt = time
          }
        }
        if (lastRecordCreatedAt && lastRecordCreatedAt < getLastHealthCheck()) {
          const lastSyncedAt = await getLastSyncedAt('lastChangeProcessedAt')
          if (!lastSyncedAt || lastSyncedAt < lastRecordCreatedAt) {
            debug('Restarting change stream')
            restart()
          }
          lastRecordCreatedAt = undefined
        }
      }
      const start = () => {
        debug('Starting change stream health monitoring')
        stopped = false
        timer = setInterval(checkHealth, healthCheckInterval)
      }
      const stop = () => {
        debug('Stopping change stream health monitoring')
        stopped = true
        clearInterval(timer)
      }
      return { start, stop }
    }

    const healthCheck = maintainHealth(options.healthCheckInterval)

    const start = async () => {
      debug('Starting change stream')
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
      const iterator = toIterator(changeStream)
      // Start the health check
      if (options.maintainHealth) {
        healthCheck.start()
      }
      // Get the change stream as an async iterator
      for await (let event of iterator) {
        debug('Change stream event %O', event)
        deferred = defer()
        // Get resume token
        const token = event?._id
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
          'lastChangeProcessedAt',
          new Date().getTime()
        )
        deferred.done()
      }
    }

    const stop = async () => {
      debug('Stopping change stream')
      // Stop the health check
      healthCheck.stop()
      // Close the change stream
      await changeStream?.close()
      // Wait for event to be processed
      await deferred?.promise
    }

    const restart = async () => {
      await stop()
      start()
    }

    return { start, stop, restart }
  }

  const reset = async () => {
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

    const emitter = new EventEmitter<Events>()
    let timer: NodeJS.Timer
    // Check for a cached schema
    let previousSchema = await getCachedCollectionSchema().then(
      (schema) => schema && maybeRemoveMetadata(schema)
    )
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
        emitter.emit('change', { previousSchema, currentSchema })
        // Previous schema is now the current schema
        previousSchema = currentSchema
      }
    }
    const start = async () => {
      debug('Starting polling for schema changes')
      // Perform an inital check
      await checkForSchemaChange()
      // Check for schema changes every interval
      timer = setInterval(checkForSchemaChange, interval)
    }
    const stop = () => {
      debug('Stopping polling for schema changes')
      clearInterval(timer)
    }
    return { start, stop, emitter }
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
     * Redis keys used for the collection.
     */
    keys,
  }
}
