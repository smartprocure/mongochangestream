import _ from 'lodash/fp.js'
import {
  ChangeStreamInsertDocument,
  Collection,
  Document,
  ObjectId,
  Db,
} from 'mongodb'
import changeStreamToIterator from './changeStreamToIterator.js'
import {
  SyncOptions,
  ProcessRecord,
  ProcessRecords,
  ScanOptions,
} from './types.js'
import _debug from 'debug'
import type { default as Redis } from 'ioredis'
import { batchQueue, defer, Deferred, QueueOptions } from 'prom-utils'
import {
  generatePipelineFromOmit,
  getCollectionKey,
  omitFieldForUpdate,
  setDefaults,
} from './util.js'
import events from 'node:events'
import ms from 'ms'

const debug = _debug('mongochangestream')

const keyPrefix = 'mongoChangeStream'

/**
 * Get Redis keys used for the given collection.
 */
const getKeys = (collection: Collection) => {
  const collectionKey = getCollectionKey(collection)
  const scanPrefix = `${keyPrefix}:${collectionKey}`
  const scanCompletedKey = `${scanPrefix}:initialScanCompletedOn`
  const lastScanIdKey = `${scanPrefix}:lastScanId`
  const changeStreamTokenKey = `${keyPrefix}:${collectionKey}:changeStreamToken`
  const schemaKey = `${keyPrefix}:${collectionKey}:schema`
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
  options?: SyncOptions
) => {
  const keys = getKeys(collection)
  const omit = options?.omit
  const omitPipeline = omit ? generatePipelineFromOmit(omit) : []
  /**
   * Run initial collection scan. `options.batchSize` defaults to 500.
   * Sorting defaults to `_id`.
   */
  const runInitialScan = async (
    processRecords: ProcessRecords,
    options?: QueueOptions & ScanOptions
  ) => {
    debug('Running initial scan')
    const sortField = options?.sortField || defaultSortField
    // Redis keys
    const { scanCompletedKey, lastScanIdKey } = keys
    // Determine if initial scan has already completed
    const scanCompleted = await redis.get(scanCompletedKey)
    // Scan already completed so return
    if (scanCompleted) {
      debug(`Initial scan previously completed on %s`, scanCompleted)
      return
    }
    const _processRecords = async (records: ChangeStreamInsertDocument[]) => {
      // Process batch of records
      await processRecords(records)
      const lastDocument = records[records.length - 1].fullDocument
      // Record last id of the batch
      const lastId = _.get(sortField.field, lastDocument)
      if (lastId) {
        await redis.set(lastScanIdKey, sortField.serialize(lastId))
      }
    }
    // Lookup last id successfully processed
    const lastId = await redis.get(lastScanIdKey)
    debug('Last scan id %s', lastId)
    // Create queue
    const queue = batchQueue(_processRecords, options)
    // Query collection
    const cursor = collection
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
      const changeStreamDoc = {
        fullDocument: doc,
        operationType: 'insert',
        ns,
      } as unknown as ChangeStreamInsertDocument
      await queue.enqueue(changeStreamDoc)
    }
    // Flush the queue
    await queue.flush()
    // Record scan complete
    await redis.set(scanCompletedKey, new Date().toString())
    debug('Completed initial scan')
  }

  const defaultOptions = { fullDocument: 'updateLookup' }

  /**
   * Process MongoDB change stream for the given collection.
   * If omit is passed to `initSync` a pipeline stage that removes
   * those fields will be prepended to the `pipeline` argument.
   *
   * Call `start` to start processing events and `stop` to close
   * the change stream.
   */
  const processChangeStream = async (
    processRecord: ProcessRecord,
    pipeline: Document[] = []
  ) => {
    const abortController = new AbortController()
    let deferred: Deferred
    // Redis keys
    const { changeStreamTokenKey } = keys
    // Lookup change stream token
    const token = await redis.get(changeStreamTokenKey)
    const options = token
      ? // Resume token found, so set change stream resume point
        { ...defaultOptions, resumeAfter: JSON.parse(token) }
      : defaultOptions
    // Get the change stream as an async iterator
    const changeStream = changeStreamToIterator(
      collection,
      [...omitPipeline, ...pipeline],
      options
    )
    const start = async () => {
      for await (let event of changeStream) {
        debug('Change stream event %O', event)
        // Don't process event if stopping
        if (abortController.signal.aborted) {
          return
        }
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
        await redis.set(changeStreamTokenKey, JSON.stringify(token))
        deferred.done()
      }
    }
    const stop = () => {
      abortController.abort()
      // Wait for event to be processed
      return deferred?.promise
    }
    return { start, stop }
  }

  /**
   * Delete all Redis keys for the given collection.
   */
  const reset = async () => {
    await redis.del(...Object.values(keys))
  }

  /**
   * Delete completed on key in Redis for the given collection.
   */
  const clearCompletedOn = async () => {
    await redis.del(keys.scanCompletedKey)
  }

  /**
   * Get the existing JSON schema for the collection.
   */
  const getCollectionSchema = async (db: Db): Promise<object | undefined> => {
    const colls = await db
      .listCollections({ name: collection.collectionName })
      .toArray()
    return _.get('0.options.validator.$jsonSchema', colls)
  }

  /**
   * Get cached collection schema
   */
  const getCachedCollectionSchema = () =>
    redis.get(keys.schemaKey).then((val: any) => val && JSON.parse(val))
  /**
   * Check for schema changes every interval and emit 'change' event if found.
   */
  const detectSchemaChange = async (db: Db, interval = ms('10s')) => {
    const emitter = new events.EventEmitter()
    let timer: NodeJS.Timer
    // Check for a cached schema
    let previousSchema = await getCachedCollectionSchema()
    if (!previousSchema) {
      const schema = await getCollectionSchema(db)
      // Persist schema
      await redis.setnx(keys.schemaKey, JSON.stringify(schema))
      previousSchema = schema
    }
    // Check for a schema change
    const checkForSchemaChange = async () => {
      const currentSchema = await getCollectionSchema(db)
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
    const start = () => {
      debug('Started polling for schema changes')
      // Perform an inital check
      checkForSchemaChange()
      // Check for schema changes every interval
      timer = setInterval(checkForSchemaChange, interval)
    }
    const stop = () => {
      debug('Stopped polling for schema changes')
      clearInterval(timer)
    }
    return { start, stop, emitter }
  }

  return {
    runInitialScan,
    processChangeStream,
    reset,
    clearCompletedOn,
    getCollectionSchema,
    detectSchemaChange,
    keys,
  }
}
