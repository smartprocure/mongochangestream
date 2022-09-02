import _ from 'lodash/fp.js'
import {
  ChangeStreamInsertDocument,
  Collection,
  Document,
  ObjectId,
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
import { batchQueue, QueueOptions } from 'prom-utils'
import {
  generatePipelineFromOmit,
  omitFieldForUpdate,
  setDefaults,
} from './util.js'

const debug = _debug('mongochangestream')

const keyPrefix = 'mongoChangeStream'

const getCollectionKey = (collection: Collection) =>
  `${collection.dbName}:${collection.collectionName}`

/**
 * Get Redis keys used for the given collection.
 */
export const getKeys = (collection: Collection) => {
  const collectionKey = getCollectionKey(collection)
  const scanPrefix = `${keyPrefix}:${collectionKey}`
  const scanCompletedKey = `${scanPrefix}:initialScanCompletedOn`
  const lastScanIdKey = `${scanPrefix}:lastScanId`
  const changeStreamTokenKey = `${keyPrefix}:${collectionKey}:changeStreamToken`
  return {
    scanCompletedKey,
    lastScanIdKey,
    changeStreamTokenKey,
  }
}

export const defaultSortField = {
  field: '_id',
  serialize: _.toString,
  deserialize: (x: string) => new ObjectId(x),
}

export const initSync = (redis: Redis, options?: SyncOptions) => {
  const omit = options?.omit
  const omitPipeline = omit ? generatePipelineFromOmit(omit) : []
  /**
   * Run initial collection scan. `options.batchSize` defaults to 500.
   * Sorting defaults to `_id`.
   */
  const runInitialScan = async (
    collection: Collection,
    processRecords: ProcessRecords,
    options?: QueueOptions & ScanOptions
  ) => {
    debug('Running initial scan')
    const sortField = options?.sortField || defaultSortField
    // Redis keys
    const { scanCompletedKey, lastScanIdKey } = getKeys(collection)
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
   */
  const processChangeStream = async (
    collection: Collection,
    processRecord: ProcessRecord,
    pipeline: Document[] = []
  ) => {
    // Redis keys
    const { changeStreamTokenKey } = getKeys(collection)
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
    // Consume the events
    for await (let event of changeStream) {
      debug('Change stream event %O', event)
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
    }
  }

  /**
   * Delete all Redis keys for the given collection.
   */
  const reset = async (collection: Collection) => {
    const keys = Object.values(getKeys(collection))
    await redis.del(...keys)
  }

  /**
   * Delete completed on key in Redis for the given collection.
   */
  const clearCompletedOn = async (collection: Collection) => {
    const keys = getKeys(collection)
    await redis.del(keys.scanCompletedKey)
  }

  return {
    runInitialScan,
    processChangeStream,
    reset,
    clearCompletedOn,
  }
}
