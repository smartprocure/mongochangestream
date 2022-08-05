import { ChangeStreamInsertDocument, Collection, ObjectId } from 'mongodb'
import changeStreamToIterator from './changeStreamToIterator.js'
import { ProcessRecord, ProcessRecords } from './types.js'
import _debug from 'debug'
import type { default as Redis } from 'ioredis'
import { batchQueue } from 'prom-utils'

const debug = _debug('mongodbChangeStream')

const keyPrefix = 'mongodbChangeStream'

const getCollectionKey = (collection: Collection) =>
  `${collection.dbName}:${collection.collectionName}`

/**
 * Get all Redis keys
 */
const getKeys = (collection: Collection) => {
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

export const initSync = (redis: Redis) => {
  /**
   * Run initial collection scan.
   */
  const runInitialScan = async (
    collection: Collection,
    processRecords: ProcessRecords,
    batchSize = 100
  ) => {
    debug('Running initial scan')
    // Redis keys
    const { scanCompletedKey, lastScanIdKey } = getKeys(collection)
    // Determine if initial scan has already completed
    const scanCompleted = await redis.get(scanCompletedKey)
    // Scan already completed so return
    if (scanCompleted) {
      debug(`Initial scan previously completed on %s`, scanCompleted)
      return
    }
    // Lookup last _id successfully processed
    const lastId = await redis.get(lastScanIdKey)
    debug('Last scan _id %s', lastId)
    const _processRecords = async (records: ChangeStreamInsertDocument[]) => {
      // Process batch of records
      await processRecords(records)
      // Record last id of the batch
      const lastId = records[records.length - 1].fullDocument._id.toString()
      await redis.set(lastScanIdKey, lastId)
    }
    // Create queue
    const queue = batchQueue(_processRecords, { batchSize })
    // Query collection
    const cursor = collection
      // Skip ids already processed
      .find(lastId ? { _id: { $gt: new ObjectId(lastId) } } : {})
      .sort({ _id: 1 })
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
    // Remove last scan id key
    await redis.del(lastScanIdKey)
    debug('Completed initial scan')
  }

  const defaultOptions = { fullDocument: 'updateLookup' }

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
    const changeStream = changeStreamToIterator(collection, pipeline, options)
    // Consume the events
    for await (const event of changeStream) {
      debug('Change stream event %O', event)
      // Get resume token
      const token = event?._id
      // Process record
      await processRecord(event)
      // Update change stream token
      await redis.set(changeStreamTokenKey, JSON.stringify(token))
    }
  }

  /**
   * Reset Redis state.
   */
  const reset = async (collection: Collection) => {
    const keys = Object.values(getKeys(collection))
    await redis.del(...keys)
  }

  return {
    runInitialScan,
    processChangeStream,
    reset,
  }
}
