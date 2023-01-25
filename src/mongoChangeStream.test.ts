/**
 * To run: MONGO_CONN="[conn string]" node dist/mongoChangeStream.test.js
 */
import _ from 'lodash/fp'
import { test } from 'node:test'
import assert from 'node:assert'
import { initSync } from './mongoChangeStream.js'
import {
  ChangeStreamDocument,
  ChangeStreamInsertDocument,
  MongoClient,
  Collection,
} from 'mongodb'
import Redis from 'ioredis'
import { faker } from '@faker-js/faker'
import ms from 'ms'
import { setTimeout } from 'node:timers/promises'

const init = _.memoize(async () => {
  const redis = new Redis({ keyPrefix: 'testing:' })
  const client = await MongoClient.connect(process.env.MONGO_CONN as string)
  const db = client.db()
  const coll = db.collection('testing')
  return { sync: initSync(redis, coll), coll }
})

const genUser = () => ({
  name: faker.name.fullName(),
  city: faker.address.city(),
  state: faker.address.state(),
  zipCode: faker.address.zipCode(),
  createdAt: faker.date.past(),
})

const numDocs = 500

const populateCollection = (collection: Collection) => {
  const users = []
  for (let i = 0; i < numDocs; i++) {
    users.push({ insertOne: { document: genUser() } })
  }
  return collection.bulkWrite(users)
}

test('should complete initial scan', async () => {
  const { sync, coll } = await init()
  // Reset state
  await sync.reset()
  await coll.deleteMany({})

  await populateCollection(coll)
  const processed = []
  const processRecords = async (docs: ChangeStreamInsertDocument[]) => {
    await setTimeout(50)
    processed.push(...docs)
  }
  const scanOptions = { batchSize: 100 }
  const initialScan = await sync.runInitialScan(processRecords, scanOptions)
  // Wait for initial scan to complete
  await initialScan.start()
  assert.equal(processed.length, numDocs)
  await initialScan.stop()
})

test('should hangle initial scan start/stop', async () => {
  const { sync, coll } = await init()
  // Reset state
  await sync.reset()
  await coll.deleteMany({})

  await populateCollection(coll)
  const processed = []
  const processRecords = async (docs: ChangeStreamInsertDocument[]) => {
    await setTimeout(50)
    processed.push(...docs)
  }
  const scanOptions = { batchSize: 100 }
  const initialScan = await sync.runInitialScan(processRecords, scanOptions)
  initialScan.start()
  await setTimeout(50)
  // Stop the initial scan
  await initialScan.stop()
  // Wait for initial scan to complete
  await initialScan.start()
  assert.equal(processed.length, numDocs)
  await initialScan.stop()
})

test('should handle change stream', async () => {
  const { sync, coll } = await init()
  const processed = []
  const processRecord = async (doc: ChangeStreamDocument) => {
    await setTimeout(5)
    processed.push(doc)
  }
  const changeStream = await sync.processChangeStream(processRecord)
  changeStream.start()
  await setTimeout(ms('1s'))
  await coll.updateMany({}, { $set: { createdAt: new Date('2022-01-01') } })
  await setTimeout(ms('10s'))
  assert.equal(processed.length, numDocs)
  await changeStream.stop()
})

test('should handle change stream start/stop', async () => {
  const { sync, coll } = await init()
  const processed = []
  // Initial scan
  const processRecords = async () => {
    await setTimeout(50)
  }
  const scanOptions = { batchSize: 100 }
  const initialScan = await sync.runInitialScan(processRecords, scanOptions)
  // Change stream
  const processRecord = async (doc: ChangeStreamDocument) => {
    await setTimeout(5)
    processed.push(doc)
  }
  const changeStream = await sync.processChangeStream(processRecord)
  const start = () => {
    initialScan.start()
    changeStream.start()
  }
  const stop = () => Promise.all([initialScan.stop(), changeStream.stop()])
  // Start both
  start()
  // Let change stream connect
  await setTimeout(ms('1s'))
  // Change documents
  await coll.updateMany({}, { $set: { createdAt: new Date('2022-01-02') } })
  // Wait for change stream events to process
  await setTimeout(ms('2s'))
  // Stop both
  await stop()
  // Start both
  start()
  await setTimeout(ms('10s'))
  // All change stream docs were processed
  assert.equal(processed.length, numDocs)
  await stop()
})

test('stopping is idempotent', async () => {
  const { sync } = await init()
  const processed = []
  // Initial scan
  const processRecords = async () => {
    await setTimeout(50)
  }
  const scanOptions = { batchSize: 100 }
  const initialScan = await sync.runInitialScan(processRecords, scanOptions)
  // Change stream
  const processRecord = async (doc: ChangeStreamDocument) => {
    await setTimeout(5)
    processed.push(doc)
  }
  const changeStream = await sync.processChangeStream(processRecord)
  const start = () => {
    initialScan.start()
    changeStream.start()
  }
  const stop = () => Promise.all([initialScan.stop(), changeStream.stop()])
  // Start both
  start()
  // NOTE: It's possible to get in a stuck state where the change stream doesn't close if
  // this line doesn't exist.
  await setTimeout(50)
  await stop()
  await stop()
})
