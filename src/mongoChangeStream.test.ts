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
  // Stop
  await initialScan.stop()
})

test('initial scan should resume properly', async () => {
  const { sync, coll } = await init()
  // Reset state
  await sync.reset()
  await coll.deleteMany({})
  // Populate data
  await populateCollection(coll)
  const processed = []
  const processRecords = async (docs: ChangeStreamInsertDocument[]) => {
    await setTimeout(50)
    processed.push(...docs)
  }
  const scanOptions = { batchSize: 100 }
  const initialScan = await sync.runInitialScan(processRecords, scanOptions)
  // Start
  initialScan.start()
  // Allow for some records to be processed
  await setTimeout(50)
  // Stop the initial scan
  await initialScan.stop()
  // Wait for the initial scan to complete
  await initialScan.start()
  assert.equal(processed.length, numDocs)
  // Stop
  await initialScan.stop()
})

test('should process records via change stream', async () => {
  const { sync, coll } = await init()
  const processed = []
  const processRecord = async (doc: ChangeStreamDocument) => {
    await setTimeout(5)
    processed.push(doc)
  }
  const changeStream = await sync.processChangeStream(processRecord)
  // Start
  changeStream.start()
  await setTimeout(ms('1s'))
  // Update records
  coll.updateMany({}, { $set: { createdAt: new Date('2022-01-01') } })
  // Wait for the change stream events to be processed
  await setTimeout(ms('10s'))
  assert.equal(processed.length, numDocs)
  // Stop
  await changeStream.stop()
})

test('change stream should resume properly', async () => {
  const { sync, coll } = await init()
  const processed = []
  // Change stream
  const processRecord = async (doc: ChangeStreamDocument) => {
    await setTimeout(5)
    processed.push(doc)
  }
  const changeStream = await sync.processChangeStream(processRecord)
  changeStream.start()
  // Let change stream connect
  await setTimeout(ms('1s'))
  // Change documents
  coll.updateMany({}, { $set: { createdAt: new Date('2022-01-02') } })
  // Wait for some change stream events to be processed
  await setTimeout(ms('2s'))
  // Stop
  await changeStream.stop()
  // Resume change stream
  changeStream.start()
  await setTimeout(ms('10s'))
  // All change stream docs were processed
  assert.equal(processed.length, numDocs)
  await changeStream.stop()
})

test('stopping change stream is idempotent', async () => {
  const { sync, coll } = await init()
  // Change stream
  const processRecord = async () => {
    await setTimeout(5)
  }
  const changeStream = await sync.processChangeStream(processRecord)
  changeStream.start()
  // Change documents
  coll.updateMany({}, { $set: { createdAt: new Date('2022-01-03') } })
  // Stop twice
  await changeStream.stop()
  await changeStream.stop()
})

test('stopping initial scan is idempotent', async () => {
  const { sync } = await init()
  const processRecords = async () => {
    await setTimeout(50)
  }
  const initialScan = await sync.runInitialScan(processRecords)
  initialScan.start()
  // Stop twice
  await initialScan.stop()
  await initialScan.stop()
})

test('starting change stream is idempotent', async () => {
  const { sync } = await init()
  // Change stream
  const processRecord = async () => {
    await setTimeout(5)
  }
  const changeStream = await sync.processChangeStream(processRecord)
  // Start twice
  changeStream.start()
  changeStream.start()
  await changeStream.stop()
})

test('starting initial scan is idempotent', async () => {
  const { sync, coll } = await init()
  // Reset state
  await sync.reset()
  await coll.deleteMany({})

  const processRecords = async () => {
    await setTimeout(50)
  }
  const initialScan = await sync.runInitialScan(processRecords)
  // Start twice
  initialScan.start()
  initialScan.start()
  await initialScan.stop()
})
