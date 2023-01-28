/**
 * To run: MONGO_CONN="[conn string]" node dist/mongoChangeStream.test.js
 */
import _ from 'lodash/fp'
import { test } from 'node:test'
import assert from 'node:assert'
import { initSync } from './mongoChangeStream.js'
import { JSONSchema, SyncOptions } from './types.js'
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

const init = _.memoize(async (options?: SyncOptions) => {
  const redis = new Redis({ keyPrefix: 'testing:' })
  const client = await MongoClient.connect(process.env.MONGO_CONN as string)
  const db = client.db()
  const coll = db.collection('testing')
  const sync = initSync(redis, coll, options)
  sync.emitter.on('stateChange', console.log)

  return { sync, db, coll, redis }
})

const genUser = () => ({
  name: faker.name.fullName(),
  city: faker.address.city(),
  state: faker.address.state(),
  zipCode: faker.address.zipCode(),
  createdAt: faker.date.past(),
})

const schema: JSONSchema = {
  bsonType: 'object',
  additionalProperties: false,
  required: ['name'],
  properties: {
    _id: { bsonType: 'objectId' },
    name: { bsonType: 'string' },
    city: { bsonType: 'string' },
    state: { bsonType: 'string' },
    zipCode: { bsonType: 'string' },
    createdAt: { bsonType: 'date' },
  },
}

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
  // Reset state
  await sync.reset()
  await coll.deleteMany({})

  await populateCollection(coll)

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
  // Reset state
  await sync.reset()
  await coll.deleteMany({})

  await populateCollection(coll)

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
  // Wait for all documents to be processed
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

test('Should resync when resync flag is set', async () => {
  const { sync, coll, redis } = await init()
  // Reset state
  await sync.reset()
  await coll.deleteMany({})

  let resyncTriggered = false
  const processed = []

  await populateCollection(coll)

  const processRecords = async (docs: ChangeStreamInsertDocument[]) => {
    await setTimeout(50)
    processed.push(...docs)
  }

  const initialScan = await sync.runInitialScan(processRecords)
  const resync = sync.detectResync(250)
  initialScan.start()
  resync.start()
  // Allow for initial scan to start
  await setTimeout(500)
  // Trigger resync
  await redis.set(sync.keys.resyncKey, 1)

  sync.emitter.on('resync', async () => {
    resyncTriggered = true
    // Stop the initial scan
    await initialScan.stop()
    // Reset keys
    await sync.reset()
    // Reset processed
    processed.length = 0
    // Start initial scan
    initialScan.start()
  })
  // Wait for initial scan to complete
  await setTimeout(ms('5s'))
  assert.ok(resyncTriggered)
  assert.equal(processed.length, numDocs)
  await initialScan.stop()
  resync.stop()
})

test('Resync start/stop is idempotent', async () => {
  const { sync } = await init()

  const resync = sync.detectResync()
  resync.start()
  resync.start()
  resync.stop()
  resync.stop()
})

test('Detect schema change', async () => {
  const { db, coll, sync } = await init()
  // Set schema
  await db.command({
    collMod: coll.collectionName,
    validator: { $jsonSchema: schema },
  })
  // Look for a new schema every 250 ms
  const schemaChange = await sync.detectSchemaChange(db, {
    shouldRemoveMetadata: true,
    interval: 250,
  })
  let newSchema: object = {}
  sync.emitter.on('schemaChange', ({ currentSchema }) => {
    newSchema = currentSchema
  })
  // Start detecting schema changes
  schemaChange.start()
  // Modify the schema
  schema.properties.email = { bsonType: 'string' }
  await db.command({
    collMod: coll.collectionName,
    validator: { $jsonSchema: schema },
  })
  await setTimeout(ms('1s'))
  assert.deepEqual(schema, newSchema)
  schemaChange.stop()
})
