/**
 * To run: MONGO_CONN="[conn string]" node dist/mongoChangeStream.test.js
 */
import _ from 'lodash/fp.js'
import { test } from 'node:test'
import assert from 'node:assert'
import { initSync } from './mongoChangeStream.js'
import {
  JSONSchema,
  SchemaChangeEvent,
  ScanOptions,
  SyncOptions,
  SortField,
  CursorErrorEvent,
} from './types.js'
import {
  Document,
  ChangeStreamDocument,
  ChangeStreamInsertDocument,
  MongoClient,
  Collection,
  ObjectId,
  Db,
} from 'mongodb'
import Redis from 'ioredis'
import { faker } from '@faker-js/faker'
import ms from 'ms'
import { setTimeout } from 'node:timers/promises'
import { QueueOptions } from 'prom-utils'
import { missingOplogEntry } from './util.js'

const getConns = _.memoize(async (x?: any) => {
  // Memoize hack
  console.log(x)
  const redis = new Redis({ keyPrefix: 'testing:' })
  const client = await MongoClient.connect(process.env.MONGO_CONN as string)
  const db = client.db()
  const coll = db.collection('testing')
  return { client, db, coll, redis }
})

const getSync = async (options?: SyncOptions) => {
  const { redis, coll } = await getConns()
  const sync = initSync(redis, coll, options)
  sync.emitter.on('stateChange', console.log)
  return sync
}

const genUser = () => ({
  name: faker.name.fullName(),
  address: {
    city: faker.address.city(),
    state: faker.address.state(),
    zipCode: faker.address.zipCode(),
  },
  createdAt: faker.date.past(),
})

const schema: JSONSchema = {
  bsonType: 'object',
  additionalProperties: false,
  required: ['name'],
  properties: {
    _id: { bsonType: 'objectId' },
    name: { bsonType: 'string' },
    address: {
      bsonType: 'object',
      properties: {
        city: { bsonType: 'string' },
        state: { bsonType: 'string' },
        zipCode: { bsonType: 'string' },
      },
    },
    createdAt: { bsonType: 'date' },
  },
}

const numDocs = 500

const populateCollection = (collection: Collection, count = numDocs) => {
  const users = []
  for (let i = 0; i < count; i++) {
    users.push({ insertOne: { document: genUser() } })
  }
  return collection.bulkWrite(users)
}

type SyncObj = Awaited<ReturnType<typeof getSync>>

const initState = async (sync: SyncObj, db: Db, coll: Collection) => {
  // Clear syncing state
  await sync.reset()
  // Delete all documents
  await coll.deleteMany({})
  // Set schema
  await db.command({
    collMod: coll.collectionName,
    validator: { $jsonSchema: schema },
  })
  // Populate data
  await populateCollection(coll)
}

test('should complete initial scan', async () => {
  const { coll, db } = await getConns()
  const sync = await getSync()
  await initState(sync, db, coll)

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

test('should exit cleanly if initial scan is already complete', async () => {
  const { coll, db, redis } = await getConns()
  const sync = await getSync()
  await initState(sync, db, coll)
  // Mark initial scan complete
  await redis.set(sync.keys.scanCompletedKey, new Date().toString())

  const processRecords = async () => {
    await setTimeout(50)
  }
  const scanOptions = { batchSize: 100 }
  const initialScan = await sync.runInitialScan(processRecords, scanOptions)
  await initialScan.start()
  await initialScan.stop()
})

test('should run initial scan in reverse sort order', async () => {
  const { coll, db } = await getConns()
  const sync = await getSync()
  await initState(sync, db, coll)

  const processed = []
  const processRecords = async (docs: ChangeStreamInsertDocument[]) => {
    await setTimeout(50)
    processed.push(...docs)
  }
  const sortField: SortField<ObjectId> = {
    field: '_id',
    serialize: _.toString,
    deserialize: (x: string) => new ObjectId(x),
    order: 'desc',
  }
  const scanOptions = { batchSize: 100, sortField }
  const initialScan = await sync.runInitialScan(processRecords, scanOptions)
  // Wait for initial scan to complete
  await initialScan.start()
  assert.equal(processed.length, numDocs)
  // Stop
  await initialScan.stop()
})

test('should omit fields from initial scan', async () => {
  const { coll, db } = await getConns()
  const sync = await getSync({ omit: ['name'] })
  await initState(sync, db, coll)

  const documents: Document[] = []
  const processRecords = async (docs: ChangeStreamInsertDocument[]) => {
    await setTimeout(50)
    documents.push(docs[0].fullDocument)
  }
  const scanOptions = { batchSize: 100 }
  const initialScan = await sync.runInitialScan(processRecords, scanOptions)
  // Wait for initial scan to complete
  await initialScan.start()
  assert.equal(documents[0].name, undefined)
  // Stop
  await initialScan.stop()
})

test('should complete initial scan if collection is empty', async () => {
  const { coll } = await getConns()
  const sync = await getSync()

  // Reset state
  await sync.reset()
  await coll.deleteMany({})

  const processed = []
  const processRecords = async (docs: ChangeStreamInsertDocument[]) => {
    await setTimeout(50)
    processed.push(...docs)
  }
  let completed = false
  sync.emitter.on('initialScanComplete', () => {
    completed = true
  })
  const scanOptions = { batchSize: 100 }
  const initialScan = await sync.runInitialScan(processRecords, scanOptions)
  // Wait for initial scan to complete
  await initialScan.start()
  assert.ok(completed)
  assert.equal(processed.length, 0)
  // Stop
  await initialScan.stop()
})

test('initial scan should resume after stop', async () => {
  const { coll, db } = await getConns()
  const sync = await getSync()
  await initState(sync, db, coll)

  const processed = []
  const processRecords = async (docs: ChangeStreamInsertDocument[]) => {
    await setTimeout(10)
    processed.push(...docs)
  }
  const scanOptions = { batchSize: 50 }
  const initialScan = await sync.runInitialScan(processRecords, scanOptions)
  let completed = false
  sync.emitter.on('initialScanComplete', () => {
    completed = true
  })
  let cursorError = false
  sync.emitter.on('cursorError', () => {
    cursorError = true
  })
  // Start
  initialScan.start()
  // Allow for some records to be processed
  await setTimeout(500)
  // Stop the initial scan
  await initialScan.stop()
  // Should not emit cursorError when stopping
  assert.equal(cursorError, false)
  // Wait for the initial scan to complete
  initialScan.start()
  // Add some more records
  await populateCollection(coll, 10)
  await setTimeout(ms('5s'))
  assert.ok(completed)
  assert.equal(processed.length, numDocs + 10)
  // Stop
  await initialScan.stop()
})

test('initial scan should not be marked as completed if connection is closed', async () => {
  // Get a new connection since we're closing the connection in the test
  const { coll, redis, db, client } = await getConns({})
  const sync = initSync(redis, coll)
  sync.emitter.on('stateChange', console.log)
  sync.emitter.on('cursorError', console.log)
  await initState(sync, db, coll)

  const processed = []
  const processRecords = async (docs: ChangeStreamInsertDocument[]) => {
    await setTimeout(10)
    processed.push(...docs)
  }
  const scanOptions = { batchSize: 50 }
  const initialScan = await sync.runInitialScan(processRecords, scanOptions)
  // Start
  initialScan.start()
  // Allow for some records to be processed
  await setTimeout(200)
  // Close the connection.
  await client.close()
  // Allow for some time
  await setTimeout(100)
  // Check if completed
  const completedAt = await redis.get(sync.keys.scanCompletedKey)
  assert.equal(completedAt, null)
  // Stop
  await initialScan.stop()
})

test('initial scan should support custom pipeline', async () => {
  const { coll, db } = await getConns()
  const sync = await getSync()
  await initState(sync, db, coll)

  sync.emitter.on('stateChange', console.log)

  const documents: Document[] = []
  const processRecords = async (docs: ChangeStreamInsertDocument[]) => {
    await setTimeout(10)
    documents.push(docs[0].fullDocument)
  }
  const scanOptions: QueueOptions & ScanOptions = {
    batchSize: 50,
    pipeline: [
      {
        $addFields: {
          cityState: { $concat: ['$address.city', '-', '$address.state'] },
        },
      },
    ],
  }
  const initialScan = await sync.runInitialScan(processRecords, scanOptions)
  // Start
  initialScan.start()
  // Allow for some records to be processed
  await setTimeout(500)
  // Stop
  await initialScan.stop()
  assert.ok(documents[0].cityState)
})

test('should process records via change stream', async () => {
  const { coll, db } = await getConns()
  const sync = await getSync()
  await initState(sync, db, coll)

  let cursorError = false
  sync.emitter.on('cursorError', () => {
    cursorError = true
  })
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
  // Should not emit cursorError when stopping
  assert.equal(cursorError, false)
})

test('should omit fields from change stream', async () => {
  const { coll, db } = await getConns()
  const sync = await getSync({ omit: ['name'] })
  await initState(sync, db, coll)

  const documents: Document[] = []
  const processRecord = async (doc: ChangeStreamDocument) => {
    await setTimeout(5)
    if (doc.operationType === 'update' && doc.fullDocument) {
      documents.push(doc.fullDocument)
    }
  }
  const changeStream = await sync.processChangeStream(processRecord)
  // Start
  changeStream.start()
  await setTimeout(ms('1s'))
  // Update records
  coll.updateMany({}, { $set: { name: 'unknown' } })
  // Wait for the change stream events to be processed
  await setTimeout(ms('2s'))
  assert.equal(documents[0].name, undefined)
  // Stop
  await changeStream.stop()
})

test('should omit nested fields when parent field is omitted from change stream', async () => {
  const { coll, db } = await getConns()
  const sync = await getSync({ omit: ['address'] })
  await initState(sync, db, coll)

  const documents: Document[] = []
  const processRecord = async (doc: ChangeStreamDocument) => {
    await setTimeout(5)
    if (doc.operationType === 'update' && doc.fullDocument) {
      documents.push(doc.fullDocument)
    }
  }
  const changeStream = await sync.processChangeStream(processRecord)
  // Start
  changeStream.start()
  await setTimeout(ms('1s'))
  // Update records
  coll.updateMany({}, { $set: { 'address.zipCode': '90210' } })
  // Wait for the change stream events to be processed
  await setTimeout(ms('2s'))
  assert.equal(documents[0].address?.zipCode, undefined)
  // Stop
  await changeStream.stop()
})

test('change stream should resume properly', async () => {
  const { coll, db } = await getConns()
  const sync = await getSync()
  await initState(sync, db, coll)

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

test('change stream handle missing oplog entry properly', async () => {
  const { coll, db, redis } = await getConns()
  const sync = await getSync()
  let cursorError: any
  sync.emitter.on('cursorError', (event: CursorErrorEvent) => {
    cursorError = event
  })

  await initState(sync, db, coll)

  // Set missing token key
  await redis.set(
    sync.keys.changeStreamTokenKey,
    '{"_data":"8263F51B8F000000012B022C0100296E5A1004F852F6C89F924F0A8711460F0C1FBD8846645F6964006463F51B8FD1AACE003022EFC80004"}'
  )

  // Change stream
  const processRecord = async () => {
    await setTimeout(5)
  }
  const changeStream = await sync.processChangeStream(processRecord)
  changeStream.start()
  // Let change stream connect
  await setTimeout(ms('1s'))

  assert.ok(missingOplogEntry(cursorError.error))
  await changeStream.stop()
})

test('change stream handle invalid oplog entry properly', async () => {
  const { coll, db, redis } = await getConns()
  const sync = await getSync()
  let cursorError: any
  sync.emitter.on('cursorError', (event: CursorErrorEvent) => {
    cursorError = event
  })

  await initState(sync, db, coll)

  // Set missing token key
  await redis.set(sync.keys.changeStreamTokenKey, '{"_data":"123"}')

  // Change stream
  const processRecord = async () => {
    await setTimeout(5)
  }
  const changeStream = await sync.processChangeStream(processRecord)
  changeStream.start()
  // Let change stream connect
  await setTimeout(ms('1s'))

  assert.ok(missingOplogEntry(cursorError.error))
  await changeStream.stop()
})

test('change stream should handle empty collection', async () => {
  const { coll, db } = await getConns()
  const sync = await getSync()
  let cursorError = false
  sync.emitter.on('cursorError', () => {
    cursorError = true
  })

  await initState(sync, db, coll)
  // Delete all documents
  await coll.deleteMany({})

  // Change stream
  const processRecord = async () => {
    await setTimeout(5)
  }
  const changeStream = await sync.processChangeStream(processRecord)
  changeStream.start()
  // Let change stream connect
  await setTimeout(ms('1s'))

  assert.equal(cursorError, false)
  await changeStream.stop()
})

test('should emit cursorError if change stream is closed', async () => {
  // Get a new connection since we're closing the connection in the test
  const { redis, coll, db, client } = await getConns({})
  const sync = initSync(redis, coll)

  let error: any
  sync.emitter.on('cursorError', (event: CursorErrorEvent) => {
    console.log(event)
    error = event.error
  })
  await initState(sync, db, coll)

  const processRecord = async () => {
    await setTimeout(500)
  }
  const changeStream = await sync.processChangeStream(processRecord)
  // Start
  changeStream.start()
  await setTimeout(ms('1s'))
  // Update records
  await coll.updateMany({}, { $set: { createdAt: new Date('2022-01-01') } })
  // Close the connection.
  await client.close()
  await setTimeout(ms('8s'))
  assert.ok(error?.message)
})

test('starting change stream is idempotent', async () => {
  const sync = await getSync()
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

test('stopping change stream is idempotent', async () => {
  const { coll, db } = await getConns()
  const sync = await getSync()
  await initState(sync, db, coll)

  // Change stream
  const processRecord = async () => {
    await setTimeout(5)
  }
  const changeStream = await sync.processChangeStream(processRecord)
  changeStream.start()
  // Change documents
  await coll.updateMany({}, { $set: { createdAt: new Date('2022-01-03') } })
  // Stop twice
  await changeStream.stop()
  await changeStream.stop()
})

test('starting initial scan is idempotent', async () => {
  const { coll, db } = await getConns()
  const sync = await getSync()
  await initState(sync, db, coll)

  const processRecords = async () => {
    await setTimeout(50)
  }
  const initialScan = await sync.runInitialScan(processRecords)
  // Start twice
  initialScan.start()
  initialScan.start()
  await initialScan.stop()
})

test('stopping initial scan is idempotent', async () => {
  const { coll, db } = await getConns()
  const sync = await getSync()
  await initState(sync, db, coll)

  const processRecords = async () => {
    await setTimeout(50)
  }
  const initialScan = await sync.runInitialScan(processRecords)
  initialScan.start()
  await setTimeout(500)
  // Stop twice
  await initialScan.stop()
  await initialScan.stop()
})

test('Should resync when resync flag is set', async () => {
  const { coll, db, redis } = await getConns()
  const sync = await getSync()
  await initState(sync, db, coll)

  let resyncTriggered = false
  const processed = []

  const processRecords = async (docs: ChangeStreamInsertDocument[]) => {
    await setTimeout(50)
    processed.push(...docs)
  }

  const initialScan = await sync.runInitialScan(processRecords)
  const resync = sync.detectResync(250)
  sync.emitter.on('resync', async () => {
    // Stop checking for resync
    resync.stop()
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
  // Start initial scan
  initialScan.start()
  // Start resync detection
  resync.start()
  // Allow for initial scan to start
  await setTimeout(500)
  // Trigger resync
  await redis.set(sync.keys.resyncKey, 1)

  // Wait for initial scan to complete
  await setTimeout(ms('5s'))
  assert.ok(resyncTriggered)
  assert.equal(processed.length, numDocs)
  await initialScan.stop()
})

test('Resync start/stop is idempotent', async () => {
  const sync = await getSync()

  const resync = sync.detectResync()
  resync.start()
  resync.start()
  resync.stop()
  resync.stop()
})

test('Detect schema change', async () => {
  const { db, coll } = await getConns()
  const sync = await getSync()
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
  sync.emitter.on('schemaChange', ({ currentSchema }: SchemaChangeEvent) => {
    console.dir(currentSchema, { depth: 10 })
    newSchema = currentSchema
  })
  // Start detecting schema changes
  schemaChange.start()
  // Modify the schema
  const modifiedSchema = _.set(
    'properties.email',
    { bsonType: 'string' },
    schema
  )
  await db.command({
    collMod: coll.collectionName,
    validator: { $jsonSchema: modifiedSchema },
  })
  await setTimeout(ms('1s'))
  assert.deepEqual(modifiedSchema, newSchema)
  schemaChange.stop()
})

test('Schema change start/stop is idempotent', async () => {
  const { db } = await getConns()
  const sync = await getSync()

  const schemaChange = await sync.detectSchemaChange(db)
  schemaChange.start()
  schemaChange.start()
  schemaChange.stop()
  schemaChange.stop()
})

test('can extend events', async () => {
  const { coll, redis } = await getConns({})
  const sync = initSync<'foo' | 'bar'>(redis, coll)
  let emitted = ''
  sync.emitter.on('foo', (x: string) => {
    emitted = x
  })
  sync.emitter.emit('foo', 'bar')
  assert.equal(emitted, 'bar')
})
