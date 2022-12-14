# Mongo Change Stream

Sync a MongoDB collection to any database. Requires Redis for state management.
An initial scan is performed while change stream events are handled. In order to
prevent a potential race condition see the strategies section below.

If the inital scan doesn't complete for any reason (e.g., server restart) the scan
will resume where it left off. This is deterministic since the collection scan is sorted
by `_id`. Change streams will likewise resume from the last resume token upon server
restarts. See the official MongoDB docs for more information on change stream resumption:

https://www.mongodb.com/docs/manual/changeStreams/#std-label-change-stream-resume

WARNING: If the Node process is stopped prior to receiving the initial change event for the
collection there is a risk that changes to documents that took place while the server
was restarting would be missed.

This library uses `debug`. To enable you can do something like:

```
DEBUG=mongochangestream node myfile.js
```

```typescript
import { ChangeStreamDocument, MongoClient } from 'mongodb'
import { default as Redis } from 'ioredis'
import { initSync } from 'mongochangestream'

const redis = new Redis()

const mongoUrl = 'mongodb+srv://...'
const client = await MongoClient.connect(mongoUrl)
const db = client.db('someDb')
const coll = db.collection('someColl')

const processRecord = async (doc: ChangeStreamDocument) => {
  console.dir(doc, { depth: 10 })
}
const processRecords = async (docs: ChangeStreamInsertDocument[]) => {
  console.dir(docs, { depth: 10 })
}

// Sync collection
const sync = initSync(redis, coll)
const initialScan = await sync.runInitialScan(processRecords)
initialScan.start()
// Process change stream
const changeStream = await sync.processChangeStream(processRecord)
changeStream.start()
setTimeout(changeStream.stop, 30000)
// Detect schema changes and ignore metadata fields (i.e., title and description)
const schemaChange = await sync.detectSchemaChange(db, {
  shouldRemoveMetadata: true,
})
schemaChange.start()
schemaChange.emitter.on('change', () => {
  initialScan.stop()
  changeStream.stop()
})
```

Below are the available methods.

The `processChangeStream` method will never complete, but `runInitialScan` will complete
once it has scanned all documents in the collection. `runInitialScan` batches records for
efficiency.

The `reset` method will delete all relevant keys for a given collection in Redis.

```typescript
import { ChangeStreamDocument, Collection, Document } from 'mongodb'

export type ProcessRecord = (doc: ChangeStreamDocument) => void | Promise<void>

export type ProcessRecords = (
  doc: ChangeStreamInsertDocument[]
) => void | Promise<void>

const runInitialScan = async (
  processRecords: ProcessRecords,
  options: QueueOptions & ScanOptions = {}
)

const processChangeStream = async (
  processRecord: ProcessRecord,
  pipeline: Document[] = []
)

const detectSchemaChange = async (db: Db, options: ChangeOptions = {})
```

## Change Stream Strategies

The idea behind these strategies is to prevent overwriting a document with
an out-of-date version of the document. In order to prevent that scenario
inserts must only succeed if the document doesn't already exist. Likewise,
updates must be capable of inserting the full document if it doesn't already
exist (i.e., perform a replace or an upsert).

The initial scan returns a simulated change event document with `operationType`
set to `insert`. An actual update change event will include the field-level changes
in addition to the full document after the change.

NOTE: Exceptions are not caught by this library. You must catch them in your
`processRecord` callback and handle them accordingly. For example, an insert
that fails due to a primary key already existing in the destination datastore.

### Elasticsearch

**Insert**

```
POST /index/_create/id
...
```

**Update**

```
POST /index/_doc/id
...
```

**Remove**

```
DELETE /index/_doc/id
```

### SQL (MySQL, CrateDB)

**Insert**

```sql
INSERT INTO table ...
```

**Update**

MySQL

```sql
INSERT INTO table ... ON DUPLICATE KEY UPDATE changedField = someValue
```

CrateDB

```sql
INSERT INTO table ... ON CONFLICT DO UPDATE SET changedField = someValue
```

**Remove**

```sql
DELETE FROM table WHERE id = someId
```

### MongoDB

**Insert**

```js
db.collection('someColl').insertOne(...)
```

**Update**

```js
db.collection('someColl').replaceOne({_id: ObjectId(...)}, ..., {upsert: true})
```

**Remove**

```js
db.collection('someColl').deleteOne({_id: ObjectId(...)})
```
