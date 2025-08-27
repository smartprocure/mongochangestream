# Mongo Change Stream

Sync a MongoDB collection to any database. Requires Redis for state management.
An initial scan is performed while change stream events are handled. In order to
prevent a potential race condition see the strategies section below.

If the inital scan doesn't complete for any reason (e.g., server restart) the scan
will resume where it left off. This is deterministic since the collection scan is sorted
by `_id` by default. Change streams will likewise resume from the last resume token upon server
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
import { default as Redis } from 'ioredis'
import { initSync } from 'mongochangestream'
import { ChangeStreamDocument, MongoClient } from 'mongodb'

const redis = new Redis()

const mongoUrl = 'mongodb+srv://...'
const client = await MongoClient.connect(mongoUrl)
const db = client.db('someDb')
const coll = db.collection('someColl')

const processCSRecords = async (docs: ChangeStreamDocument[]) => {
  console.dir(docs, { depth: 10 })
}
const processRecords = async (docs: ChangeStreamInsertDocument[]) => {
  console.dir(docs, { depth: 10 })
}

// Sync collection
const sync = initSync(redis, coll)
const initialScan = await sync.runInitialScan(processRecords)
initialScan.start()
// Process change stream
const changeStream = await sync.processChangeStream(processCSRecords)
changeStream.start()
setTimeout(changeStream.stop, 30000)
// Detect schema changes and ignore metadata fields (i.e., title and description)
const schemaChange = await sync.detectSchemaChange(db, {
  shouldRemoveMetadata: true,
})
schemaChange.start()
sync.emitter.on('schemaChange', () => {
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

export type ProcessChangeStreamRecords = (
  docs: ChangeStreamDocument[]
) => MaybePromise<void>

export type ProcessInitialScanRecords = (
  docs: ChangeStreamInsertDocument[]
) => MaybePromise<void>

const runInitialScan = async (
  processRecords: ProcessInitialScanRecords,
  options: QueueOptions & ScanOptions = {}
)

const processChangeStream = async (
  processRecords: ProcessChangeStreamRecords,
  options: QueueOptions & ChangeStreamOptions = {}
)

const detectSchemaChange = async (db: Db, options: ChangeOptions = {})
```

## Maintaining Health

Look for the `processError` and `cursorError` events and restart the process or
resync as needed. See also the `missingOplogEntry` utility function that helps
determine if an oplog entry is no longer present and resumption of a change
stream from a previous point is not possible.

It is recommended that you run a periodic check (e.g., every minute) to determine
the health of the destination database that data is being synced to.

## Companion Libraries

This library is meant to be built on. To that end, the following libraries are
currently implemented and maintained.

Sync MongoDB to MongoDB
[mongo2mongo](https://www.npmjs.com/package/mongo2mongo)

Sync MongoDB to Elasticsearch
[mongo2elastic](https://www.npmjs.com/package/mongo2elastic)

Sync MongoDB to CrateDB
[mongo2crate](https://www.npmjs.com/package/mongo2crate)

## Resilience

Both the initial scan and change stream processing are designed to handle
and resume from failures. Here are some scenarios:

### The syncing server goes down

In this scenario, processing will continue with the last recorded state
when resumed.

### The syncing server is being shutdown with a sigterm

In this scenario, calling `stop` for the initial scan and change stream
will cleanly end processing.

### The MongoDB primary goes down and a new primary is elected

In this scenario, you will need to subscribe to the `cursorError` event and
restart the process or handle otherwise.

### Handling other errors

The `initialScan` and `processChangeStream` functions are designed to
automatically retry the provided `processRecords` function when an error is
thrown. This provides resilience in the event of transient issues like the
database being temporarily unavailable. The retries are implemented using
[p-retry](https://github.com/sindresorhus/p-retry). See `defaultRetryOptions`
for default options. You can also customize the retry behavior by providing your
own p-retry options (`options.retry`) to override the default settings.

If the error is still happening once all the retries are exhausted, a
`processError` event is emitted. These represent persistent errors that require
intervention of some kind.

### Limit throughput to prevent overloading the destination database

The `initialScan` and `processChangeStream` functions support throttling
via the `batchQueue` options - `maxItemsPerSec` and `maxBytesPerSec`. See
the [prom-utils](https://www.npmjs.com/package/prom-utils) library for more details.

## Change Stream Strategies

The idea behind these strategies is to prevent overwriting a document with
an out-of-date version of the document. In order to prevent that scenario
inserts must only succeed if the document doesn't already exist. Likewise,
updates must be capable of inserting the full document if it doesn't already
exist (i.e., perform a replace or an upsert).

The initial scan returns a simulated change event document with `operationType`
set to `insert`. An actual update change event will include the field-level changes
in addition to the full document after the change.

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

## Running tests

There are tests in this repo that can be run with `npm test`. The tests require
a running MongoDB instance and a running Redis instance.

By default, the tests will attempt to connect to Mongo at `localhost:27017` and
Redis at `localhost:6379`. You can override the Mongo connection string by
setting the `MONGO_CONN` environment variable, e.g. in a `.env` file.

> [!TIP]
> Before running the tests, make sure you:
>
> - ...have Node version 22 or higher installed.
> - ...have a MongoDB instance running and `MONGO_CONN` set to the connection
>   string.
> - ...have a Redis instance running on `localhost:6379`.
