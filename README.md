# Mongo Change Stream

Sync a MongoDB collection to any database. Requires Redis for state management.
An initial scan is performed while change stream events are handled. In order to
prevent a potential race condition see the strategies section below.

Resumption will take place if the inital sync doesn't complete and the server is
restarted. Change streams will likewise resume from the last resume token. See docs
for more info: https://www.mongodb.com/docs/manual/changeStreams/#std-label-change-stream-resume

```ts
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

// Sync collection
const sync = initSync(redis)
await sync.syncCollection(coll, processRecord)
```

## Change Stream Strategies

### Elasticsearch

**Update**
```
POST /index/_doc/id
document
```

**Insert**
This will fail if the document already exists.

```
POST /index/_create/id
document
```

**Remove**
```
DELETE /index/_doc/id
```

### CrateDB

**Update**
```sql
INSERT INTO table document ON CONFLICT DO UPDATE SET changedField = someValue
```

**Insert**
This will fail if the record already exits due to the primary key.

```sql
INSERT INTO table document
```

**Remove**
```sql
DELETE FROM table WHERE id = someId
```
