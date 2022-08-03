# MongoDB Change Stream

```ts
const redis = new Redis()

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

**Elasticsearch**

Updates
POST /index/_doc/id
document

Inserts
This will fail if the document already exists.

POST /index/_create/id
document

Remove
DELETE /index/_doc/id

**CrateDB**

Updates
INSERT INTO table document ON CONFLICT DO UPDATE SET changedField = someValue

Inserts
This will fail if the record already exits due to the primary key.

INSERT INTO table document

Remove
DELETE FROM table WHERE id = someId
