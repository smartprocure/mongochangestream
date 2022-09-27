import { defer } from 'prom-utils'
import type { ChangeStreamDocument, ChangeStream } from 'mongodb'

const done = { value: {} as ChangeStreamDocument, done: true }

const toIterator = (changeStream: ChangeStream) => {
  const deferred = defer()
  changeStream.once('close', deferred.done)
  return {
    [Symbol.asyncIterator]() {
      return {
        async next() {
          return Promise.race([
            deferred.promise.then(() => done),
            changeStream.next().then((data) => ({ value: data, done: false })),
          ])
        },
        async return() {
          await changeStream.close()
          return done
        },
      }
    },
  }
}
export default toIterator
