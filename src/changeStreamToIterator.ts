import {
  ChangeStreamDocument,
  ChangeStreamOptions,
  Collection,
  Document,
} from 'mongodb'
import _debug from 'debug'
import { defer } from 'prom-utils'

const debug = _debug('mongochangestream')

const changeStreamToIterator = (
  collection: Collection,
  pipeline: Document[],
  signal: AbortSignal,
  options: ChangeStreamOptions
) => {
  const changeStream = collection.watch(pipeline, options)
  const deferred = defer()
  signal.onabort = async () => {
    deferred.done()
    await changeStream.close()
    debug('Closed change stream')
  }
  debug('Started change stream - pipeline %O options %O', pipeline, options)
  return {
    [Symbol.asyncIterator]() {
      return {
        async next() {
          return Promise.race([
            deferred.promise.then(() => ({
              value: {} as ChangeStreamDocument,
              done: true,
            })),
            changeStream.next().then((data) => ({ value: data, done: false })),
          ])
        },
      }
    },
  }
}
export default changeStreamToIterator
