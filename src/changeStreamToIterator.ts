import {
  ChangeStreamDocument,
  ChangeStreamOptions,
  Collection,
  Document,
} from 'mongodb'
import _debug from 'debug'

const debug = _debug('mongoChangeStream')

const changeStreamToIterator = (
  collection: Collection,
  pipeline: Document[],
  options: ChangeStreamOptions
) => {
  const changeStream = collection.watch(pipeline, options)
  debug('Started change stream - pipeline %O options %O', pipeline, options)
  return {
    [Symbol.asyncIterator]() {
      return {
        async next() {
          if (changeStream.closed) {
            return { value: {} as ChangeStreamDocument, done: true }
          }
          return changeStream
            .next()
            .then((data) => ({ value: data, done: false }))
        },
      }
    },
  }
}
export default changeStreamToIterator
