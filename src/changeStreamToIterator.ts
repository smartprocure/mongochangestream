import { ChangeStreamOptions, Collection, Document } from 'mongodb'
import _debug from 'debug'

const debug = _debug('mongochangestream')

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
          return changeStream
            .next()
            .then((data) => ({ value: data, done: false }))
        },
      }
    },
  }
}
export default changeStreamToIterator
