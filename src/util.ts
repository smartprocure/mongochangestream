import { Collection } from 'mongodb'
import _ from 'lodash/fp.js'
import { Node, walkie } from 'obj-walker'
import { Cursor, JSONSchema } from './types.js'
import _debug from 'debug'

const debug = _debug('mongochangestream')

export const setDefaults = (keys: string[], val: any) => {
  const obj: Record<string, any> = {}
  for (const key of keys) {
    obj[key] = val
  }
  return obj
}

export const generatePipelineFromOmit = (omit: string[]) => {
  const fields = omit.flatMap((field) => [
    `fullDocument.${field}`,
    `updateDescription.updatedFields.${field}`,
  ])
  return [{ $unset: fields }]
}

export const omitFields = (omitPaths: string[]) =>
  _.omitBy((val, key) =>
    _.find((omitPath) => _.startsWith(`${omitPath}.`, key), omitPaths)
  )

export const omitFieldForUpdate = (omitPaths: string[]) =>
  _.update('updateDescription.updatedFields', omitFields(omitPaths))

export const getCollectionKey = (collection: Collection) =>
  `${collection.dbName}:${collection.collectionName}`

export const traverseSchema = (x: JSONSchema) =>
  x.properties || (x.items && { _items: x.items })

/**
 * Remove title and description from a JSON schema.
 */
export const removeMetadata = (schema: JSONSchema): JSONSchema => {
  const walkFn = ({ val }: Node) => {
    if ('title' in val) {
      delete val.title
    }
    if ('description' in val) {
      delete val.description
    }
  }
  return walkie(schema, walkFn, { traverse: traverseSchema })
}

export function when<T, R>(condition: any, fn: (x: T) => R) {
  return function (x: T) {
    return condition ? fn(x) : x
  }
}

/**
 * Check if the cursor has next without throwing an exception.
 * Get the last error safely via `getLastError`.
 */
export const safelyCheckNext = (cursor: Cursor) => {
  let lastError: unknown

  const hasNext = async () => {
    debug('safelyCheckNext called')
    try {
      // Prevents hasNext from hanging when the cursor is already closed
      if (cursor.closed) {
        debug('safelyCheckNext cursor closed')
        lastError = new Error('cursor closed')
        return false
      }
      return await cursor.hasNext()
    } catch (e) {
      debug('safelyCheckNext error: %o', e)
      lastError = e
      return false
    }
  }

  const errorExists = () => Boolean(lastError)
  const getLastError = () => ({ error: lastError })

  return { hasNext, errorExists, getLastError }
}

/**
 * Check if error message indicates a missing oplog entry.
 */
export const missingOplogEntry = (x: string) =>
  x.includes('resume point may no longer be in the oplog')

/**
 * Creates a delayed function that only invokes fn at most once every ms.
 * The first invocation will be the one that gets executed. Subsequent calls
 * to the delayed function will be dropped if there is a pending invocation.
 *
 * The delayed function comes with a cancel method to cancel the delayed fn
 * invocation and a flush method to immediately invoke it.
 */
export const delayed = (fn: (...args: any[]) => void, ms: number) => {
  let timeoutId: NodeJS.Timeout
  let scheduled = false
  let callback: () => void

  const delayedFn = (...args: any[]) => {
    if (!scheduled) {
      scheduled = true
      callback = () => {
        fn(...args)
        scheduled = false
      }
      timeoutId = setTimeout(callback, ms)
    }
  }
  delayedFn.cancel = () => {
    clearTimeout(timeoutId)
    scheduled = false
  }
  delayedFn.flush = () => {
    if (scheduled) {
      delayedFn.cancel()
      callback()
    }
  }

  return delayedFn
}
