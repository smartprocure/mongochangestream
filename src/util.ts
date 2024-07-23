import _debug from 'debug'
import _ from 'lodash/fp.js'
import { type Collection, MongoServerError } from 'mongodb'
import { type Node, walkEach } from 'obj-walker'

import type { Cursor, CursorError, JSONSchema } from './types.js'

const debug = _debug('mongochangestream')

export const setDefaults = (keys: string[], val: any) => {
  const obj: Record<string, any> = {}
  for (const key of keys) {
    obj[key] = val
  }
  return obj
}

/**
 * Dotted path updates like { $set: {'a.b.c': 'foo'} } result in the following:
 * ```ts
 * {
 *   updatedDescription: {
 *     updateFields: {
 *       'a.b.c': 'foo'
 *     }
 *   }
 * }
 * ```
 * Therefore, to remove 'a.b' we have to convert the `updateFields`
 * object to an array, filter the array with a regex, and convert
 * the array back to an object.
 */
const removeDottedPaths = (omit: string[]) => {
  const dottedFields = omit
    .filter((x) => x.includes('.'))
    // Escape periods
    .map((x) => x.replaceAll('.', '\\.'))
  if (dottedFields.length) {
    return {
      $set: {
        'updateDescription.updatedFields': {
          $arrayToObject: {
            $filter: {
              input: { $objectToArray: '$updateDescription.updatedFields' },
              cond: {
                $regexMatch: {
                  input: '$$this.k',
                  regex: `^(?!${dottedFields.join('|')})`,
                },
              },
            },
          },
        },
      },
    }
  }
}

export const generatePipelineFromOmit = (omit: string[]) => {
  const fields = omit.flatMap((field) => [
    `fullDocument.${field}`,
    `updateDescription.updatedFields.${field}`,
  ])
  const dottedPathsStage = removeDottedPaths(omit)
  const pipeline: any[] = [{ $unset: fields }]
  return dottedPathsStage ? pipeline.concat([dottedPathsStage]) : pipeline
}

export const omitFields = (omitPaths: string[]) =>
  _.omitBy((_val, key) =>
    _.find((omitPath) => _.startsWith(`${omitPath}.`, key), omitPaths)
  )

export const omitFieldForUpdate = (omitPaths: string[]) =>
  _.update('updateDescription.updatedFields', omitFields(omitPaths))

export const getCollectionKey = (collection: Collection) =>
  `${collection.dbName}:${collection.collectionName}`

export const traverseSchema = (x: JSONSchema) =>
  x.properties || (x.items && { _items: x.items })

const usedSchemaFields = [
  'bsonType',
  'properties',
  'additionalProperties',
  'items',
  'enum',
]

/**
 * Remove unused schema fields
 */
export const removeUnusedFields = (schema: JSONSchema): JSONSchema => {
  const walkFn = ({ val }: Node) => {
    for (const key in val) {
      if (!usedSchemaFields.includes(key)) {
        delete val[key]
      }
    }
  }
  return walkEach(schema, walkFn, {
    traverse: traverseSchema,
    modifyInPlace: true,
  })
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
  const getLastError = () => lastError

  return { hasNext, errorExists, getLastError }
}

const oplogErrorCodeNames = [
  'ChangeStreamHistoryLost',
  'InvalidResumeToken',
  'FailedToParse',
]

/**
 * Check if error message indicates a missing or invalid oplog entry.
 */
export const missingOplogEntry = (error: CursorError) => {
  if (error instanceof MongoServerError) {
    return oplogErrorCodeNames.includes(error?.codeName ?? '')
  }
  return false
}

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
