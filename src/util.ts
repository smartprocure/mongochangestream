import lodash from 'lodash'
import {
  ChangeStreamInsertDocument,
  type ChangeStreamUpdateDocument,
  type Collection,
  Document,
  MongoServerError,
} from 'mongodb'
import { map, type Node, walkEach } from 'obj-walker'

import type { CursorError, JSONSchema } from './types.js'

const { set } = lodash

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
 * Therefore, to remove 'a.b' we have to walk the `updateFields` object
 * and unset the omitted paths.
 */
export const omitFieldsForUpdate = (
  omittedPaths: string[],
  event: ChangeStreamUpdateDocument
) => {
  const shouldOmit = (path: string) =>
    omittedPaths.find(
      (omittedPath) =>
        path === omittedPath || path.startsWith(`${omittedPath}.`)
    )

  if (event.updateDescription.updatedFields) {
    map(
      event.updateDescription.updatedFields,
      (node) => {
        const fullPath = node.path.join('.')
        if (!shouldOmit(fullPath)) {
          return node.val
        }
      },
      { modifyInPlace: true }
    )
  }
  if (event.updateDescription.removedFields) {
    const removedFields = event.updateDescription.removedFields.filter(
      (removedPath) => !shouldOmit(removedPath)
    )
    set(event, 'updateDescription.removedFields', removedFields)
  }
}

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

/**
 * Convert an initial scan document to a change stream insert document
 * suitable for downstream consumption. Note: not all fields are present,
 * such as, _id (resume token).
 */
export const docToChangeStreamInsert = (collection: Collection) => {
  const ns = { db: collection.dbName, coll: collection.collectionName }
  return (doc: Document) =>
    ({
      fullDocument: doc,
      operationType: 'insert',
      ns,
      documentKey: { _id: doc._id },
    }) as ChangeStreamInsertDocument
}

/**
 * Rename key, mutating the given document.
 */
const renameKey = (doc: Document, key: string, newKey: string) => {
  const temp = doc[key]
  delete doc[key]
  doc[newKey] = temp
}

/**
 * Rename keys, mutating the given document. Used in downstream libraries.
 */
export const renameKeys = (doc: Document, keys: Record<string, string>) => {
  for (const key in keys) {
    if (key in doc) {
      const newKey = keys[key]
      renameKey(doc, key, newKey)
    }
  }
}
