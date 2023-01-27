import { Collection, ChangeStream } from 'mongodb'
import _ from 'lodash/fp.js'
import { Node, walkie } from 'obj-walker'
import { JSONSchema } from './types'

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

export const safelyCheckNext = async (changeStream: ChangeStream) => {
  try {
    if (changeStream.closed) {
      return false
    }
    return await changeStream.hasNext()
  } catch (e) {
    return false
  }
}
