import { Collection } from 'mongodb'
import _ from 'lodash/fp.js'
import { walkie, Node } from 'obj-walker'

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

/**
 * Remove title and description from a JSON schema.
 */
export const removeMetadata = (schema: object) => {
  const traverse = (x: any) => x.properties || (x.items && { items: x.items })
  const walkFn = ({ val }: Node) => {
    if ('title' in val) {
      delete val.title
    }
    if ('description' in val) {
      delete val.description
    }
  }
  return walkie(schema, walkFn, { traverse })
}

export function when<T>(condition: any, fn: (x: T) => any) {
  return function(x: T) {
    return condition ? fn(x) : x
  }
} 
