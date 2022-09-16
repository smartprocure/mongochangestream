import { Collection } from 'mongodb'
import _ from 'lodash/fp.js'

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
