import { Collection } from 'mongodb'
import _ from 'lodash/fp.js'
import { Node, walkie } from 'obj-walker'
import { JSONSchema, StateTransitions } from './types'
import { waitUntil } from 'async-wait-until'
import _debug from 'debug'
import makeError from 'make-error'

export const StateError = makeError('StateError')

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

export function manageState<T extends string>(
  stateTransitions: StateTransitions,
  initState: T,
  entity: string
) {
  let state = initState

  const is = (...states: T[]) => states.includes(state)

  const change = (newState: T) => {
    debug('%s changing state from %s to %s', entity, state, newState)
    if (!stateTransitions[state]?.includes(newState)) {
      throw new StateError(
        `${entity} invalid state transition - ${state} to ${newState}`
      )
    }
    state = newState
    debug('%s changed state to %s', entity, newState)
  }

  const waitForChange = (...newStates: T[]) => {
    debug(
      '%s waiting for state change from %s to %s',
      entity,
      state,
      newStates.join(' or ')
    )
    return waitUntil(() => newStates.includes(state))
  }

  return {
    /**
     * Transition state to `newState`. Throws if `newState` is not a valid
     * transitional state for the current state.
     */
    change,
    /**
     * Wait for state to change to one of `newStates`. Times out after 5 seconds.
     */
    waitForChange,
    /**
     * Is state currently one of `states`.
     */
    is,
  }
}
