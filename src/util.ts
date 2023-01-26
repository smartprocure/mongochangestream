import { Collection } from 'mongodb'
import _ from 'lodash/fp.js'
import { Node, walkie } from 'obj-walker'
import { FsmOptions, JSONSchema, StateTransitions } from './types'
import { waitUntil, Options } from 'async-wait-until'
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

/**
 * Simple finite state machine with explicit allowed state transitions,
 * initial state, and options around how long to wait for state transitions
 * when using waitForChange as well as explicitly naming the machine for
 * better debugging when using multiple machines simultaneously.
 */
export function fsm<T extends string>(
  stateTransitions: StateTransitions<T>,
  initState: T,
  options: Options & FsmOptions<T> = {}
) {
  let state = initState
  const name = options.name || 'fsm'
  const onStateChange = options.onStateChange

  const is = (...states: T[]) => states.includes(state)

  const change = (newState: T) => {
    debug('%s changing state from %s to %s', name, state, newState)
    if (!stateTransitions[state]?.includes(newState)) {
      throw new StateError(
        `${name} invalid state transition - ${state} to ${newState}`
      )
    }
    const oldState = state
    state = newState
    debug('%s changed state from %s to %s', name, oldState, newState)
    if (onStateChange) {
      onStateChange({ name, from: oldState, to: newState })
    }
  }

  const waitForChange = (...newStates: T[]) => {
    debug(
      '%s waiting for state change from %s to %s',
      name,
      state,
      newStates.join(' or ')
    )
    return waitUntil(() => newStates.includes(state), options)
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
