import _debug from 'debug'

import type { Cursor } from './types.js'

const debug = _debug('mongochangestream:safelyCheckNext')

/**
 * Get next record without throwing an exception.
 * Get the last error safely via `getLastError`.
 */
export const safelyCheckNext = (cursor: Cursor) => {
  let lastError: unknown

  const getNext = async () => {
    debug('getNext called')
    try {
      return await cursor.tryNext()
    } catch (e) {
      debug('getNext error: %o', e)
      lastError = e
      return null
    }
  }

  const hasNext = async () => {
    debug('hasNext called')
    try {
      return await cursor.hasNext()
    } catch (e) {
      debug('hasNext error: %o', e)
      if (cursor.closed) {
        debug('hasNext cursor closed')
      }
      lastError = e
      return false
    }
  }

  const errorExists = () => Boolean(lastError)
  const getLastError = () => lastError

  return { hasNext, getNext, errorExists, getLastError }
}
