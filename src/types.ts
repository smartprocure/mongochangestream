import {
  AggregationCursor,
  ChangeStream,
  ChangeStreamDocument,
  ChangeStreamInsertDocument,
  Document,
  MongoAPIError,
  MongoServerError,
} from 'mongodb'
import { QueueStats } from 'prom-utils'

export type Cursor = ChangeStream | AggregationCursor
export type JSONSchema = Record<string, any>

type MaybePromise<T> = T | Promise<T>

export type ProcessChangeStreamRecords = (
  docs: ChangeStreamDocument[]
) => MaybePromise<void>

export type ProcessInitialScanRecords = (
  docs: ChangeStreamInsertDocument[]
) => MaybePromise<void>

// Options

export interface SyncOptions {
  /** Field paths to omit. */
  omit?: string[]
  /**
   * Added to all Redis keys to allow the same collection
   * to be synced in parallel. Otherwise, the Redis keys
   * would be the same for a given collection and parallel
   * syncing jobs would overwrite each other.
   */
  uniqueId?: string
  /** When enabled, the `stats` event will be emitted after each batch is processed. */
  emitStats?: boolean
}

export interface SortField<T> {
  field: string
  /** Function to serialize value to string. */
  serialize: (x: T) => string
  deserialize: (x: string) => T
  /** Sort order: asc or desc. Defaults to asc */
  order?: 'asc' | 'desc'
}

export interface ScanOptions<T = any> {
  /** Defaults to _id */
  sortField?: SortField<T>
  /** Extend the pipeline. Be careful not to exclude the sort field or change the sort order. */
  pipeline?: Document[]
}

export interface ChangeStreamOptions {
  pipeline?: Document[]
  operationTypes?: ChangeStreamDocument['operationType'][]
}

export interface ChangeOptions {
  /** How often to retrieve the schema and look for a change. */
  interval?: number
  /** @deprecated Use shouldRemoveUnusedFields instead.*/
  shouldRemoveMetadata?: boolean
  /**
   * Remove fields that are not used when converting the schema
   * in a downstream library like mongo2elastic or mongo2crate.
   * Preserves bsonType, properties, additionalProperties, items, and enum.
   */
  shouldRemoveUnusedFields?: boolean
}

// Events

export type Events =
  | 'cursorError'
  | 'resync'
  | 'schemaChange'
  | 'stateChange'
  | 'initialScanComplete'
  | 'stats'

export interface ResyncEvent {
  type: 'resync'
}

export interface SchemaChangeEvent {
  type: 'schemaChange'
  previousSchema?: JSONSchema
  currentSchema: JSONSchema
}

export interface StateChangeEvent {
  type: 'stateChange'
  name: string
  from: string
  to: string
}

export interface InitialScanCompleteEvent {
  type: 'initialScanComplete'
}

export type CursorError = MongoServerError | MongoAPIError

export interface CursorErrorEvent {
  type: 'cursorError'
  name: 'runInitialScan' | 'processChangeStream'
  error: CursorError
}

export interface StatsEvent {
  type: 'stats'
  name: 'runInitialScan' | 'processChangeStream'
  stats: QueueStats
}

// State

export type State = 'starting' | 'started' | 'stopping' | 'stopped'

export type SimpleState = 'started' | 'stopped'
