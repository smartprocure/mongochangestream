import type {
  AggregationCursor,
  ChangeStream,
  ChangeStreamDocument,
  ChangeStreamInsertDocument,
  Document,
  MongoAPIError,
  MongoServerError,
} from 'mongodb'
import { type Options as RetryOptions } from 'p-retry'
import type { LastFlush, QueueStats } from 'prom-utils'

export type Cursor = ChangeStream | AggregationCursor
export type JSONSchema = Record<string, any>

type MaybePromise<T> = T | Promise<T>

/** Utility type to extract the operationType from a document type */
type ExtractOperationType<T> = T extends { operationType: infer O } ? O : never

/** Mapping from operation type to document type */
type OperationTypeMap = {
  [K in ExtractOperationType<ChangeStreamDocument>]: Extract<
    ChangeStreamDocument,
    { operationType: K }
  >
}

export type OperationType = keyof OperationTypeMap

/** Extract the specific document types from an array of operation types */
export type DocumentsForOperationTypes<T extends OperationType[] | undefined> =
  T extends OperationType[] ? OperationTypeMap[T[number]] : ChangeStreamDocument

export type ProcessChangeStreamRecords<
  T extends OperationType[] | undefined = undefined,
> = (docs: DocumentsForOperationTypes<T>[]) => MaybePromise<void>

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
  /**
   * Maximum time to pause in ms after calling `pausable.pause()`.
   * By default, pausing is indefinite.
   */
  maxPauseTime?: number
  /**
   * Options for handling retry logic when calling `processRecords`.
   * Defaults to `{ minTimeout: 30s, maxTimeout: 1h, retries: 30 }`.
   * Options passed here will be applied over the default options.
   */
  retry?: RetryOptions
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

export interface ChangeStreamOptions<
  T extends OperationType[] | undefined = undefined,
> {
  pipeline?: Document[]
  operationTypes?: T
}

export interface ChangeOptions {
  /** How often to retrieve the schema and look for a change. */
  interval?: number
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
  | 'processError'

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

export interface ProcessErrorEvent {
  type: 'processError'
  name: 'runInitialScan' | 'processChangeStream'
  error: unknown
}

/**
 * If `maxItemsPerSec` is not set, `stats.itemsPerSec` will be 0.
 * If `maxBytesPerSec` is not set, `stats.bytesPerSec` will be 0.
 */
export interface StatsEvent {
  type: 'stats'
  name: 'runInitialScan' | 'processChangeStream'
  stats: QueueStats
  lastFlush?: LastFlush
}

// State

export type State = 'starting' | 'started' | 'stopping' | 'stopped'

export type SimpleState = 'started' | 'stopped'
