import {
  ChangeStream,
  AggregationCursor,
  ChangeStreamDocument,
  ChangeStreamInsertDocument,
  Document,
  MongoServerError,
  MongoAPIError,
} from 'mongodb'

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
}

export interface ChangeOptions {
  /** How often to retrieve the schema and look for a change. */
  interval?: number
  /** Should fields like title and description be ignored when detecting a change. */
  shouldRemoveMetadata?: boolean
}

// Events

export type Events =
  | 'cursorError'
  | 'resync'
  | 'schemaChange'
  | 'stateChange'
  | 'initialScanComplete'

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

// State

export type State = 'starting' | 'started' | 'stopping' | 'stopped'

export type SimpleState = 'started' | 'stopped'
