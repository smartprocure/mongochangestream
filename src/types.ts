import {
  ChangeStreamDocument,
  ChangeStreamInsertDocument,
  Document,
} from 'mongodb'

export type JSONSchema = Record<string, any>

export type ProcessRecord = (doc: ChangeStreamDocument) => void | Promise<void>

export type ProcessRecords = (
  doc: ChangeStreamInsertDocument[]
) => void | Promise<void>

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
}

export interface ScanOptions<T = any> {
  healthCheck?: {
    enabled: boolean
    /** How often to run the health check. */
    interval?: number
  }
  /** Defaults to _id */
  sortField?: SortField<T>
  pipeline?: Document[]
}

export interface ChangeStreamOptions {
  healthCheck?: {
    enabled: boolean
    /** The date field that contains the time the record was last updated */
    field: string
    /** How often to run the health check. */
    interval?: number
    /** The max allowed time for a modified record to be synced */
    maxSyncDelay?: number
  }
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
  | 'healthCheckFail'
  | 'resync'
  | 'schemaChange'
  | 'stateChange'
  | 'initialScanComplete'

interface InitialScanFailEvent {
  type: 'healthCheckFail'
  failureType: 'initialScan'
  lastSyncedAt: number
}

interface ChangeStreamFailEvent {
  type: 'healthCheckFail'
  failureType: 'changeStream'
  lastRecordUpdatedAt: number
  lastSyncedAt: number
}

export type HealthCheckFailEvent = InitialScanFailEvent | ChangeStreamFailEvent

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
  from: string
  to: string
}

export interface InitialScanComplete {
  type: 'initialScanComplete'
  lastId: string
}

// State

export type State = 'starting' | 'started' | 'stopping' | 'stopped'

export type SimpleState = 'started' | 'stopped'
