import {
  ChangeStreamDocument,
  ChangeStreamInsertDocument,
  Document,
} from 'mongodb'

export type ProcessRecord = (doc: ChangeStreamDocument) => void | Promise<void>

export type ProcessRecords = (
  doc: ChangeStreamInsertDocument[]
) => void | Promise<void>

export interface SyncOptions {
  /** Field paths to omit. */
  omit?: string[]
}

export interface ScanOptions<T = any> {
  /** Set to true to run a health check in the background. */
  maintainHealth?: boolean
  /** How often to run the health check. */
  healthCheckInterval?: number
  sortField?: {
    field: string
    serialize: (x: T) => string
    deserialize: (x: string) => T
  }
}

export interface ChangeStreamOptions {
  /** Set to true to run a health check in the background. */
  maintainHealth?: boolean
  /** How often to run the health check. */
  healthCheckInterval?: number
  pipeline?: Document[]
}

export interface ChangeOptions {
  /** How often to retrieve the schema and look for a change. */
  interval?: number
  /** Should fields like title and description be ignored when detecting a change. */
  shouldRemoveMetadata?: boolean
}

export type JSONSchema = Record<string, any>

export type Events = 'change' | 'healthCheckFail'
