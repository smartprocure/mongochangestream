import { ChangeStreamDocument, ChangeStreamInsertDocument } from 'mongodb'
import { JSONSchema4, JSONSchema6, JSONSchema7 } from 'json-schema'

export type ProcessRecord = (doc: ChangeStreamDocument) => void | Promise<void>

export type ProcessRecords = (
  doc: ChangeStreamInsertDocument[]
) => void | Promise<void>

export interface SyncOptions {
  omit?: string[]
}

export interface ScanOptions<T = any> {
  maintainHealth?: boolean
  healthCheckInterval?: number
  sortField?: {
    field: string
    serialize: (x: T) => string
    deserialize: (x: string) => T
  }
}

export interface ChangeStreamOptions {
  maintainHealth?: boolean
  healthCheckInterval?: number
  pipeline?: Document[]
}

export interface ChangeOptions {
  interval?: number
  shouldRemoveMetadata?: boolean
}

export type JSONSchema = JSONSchema4 | JSONSchema6 | JSONSchema7

export type Events = 'change'
