import { ChangeStreamDocument, ChangeStreamInsertDocument } from 'mongodb'

export type ProcessRecord = (doc: ChangeStreamDocument) => void | Promise<void>

export type ProcessRecords = (
  doc: ChangeStreamInsertDocument[]
) => void | Promise<void>

export interface Options {
  omit?: string[]
}

export interface ScanOptions<T = any> {
  sortField?: {
    field: string
    serialize: (x: T) => string
    deserialize: (x: string) => T
  }
}
