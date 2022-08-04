import { ChangeStreamDocument, Collection, Document } from 'mongodb'

export type ProcessRecord = (doc: ChangeStreamDocument) => void | Promise<void>

export type ScanCollection = (
  collection: Collection,
  processRecord: ProcessRecord
) => Promise<void>

export type SyncCollection = (
  collection: Collection,
  processRecord: ProcessRecord,
  pipeline?: Document[]
) => Promise<void>

export type Reset = (collection: Collection) => Promise<void>
