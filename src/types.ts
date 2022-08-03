import { ChangeStreamDocument, Collection, Document } from 'mongodb'

export type WriteRecord = (doc: ChangeStreamDocument) => void | Promise<void>

export type ScanCollection = (
  collection: Collection,
  writeRecord: WriteRecord
) => Promise<void>

export type SyncCollection = (
  collection: Collection,
  writeRecord: WriteRecord,
  pipeline?: Document[]
) => Promise<void>

export type Reset = (collection: Collection) => Promise<void>
