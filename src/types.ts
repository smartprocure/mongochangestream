import { ChangeStreamDocument } from 'mongodb'

export type ProcessRecord = (doc: ChangeStreamDocument) => void | Promise<void>
