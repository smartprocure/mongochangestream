import { Collection } from 'mongodb'
import assert from 'node:assert'
import { describe, test } from 'vitest'

import {
  docToChangeStreamInsert,
  generatePipelineFromOmit,
  omitFieldsForUpdate,
  removeUnusedFields,
} from './util.js'

describe('util', () => {
  describe('generatePipelineFromOmit', () => {
    test('should generate pipeline from omit with no dotted fields', () => {
      const pipeline = generatePipelineFromOmit(['documents', 'createdAt'])
      assert.deepEqual(pipeline, [
        {
          $unset: [
            'fullDocument.documents',
            'updateDescription.updatedFields.documents',
            'fullDocument.createdAt',
            'updateDescription.updatedFields.createdAt',
          ],
        },
      ])
    })
  })
  describe('removeUnusedFields', () => {
    test('should remove all unneeded fields', () => {
      const schema = {
        bsonType: 'object',
        required: ['a', 'b'],
        additionalProperties: false,
        properties: {
          a: {
            title: 'An array of strings',
            bsonType: 'array',
            items: {
              bsonType: 'string',
              title: 'A string',
            },
          },
          b: {
            description: 'foo or bar',
            bsonType: 'string',
            enum: ['foo', 'bar'],
            oneOf: [
              { bsonType: 'string', const: 'foo' },
              { bsonType: 'string', const: 'bar' },
            ],
          },
          c: {
            bsonType: 'object',
            additionalProperties: true,
            properties: {
              d: {
                bsonType: 'number',
                description: 'A number',
              },
            },
          },
        },
      }
      removeUnusedFields(schema)
      assert.deepEqual(schema, {
        bsonType: 'object',
        additionalProperties: false,
        properties: {
          a: {
            bsonType: 'array',
            items: {
              bsonType: 'string',
            },
          },
          b: {
            bsonType: 'string',
            enum: ['foo', 'bar'],
          },
          c: {
            bsonType: 'object',
            additionalProperties: true,
            properties: {
              d: {
                bsonType: 'number',
              },
            },
          },
        },
      })
    })
  })
  describe('omitFieldsForUpdate', () => {
    test('should remove omitted fields from removedFields - exact', () => {
      const event: any = {
        updateDescription: {
          updatedFields: {},
          removedFields: ['address.geo.long'],
          truncatedArrays: [],
        },
      }
      const expected = {
        updateDescription: {
          updatedFields: {},
          removedFields: [],
          truncatedArrays: [],
        },
      }
      omitFieldsForUpdate(['address.geo.long'], event)
      assert.deepEqual(event, expected)
    })
    test('should remove omitted fields from removedFields - prefix', () => {
      const event: any = {
        updateDescription: {
          updatedFields: {},
          removedFields: ['address.geo.long'],
          truncatedArrays: [],
        },
      }
      const expected = {
        updateDescription: {
          updatedFields: {},
          removedFields: [],
          truncatedArrays: [],
        },
      }
      omitFieldsForUpdate(['address.geo'], event)
      assert.deepEqual(event, expected)
    })
    test('should remove omitted fields from updatedFields - exact', () => {
      const event: any = {
        updateDescription: {
          updatedFields: {
            name: 'unknown',
            'address.city': 'San Diego',
          },
          removedFields: [],
          truncatedArrays: [],
        },
      }
      const expected = {
        updateDescription: {
          updatedFields: {
            name: 'unknown',
          },
          removedFields: [],
          truncatedArrays: [],
        },
      }
      omitFieldsForUpdate(['address.city'], event)
      assert.deepEqual(event, expected)
    })
    test('should remove omitted fields from updatedFields - prefix', () => {
      const event: any = {
        updateDescription: {
          updatedFields: {
            name: 'unknown',
            'address.geo.lat': 24,
          },
          removedFields: [],
          truncatedArrays: [],
        },
      }
      const expected = {
        updateDescription: {
          updatedFields: {
            name: 'unknown',
          },
          removedFields: [],
          truncatedArrays: [],
        },
      }
      omitFieldsForUpdate(['address.geo'], event)
      assert.deepEqual(event, expected)
    })
    test('should remove omitted fields from updatedFields - nested', () => {
      const event: any = {
        updateDescription: {
          updatedFields: {
            name: 'unknown',
            'address.geo': { lat: 24, long: 25 },
          },
          removedFields: [],
          truncatedArrays: [],
        },
      }
      const expected = {
        updateDescription: {
          updatedFields: {
            name: 'unknown',
            'address.geo': { long: 25 },
          },
          removedFields: [],
          truncatedArrays: [],
        },
      }
      omitFieldsForUpdate(['address.geo.lat'], event)
      assert.deepEqual(event, expected)
    })
  })
  describe('docToChangeStreamInsert', () => {
    test('should produce a correct change stream "insert" event', () => {
      const collection = {
        dbName: 'testdb',
        collectionName: 'testcoll',
      } as Collection
      const doc = {
        _id: '123',
        name: 'Joe',
      }
      const result = docToChangeStreamInsert(collection)(doc)
      assert.deepEqual(result, {
        fullDocument: { _id: '123', name: 'Joe' },
        operationType: 'insert',
        ns: { db: 'testdb', coll: 'testcoll' },
        documentKey: { _id: '123' },
      })
    })
  })
})
