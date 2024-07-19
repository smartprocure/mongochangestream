import assert from 'node:assert'
import { describe, test } from 'node:test'

import { generatePipelineFromOmit, removeUnusedFields } from './util.js'

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

    test('should generate pipeline from omit with dotted fields', () => {
      const pipeline = generatePipelineFromOmit([
        'documents.agenda.parsedText',
        'documents.agenda.contentType',
        'createdAt',
      ])
      assert.deepEqual(pipeline, [
        {
          $unset: [
            'fullDocument.documents.agenda.parsedText',
            'updateDescription.updatedFields.documents.agenda.parsedText',
            'fullDocument.documents.agenda.contentType',
            'updateDescription.updatedFields.documents.agenda.contentType',
            'fullDocument.createdAt',
            'updateDescription.updatedFields.createdAt',
          ],
        },
        {
          $set: {
            'updateDescription.updatedFields': {
              $arrayToObject: {
                $filter: {
                  input: { $objectToArray: '$updateDescription.updatedFields' },
                  cond: {
                    $regexMatch: {
                      input: '$$this.k',
                      regex:
                        '^(?!documents\\.agenda\\.parsedText|documents\\.agenda\\.contentType)',
                    },
                  },
                },
              },
            },
          },
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
})
