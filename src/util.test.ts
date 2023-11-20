import { test } from 'node:test'
import assert from 'node:assert'
import { generatePipelineFromOmit } from './util.js'

test('should generate pipeline from omit with no dotted fields', async () => {
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

test('should generate pipeline from omit with dotted fields', async () => {
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
                    '^(?!documents.agenda.parsedText|documents.agenda.contentType)',
                },
              },
            },
          },
        },
      },
    },
  ])
})
