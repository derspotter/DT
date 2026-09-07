import { buildGraph3dRequest } from '../src/app.js'

const req = (query) => ({ query })

describe('buildGraph3dRequest corpus scoping', () => {
  test('a positive integer corpus_id scopes the graph', () => {
    expect(buildGraph3dRequest(req({ corpus_id: '130' })).corpusId).toBe(130)
  })

  test.each([
    ['absent', {}],
    ['all', { corpus_id: 'all' }],
    ['zero', { corpus_id: '0' }],
    ['negative', { corpus_id: '-3' }],
    ['non-integer', { corpus_id: '1.5' }],
    ['garbage', { corpus_id: 'abc' }],
  ])('%s corpus_id means the global view, never an argparse-rejected value', (_label, query) => {
    expect(buildGraph3dRequest(req(query)).corpusId).toBeNull()
  })
})
