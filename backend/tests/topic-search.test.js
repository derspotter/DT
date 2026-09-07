import request from 'supertest'
import { jest } from '@jest/globals'
import { createApp, parseTopicSelection, normalizeTopicSuggestions, buildKeywordSearchArgs } from '../src/app.js'

describe('parseTopicSelection', () => {
  test('accepts objects, bare ids and OpenAlex URLs; drops garbage; dedupes', () => {
    const parsed = parseTopicSelection([
      { id: 'T10208', label: 'Labor market dynamics' },
      'https://openalex.org/T11421',
      { id: 't10208', label: 'dupe' },
      { id: 'W123', label: 'a work' },
      '',
      null,
      { id: 'T1;DROP TABLE', label: 'x' },
    ])
    expect(parsed).toEqual({ ids: ['T10208', 'T11421'], labels: ['Labor market dynamics', 'T11421'] })
  })

  test('caps at 50 topics and tolerates a non-array', () => {
    const many = Array.from({ length: 60 }, (_, i) => `T${10000 + i}`)
    expect(parseTopicSelection(many).ids).toHaveLength(50)
    expect(parseTopicSelection('T10208')).toEqual({ ids: [], labels: [] })
    expect(parseTopicSelection(undefined)).toEqual({ ids: [], labels: [] })
  })
})

describe('buildKeywordSearchArgs with topics', () => {
  test('a topic-only body is valid and passes ids and labels to the script', () => {
    const built = buildKeywordSearchArgs({ body: { query: '', topics: [{ id: 'T10208', label: 'Labor market dynamics' }, { id: 'T11421', label: 'Unions' }] } })
    expect(built.error).toBeUndefined()
    const i = built.args.indexOf('--topics')
    expect(built.args[i + 1]).toBe('T10208,T11421')
    expect(JSON.parse(built.args[built.args.indexOf('--topic-labels') + 1])).toEqual(['Labor market dynamics', 'Unions'])
    expect(built.args).toContain('--query')
  })

  test('nothing to search on is still an error', () => {
    expect(buildKeywordSearchArgs({ body: { query: '', topics: ['W1'] } }).error).toMatch(/required/)
  })
})

describe('normalizeTopicSuggestions', () => {
  test('keeps id, name, hint and works count; drops entries without an id', () => {
    const out = normalizeTopicSuggestions({ results: [
      { id: 'https://openalex.org/T11421', display_name: 'Labor Movements and Unions', hint: 'This cluster…', works_count: 210535, cited_by_count: 1 },
      { display_name: 'no id' },
    ] })
    expect(out).toEqual([{ id: 'T11421', label: 'Labor Movements and Unions', hint: 'This cluster…', works_count: 210535 }])
  })
})

describe('GET /api/openalex/topics', () => {
  let app
  const realFetch = globalThis.fetch
  beforeEach(() => {
    process.env.RAG_FEEDER_STUB = '1'
    app = createApp({ broadcast: () => {} })
  })
  afterEach(() => {
    delete process.env.RAG_FEEDER_STUB
    globalThis.fetch = realFetch
  })

  test('proxies the free autocomplete endpoint and normalizes the result', async () => {
    const calls = []
    globalThis.fetch = jest.fn(async (url) => {
      calls.push(String(url))
      return { ok: true, status: 200, json: async () => ({ results: [{ id: 'https://openalex.org/T10208', display_name: 'Labor market dynamics', hint: 'h', works_count: 91061 }] }) }
    })
    const res = await request(app).get('/api/openalex/topics?q=labor')
    expect(res.status).toBe(200)
    expect(res.body.topics).toEqual([{ id: 'T10208', label: 'Labor market dynamics', hint: 'h', works_count: 91061 }])
    expect(calls[0]).toMatch(/^https:\/\/api\.openalex\.org\/autocomplete\/topics\?q=labor/)
  })

  test('rejects an empty query and reports an upstream failure', async () => {
    globalThis.fetch = jest.fn(async () => ({ ok: false, status: 503, json: async () => ({}) }))
    expect((await request(app).get('/api/openalex/topics?q=')).status).toBe(400)
    expect((await request(app).get('/api/openalex/topics?q=labor')).status).toBe(502)
  })
})

describe('POST /api/keyword-search/preview credits', () => {
  let app
  beforeEach(() => { process.env.RAG_FEEDER_STUB = '1'; app = createApp({ broadcast: () => {} }) })
  afterEach(() => { delete process.env.RAG_FEEDER_STUB })

  test('reports credits per page for text vs topic-only searches', async () => {
    const text = await request(app).post('/api/keyword-search/preview').send({ query: 'labour' })
    expect(text.status).toBe(200)
    expect(text.body.creditsPerPage).toBe(10)
    const topic = await request(app).post('/api/keyword-search/preview').send({ query: '', topics: [{ id: 'T10208', label: 'x' }] })
    expect(topic.status).toBe(200)
    expect(topic.body.creditsPerPage).toBe(1)
  })
})
