import request from 'supertest'
import { createApp } from '../src/app.js'

describe('POST /api/keyword-search', () => {
  const app = createApp({ broadcast: () => {} })

  beforeAll(() => {
    process.env.RAG_FEEDER_STUB = '1'
  })

  afterAll(() => {
    delete process.env.RAG_FEEDER_STUB
  })

  test('requires query or seedJson', async () => {
    const res = await request(app).post('/api/keyword-search').send({})
    expect(res.status).toBe(400)
  })

  test('returns stubbed results', async () => {
    const res = await request(app)
      .post('/api/keyword-search')
      .send({ query: 'institutional economics' })
    expect(res.status).toBe(200)
    expect(res.body).toHaveProperty('results')
    expect(Array.isArray(res.body.results)).toBe(true)
    expect(res.body.results[0]).toHaveProperty('title')
  })

  test('accepts separate downstream/upstream depth settings', async () => {
    const res = await request(app)
      .post('/api/keyword-search')
      .send({
        query: 'institutional economics',
        relatedDepthDownstream: 3,
        relatedDepthUpstream: 2,
      })
    expect(res.status).toBe(200)
    expect(res.body.source).toBe('stub')
  })

  test('accepts seedJson mode in stub', async () => {
    const res = await request(app)
      .post('/api/keyword-search')
      .send({ seedJson: ['W2015930340'] })
    expect(res.status).toBe(200)
    expect(Array.isArray(res.body.results)).toBe(true)
    expect(res.body.source).toBe('stub')
  })

  test('supports seed mode expansion controls in stub', async () => {
    const res = await request(app)
      .post('/api/keyword-search')
      .send({
        seedJson: ['10.1234/example'],
        relatedDepthDownstream: 2,
        relatedDepthUpstream: 2,
        includeDownstream: false,
        includeUpstream: true,
      })
    expect(res.status).toBe(200)
    expect(res.body.source).toBe('stub')
  })

  test('returns centralized recursion config', async () => {
    const res = await request(app).get('/api/recursion-config')
    expect(res.status).toBe(200)
    expect(res.body).toHaveProperty('keyword')
    expect(res.body).toHaveProperty('uploadedDocs')
    expect(res.body.keyword).toHaveProperty('relatedDepthDownstream')
    expect(res.body.uploadedDocs).toHaveProperty('relatedDepth')
    expect(res.body.keyword.includeDownstream).toBe(false)
    expect(res.body.keyword.includeUpstream).toBe(false)
    expect(res.body.keyword.relatedDepthDownstream).toBe(0)
    expect(res.body.keyword.relatedDepthUpstream).toBe(0)
    expect(res.body.uploadedDocs.includeDownstream).toBe(false)
    expect(res.body.uploadedDocs.includeUpstream).toBe(false)
    expect(res.body.uploadedDocs.relatedDepth).toBe(0)
    expect(res.body.uploadedDocs.relatedDepthDownstream).toBe(0)
    expect(res.body.uploadedDocs.relatedDepthUpstream).toBe(0)
  })
})
