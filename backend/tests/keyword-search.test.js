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

  test('requires query', async () => {
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
})
