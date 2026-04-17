import request from 'supertest'
import { createApp } from '../src/app.js'

describe('GET /api/corpus', () => {
  let app

  beforeAll(() => {
    process.env.RAG_FEEDER_STUB = '1'
    app = createApp({ broadcast: () => {} })
  })

  afterAll(() => {
    delete process.env.RAG_FEEDER_STUB
  })

  test('returns corpus items', async () => {
    const res = await request(app).get('/api/corpus')
    expect(res.status).toBe(200)
    expect(res.body).toHaveProperty('items')
    expect(Array.isArray(res.body.items)).toBe(true)
  })
})
