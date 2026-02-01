import request from 'supertest'
import { createApp } from '../src/app.js'

describe('GET /api/graph', () => {
  const app = createApp({ broadcast: () => {} })

  beforeAll(() => {
    process.env.RAG_FEEDER_STUB = '1'
  })

  afterAll(() => {
    delete process.env.RAG_FEEDER_STUB
  })

  test('returns graph payload', async () => {
    const res = await request(app).get('/api/graph')
    expect(res.status).toBe(200)
    expect(res.body).toHaveProperty('nodes')
    expect(res.body).toHaveProperty('edges')
    expect(res.body).toHaveProperty('stats')
    expect(res.body.stats).toHaveProperty('node_count')
    expect(res.body.stats).toHaveProperty('edge_count')
  })
})
