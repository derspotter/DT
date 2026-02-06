import request from 'supertest'
import { createApp } from '../src/app.js'

describe('GET /api/downloads', () => {
  const app = createApp({ broadcast: () => {} })

  beforeAll(() => {
    process.env.RAG_FEEDER_STUB = '1'
  })

  afterAll(() => {
    delete process.env.RAG_FEEDER_STUB
  })

  test('returns download queue', async () => {
    const res = await request(app).get('/api/downloads')
    expect(res.status).toBe(200)
    expect(res.body).toHaveProperty('items')
    expect(Array.isArray(res.body.items)).toBe(true)
  })

  test('exposes worker status (stub)', async () => {
    const res = await request(app).get('/api/downloads/worker/status')
    expect(res.status).toBe(200)
    expect(res.body).toHaveProperty('running')
    expect(res.body).toHaveProperty('config')
  })

  test('can start/stop worker (stub)', async () => {
    const start = await request(app).post('/api/downloads/worker/start').send({ intervalSeconds: 60, batchSize: 1 })
    expect(start.status).toBe(200)
    expect(start.body).toHaveProperty('running')

    const stop = await request(app).post('/api/downloads/worker/stop').send({ force: false })
    expect(stop.status).toBe(200)
    expect(stop.body).toHaveProperty('running')
  })
})
