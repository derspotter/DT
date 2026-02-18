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

  test('exposes global pipeline worker controls (stub)', async () => {
    const status = await request(app).get('/api/pipeline/worker/status')
    expect(status.status).toBe(200)
    expect(status.body).toHaveProperty('running')
    expect(status.body).toHaveProperty('config')

    const start = await request(app)
      .post('/api/pipeline/worker/start')
      .send({ intervalSeconds: 30, promoteBatchSize: 10, downloadBatchSize: 3, downloadWorkers: 0 })
    expect(start.status).toBe(200)
    expect(start.body).toHaveProperty('running')

    const pause = await request(app).post('/api/pipeline/worker/pause').send({ force: false })
    expect(pause.status).toBe(200)
    expect(pause.body).toHaveProperty('running')
  })

  test('supports global pause-all worker controls (stub)', async () => {
    const startPipeline = await request(app)
      .post('/api/pipeline/worker/start')
      .send({ intervalSeconds: 30, promoteBatchSize: 10, downloadBatchSize: 3, downloadWorkers: 0 })
    expect(startPipeline.status).toBe(200)

    const pauseAll = await request(app).post('/api/pipeline/worker/pause-all').send({ force: true })
    expect(pauseAll.status).toBe(200)
    expect(pauseAll.body).toHaveProperty('pipeline')
    expect(pauseAll.body).toHaveProperty('downloads')
    expect(typeof pauseAll.body.pipeline.stopped).toBe('number')
    expect(typeof pauseAll.body.downloads.stopped).toBe('number')

    const stopDownload = await request(app).post('/api/downloads/worker/stop').send({ force: true })
    expect(stopDownload.status).toBe(200)
  })
})
