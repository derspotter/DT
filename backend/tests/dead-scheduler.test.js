import fs from 'fs'
import path from 'path'
import { fileURLToPath } from 'url'
import request from 'supertest'
import { createApp } from '../src/app.js'

const here = path.dirname(fileURLToPath(import.meta.url))
const appSource = path.join(here, '../src/app.js')
const scriptsDir = path.join(here, '../scripts')

// Bodies captured from the stub-mode routes before the in-memory scheduler was
// deleted. The frontend polls these, so paths and shapes must stay identical.
describe('worker routes (stub) keep their frozen responses', () => {
  let app

  beforeAll(() => {
    process.env.RAG_FEEDER_STUB = '1'
    app = createApp({ broadcast: () => {} })
  })

  afterAll(() => {
    delete process.env.RAG_FEEDER_STUB
  })

  test('GET /api/pipeline/worker/status', async () => {
    const res = await request(app).get('/api/pipeline/worker/status')
    expect(res.status).toBe(200)
    expect(res.body).toEqual({
      running: false,
      in_flight: false,
      started_at: null,
      last_tick_at: null,
      last_result: null,
      last_error: null,
      resolved_download_workers: 1,
      config: { intervalSeconds: 15, promoteBatchSize: 25, promoteWorkers: 6, downloadBatchSize: 5, downloadWorkers: 0 },
    })
  })

  test('POST /api/pipeline/worker/start', async () => {
    const res = await request(app).post('/api/pipeline/worker/start').send({ intervalSeconds: 30 })
    expect(res.status).toBe(200)
    expect(res.body).toEqual({
      running: true,
      in_flight: false,
      config: { mode: 'db-driven' },
      message: 'DB-driven enrich/download workers run continuously. Manual pipeline start is no longer used.',
    })
  })

  test('POST /api/pipeline/worker/pause', async () => {
    const res = await request(app).post('/api/pipeline/worker/pause').send({ force: false })
    expect(res.status).toBe(200)
    expect(res.body).toEqual({
      running: true,
      in_flight: false,
      force: false,
      message: 'DB-driven workers are managed outside the app. Manual pipeline pause is no longer used.',
    })
    const forced = await request(app).post('/api/pipeline/worker/pause').send({ force: true })
    expect(forced.body.force).toBe(true)
  })

  test('GET /api/downloads/worker/status', async () => {
    const res = await request(app).get('/api/downloads/worker/status')
    expect(res.status).toBe(200)
    expect(res.body).toEqual({
      running: false,
      in_flight: false,
      last_result: null,
      last_error: null,
      config: { intervalSeconds: 60, batchSize: 3 },
    })
  })

  test('POST /api/downloads/worker/start', async () => {
    const res = await request(app).post('/api/downloads/worker/start').send({ intervalSeconds: 60, batchSize: 1 })
    expect(res.status).toBe(200)
    expect(res.body).toEqual({
      running: true,
      queued_jobs: 0,
      queued_download_items: 0,
      outstanding_jobs_before: 0,
      desired_jobs: 0,
      per_job_max: 0,
      batch_size: 0,
      mode: 'db-driven',
      message: 'DB-driven download worker runs continuously. Manual start is no longer used.',
    })
  })

  test('POST /api/downloads/worker/stop', async () => {
    const res = await request(app).post('/api/downloads/worker/stop').send({ force: false })
    expect(res.status).toBe(200)
    expect(res.body).toEqual({
      running: true,
      cancelled_jobs: { pending: 0, running: 0 },
      message: 'DB-driven download worker is managed outside the app. Manual stop is no longer used.',
    })
  })
})

describe('test database isolation', () => {
  test('RAG_FEEDER_DB_PATH points at a temp DB, never dl_lit_project/data', () => {
    const dbPath = process.env.RAG_FEEDER_DB_PATH
    expect(dbPath).toBeTruthy()
    const realDataDir = path.resolve(here, '../../dl_lit_project/data')
    expect(path.resolve(dbPath).startsWith(realDataDir + path.sep)).toBe(false)
  })
})

describe('dead in-memory scheduler is gone', () => {
  test('app.js has no scheduler symbols', () => {
    // The frozen pipeline status body carries a `downloadWorkers` config key;
    // drop object-key uses (`downloadWorkers:`) so only code references count.
    const src = fs.readFileSync(appSource, 'utf8').replace(/\bdownloadWorkers:/g, '')
    expect(src).not.toMatch(/\b(startDownloadWorker|tickDownloadWorker|getOrInitWorkerState|downloadWorkers|pipelineWorkers|emitPromotionEvent)\b/)
  })

  test.each(['downloads_list.py', 'ingest_runs.py', 'ingest_stats.py'])('scripts/%s is removed', (name) => {
    expect(fs.existsSync(path.join(scriptsDir, name))).toBe(false)
  })
})
