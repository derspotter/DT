import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import request from 'supertest'
import { createApp } from '../src/app.js'

function tmpQuotaPath() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'quota-'))
  return path.join(dir, 'openalex_quota.json')
}

describe('GET /api/openalex/quota', () => {
  let app
  let quotaPath

  beforeEach(() => {
    quotaPath = tmpQuotaPath()
    process.env.RAG_FEEDER_STUB = '1'
    process.env.RAG_FEEDER_OPENALEX_QUOTA_PATH = quotaPath
    app = createApp({ broadcast: () => {} })
  })

  afterEach(() => {
    delete process.env.RAG_FEEDER_STUB
    delete process.env.RAG_FEEDER_OPENALEX_QUOTA_PATH
  })

  test('reports unavailable when no snapshot exists', async () => {
    const res = await request(app).get('/api/openalex/quota')
    expect(res.status).toBe(200)
    expect(res.body).toEqual({ available: false })
  })

  test('returns the snapshot with a live reset countdown', async () => {
    const resetAt = new Date(Date.now() + 3600 * 1000)
    fs.writeFileSync(quotaPath, JSON.stringify({
      limit: 100000, remaining: 87412, credits_used: 1, reset_seconds: 3600,
      observed_at: new Date().toISOString(), reset_at: resetAt.toISOString(),
      api_key_present: true,
    }))
    const res = await request(app).get('/api/openalex/quota')
    expect(res.status).toBe(200)
    expect(res.body.available).toBe(true)
    expect(res.body.stale).toBe(false)
    expect(res.body.remaining).toBe(87412)
    expect(res.body.reset_in_seconds).toBeGreaterThan(3500)
    expect(res.body.reset_in_seconds).toBeLessThanOrEqual(3600)
  })

  test('flags a snapshot older than 24 hours as stale', async () => {
    fs.writeFileSync(quotaPath, JSON.stringify({
      limit: 100000, remaining: 5, observed_at: new Date(Date.now() - 25 * 3600 * 1000).toISOString(),
      reset_at: new Date(Date.now() - 20 * 3600 * 1000).toISOString(), api_key_present: true,
    }))
    const res = await request(app).get('/api/openalex/quota')
    expect(res.body.stale).toBe(true)
    expect(res.body.reset_in_seconds).toBe(0)
  })

  test('passes through the no-key marker', async () => {
    fs.writeFileSync(quotaPath, JSON.stringify({ api_key_present: false, observed_at: new Date().toISOString() }))
    const res = await request(app).get('/api/openalex/quota')
    expect(res.body).toMatchObject({ available: true, api_key_present: false, stale: false })
  })

  test('treats an unreadable file as unavailable', async () => {
    fs.writeFileSync(quotaPath, '{not json')
    const res = await request(app).get('/api/openalex/quota')
    expect(res.status).toBe(200)
    expect(res.body).toEqual({ available: false })
  })
})
