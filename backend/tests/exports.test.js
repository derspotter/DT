import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import request from 'supertest'
import { createApp } from '../src/app.js'

describe('GET /api/exports/:format', () => {
  let app
  let token = ''
  let tmpDir = ''
  let dbPath = ''

  beforeAll(async () => {
    process.env.RAG_FEEDER_STUB = '1'
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'dt-exports-'))
    dbPath = path.join(tmpDir, 'exports.db')
    process.env.RAG_FEEDER_DB_PATH = dbPath
    app = createApp({ broadcast: () => {} })
    const login = await request(app)
      .post('/api/auth/login')
      .set('Content-Type', 'application/json')
      .send({ username: 'stub-admin', password: 'stub-password' })
    token = login.body?.token || ''
  })

  afterAll(() => {
    delete process.env.RAG_FEEDER_STUB
    delete process.env.RAG_FEEDER_DB_PATH
    if (tmpDir && fs.existsSync(tmpDir)) {
      fs.rmSync(tmpDir, { recursive: true, force: true })
    }
  })

  test('returns JSON export download', async () => {
    const res = await request(app).get('/api/exports/json').set('Authorization', `Bearer ${token}`)
    expect(res.status).toBe(200)
    expect(res.headers['content-disposition']).toContain('.json')
  })

  test('returns BibTeX export download', async () => {
    const res = await request(app).get('/api/exports/bibtex').set('Authorization', `Bearer ${token}`)
    expect(res.status).toBe(200)
    expect(res.headers['content-disposition']).toContain('.bib')
  })

  test('returns PDF ZIP export download', async () => {
    const res = await request(app).get('/api/exports/pdfs').set('Authorization', `Bearer ${token}`)
    expect(res.status).toBe(200)
    expect(res.headers['content-disposition']).toContain('.zip')
  })

  test('returns bundle ZIP export download', async () => {
    const res = await request(app).get('/api/exports/bundle').set('Authorization', `Bearer ${token}`)
    expect(res.status).toBe(200)
    expect(res.headers['content-disposition']).toContain('.zip')
  })

  test('accepts export filters', async () => {
    const res = await request(app)
      .get('/api/exports/json')
      .set('Authorization', `Bearer ${token}`)
      .query({ status: 'matched', year_from: '1980', year_to: '2020', source: 'journal' })
    expect(res.status).toBe(200)
    expect(res.headers['content-disposition']).toContain('.json')
  })

  test('rejects invalid year range', async () => {
    const res = await request(app).get('/api/exports/json').set('Authorization', `Bearer ${token}`).query({ year_from: '2025', year_to: '2020' })
    expect(res.status).toBe(400)
  })

  test('rejects unknown format', async () => {
    const res = await request(app).get('/api/exports/invalid').set('Authorization', `Bearer ${token}`)
    expect(res.status).toBe(400)
  })
})
