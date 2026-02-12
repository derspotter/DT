import request from 'supertest'
import { createApp } from '../src/app.js'

describe('GET /api/exports/:format', () => {
  const app = createApp({ broadcast: () => {} })

  beforeAll(() => {
    process.env.RAG_FEEDER_STUB = '1'
  })

  afterAll(() => {
    delete process.env.RAG_FEEDER_STUB
  })

  test('returns JSON export download', async () => {
    const res = await request(app).get('/api/exports/json')
    expect(res.status).toBe(200)
    expect(res.headers['content-disposition']).toContain('.json')
  })

  test('returns BibTeX export download', async () => {
    const res = await request(app).get('/api/exports/bibtex')
    expect(res.status).toBe(200)
    expect(res.headers['content-disposition']).toContain('.bib')
  })

  test('returns PDF ZIP export download', async () => {
    const res = await request(app).get('/api/exports/pdfs')
    expect(res.status).toBe(200)
    expect(res.headers['content-disposition']).toContain('.zip')
  })

  test('returns bundle ZIP export download', async () => {
    const res = await request(app).get('/api/exports/bundle')
    expect(res.status).toBe(200)
    expect(res.headers['content-disposition']).toContain('.zip')
  })

  test('accepts export filters', async () => {
    const res = await request(app)
      .get('/api/exports/json')
      .query({ status: 'with_metadata,to_download_references', year_from: '1980', year_to: '2020', source: 'journal' })
    expect(res.status).toBe(200)
    expect(res.headers['content-disposition']).toContain('.json')
  })

  test('rejects invalid year range', async () => {
    const res = await request(app).get('/api/exports/json').query({ year_from: '2025', year_to: '2020' })
    expect(res.status).toBe(400)
  })

  test('rejects unknown format', async () => {
    const res = await request(app).get('/api/exports/invalid')
    expect(res.status).toBe(400)
  })
})
