import Database from 'better-sqlite3'
import request from 'supertest'
import { createApp, CORPUS_MAX_LIMIT } from '../src/app.js'

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

describe('GET /api/corpus limit clamp', () => {
  let app
  let token = ''
  let corpusId = null

  beforeAll(async () => {
    delete process.env.RAG_FEEDER_STUB
    process.env.RAG_FEEDER_JWT_SECRET = 'corpus-clamp-secret'
    process.env.RAG_ADMIN_USER = 'corpus-clamp-admin'
    process.env.RAG_ADMIN_PASSWORD = 'corpus-clamp-password'
    app = createApp({ broadcast: () => {} })
    const login = await request(app).post('/api/auth/login').send({
      username: 'corpus-clamp-admin',
      password: 'corpus-clamp-password',
    })
    expect(login.status).toBe(200)
    token = login.body.token
    corpusId = login.body.user?.last_corpus_id
    const db = new Database(process.env.RAG_FEEDER_DB_PATH)
    try {
      const insertWork = db.prepare(
        `INSERT INTO works (title, normalized_title, metadata_status, download_status)
         VALUES (?, ?, 'matched', 'not_requested')`
      )
      const link = db.prepare('INSERT INTO corpus_works (corpus_id, work_id) VALUES (?, ?)')
      db.transaction(() => {
        for (let i = 0; i < 1005; i += 1) {
          const title = `Clamp work ${i}`
          const work = insertWork.run(title, title.toLowerCase())
          link.run(corpusId, Number(work.lastInsertRowid))
        }
      })()
    } finally {
      db.close()
    }
  })

  afterAll(() => {
    delete process.env.RAG_FEEDER_JWT_SECRET
    delete process.env.RAG_ADMIN_USER
    delete process.env.RAG_ADMIN_PASSWORD
  })

  test('GET /api/corpus clamps limit to 1000', async () => {
    expect(CORPUS_MAX_LIMIT).toBe(1000)
    const res = await request(app).get('/api/corpus?limit=5000').set('Authorization', `Bearer ${token}`)
    expect(res.status).toBe(200)
    expect(res.body.items).toHaveLength(1000)
    expect(res.body.total).toBe(1005)
  })

  test('a limit within the bound is kept', async () => {
    const res = await request(app).get('/api/corpus?limit=7').set('Authorization', `Bearer ${token}`)
    expect(res.status).toBe(200)
    expect(res.body.items).toHaveLength(7)
    expect(res.body.total).toBe(1005)
  })
})
