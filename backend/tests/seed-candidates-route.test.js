import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import Database from 'better-sqlite3'
import request from 'supertest'

// IMPORTANT: app.js reads process.env.RAG_FEEDER_DB_PATH into a top-level
// `const DB_PATH` the moment the module is first evaluated. A static
// `import ... from '../src/app.js'` at the top of this file would run before
// any beforeAll has a chance to set that env var, and — since ES modules are
// cached singletons — a later dynamic import() would just hand back that
// already-evaluated module, pointed at the wrong DB file. So the whole
// module is loaded once, dynamically, from a single beforeAll after the env
// is set (mirrors tests/search-run-cancel.test.js).
let parseCandidatePaging
let app
let authToken = ''
let corpusId = null
let dbPath = ''
let tempDir = ''
let originalDbPath
let originalJwtSecret
let originalAdminUser
let originalAdminPassword

beforeAll(async () => {
  tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'dt-seed-candidates-route-'))
  dbPath = path.join(tempDir, 'route.db')
  originalDbPath = process.env.RAG_FEEDER_DB_PATH
  originalJwtSecret = process.env.RAG_FEEDER_JWT_SECRET
  originalAdminUser = process.env.RAG_ADMIN_USER
  originalAdminPassword = process.env.RAG_ADMIN_PASSWORD
  process.env.RAG_FEEDER_DB_PATH = dbPath
  process.env.RAG_FEEDER_JWT_SECRET = 'seed-candidates-route-secret'
  process.env.RAG_ADMIN_USER = 'seed-candidates-admin'
  process.env.RAG_ADMIN_PASSWORD = 'seed-candidates-password'

  const mod = await import('../src/app.js')
  parseCandidatePaging = mod.parseCandidatePaging
  app = mod.createApp({ broadcast: () => {} })

  const login = await request(app).post('/api/auth/login').send({
    username: process.env.RAG_ADMIN_USER,
    password: process.env.RAG_ADMIN_PASSWORD,
  })
  authToken = login.body.token
  corpusId = login.body?.user?.last_corpus_id

  // createApp() already ran ensureAuthSchema/ensureSeedSchema against
  // dbPath; add the search tables it assumes exist (normally created by the
  // Python migration) and some data owned by the logged-in corpus.
  const db = new Database(dbPath)
  try {
    db.exec(`
      CREATE TABLE ingest_entries (
        id INTEGER PRIMARY KEY,
        corpus_id INTEGER NOT NULL,
        ingest_source TEXT NOT NULL,
        title TEXT,
        authors TEXT,
        year TEXT,
        doi TEXT,
        source TEXT,
        publisher TEXT,
        url TEXT,
        source_pdf TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
      CREATE TABLE search_runs (
        id INTEGER PRIMARY KEY,
        query TEXT,
        filters_json TEXT,
        status TEXT,
        fetched_count INTEGER,
        expected_count INTEGER,
        error TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
      CREATE TABLE search_results (
        id INTEGER PRIMARY KEY,
        search_run_id INTEGER NOT NULL,
        title TEXT,
        doi TEXT,
        openalex_id TEXT,
        year TEXT,
        raw_json TEXT
      );
    `)
    db.prepare(`INSERT INTO search_runs (id, query, status) VALUES (1, 'route test', 'completed')`).run()
    db.prepare(`INSERT INTO search_run_corpora (search_run_id, corpus_id) VALUES (1, ?)`).run(corpusId)
    const ins = db.prepare(
      `INSERT INTO search_results (id, search_run_id, title, year, raw_json) VALUES (?, 1, ?, ?, ?)`
    )
    for (let i = 1; i <= 5; i += 1) {
      ins.run(i, `Route Title ${i}`, String(2000 + i), JSON.stringify({
        authorships: [{ author: { display_name: i % 2 ? 'Zed Author' : 'Anna Author' } }],
      }))
    }
  } finally {
    db.close()
  }
})

afterAll(() => {
  if (originalDbPath === undefined) delete process.env.RAG_FEEDER_DB_PATH
  else process.env.RAG_FEEDER_DB_PATH = originalDbPath
  if (originalJwtSecret === undefined) delete process.env.RAG_FEEDER_JWT_SECRET
  else process.env.RAG_FEEDER_JWT_SECRET = originalJwtSecret
  if (originalAdminUser === undefined) delete process.env.RAG_ADMIN_USER
  else process.env.RAG_ADMIN_USER = originalAdminUser
  if (originalAdminPassword === undefined) delete process.env.RAG_ADMIN_PASSWORD
  else process.env.RAG_ADMIN_PASSWORD = originalAdminPassword
  if (tempDir && fs.existsSync(tempDir)) {
    try { fs.rmSync(tempDir, { recursive: true, force: true }) } catch { /* noop */ }
  }
})

describe('parseCandidatePaging', () => {
  test('truncates a fractional limit', () => {
    expect(parseCandidatePaging({ limit: '10.5' }).limit).toBe(10)
  })

  test('clamps a zero limit up to 1, not the 200 default', () => {
    expect(parseCandidatePaging({ limit: '0' }).limit).toBe(1)
  })

  test('clamps an oversized limit down to 2000', () => {
    expect(parseCandidatePaging({ limit: '5000' }).limit).toBe(2000)
  })

  test('clamps a negative offset up to 0', () => {
    expect(parseCandidatePaging({ offset: '-3' }).offset).toBe(0)
  })

  test('falls back an unknown sort key to empty string', () => {
    expect(parseCandidatePaging({ sort: 'bogus' }).sort).toBe('')
  })

  test('defaults are limit 200, offset 0, sort "", dir "asc"', () => {
    expect(parseCandidatePaging({})).toEqual({ limit: 200, offset: 0, sort: '', dir: 'asc' })
  })

  test('accepts a valid sort key and desc direction', () => {
    expect(parseCandidatePaging({ sort: 'refs', dir: 'DESC' })).toEqual({ limit: 200, offset: 0, sort: 'refs', dir: 'desc' })
  })
})

describe('seed candidates route: paging clamps and dismiss-all', () => {
  test('clamps a fractional limit and a negative offset, and ignores an unknown sort', async () => {
    const res = await request(app)
      .get('/api/seed/sources/search/1/candidates')
      .query({ limit: '10.5', offset: '-3', sort: 'bogus' })
      .set('Authorization', `Bearer ${authToken}`)
    expect(res.status).toBe(200)
    expect(res.body.limit).toBe(10)
    expect(res.body.offset).toBe(0)
    expect(res.body.total).toBe(5)
    expect(res.body.candidates).toHaveLength(5)
  })

  test('clamps limit=0 up to 1', async () => {
    const res = await request(app)
      .get('/api/seed/sources/search/1/candidates')
      .query({ limit: '0' })
      .set('Authorization', `Bearer ${authToken}`)
    expect(res.status).toBe(200)
    expect(res.body.limit).toBe(1)
    expect(res.body.candidates).toHaveLength(1)
  })

  test('clamps an oversized limit down to 2000', async () => {
    const res = await request(app)
      .get('/api/seed/sources/search/1/candidates')
      .query({ limit: '5000' })
      .set('Authorization', `Bearer ${authToken}`)
    expect(res.status).toBe(200)
    expect(res.body.limit).toBe(2000)
  })

  test('dismiss route accepts { all: true, q } instead of candidateKeys', async () => {
    const res = await request(app)
      .post('/api/seed/candidates/dismiss')
      .set('Authorization', `Bearer ${authToken}`)
      .send({ sourceType: 'search', sourceKey: '1', all: true, q: 'zed author' })
    expect(res.status).toBe(200)
    // i % 2 truthy for odd i => 'Zed Author'; that's ids 1, 3, 5 (3 of 5).
    expect(res.body).toEqual({ success: true, dismissed: 3 })

    const after = await request(app)
      .get('/api/seed/sources/search/1/candidates')
      .set('Authorization', `Bearer ${authToken}`)
    expect(after.body.total).toBe(2)
  })
})
