import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import Database from 'better-sqlite3'
import request from 'supertest'
import { listSeedCandidates } from '../src/seed.js'

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

describe('seed candidates route: state sorts are refused above the resolve limit', () => {
  // Sorting by metadata/download resolves every candidate in JS, so the route
  // must refuse it above RAG_FEEDER_SEED_STATE_COUNT_LIMIT rather than
  // materializing an unbounded seed.
  let originalLimit

  beforeAll(() => {
    const db = new Database(dbPath)
    try {
      db.prepare(`INSERT INTO search_runs (id, query, status) VALUES (9, 'state sort guard', 'done')`).run()
      db.prepare(`INSERT INTO search_run_corpora (search_run_id, corpus_id) VALUES (9, ?)`).run(corpusId)
      const ins = db.prepare(
        `INSERT INTO search_results (id, search_run_id, title, year, raw_json) VALUES (?, 9, ?, ?, '{}')`
      )
      for (let i = 1; i <= 3; i += 1) ins.run(900 + i, `Guard Title ${i}`, String(2010 + i))
    } finally {
      db.close()
    }
    originalLimit = process.env.RAG_FEEDER_SEED_STATE_COUNT_LIMIT
  })

  afterAll(() => {
    if (originalLimit === undefined) delete process.env.RAG_FEEDER_SEED_STATE_COUNT_LIMIT
    else process.env.RAG_FEEDER_SEED_STATE_COUNT_LIMIT = originalLimit
  })

  test.each(['metadata', 'download'])('400s a %s sort over a seed larger than the limit', async (sort) => {
    process.env.RAG_FEEDER_SEED_STATE_COUNT_LIMIT = '2'
    const res = await request(app)
      .get('/api/seed/sources/search/9/candidates')
      .query({ sort, dir: 'asc' })
      .set('Authorization', `Bearer ${authToken}`)
    expect(res.status).toBe(400)
    expect(res.body.error).toBe('Sorting by state needs every item resolved; not available above 2 items')
  })

  test('allows the same sort once the seed fits under the limit', async () => {
    process.env.RAG_FEEDER_SEED_STATE_COUNT_LIMIT = '10'
    const res = await request(app)
      .get('/api/seed/sources/search/9/candidates')
      .query({ sort: 'download', dir: 'asc' })
      .set('Authorization', `Bearer ${authToken}`)
    expect(res.status).toBe(200)
    expect(res.body.total).toBe(3)
    expect(res.body.candidates).toHaveLength(3)
  })

  test('a SQL sort is never subject to the guard', async () => {
    process.env.RAG_FEEDER_SEED_STATE_COUNT_LIMIT = '1'
    const res = await request(app)
      .get('/api/seed/sources/search/9/candidates')
      .query({ sort: 'year', dir: 'asc' })
      .set('Authorization', `Bearer ${authToken}`)
    expect(res.status).toBe(200)
    expect(res.body.candidates.map((c) => c.year)).toEqual(['2011', '2012', '2013'])
  })
})

describe('seed sources route: the list skips per-candidate state resolution', () => {
  test('state_counts is null in the list but present for an expanded seed', async () => {
    const list = await request(app)
      .get('/api/seed/sources')
      .set('Authorization', `Bearer ${authToken}`)
    expect(list.status).toBe(200)
    const listed = list.body.sources.find((s) => String(s.source_key) === '9')
    expect(listed).toBeTruthy()
    expect(listed.state_counts).toBeNull()
    expect(listed.candidate_count).toBe(3)

    const expanded = await request(app)
      .get('/api/seed/sources/search/9/candidates')
      .set('Authorization', `Bearer ${authToken}`)
    expect(expanded.status).toBe(200)
    expect(expanded.body.source_summary.state_counts).toMatchObject({ pending: 3 })
  })

  test('summary=light drops the summary state counts but keeps the rest of the response', async () => {
    const res = await request(app)
      .get('/api/seed/sources/search/9/candidates')
      .query({ summary: 'light' })
      .set('Authorization', `Bearer ${authToken}`)
    expect(res.status).toBe(200)
    expect(res.body.source_summary).toBeTruthy()
    expect(res.body.source_summary.state_counts).toBeNull()
    // Everything a background poll actually reads is unchanged.
    expect(res.body.source_summary.candidate_count).toBe(3)
    expect(res.body.total).toBe(3)
    expect(res.body.candidates).toHaveLength(3)
  })

  test('any other summary value keeps the full counts', async () => {
    const res = await request(app)
      .get('/api/seed/sources/search/9/candidates')
      .query({ summary: 'full' })
      .set('Authorization', `Bearer ${authToken}`)
    expect(res.status).toBe(200)
    expect(res.body.source_summary.state_counts).toMatchObject({ pending: 3 })
  })
})

describe('promote route: filtered "promote all" only resolves candidates matching q', () => {
  // The /promote route hands off to a Python subprocess for the actual
  // promotion, which isn't safe to drive in a unit test — so this exercises
  // the exact resolution step the route performs when candidateKeys is empty
  // (backend/src/app.js: `listSeedCandidates(..., { q })`, then filtered down
  // to promotable candidates), proving a filtered "promote all" only ever
  // sees candidates matching the active seed filter. Inserts its own
  // dedicated rows (ids 201/202/203) rather than reusing the shared fixture,
  // so the assertions hold regardless of what earlier tests in this file
  // have dismissed.
  test('listSeedCandidates with q excludes non-matching candidates from the promotable set', () => {
    const db = new Database(dbPath)
    try {
      const ins = db.prepare(
        `INSERT INTO search_results (id, search_run_id, title, year, raw_json) VALUES (?, 1, ?, ?, ?)`
      )
      ins.run(201, 'Filter Probe Result A', '2021', JSON.stringify({
        authorships: [{ author: { display_name: 'Probe Author' } }],
      }))
      ins.run(202, 'Filter Probe Result B', '2022', JSON.stringify({
        authorships: [{ author: { display_name: 'Probe Author' } }],
      }))
      ins.run(203, 'Unrelated Decoy Result', '2023', JSON.stringify({
        authorships: [{ author: { display_name: 'Someone Else' } }],
      }))

      const unfiltered = listSeedCandidates(db, corpusId, 'search', '1', {
        stateResolver: null,
        resolveDownloadedFilePath: null,
      })
      const unfilteredKeys = new Set(unfiltered.map((c) => c.candidate_key))
      expect(unfilteredKeys.has('search:201')).toBe(true)
      expect(unfilteredKeys.has('search:202')).toBe(true)
      expect(unfilteredKeys.has('search:203')).toBe(true)

      const availableCandidates = listSeedCandidates(db, corpusId, 'search', '1', {
        stateResolver: null,
        resolveDownloadedFilePath: null,
        q: 'probe author',
      })
      const promotableCandidates = availableCandidates.filter((candidate) => {
        const state = String(candidate?.state || '').trim().toLowerCase()
        return !Boolean(candidate?.in_corpus) && state !== 'downloaded'
      })
      expect(promotableCandidates.map((c) => c.candidate_key).sort()).toEqual(['search:201', 'search:202'])
    } finally {
      db.close()
    }
  })
})
