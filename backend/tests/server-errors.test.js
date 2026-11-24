import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import Database from 'better-sqlite3'
import bcrypt from 'bcryptjs'
import request from 'supertest'
import { jest } from '@jest/globals'

// One 500 rule (D10): a non-admin reads the route's fallback, never child
// output, SQLite text or paths; the admin reads error.message; the server log
// always gets the whole error.
describe('server errors: the detail goes to admins only', () => {
  let app
  let tmpDir = ''
  let dbPath = ''
  let uploadsDir = ''
  let errorSpy
  const password = 'server-errors-password'
  const tokens = {}
  const ids = {}
  const saved = {}
  const FAKE_ENV = ['FAKE_STDERR', 'FAKE_STDOUT', 'FAKE_EXIT_CODE', 'FAKE_EXPAND_STDERR']

  const setEnv = (key, value) => {
    saved[key] = process.env[key]
    process.env[key] = value
  }
  const withDb = (fn) => {
    const db = new Database(dbPath)
    try {
      return fn(db)
    } finally {
      db.close()
    }
  }
  const as = (who) => ({ Authorization: `Bearer ${tokens[who]}` })
  // console.error calls whose first argument is `tag` and whose second is an
  // Error carrying `text`.
  const loggedErrors = (tag, text) => errorSpy.mock.calls.filter(
    ([first, second]) => first === tag && second instanceof Error && second.message.includes(text)
  )

  beforeAll(async () => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'dt-server-errors-'))
    dbPath = path.join(tmpDir, 'errors.db')
    const fakePython = path.join(tmpDir, 'fake-python')
    // The promotion answers a summary; the expansion child fails with
    // FAKE_EXPAND_STDERR; every other script writes FAKE_STDERR, prints
    // FAKE_STDOUT if set and exits FAKE_EXIT_CODE.
    fs.writeFileSync(fakePython, `#!/usr/bin/env node
const script = require('path').basename(process.argv[2] || '')
if (script === 'seed_promote.py') {
  console.log(JSON.stringify({ promoted: 1, pending_work_ids: [], already_enriched: 1 }))
  process.exit(0)
}
if (script === 'seed_expand.py') {
  process.stderr.write(process.env.FAKE_EXPAND_STDERR || 'expand failed')
  process.exit(3)
}
if (process.env.FAKE_STDERR) process.stderr.write(process.env.FAKE_STDERR)
if (process.env.FAKE_STDOUT) console.log(process.env.FAKE_STDOUT)
process.exit(Number(process.env.FAKE_EXIT_CODE || 0))
`)
    fs.chmodSync(fakePython, 0o755)
    setEnv('RAG_FEEDER_DB_PATH', dbPath)
    setEnv('RAG_FEEDER_PYTHON', fakePython)
    setEnv('RAG_FEEDER_ARTIFACTS_DIR', path.join(tmpDir, 'artifacts'))
    uploadsDir = path.join(tmpDir, 'uploads')
    fs.mkdirSync(uploadsDir)
    setEnv('RAG_FEEDER_UPLOADS_DIR', uploadsDir)
    setEnv('RAG_FEEDER_JWT_SECRET', 'server-errors-secret')
    setEnv('RAG_ADMIN_USER', 'server-errors-admin')
    setEnv('RAG_ADMIN_PASSWORD', 'server-errors-admin-password')
    delete process.env.RAG_FEEDER_STUB

    const { createApp } = await import('../src/app.js')
    app = createApp({ broadcast() {} })

    const adminLogin = await request(app).post('/api/auth/login')
      .send({ username: process.env.RAG_ADMIN_USER, password: process.env.RAG_ADMIN_PASSWORD })
    expect(adminLogin.status).toBe(200)
    tokens.admin = adminLogin.body.token
    ids.corpus = Number(adminLogin.body.user.last_corpus_id)

    withDb((db) => {
      const insertUser = (name, role) => {
        const userId = Number(db.prepare(
          `INSERT INTO users (username, password_hash, email, account_status, password_set_at, last_corpus_id)
           VALUES (?, ?, ?, 'active', CURRENT_TIMESTAMP, ?)`
        ).run(name, bcrypt.hashSync(password, 4), `${name}@example.test`, ids.corpus).lastInsertRowid)
        db.prepare('INSERT INTO user_corpora (user_id, corpus_id, role) VALUES (?, ?, ?)').run(userId, ids.corpus, role)
      }
      insertUser('errors-editor', 'editor')
      insertUser('errors-member', 'viewer')
      // Search tables normally come from the Python migration
      // (seed-candidates-route.test.js); one search seed with one result.
      db.exec(`
        CREATE TABLE IF NOT EXISTS ingest_entries (
          id INTEGER PRIMARY KEY, corpus_id INTEGER NOT NULL, ingest_source TEXT NOT NULL, title TEXT,
          authors TEXT, year TEXT, doi TEXT, source TEXT, publisher TEXT, url TEXT, source_pdf TEXT,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
        CREATE TABLE IF NOT EXISTS search_runs (
          id INTEGER PRIMARY KEY, query TEXT, filters_json TEXT, status TEXT, fetched_count INTEGER,
          expected_count INTEGER, error TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, finished_at TIMESTAMP);
        CREATE TABLE IF NOT EXISTS search_results (
          id INTEGER PRIMARY KEY, search_run_id INTEGER NOT NULL, title TEXT, doi TEXT, openalex_id TEXT,
          year TEXT, raw_json TEXT);
      `)
      db.prepare(`INSERT INTO search_runs (id, query, status) VALUES (41, 'promote me', 'done')`).run()
      db.prepare('INSERT INTO search_run_corpora (search_run_id, corpus_id) VALUES (41, ?)').run(ids.corpus)
      db.prepare(
        `INSERT INTO search_results (id, search_run_id, title, doi, openalex_id, year, raw_json)
         VALUES (1, 41, 'Promotable work', '10.1000/promote', 'W4100', '2020', '{}')`
      ).run()
    })
    for (const who of ['editor', 'member']) {
      const res = await request(app).post('/api/auth/login').send({ username: `errors-${who}`, password })
      expect(res.status).toBe(200)
      tokens[who] = res.body.token
    }
  })

  afterAll(() => {
    for (const [key, value] of Object.entries(saved)) {
      if (value === undefined) delete process.env[key]
      else process.env[key] = value
    }
    for (const key of FAKE_ENV) delete process.env[key]
    if (tmpDir) fs.rmSync(tmpDir, { recursive: true, force: true })
  })

  beforeEach(() => {
    for (const key of FAKE_ENV) delete process.env[key]
    errorSpy = jest.spyOn(console, 'error').mockImplementation(() => {})
  })

  afterEach(() => {
    errorSpy.mockRestore()
  })

  test('a failed export hides the child diagnostic from a member and shows it to the admin', async () => {
    process.env.FAKE_STDERR = 'SECRET-DIAG /srv/private/path'
    process.env.FAKE_EXIT_CODE = '3'

    const member = await request(app).get('/api/exports/json').set(as('member'))
    expect(member.status).toBe(500)
    expect(member.body).toEqual({ error: 'Failed to export corpus data' })
    expect(JSON.stringify(member.body)).not.toContain('SECRET-DIAG')

    const admin = await request(app).get('/api/exports/json').set(as('admin'))
    expect(admin.status).toBe(500)
    expect(admin.body.error).toContain('SECRET-DIAG')

    expect(loggedErrors('[/api/exports] Error:', 'SECRET-DIAG')).toHaveLength(2)
  })

  test('an unmapped keyword-search failure hides the diagnostic', async () => {
    process.env.FAKE_STDERR = 'SECRET-DIAG /srv/private/path'
    process.env.FAKE_EXIT_CODE = '3'
    const search = { query: 'labour' }

    const preview = await request(app).post('/api/keyword-search/preview').set(as('editor')).send(search)
    expect(preview.status).toBe(502)
    expect(preview.body).toEqual({ error: 'Keyword search failed' })
    const launch = await request(app).post('/api/keyword-search').set(as('editor')).send(search)
    expect(launch.status).toBe(500)
    expect(launch.body).toEqual({ error: 'Keyword search failed' })

    const adminPreview = await request(app).post('/api/keyword-search/preview').set(as('admin')).send(search)
    expect(adminPreview.status).toBe(502)
    expect(adminPreview.body.error).toContain('SECRET-DIAG')
    const adminLaunch = await request(app).post('/api/keyword-search').set(as('admin')).send(search)
    expect(adminLaunch.status).toBe(500)
    expect(adminLaunch.body.error).toContain('SECRET-DIAG')

    expect(loggedErrors('[/api/keyword-search/preview] Error:', 'SECRET-DIAG')).toHaveLength(2)
    expect(loggedErrors('[/api/keyword-search] Search script failed:', 'SECRET-DIAG')).toHaveLength(2)
  })

  test('mapped keyword-search failures keep their text', async () => {
    process.env.FAKE_EXIT_CODE = '3'
    const routes = ['/api/keyword-search/preview', '/api/keyword-search']

    process.env.FAKE_STDERR = 'OpenAlex rate limit exceeded. Retry after 5s.'
    for (const route of routes) {
      const res = await request(app).post(route).set(as('editor')).send({ query: 'labour' })
      expect(res.status).toBe(429)
      expect(res.body).toEqual({ error: 'OpenAlex rate limit exceeded. Retry after 5s.' })
    }

    // Today's ValueError mapping stays for everyone (BE-56, parked).
    process.env.FAKE_STDERR = 'ValueError: bad query'
    for (const route of routes) {
      const res = await request(app).post(route).set(as('editor')).send({ query: 'labour' })
      expect(res.status).toBe(400)
      expect(res.body).toEqual({ error: 'bad query' })
    }
  })

  test('a failed promotion expansion hides the diagnostic from an editor', async () => {
    process.env.FAKE_EXPAND_STDERR = 'SECRET-DIAG /srv/private/path'
    const candidates = await request(app).get('/api/seed/sources/search/41/candidates').set(as('editor'))
    expect(candidates.status).toBe(200)
    const candidateKeys = candidates.body.candidates.map((candidate) => candidate.candidate_key)
    expect(candidateKeys).toHaveLength(1)
    const promote = (who) => request(app).post('/api/seed/sources/search/41/promote').set(as(who)).send({
      candidateKeys,
      promotionMode: 'new_seed',
      includeDownstream: true,
      relatedDepthDownstream: 1,
    })

    const editor = await promote('editor')
    expect(editor.status).toBe(200)
    expect(editor.body.expansion_error).toBe('Failed to build expansion seeds')
    expect(editor.body.message).toContain('Expansion seeds could not be built: Failed to build expansion seeds')
    expect(JSON.stringify(editor.body)).not.toContain('SECRET-DIAG')

    const admin = await promote('admin')
    expect(admin.status).toBe(200)
    expect(admin.body.expansion_error).toContain('SECRET-DIAG')
    expect(admin.body.message).toContain('SECRET-DIAG')
    expect(admin.body.promotion).toEqual(editor.body.promotion)
    expect(admin.body.jobs).toEqual(editor.body.jobs)
    expect(editor.body.jobs.map((job) => job.type)).toEqual(['download'])

    expect(loggedErrors(
      '[/api/seed/sources/:sourceType/:sourceKey/promote] Expansion seeding failed:',
      'SECRET-DIAG'
    )).toHaveLength(2)
  })

  // BE-59: errors no route catches get JSON from the last handler, not
  // Express's HTML page with the stack.
  test('malformed JSON answers a JSON 400, not HTML', async () => {
    const res = await request(app).post('/api/keyword-search/preview').set(as('editor'))
      .set('Content-Type', 'application/json').send('{"query": ')
    expect(res.status).toBe(400)
    expect(res.headers['content-type']).toMatch(/application\/json/)
    expect(typeof res.body.error).toBe('string')
    expect(res.text).not.toContain('<')
  })

  test('a multer write failure hides the path from an editor and shows it to the admin', async () => {
    // The uploads dir becomes a plain file, so multer's disk write fails.
    fs.rmSync(uploadsDir, { recursive: true, force: true })
    fs.writeFileSync(uploadsDir, 'not a directory')
    try {
      const upload = (who) => request(app).post('/api/process_pdf').set(as(who))
        .attach('file', Buffer.from('%PDF-1.4\n%%EOF\n'), { filename: 'paper.pdf', contentType: 'application/pdf' })

      const editor = await upload('editor')
      expect(editor.status).toBe(500)
      expect(editor.body).toEqual({ error: 'Internal server error' })
      expect(editor.text).not.toContain('<')
      expect(editor.text).not.toContain(tmpDir)

      const admin = await upload('admin')
      expect(admin.status).toBe(500)
      expect(admin.body.error).toMatch(/ENOTDIR|ENOENT/)

      // fs errors come from Node's realm, so no instanceof check against the test's Error.
      expect(errorSpy.mock.calls.filter(
        ([tag, err]) => tag === '[express] Error:' && /ENOTDIR|ENOENT/.test(String(err?.message))
      )).toHaveLength(2)
    } finally {
      fs.rmSync(uploadsDir, { force: true })
      fs.mkdirSync(uploadsDir)
    }
  })
})
