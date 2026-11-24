import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import Database from 'better-sqlite3'
import bcrypt from 'bcryptjs'
import request from 'supertest'
import { jest } from '@jest/globals'

// The search child owns its own row: it marks the run done/failed/cancelled
// before exiting. These tests cover the two ways that contract can break —
// the child dying without running its handlers (C2), and the parent being
// unable to register corpus ownership at all (I8).
describe('background search run: dead children and failed registration', () => {
  let app = null
  let finalizeDeadSearchRun = null
  let openAlexQuotaStop = null
  let activeSearchRuns = null
  let authToken = ''
  let editorToken = ''
  let corpusId = null
  let dbPath = ''
  let tempDir = ''
  let fakePython = ''
  const originalEnv = {}

  const setEnv = (key, value) => {
    originalEnv[key] = process.env[key]
    process.env[key] = value
  }

  const waitUntil = async (predicate, { timeoutMs = 4000, intervalMs = 20 } = {}) => {
    const deadline = Date.now() + timeoutMs
    while (Date.now() < deadline) {
      if (predicate()) return true
      await new Promise((resolve) => setTimeout(resolve, intervalMs))
    }
    return predicate()
  }

  const runStatus = (runId) => {
    const db = new Database(dbPath, { readonly: true })
    try {
      return db.prepare('SELECT status, error, finished_at FROM search_runs WHERE id = ?').get(runId) || null
    } finally {
      db.close()
    }
  }

  beforeAll(async () => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'dt-search-run-failure-'))
    dbPath = path.join(tempDir, 'failure.db')
    fakePython = path.join(tempDir, 'fake-python')
    // Announces the run (the real script does this the instant the row
    // exists) and then dies without ever settling it — the SIGKILL / OOM /
    // hard-container-stop shape.
    fs.writeFileSync(fakePython, `#!/usr/bin/env node
console.log(JSON.stringify({ event: 'run_created', runId: Number(process.env.FAKE_RUN_ID || '1') }))
if (process.env.FAKE_STDERR) process.stderr.write(process.env.FAKE_STDERR)
if (process.env.FAKE_FINAL_JSON) console.log(process.env.FAKE_FINAL_JSON)
setTimeout(() => process.exit(Number(process.env.FAKE_EXIT_CODE || '9')), 30)
`)
    fs.chmodSync(fakePython, 0o755)

    setEnv('RAG_FEEDER_DB_PATH', dbPath)
    setEnv('RAG_FEEDER_PYTHON', fakePython)
    setEnv('RAG_FEEDER_JWT_SECRET', 'search-run-failure-secret')
    setEnv('RAG_ADMIN_USER', 'search-failure-admin')
    setEnv('RAG_ADMIN_PASSWORD', 'search-failure-password')

    const mod = await import('../src/app.js')
    app = mod.createApp({ broadcast: () => {} })
    finalizeDeadSearchRun = mod.finalizeDeadSearchRun
    openAlexQuotaStop = mod.openAlexQuotaStop
    activeSearchRuns = mod.activeSearchRuns

    const login = await request(app).post('/api/auth/login').send({
      username: process.env.RAG_ADMIN_USER,
      password: process.env.RAG_ADMIN_PASSWORD,
    })
    authToken = login.body.token
    corpusId = login.body?.user?.last_corpus_id

    // search_runs normally comes from the Python migration.
    const db = new Database(dbPath)
    try {
      db.exec(`
        CREATE TABLE search_runs (
          id INTEGER PRIMARY KEY,
          query TEXT,
          filters_json TEXT,
          status TEXT,
          fetched_count INTEGER,
          expected_count INTEGER,
          error TEXT,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          finished_at TIMESTAMP
        );
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
      // An editor of the admin's corpus, the corpus every run here is registered to.
      const editorId = Number(db.prepare(
        `INSERT INTO users (username, password_hash, email, account_status, password_set_at, last_corpus_id)
         VALUES ('failure-editor', ?, 'failure-editor@example.test', 'active', CURRENT_TIMESTAMP, ?)`
      ).run(bcrypt.hashSync('failure-editor-password', 4), corpusId).lastInsertRowid)
      db.prepare("INSERT INTO user_corpora (user_id, corpus_id, role) VALUES (?, ?, 'editor')").run(editorId, corpusId)
    } finally {
      db.close()
    }
    const editorLogin = await request(app).post('/api/auth/login').send({
      username: 'failure-editor',
      password: 'failure-editor-password',
    })
    expect(editorLogin.status).toBe(200)
    editorToken = editorLogin.body.token
  })

  afterAll(() => {
    for (const child of activeSearchRuns?.values() || []) {
      try { child.kill('SIGKILL') } catch { /* noop */ }
    }
    for (const [key, value] of Object.entries(originalEnv)) {
      if (value === undefined) delete process.env[key]
      else process.env[key] = value
    }
    delete process.env.FAKE_RUN_ID
    delete process.env.FAKE_EXIT_CODE
    delete process.env.FAKE_FINAL_JSON
    delete process.env.FAKE_STDERR
    if (tempDir && fs.existsSync(tempDir)) {
      try { fs.rmSync(tempDir, { recursive: true, force: true }) } catch { /* noop */ }
    }
  })

  const insertRunningRun = (runId) => {
    const db = new Database(dbPath)
    try {
      db.prepare(`INSERT INTO search_runs (id, query, status, fetched_count) VALUES (?, 'dead child', 'running', 0)`).run(runId)
    } finally {
      db.close()
    }
  }

  test('a child that exits without settling its run leaves the row failed, not running', async () => {
    insertRunningRun(6001)
    process.env.FAKE_RUN_ID = '6001'
    process.env.FAKE_EXIT_CODE = '137' // SIGKILL's shell exit code
    const res = await request(app)
      .post('/api/keyword-search')
      .set('Authorization', `Bearer ${authToken}`)
      .send({ query: 'dead child' })
    expect(res.status).toBe(202)
    expect(res.body).toEqual({ runId: 6001, status: 'running' })

    const settled = await waitUntil(() => runStatus(6001)?.status !== 'running')
    expect(settled).toBe(true)
    const row = runStatus(6001)
    expect(row.status).toBe('failed')
    expect(row.error).toMatch(/^search process exited \(/)
    expect(row.finished_at).toBeTruthy()
    expect(activeSearchRuns.has(6001)).toBe(false)
  })

  // A failed run's stored error is child output: the seed API shows it to the
  // admin only; everyone else reads 'search failed' (D10). A done run's quota
  // note stays for everyone.
  test('a failed run shows its error only to the admin', async () => {
    const withDb = (fn) => {
      const db = new Database(dbPath)
      try { return fn(db) } finally { db.close() }
    }
    const addResult = (runId) => withDb((db) => db.prepare(
      `INSERT INTO search_results (search_run_id, title, year, raw_json) VALUES (?, ?, '2020', '{}')`
    ).run(runId, `Result of ${runId}`))
    const register = (runId) => withDb((db) => db.prepare(
      'INSERT INTO search_run_corpora (search_run_id, corpus_id) VALUES (?, ?)'
    ).run(runId, corpusId))
    const note = 'OpenAlex quota exhausted; expansion stopped early. Results fetched before the stop are kept.'
    const runErrors = async (token, runId) => {
      const list = await request(app).get('/api/seed/sources').set('Authorization', `Bearer ${token}`)
      expect(list.status).toBe(200)
      const source = list.body.sources.find((item) => item.source_type === 'search' && String(item.source_key) === String(runId))
      expect(source).toBeTruthy()
      const detail = await request(app)
        .get(`/api/seed/sources/search/${runId}/candidates`)
        .set('Authorization', `Bearer ${token}`)
      expect(detail.status).toBe(200)
      return [source.run.error, detail.body.source_summary.run.error]
    }

    const errorSpy = jest.spyOn(console, 'error').mockImplementation(() => {})
    let logged = null
    try {
      insertRunningRun(6101)
      addResult(6101)
      process.env.FAKE_RUN_ID = '6101'
      process.env.FAKE_EXIT_CODE = '3'
      process.env.FAKE_STDERR = 'SECRET-DIAG /srv/private/path'
      const res = await request(app)
        .post('/api/keyword-search')
        .set('Authorization', `Bearer ${authToken}`)
        .send({ query: 'secret failure' })
      expect(res.status).toBe(202)
      expect(await waitUntil(() => runStatus(6101)?.status === 'failed')).toBe(true)
      expect(runStatus(6101).error).toContain('SECRET-DIAG')
      expect(await waitUntil(() => errorSpy.mock.calls.some(([tag]) => tag === '[/api/keyword-search] Search script failed:'))).toBe(true)
      logged = errorSpy.mock.calls.find(([tag]) => tag === '[/api/keyword-search] Search script failed:')
    } finally {
      delete process.env.FAKE_STDERR
      errorSpy.mockRestore()
    }

    expect(await runErrors(editorToken, 6101)).toEqual(['search failed', 'search failed'])
    for (const text of await runErrors(authToken, 6101)) expect(text).toContain('SECRET-DIAG')
    // The launch's log line has the whole error, not just its message.
    expect(logged[1]).toBeInstanceOf(Error)
    expect(logged[1].message).toContain('SECRET-DIAG')

    // A row that failed before this change holds the same kind of text.
    withDb((db) => db.prepare(
      `INSERT INTO search_runs (id, query, status, fetched_count, error) VALUES (6102, 'old failure', 'failed', 1, ?)`
    ).run('Traceback: SECRET-DIAG /srv/private/path'))
    addResult(6102)
    register(6102)
    expect(await runErrors(editorToken, 6102)).toEqual(['search failed', 'search failed'])
    for (const text of await runErrors(authToken, 6102)) expect(text).toContain('SECRET-DIAG')

    withDb((db) => db.prepare(
      `INSERT INTO search_runs (id, query, status, fetched_count, error) VALUES (6103, 'quota stop', 'done', 1, ?)`
    ).run(note))
    addResult(6103)
    register(6103)
    expect(await runErrors(editorToken, 6103)).toEqual([note, note])
  })

  test('a run the child already settled is left alone', async () => {
    insertRunningRun(6002)
    const db = new Database(dbPath)
    try {
      db.prepare(`UPDATE search_runs SET status = 'done', finished_at = CURRENT_TIMESTAMP WHERE id = 6002`).run()
    } finally {
      db.close()
    }
    process.env.FAKE_RUN_ID = '6002'
    process.env.FAKE_EXIT_CODE = '0'
    const res = await request(app)
      .post('/api/keyword-search')
      .set('Authorization', `Bearer ${authToken}`)
      .send({ query: 'already done' })
    expect(res.status).toBe(202)

    await waitUntil(() => !activeSearchRuns.has(6002))
    // Give the settle hop a beat; the assertion is that it changed nothing.
    await new Promise((resolve) => setTimeout(resolve, 50))
    expect(runStatus(6002).status).toBe('done')
    expect(runStatus(6002).error).toBeNull()
  })

  test('a run stopped by OpenAlex quota is logged and keeps its done row with the note (S-I1)', async () => {
    const note = 'OpenAlex quota exhausted; expansion stopped early. Results fetched before the stop are kept.'
    insertRunningRun(6005)
    const db = new Database(dbPath)
    try {
      // The script settles its own row: done, with the stop as its note.
      db.prepare(`UPDATE search_runs SET status = 'done', error = ?, finished_at = CURRENT_TIMESTAMP WHERE id = 6005`).run(note)
    } finally {
      db.close()
    }
    const warn = jest.spyOn(console, 'warn').mockImplementation(() => {})
    try {
      process.env.FAKE_RUN_ID = '6005'
      process.env.FAKE_EXIT_CODE = '0'
      process.env.FAKE_FINAL_JSON = JSON.stringify({ runId: 6005, results: [], quota_exhausted: true, message: note })
      const res = await request(app)
        .post('/api/keyword-search')
        .set('Authorization', `Bearer ${authToken}`)
        .send({ query: 'quota stop' })
      expect(res.status).toBe(202)
      const logged = await waitUntil(() => warn.mock.calls.some((args) => String(args[0]).includes(note)))
      expect(logged).toBe(true)
      expect(warn.mock.calls.find((args) => String(args[0]).includes(note))[0]).toContain('[/api/keyword-search]')
      expect(runStatus(6005)).toMatchObject({ status: 'done', error: note })
    } finally {
      delete process.env.FAKE_FINAL_JSON
      warn.mockRestore()
    }
  })

  test('openAlexQuotaStop logs and returns the message only for a stopped payload', () => {
    const warn = jest.spyOn(console, 'warn').mockImplementation(() => {})
    try {
      expect(openAlexQuotaStop('/r', { runs: [] })).toBeNull()
      expect(openAlexQuotaStop('/r', null)).toBeNull()
      expect(warn).not.toHaveBeenCalled()
      expect(openAlexQuotaStop('/r', { runs: [], quota_exhausted: true, message: 'stopped' })).toBe('stopped')
      expect(warn).toHaveBeenCalledWith('[/r] stopped')
    } finally {
      warn.mockRestore()
    }
  })

  test('finalizeDeadSearchRun only touches rows still marked running', () => {
    insertRunningRun(6003)
    const db = new Database(dbPath)
    try {
      expect(finalizeDeadSearchRun(db, 6003, new Error('boom'))).toBe(true)
      expect(finalizeDeadSearchRun(db, 6003, new Error('boom'))).toBe(false)
      const row = db.prepare('SELECT status, error FROM search_runs WHERE id = 6003').get()
      expect(row.status).toBe('failed')
      expect(row.error).toBe('search process exited (boom)')
      // A missing run and a nonsense id are both no-ops, never throws.
      expect(finalizeDeadSearchRun(db, 999999, null)).toBe(false)
      expect(finalizeDeadSearchRun(db, 'nope', null)).toBe(false)
    } finally {
      db.close()
    }
  })

  test('a run that cannot be registered is killed and 500s instead of 202ing a ghost', async () => {
    insertRunningRun(6004)
    process.env.FAKE_RUN_ID = '6004'
    process.env.FAKE_EXIT_CODE = '0'

    // Drop the ownership table so upsertSearchRunCorpus throws at prepare time:
    // the "registration failed" shape, without monkeypatching the module.
    const dropDb = new Database(dbPath)
    let recreate = ''
    try {
      recreate = dropDb.prepare(`SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'search_run_corpora'`).get().sql
      dropDb.exec('DROP TABLE search_run_corpora')
    } finally {
      dropDb.close()
    }

    try {
      const res = await request(app)
        .post('/api/keyword-search')
        .set('Authorization', `Bearer ${authToken}`)
        .send({ query: 'unregisterable' })
      expect(res.status).toBe(500)
      expect(res.body).toEqual({ error: 'Could not register the search run' })
      // Never registered, so never cancelable — which is exactly why it had
      // to be killed rather than left fetching.
      expect(activeSearchRuns.has(6004)).toBe(false)
      const gone = await waitUntil(() => runStatus(6004)?.status !== 'running')
      expect(gone).toBe(true)
      expect(runStatus(6004).status).toBe('failed')
    } finally {
      const restoreDb = new Database(dbPath)
      try { restoreDb.exec(recreate) } finally { restoreDb.close() }
    }
  })
})
