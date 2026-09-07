import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import Database from 'better-sqlite3'
import request from 'supertest'

// The search child owns its own row: it marks the run done/failed/cancelled
// before exiting. These tests cover the two ways that contract can break —
// the child dying without running its handlers (C2), and the parent being
// unable to register corpus ownership at all (I8).
describe('background search run: dead children and failed registration', () => {
  let app = null
  let finalizeDeadSearchRun = null
  let activeSearchRuns = null
  let authToken = ''
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
      `)
    } finally {
      db.close()
    }
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
