import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import Database from 'better-sqlite3'
import request from 'supertest'

describe('background search run: registration, cancel, ownership, cleanup', () => {
  let app = null
  let authToken = ''
  let currentCorpusId = null
  let activeSearchRuns = null
  let dbPath = ''
  let tempDir = ''
  let originalPython
  let originalDbPath

  const createBlockingFakePython = () => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'dt-fake-python-cancel-'))
    const fakePython = path.join(tempDir, 'fake-python')
    // Emits run_created immediately, then blocks forever (until SIGTERM) so
    // the cancel route has a live child to signal.
    const script = `#!/usr/bin/env node
const runId = process.env.FAKE_RUN_ID || '1'
console.log(JSON.stringify({ event: 'run_created', runId: Number(runId) }))
// Mirror keyword_search.py's real contract: exit 0 on SIGTERM instead of
// dying by signal, so cancellation doesn't look like a script failure.
process.on('SIGTERM', () => process.exit(0))
setInterval(() => {}, 1000)
`
    fs.writeFileSync(fakePython, script)
    fs.chmodSync(fakePython, 0o755)
    dbPath = path.join(tempDir, 'cancel.db')
    originalPython = process.env.RAG_FEEDER_PYTHON
    originalDbPath = process.env.RAG_FEEDER_DB_PATH
    process.env.RAG_FEEDER_PYTHON = fakePython
    process.env.RAG_FEEDER_DB_PATH = dbPath
  }

  const waitUntil = async (predicate, { timeoutMs = 2000, intervalMs = 20 } = {}) => {
    const deadline = Date.now() + timeoutMs
    while (Date.now() < deadline) {
      if (predicate()) return true
      await new Promise((resolve) => setTimeout(resolve, intervalMs))
    }
    return predicate()
  }

  beforeAll(async () => {
    createBlockingFakePython()
    process.env.RAG_FEEDER_JWT_SECRET = 'cancel-secret'
    process.env.RAG_ADMIN_USER = 'cancel-admin'
    process.env.RAG_ADMIN_PASSWORD = 'cancel-password'
    const mod = await import('../src/app.js')
    app = mod.createApp({ broadcast: () => {} })
    activeSearchRuns = mod.activeSearchRuns

    const login = await request(app).post('/api/auth/login').send({
      username: process.env.RAG_ADMIN_USER,
      password: process.env.RAG_ADMIN_PASSWORD,
    })
    authToken = login.body.token
    currentCorpusId = login.body?.user?.last_corpus_id || null
  })

  afterAll(() => {
    // Best-effort: nothing should still be running, but don't leak a child
    // into the test process's exit if an assertion failed mid-flight.
    for (const child of activeSearchRuns.values()) {
      try { child.kill('SIGKILL') } catch { /* noop */ }
    }
    if (originalPython === undefined) delete process.env.RAG_FEEDER_PYTHON
    else process.env.RAG_FEEDER_PYTHON = originalPython
    if (originalDbPath === undefined) delete process.env.RAG_FEEDER_DB_PATH
    else process.env.RAG_FEEDER_DB_PATH = originalDbPath
    delete process.env.RAG_FEEDER_JWT_SECRET
    delete process.env.RAG_ADMIN_USER
    delete process.env.RAG_ADMIN_PASSWORD
    if (tempDir && fs.existsSync(tempDir)) {
      try { fs.rmSync(tempDir, { recursive: true, force: true }) } catch { /* noop */ }
    }
  })

  test('background search responds 202 with runId/status and registers the child', async () => {
    process.env.FAKE_RUN_ID = '5001'
    const res = await request(app)
      .post('/api/keyword-search')
      .set('Authorization', `Bearer ${authToken}`)
      .send({ query: 'cancel test one' })
    expect(res.status).toBe(202)
    expect(res.body).toEqual({ runId: 5001, status: 'running' })
    expect(activeSearchRuns.has(5001)).toBe(true)
  })

  test('cancel returns 200 for a live child owned by the request corpus', async () => {
    const res = await request(app)
      .post('/api/keyword-search/5001/cancel')
      .set('Authorization', `Bearer ${authToken}`)
    expect(res.status).toBe(200)
    expect(res.body).toEqual({ cancelled: true })
  })

  test('the map entry is gone after the child exits', async () => {
    const gone = await waitUntil(() => !activeSearchRuns.has(5001))
    expect(gone).toBe(true)
  })

  test('cancel returns 404 when the run belongs to another corpus', async () => {
    process.env.FAKE_RUN_ID = '5002'
    const started = await request(app)
      .post('/api/keyword-search')
      .set('Authorization', `Bearer ${authToken}`)
      .send({ query: 'cancel test two' })
    expect(started.status).toBe(202)
    expect(activeSearchRuns.has(5002)).toBe(true)

    // Reassign ownership to a corpus the request does not belong to.
    const db = new Database(dbPath)
    try {
      db.prepare('UPDATE search_run_corpora SET corpus_id = ? WHERE search_run_id = ?').run(
        Number(currentCorpusId) + 999,
        5002
      )
    } finally {
      db.close()
    }

    const res = await request(app)
      .post('/api/keyword-search/5002/cancel')
      .set('Authorization', `Bearer ${authToken}`)
    expect(res.status).toBe(404)
    expect(activeSearchRuns.has(5002)).toBe(true) // untouched: cancel refused before killing

    // Clean up directly since the API correctly refused to kill it.
    activeSearchRuns.get(5002)?.kill('SIGTERM')
    await waitUntil(() => !activeSearchRuns.has(5002))
  })
})
