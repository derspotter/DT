import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import Database from 'better-sqlite3'
import request from 'supertest'

const readArgFile = (filePath) => {
  const raw = fs.readFileSync(filePath, 'utf8').trim()
  if (!raw) return []
  return JSON.parse(raw)
}

describe('recursion arg propagation', () => {
  let app = null
  let authToken = ''
  let currentCorpusId = null
  let argFile = ''
  let originalPython
  let originalDbPath
  let tempDir = ''

  const createFakePython = () => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'dt-fake-python-'))
    argFile = path.join(tempDir, 'argv.json')
    const fakePython = path.join(tempDir, 'fake-python')

    const script = `#!/usr/bin/env node
const fs = require('node:fs')
const dumpFile = process.env.ARG_DUMP_FILE
if (dumpFile) {
  fs.writeFileSync(dumpFile, JSON.stringify(process.argv.slice(2)))
}
console.log(JSON.stringify({ results: [], source: 'fake-python' }))
`
    fs.writeFileSync(fakePython, script)
    fs.chmodSync(fakePython, 0o755)

    fs.writeFileSync(argFile, '[]', 'utf8')
    originalPython = process.env.RAG_FEEDER_PYTHON
    originalDbPath = process.env.RAG_FEEDER_DB_PATH
    process.env.RAG_FEEDER_PYTHON = fakePython
    process.env.RAG_FEEDER_DB_PATH = path.join(tempDir, 'recursion.db')
    process.env.ARG_DUMP_FILE = argFile
  }

  beforeAll(async () => {
    createFakePython()
    process.env.RAG_FEEDER_JWT_SECRET = 'recursion-secret'
    process.env.RAG_ADMIN_USER = 'recursion-admin'
    process.env.RAG_ADMIN_PASSWORD = 'recursion-password'
    const { createApp } = await import('../src/app.js')
    app = createApp({ broadcast: () => {} })

    const login = await request(app).post('/api/auth/login').send({
      username: process.env.RAG_ADMIN_USER,
      password: process.env.RAG_ADMIN_PASSWORD,
    })
    authToken = login.body.token
    currentCorpusId = login.body?.user?.last_corpus_id || null
  })

  afterAll(() => {
    if (originalPython === undefined) {
      delete process.env.RAG_FEEDER_PYTHON
    } else {
      process.env.RAG_FEEDER_PYTHON = originalPython
    }
    if (originalDbPath === undefined) {
      delete process.env.RAG_FEEDER_DB_PATH
    } else {
      process.env.RAG_FEEDER_DB_PATH = originalDbPath
    }
    delete process.env.ARG_DUMP_FILE
    delete process.env.RAG_FEEDER_JWT_SECRET
    delete process.env.RAG_ADMIN_USER
    delete process.env.RAG_ADMIN_PASSWORD
    if (tempDir && fs.existsSync(tempDir)) {
      try {
        fs.rmSync(tempDir, { recursive: true, force: true })
      } catch {
        // best effort cleanup
      }
    }
  })

  const doKeywordSearch = (body) =>
      request(app)
      .post('/api/keyword-search')
      .set('Authorization', `Bearer ${authToken}`)
      .send(body)

  const doIngestProcess = (body) =>
    request(app)
      .post('/api/ingest/process-marked')
      .set('Authorization', `Bearer ${authToken}`)
      .send(body)

  beforeEach(() => {
    if (argFile) {
      fs.writeFileSync(argFile, '[]', 'utf8')
    }
    const db = new Database(process.env.RAG_FEEDER_DB_PATH)
    try {
      db.prepare('DELETE FROM pipeline_jobs').run()
      db.prepare('DELETE FROM corpus_works').run()
      db.prepare('DELETE FROM works').run()
    } finally {
      db.close()
    }
  })

  const readLatestJob = (jobType) => {
    const db = new Database(process.env.RAG_FEEDER_DB_PATH)
    try {
      const row = db
        .prepare('SELECT parameters_json FROM pipeline_jobs WHERE job_type = ? ORDER BY id DESC LIMIT 1')
        .get(jobType)
      return row ? JSON.parse(row.parameters_json || '{}') : null
    } finally {
      db.close()
    }
  }

  const seedPendingWork = () => {
    const db = new Database(process.env.RAG_FEEDER_DB_PATH)
    try {
      const result = db
        .prepare(
          `INSERT INTO works (
            title,
            normalized_title,
            metadata_status,
            download_status
          ) VALUES (?, ?, 'pending', 'not_requested')`
        )
        .run('Seeded work', 'seeded work')
      db
        .prepare('INSERT OR IGNORE INTO corpus_works (corpus_id, work_id) VALUES (?, ?)')
        .run(currentCorpusId, Number(result.lastInsertRowid))
    } finally {
      db.close()
    }
  }

  test('passes separate downstream/upstream depths for keyword search', async () => {
    const res = await doKeywordSearch({
      query: 'institutional economics',
      relatedDepthDownstream: 4,
      relatedDepthUpstream: 3,
      includeDownstream: true,
      includeUpstream: true,
    })

    expect(res.status).toBe(200)
    const args = readArgFile(argFile)
    expect(args).toContain('--related-depth-downstream')
    expect(args).toContain('4')
    expect(args).toContain('--related-depth-upstream')
    expect(args).toContain('3')
    expect(args).toContain('--include-upstream')
  })

  test('maps keyword search to downstream-disabled mode', async () => {
    const res = await doKeywordSearch({
      query: 'institutional economics',
      relatedDepthDownstream: 4,
      includeDownstream: false,
      includeUpstream: false,
    })
    expect(res.status).toBe(200)
    const args = readArgFile(argFile)
    expect(args).toContain('--no-include-downstream')
    expect(args).not.toContain('--include-upstream')
  })

  test('upload process queues DB-driven background jobs', async () => {
    seedPendingWork()
    const res = await doIngestProcess({
      limit: 3,
      workers: 2,
      relatedDepthDownstream: 4,
      relatedDepthUpstream: 3,
      includeDownstream: true,
      includeUpstream: true,
    })
    expect(res.status).toBe(200)
    expect(Array.isArray(res.body.jobs)).toBe(true)
    expect(res.body.enrich_job_id).not.toBeNull()
    expect(res.body.tracking).toBeNull()
    expect(typeof res.body.message).toBe('string')
  })

  test('upload process returns DB-driven background message for upstream-only mode', async () => {
    seedPendingWork()
    const res = await doIngestProcess({
      limit: 2,
      workers: 2,
      includeDownstream: false,
      includeUpstream: true,
      relatedDepthUpstream: 2,
    })
    expect(res.status).toBe(200)
    const params = readLatestJob('enrich')
    expect(params?.expansion?.includeDownstream).toBe(false)
    expect(params?.expansion?.includeUpstream).toBe(true)
    expect(params?.expansion?.relatedDepthUpstream).toBe(2)
  })

  test('defaults upload process to DB-driven background mode', async () => {
    const res = await doIngestProcess({
      limit: 2,
      workers: 2,
    })
    expect(res.status).toBe(200)
    expect(Array.isArray(res.body.jobs)).toBe(true)
    expect(res.body.backlog).toBeTruthy()
    expect(typeof res.body.backlog.raw_pending).toBe('number')
  })
})
