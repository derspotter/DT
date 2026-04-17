import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import request from 'supertest'

const readArgFile = (filePath) => {
  const raw = fs.readFileSync(filePath, 'utf8').trim()
  if (!raw) return []
  return JSON.parse(raw)
}

describe('recursion arg propagation', () => {
  let app = null
  let authToken = ''
  let argFile = ''
  let originalPython
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
    process.env.RAG_FEEDER_PYTHON = fakePython
    process.env.ARG_DUMP_FILE = argFile
  }

  beforeAll(async () => {
    createFakePython()
    const { createApp } = await import('../src/app.js')
    app = createApp({ broadcast: () => {} })

    const login = await request(app).post('/api/auth/login').send({
      username: 'admin',
      password: process.env.ADMIN_PASSWORD || 'admin',
    })
    authToken = login.body.token
  })

  afterAll(() => {
    if (originalPython === undefined) {
      delete process.env.RAG_FEEDER_PYTHON
    } else {
      process.env.RAG_FEEDER_PYTHON = originalPython
    }
    delete process.env.ARG_DUMP_FILE
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
  })

  test('passes separate downstream/upstream depths for keyword search', async () => {
    const res = await doKeywordSearch({
      query: 'institutional economics',
      relatedDepthDownstream: 3,
      relatedDepthUpstream: 2,
      includeDownstream: true,
      includeUpstream: true,
    })

    expect(res.status).toBe(200)
    const args = readArgFile(argFile)
    expect(args).toContain('--related-depth-downstream')
    expect(args).toContain('3')
    expect(args).toContain('--related-depth-upstream')
    expect(args).toContain('2')
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

  test('upload process no longer creates pipeline jobs', async () => {
    const res = await doIngestProcess({
      limit: 3,
      workers: 2,
      relatedDepthDownstream: 4,
      relatedDepthUpstream: 3,
      includeDownstream: true,
      includeUpstream: true,
    })
    expect(res.status).toBe(200)
    expect(res.body.jobs).toEqual([])
    expect(res.body.enrich_job_id).toBeNull()
    expect(res.body.download_job_id).toBeNull()
    expect(res.body.tracking).toBeNull()
    expect(typeof res.body.message).toBe('string')
  })

  test('upload process returns DB-driven background message for upstream-only mode', async () => {
    const res = await doIngestProcess({
      limit: 2,
      workers: 2,
      includeDownstream: false,
      includeUpstream: true,
      relatedDepthUpstream: 2,
    })
    expect(res.status).toBe(200)
    expect(res.body.jobs).toEqual([])
    expect(res.body.message).toMatch(/background workers/i)
  })

  test('defaults upload process to DB-driven background mode', async () => {
    const res = await doIngestProcess({
      limit: 2,
      workers: 2,
    })
    expect(res.status).toBe(200)
    expect(res.body.jobs).toEqual([])
    expect(res.body.backlog).toBeTruthy()
    expect(typeof res.body.backlog.raw_pending).toBe('number')
  })
})
