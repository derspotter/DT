import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import zlib from 'node:zlib'
import Database from 'better-sqlite3'
import bcrypt from 'bcryptjs'
import request from 'supertest'
import { jest } from '@jest/globals'

// app.js reads RAG_FEEDER_DB_PATH and RAG_FEEDER_PYTHON at load, so it is
// imported once, in beforeAll, after the environment is set (as
// graph-access.test.js does).
const dbManagerSource = fs.readFileSync(
  new URL('../../dl_lit_project/dl_lit/db_manager.py', import.meta.url),
  'utf8'
)
const pythonIndexDdl = (name) => {
  const match = dbManagerSource.match(new RegExp(`"(CREATE UNIQUE INDEX IF NOT EXISTS ${name} [^"]*)"`))
  if (!match) throw new Error(`${name} not found in db_manager.py`)
  return match[1]
}
const collapse = (text) => String(text).replace(/\s+/g, ' ').trim()

const NODE_COLUMNS = ['id', 'title', 'doi', 'openalex_id']

describe('graph read path', () => {
  let app
  let appModule
  let tmpDir = ''
  let dbPath = ''
  let snapshotDir = ''
  const adminUsername = 'graph-read-admin'
  const adminPassword = 'graph-read-admin-password'
  const fx = {}

  const withDb = (fn) => {
    const db = new Database(dbPath)
    try {
      return fn(db)
    } finally {
      db.close()
    }
  }

  let unitCounter = 0
  const openUnitDb = () => {
    unitCounter += 1
    return new Database(path.join(tmpDir, `unit-${unitCounter}.db`))
  }

  const columnsOf = (db) => new Set(db.prepare('PRAGMA table_info(works)').all().map((row) => row.name))
  const selectListFor = (columns) =>
    [...NODE_COLUMNS, 'normalized_doi'].filter((c) => columns.has(c)).map((c) => `w.${c}`).join(', ')
  const find = (db, nodeId) => {
    const columns = columnsOf(db)
    return appModule.findGraphNodeWork(db, nodeId, selectListFor(columns), columns)
  }

  const insertWork = (db, { id, title = `Work ${id}`, openalex = null, doi = null, normalizedDoi = null }) =>
    db
      .prepare('INSERT INTO works (id, title, openalex_id, doi, normalized_doi) VALUES (?, ?, ?, ?, ?)')
      .run(id, title, openalex, doi, normalizedDoi)

  const login = async (username, password) => {
    const res = await request(app).post('/api/auth/login').send({ username, password })
    expect(res.status).toBe(200)
    return res.body.token
  }

  beforeAll(async () => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'dt-graph-read-path-'))
    dbPath = path.join(tmpDir, 'graph-read-path.db')
    snapshotDir = path.join(tmpDir, 'snapshots')
    fx.fakePython = path.join(tmpDir, 'fake-python.mjs')
    fs.writeFileSync(
      fx.fakePython,
      `#!/usr/bin/env node
import fs from 'node:fs'
import path from 'node:path'
import crypto from 'node:crypto'
const args = process.argv.slice(2)
const dir = args[args.indexOf('--snapshot-dir') + 1]
fs.mkdirSync(dir, { recursive: true })
fs.writeFileSync(path.join(dir, 'nodes.bin'), crypto.randomBytes(2 * 1024 * 1024))
fs.writeFileSync(path.join(dir, 'clusters.json'), JSON.stringify([{ id: 0, label: 'cluster zero' }]))
fs.writeFileSync(path.join(dir, 'nodes_meta.json'), JSON.stringify([{ id: 'W1' }]))
fs.writeFileSync(path.join(dir, 'manifest.json'), JSON.stringify({ schema_version: 2, generated_at: 'fake-' + Date.now(), files: {} }))
process.stdout.write('{}')
`
    )
    fs.chmodSync(fx.fakePython, 0o755)

    process.env.RAG_FEEDER_DB_PATH = dbPath
    process.env.RAG_FEEDER_PYTHON = fx.fakePython
    process.env.RAG_FEEDER_GRAPH_3D_SNAPSHOT_DIR = snapshotDir
    process.env.RAG_FEEDER_JWT_SECRET = 'graph-read-path-secret'
    process.env.RAG_ADMIN_USER = adminUsername
    process.env.RAG_ADMIN_PASSWORD = adminPassword

    appModule = await import('../src/app.js')
    app = appModule.createApp({ broadcast: () => {} })

    withDb((db) => {
      const insertUser = (username, password) =>
        Number(
          db
            .prepare(
              `INSERT INTO users (username, password_hash, email, account_status, password_set_at)
               VALUES (?, ?, ?, 'active', CURRENT_TIMESTAMP)`
            )
            .run(username, bcrypt.hashSync(password, 10), `${username}@example.test`).lastInsertRowid
        )
      const viewer = insertUser('graph-read-viewer', 'viewer-password')
      const insertCorpus = (name, ownerId) =>
        Number(db.prepare('INSERT INTO corpora (name, owner_user_id) VALUES (?, ?)').run(name, ownerId).lastInsertRowid)
      fx.corpusA = insertCorpus('Read path A', viewer)
      fx.corpusB = insertCorpus('Read path B', viewer)
      db.prepare("INSERT INTO user_corpora (user_id, corpus_id, role) VALUES (?, ?, 'viewer')").run(viewer, fx.corpusA)
      db.prepare('UPDATE users SET last_corpus_id = ? WHERE id = ?').run(fx.corpusA, viewer)
      db.prepare(
        `INSERT INTO works (id, title, openalex_id, doi, normalized_doi, metadata_status, download_status)
         VALUES (12, 'Unreadable work', 'W77', '10.1/abc', '10.1/abc', 'matched', 'downloaded')`
      ).run()
      db.prepare('INSERT INTO corpus_works (corpus_id, work_id) VALUES (?, 12)').run(fx.corpusB)
    })

    fx.adminToken = await login(adminUsername, adminPassword)
    fx.viewerToken = await login('graph-read-viewer', 'viewer-password')
  }, 30_000)

  afterAll(() => {
    for (const name of [
      'RAG_FEEDER_DB_PATH',
      'RAG_FEEDER_PYTHON',
      'RAG_FEEDER_GRAPH_3D_SNAPSHOT_DIR',
      'RAG_FEEDER_JWT_SECRET',
      'RAG_ADMIN_USER',
      'RAG_ADMIN_PASSWORD',
    ]) {
      delete process.env[name]
    }
    if (tmpDir && fs.existsSync(tmpDir)) fs.rmSync(tmpDir, { recursive: true, force: true })
  })

  afterEach(() => {
    jest.restoreAllMocks()
  })

  const get = (url, token) => request(app).get(url).set('Authorization', `Bearer ${token}`)

  test('findGraphNodeWork never scans works on the schema ensureAuthSchema builds', () => {
    const db = openUnitDb()
    try {
      appModule.ensureAuthSchema(db)
      for (const name of ['idx_works_openalex_id', 'idx_works_normalized_doi']) {
        const stored = db.prepare("SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?").get(name)?.sql
        expect(collapse(stored)).toBe(collapse(pythonIndexDdl(name).replace(' IF NOT EXISTS', '')))
      }
      insertWork(db, { id: 12, openalex: 'W77', doi: '10.1/abc', normalizedDoi: '10.1/ABC' })

      const recorded = []
      const orig = db.prepare.bind(db)
      jest.spyOn(db, 'prepare').mockImplementation((sql) => {
        const stmt = orig(sql)
        const entry = { sql, args: [] }
        recorded.push(entry)
        for (const method of ['get', 'all']) {
          const fn = stmt[method].bind(stmt)
          stmt[method] = (...args) => {
            entry.args = args
            return fn(...args)
          }
        }
        return stmt
      })
      const columns = columnsOf(db)
      const selectList = selectListFor(columns)
      for (const nodeId of ['12', 'work:12', 'W77', '10.1/ABC']) {
        expect(appModule.findGraphNodeWork(db, nodeId, selectList, columns)?.id).toBe(12)
      }
      jest.restoreAllMocks()
      expect(recorded.length).toBeGreaterThan(0)
      for (const { sql, args } of recorded) {
        const plan = db
          .prepare(`EXPLAIN QUERY PLAN ${sql}`)
          .all(...args)
          .map((row) => row.detail)
          .join(' | ')
        expect([sql, plan]).not.toEqual([sql, expect.stringMatching(/SCAN w\b/)])
        expect([sql, plan]).not.toEqual([sql, expect.stringMatching(/SCAN works/)])
      }
    } finally {
      db.close()
    }
  })

  test('ensureAuthSchema is idempotent on a DatabaseManager-shaped schema with both indexes', () => {
    const db = openUnitDb()
    try {
      db.exec(`CREATE TABLE works (
        id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT NOT NULL, year INTEGER, doi TEXT,
        normalized_doi TEXT, openalex_id TEXT, metadata_status TEXT, download_status TEXT
      )`)
      db.exec(pythonIndexDdl('idx_works_openalex_id'))
      db.exec(pythonIndexDdl('idx_works_normalized_doi'))
      appModule.ensureAuthSchema(db)
      appModule.ensureAuthSchema(db)
      for (const name of ['idx_works_openalex_id', 'idx_works_normalized_doi']) {
        expect(db.prepare("SELECT COUNT(*) AS n FROM sqlite_master WHERE type = 'index' AND name = ?").get(name).n).toBe(1)
      }
    } finally {
      db.close()
    }
  })

  test('a Node-only schema with duplicate identifiers boots, logs the error and still resolves by scan', async () => {
    const db = openUnitDb()
    try {
      appModule.ensureAuthSchema(db)
      db.exec('DROP INDEX IF EXISTS idx_works_openalex_id; DROP INDEX IF EXISTS idx_works_normalized_doi;')
      insertWork(db, { id: 12, title: 'Plain' })
      insertWork(db, { id: 30, openalex: 'W77' })
      insertWork(db, { id: 31, openalex: 'W77' })
      insertWork(db, { id: 32, doi: '10.1/DUP', normalizedDoi: '10.1/dup' })
      insertWork(db, { id: 33, doi: '10.1/DUP', normalizedDoi: '10.1/dup' })
      const errorSpy = jest.spyOn(console, 'error').mockImplementation(() => {})
      expect(() => appModule.ensureAuthSchema(db)).not.toThrow()
      const messages = errorSpy.mock.calls.map((call) => call.map(String).join(' '))
      expect(messages.some((m) => m.includes('idx_works_openalex_id') && m.includes('works.openalex_id'))).toBe(true)
      expect(messages.some((m) => m.includes('idx_works_normalized_doi') && m.includes('works.normalized_doi'))).toBe(true)
      errorSpy.mockRestore()

      const byOpenalex = find(db, 'W77')
      expect(byOpenalex?.openalex_id).toBe('W77')
      expect([30, 31]).toContain(byOpenalex.id)
      expect([32, 33]).toContain(find(db, '10.1/dup')?.id)
      expect(find(db, '12')?.id).toBe(12)
      expect(find(db, 'W404')).toBeUndefined()
    } finally {
      db.close()
    }

    // The same degraded schema in the file's own DB, through the route.
    withDb((fileDb) => {
      fileDb.exec('DROP INDEX IF EXISTS idx_works_openalex_id; DROP INDEX IF EXISTS idx_works_normalized_doi;')
      insertWork(fileDb, { id: 30, openalex: 'W77' })
      insertWork(fileDb, { id: 31, openalex: 'W77' })
    })
    const admin = await get('/api/graph/3d/node/W77', fx.adminToken)
    expect(admin.status).toBe(200)
    expect(admin.body.openalex_id).toBe('W77')
    const viewer = await get('/api/graph/3d/node/W77', fx.viewerToken)
    expect(viewer.status).toBe(404)
  })

  test('ensureAuthSchema rethrows any other index DDL error', () => {
    const db = openUnitDb()
    try {
      const exec = db.exec.bind(db)
      jest.spyOn(db, 'exec').mockImplementation((sql) => {
        if (String(sql).includes('idx_works_openalex_id')) {
          const error = new Error('simulated DDL failure')
          error.code = 'SQLITE_ERROR'
          throw error
        }
        return exec(sql)
      })
      expect(() => appModule.ensureAuthSchema(db)).toThrow('simulated DDL failure')
    } finally {
      jest.restoreAllMocks()
      db.close()
    }
  })

  test('findGraphNodeWork resolves id, work:<id>, openalex id and normalized DOI; raw-only doi and other numeric spellings no longer match', () => {
    const db = openUnitDb()
    try {
      appModule.ensureAuthSchema(db)
      insertWork(db, { id: 12, title: 'Twelve' })
      insertWork(db, { id: 20, openalex: 'W77' })
      insertWork(db, { id: 21, doi: '10.1/abc', normalizedDoi: '10.1/ABC' })
      insertWork(db, { id: 22, doi: 'RAW', normalizedDoi: null })
      insertWork(db, { id: 23, openalex: '', normalizedDoi: '' })
      expect(find(db, '12')?.id).toBe(12)
      expect(find(db, 'work:12')?.id).toBe(12)
      expect(find(db, 'W77')?.openalex_id).toBe('W77')
      expect(find(db, '10.1/ABC')?.id).toBe(21)
      expect(find(db, 'RAW')).toBeUndefined()
      expect(find(db, '')).toBeUndefined()
      for (const spelling of ['12.0', '1e1', '0x0c']) {
        expect([spelling, find(db, spelling)]).toEqual([spelling, undefined])
      }
    } finally {
      db.close()
    }
  })

  test('the node route answers 404 for an unreadable work in every accepted form', async () => {
    for (const nodeId of ['12', 'work:12', 'W77', '10.1%2Fabc']) {
      const viewer = await get(`/api/graph/3d/node/${nodeId}`, fx.viewerToken)
      expect([nodeId, viewer.status]).toEqual([nodeId, 404])
      const admin = await get(`/api/graph/3d/node/${nodeId}`, fx.adminToken)
      expect([nodeId, admin.status]).toEqual([nodeId, 200])
    }
  })

  test('the snapshot build streams its gzip: no sync gzip and no sync payload read', async () => {
    const gz = jest.spyOn(zlib, 'gzipSync')
    const rs = jest.spyOn(fs, 'createReadStream')
    const rf = jest.spyOn(fs, 'readFileSync')
    const res = await get('/api/graph/3d/snapshot', fx.adminToken)
    expect(res.status).toBe(200)
    expect(gz).not.toHaveBeenCalled()
    const streamed = rs.mock.calls.map(([arg]) => path.basename(String(arg))).sort()
    expect(streamed).toEqual(['clusters.json', 'nodes.bin', 'nodes_meta.json'])
    const payloadNames = new Set(['nodes.bin', 'clusters.json', 'nodes_meta.json'])
    expect(rf.mock.calls.some(([arg]) => payloadNames.has(path.basename(String(arg))))).toBe(false)
    jest.restoreAllMocks()

    const dir = path.join(snapshotDir, res.body.snapshot_key)
    const written = fs.readFileSync(path.join(dir, 'nodes.bin'))
    expect(written.length).toBe(2 * 1024 * 1024)
    // Buffer.equals: a deep toEqual over 2 MB takes seconds.
    expect(zlib.gunzipSync(fs.readFileSync(path.join(dir, 'nodes.bin.gz'))).equals(written)).toBe(true)
    expect(fs.existsSync(path.join(dir, 'edges.bin.gz'))).toBe(false)
  })

  test('the cluster detail route is gone', async () => {
    // Its own snapshot, so this test does not depend on the build test.
    const key = 'e5e5e5e5e5e5e5e5e5e5'
    const dir = path.join(snapshotDir, key)
    fs.mkdirSync(dir, { recursive: true })
    fs.writeFileSync(path.join(dir, 'manifest.json'), JSON.stringify({ schema_version: 2, generated_at: 'cluster-test', files: {} }))
    fs.writeFileSync(path.join(dir, 'clusters.json'), JSON.stringify([{ id: 0, label: 'cluster zero' }]))
    const file = await get(`/api/graph/3d/snapshot/${key}/clusters.json`, fx.adminToken)
    expect(file.status).toBe(200)
    const res = await get(`/api/graph/3d/cluster/${key}/0`, fx.adminToken)
    expect(res.status).toBe(404)
  })
  describe('versioned snapshot files', () => {
    const key = 'f6f6f6f6f6f6f6f6f6f6'
    const bytes = Buffer.from([1, 2, 3, 4])
    const fileUrl = `/api/graph/3d/snapshot/${key}/nodes.bin`

    beforeAll(() => {
      const dir = path.join(snapshotDir, key)
      fs.mkdirSync(dir, { recursive: true })
      fs.writeFileSync(path.join(dir, 'manifest.json'), JSON.stringify({ schema_version: 2, generated_at: 'A', files: {} }))
      fs.writeFileSync(path.join(dir, 'nodes.bin'), bytes)
      fs.writeFileSync(path.join(dir, 'nodes.bin.gz'), zlib.gzipSync(bytes))
    })

    const binary = (req) =>
      req.buffer(true).parse((res, done) => {
        const chunks = []
        res.on('data', (chunk) => chunks.push(chunk))
        res.on('end', () => done(null, Buffer.concat(chunks)))
      })

    test('a versioned file request is checked against the live build', async () => {
      const current = await binary(get(`${fileUrl}?v=A`, fx.adminToken))
      expect(current.status).toBe(200)
      // The gzip branch served it (supertest decodes the body).
      expect(current.headers['content-encoding']).toBe('gzip')
      expect([...current.body]).toEqual([1, 2, 3, 4])

      const stale = await get(`${fileUrl}?v=B`, fx.adminToken)
      expect(stale.status).toBe(409)
      expect(stale.body).toEqual({ error: 'The graph snapshot was rebuilt; reload the graph.', code: 'snapshot_rebuilt' })
      expect(stale.headers['cache-control']).toBe('no-store')

      const unversioned = await binary(get(fileUrl, fx.adminToken))
      expect(unversioned.status).toBe(200)
      expect([...unversioned.body]).toEqual([1, 2, 3, 4])
    })

    test('a matching version keeps sendFile for a client without gzip, ranges included', async () => {
      const whole = await binary(get(`${fileUrl}?v=A`, fx.adminToken).set('Accept-Encoding', 'identity'))
      expect(whole.status).toBe(200)
      expect(whole.headers['content-type']).toMatch(/application\/octet-stream/)
      expect(whole.headers['content-length']).toBe('4')
      expect(whole.headers['content-encoding']).toBeUndefined()
      expect([...whole.body]).toEqual([1, 2, 3, 4])

      const range = await binary(
        get(`${fileUrl}?v=A`, fx.adminToken).set('Accept-Encoding', 'identity').set('Range', 'bytes=0-1')
      )
      expect(range.status).toBe(206)
      expect(range.body.length).toBe(2)
      expect([...range.body]).toEqual([1, 2])
    })

    // Only a missing manifest means "rebuilt" (409); an unreadable one is a
    // server error, logged, not a reason to reload the graph forever.
    test('an unreadable manifest answers 500 and is logged; a missing one still answers 409', async () => {
      const broken = 'a7a7a7a7a7a7a7a7a7a7'
      const brokenDir = path.join(snapshotDir, broken)
      fs.mkdirSync(brokenDir, { recursive: true })
      fs.writeFileSync(path.join(brokenDir, 'manifest.json'), '{"generated_at": "A", trunc')
      fs.writeFileSync(path.join(brokenDir, 'nodes.bin'), bytes)
      const errors = jest.spyOn(console, 'error').mockImplementation(() => {})
      try {
        const res = await get(`/api/graph/3d/snapshot/${broken}/nodes.bin?v=A`, fx.adminToken)
        expect(res.status).toBe(500)
        expect(res.body.code).not.toBe('snapshot_rebuilt')
        expect(res.headers['cache-control']).toBe('no-store')
        expect(errors.mock.calls.some((args) => String(args[0]).startsWith('[graph3d] could not read the manifest version'))).toBe(true)
      } finally {
        errors.mockRestore()
      }

      const missing = 'b8b8b8b8b8b8b8b8b8b8'
      fs.mkdirSync(path.join(snapshotDir, missing), { recursive: true })
      fs.writeFileSync(path.join(snapshotDir, missing, 'nodes.bin'), bytes)
      const gone = await get(`/api/graph/3d/snapshot/${missing}/nodes.bin?v=A`, fx.adminToken)
      expect(gone.status).toBe(409)
      expect(gone.body.code).toBe('snapshot_rebuilt')
    })
  })
})
