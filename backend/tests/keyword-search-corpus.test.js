import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import Database from 'better-sqlite3'
import bcrypt from 'bcryptjs'
import request from 'supertest'

// The search preview and launch run in the corpus the user submitted from
// (FE-62), with the caller's access to that corpus checked; the preview passes
// an unknown count through as null, never 0 (FE-55).
describe('keyword search: submitted corpus and unknown counts', () => {
  let app
  let tmpDir = ''
  let dbPath = ''
  let argvLog = ''
  const password = 'kw-corpus-password'
  const tokens = {}
  const ids = {}
  const saved = {}

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
  // --corpus-id of every fake-python call, in order
  const corpusArgs = () =>
    fs.existsSync(argvLog)
      ? fs.readFileSync(argvLog, 'utf8').trim().split('\n').filter(Boolean).map((line) => {
        const argv = JSON.parse(line)
        const i = argv.indexOf('--corpus-id')
        return i >= 0 ? Number(argv[i + 1]) : null
      })
      : []

  beforeAll(async () => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'dt-kw-corpus-'))
    dbPath = path.join(tmpDir, 'kw.db')
    argvLog = path.join(tmpDir, 'argv.log')
    const fakePython = path.join(tmpDir, 'fake-python')
    fs.writeFileSync(fakePython, `#!/usr/bin/env node
const fs = require('fs')
fs.appendFileSync(${JSON.stringify(argvLog)}, JSON.stringify(process.argv.slice(2)) + '\\n')
if (process.argv.includes('--count-only')) {
  console.log(process.env.FAKE_COUNT_JSON || '{"count": 5}')
} else {
  console.log(JSON.stringify({ event: 'run_created', runId: 7001 }))
  setTimeout(() => process.exit(0), 20)
}
`)
    fs.chmodSync(fakePython, 0o755)
    setEnv('RAG_FEEDER_DB_PATH', dbPath)
    setEnv('RAG_FEEDER_PYTHON', fakePython)
    setEnv('RAG_FEEDER_JWT_SECRET', 'kw-corpus-secret')
    setEnv('RAG_ADMIN_USER', 'kw-corpus-admin')
    setEnv('RAG_ADMIN_PASSWORD', 'kw-corpus-admin-password')
    delete process.env.RAG_FEEDER_STUB

    const { createApp } = await import('../src/app.js')
    app = createApp({ broadcast() {} })
    withDb((db) => {
      const insertUser = (name) => Number(db.prepare(
        `INSERT INTO users (username, password_hash, email, account_status, password_set_at)
         VALUES (?, ?, ?, 'active', CURRENT_TIMESTAMP)`
      ).run(name, bcrypt.hashSync(password, 4), `${name}@example.test`).lastInsertRowid)
      const insertCorpus = (name, ownerId) => Number(
        db.prepare('INSERT INTO corpora (name, owner_user_id) VALUES (?, ?)').run(name, ownerId).lastInsertRowid
      )
      ids.editor = insertUser('kw-editor')
      ids.a = insertCorpus('A', ids.editor)
      ids.b = insertCorpus('B', ids.editor)
      ids.viewed = insertCorpus('Viewed', ids.editor)
      ids.foreign = insertCorpus('Foreign', ids.editor)
      const member = db.prepare('INSERT INTO user_corpora (user_id, corpus_id, role) VALUES (?, ?, ?)')
      member.run(ids.editor, ids.a, 'editor')
      member.run(ids.editor, ids.b, 'editor')
      member.run(ids.editor, ids.viewed, 'viewer')
      db.prepare('DELETE FROM user_corpora WHERE corpus_id = ?').run(ids.foreign)
      // B is the server's "current corpus": the switch was saved before the launch.
      db.prepare('UPDATE users SET last_corpus_id = ? WHERE id = ?').run(ids.b, ids.editor)
      db.exec(`CREATE TABLE IF NOT EXISTS search_runs (
        id INTEGER PRIMARY KEY, query TEXT, filters_json TEXT, status TEXT, fetched_count INTEGER,
        expected_count INTEGER, error TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, finished_at TIMESTAMP)`)
    })
    const login = await request(app).post('/api/auth/login').send({ username: 'kw-editor', password })
    expect(login.status).toBe(200)
    tokens.editor = login.body.token
  })

  afterAll(() => {
    for (const [key, value] of Object.entries(saved)) {
      if (value === undefined) delete process.env[key]
      else process.env[key] = value
    }
    delete process.env.FAKE_COUNT_JSON
    if (tmpDir) fs.rmSync(tmpDir, { recursive: true, force: true })
  })

  beforeEach(() => {
    fs.rmSync(argvLog, { force: true })
    delete process.env.FAKE_COUNT_JSON
  })

  test('the preview and the launch use the submitted corpus, not the current one (FE-62)', async () => {
    const preview = await request(app).post('/api/keyword-search/preview').set(as('editor'))
      .send({ query: 'labour', corpusId: ids.a })
    expect(preview.status).toBe(200)
    const launch = await request(app).post('/api/keyword-search').set(as('editor'))
      .send({ query: 'labour', corpusId: ids.a })
    expect(launch.status).toBe(202)
    expect(corpusArgs()).toEqual([ids.a, ids.a])
    // without a submitted corpus the current one applies, as before
    const plain = await request(app).post('/api/keyword-search/preview').set(as('editor')).send({ query: 'labour' })
    expect(plain.status).toBe(200)
    expect(corpusArgs()).toEqual([ids.a, ids.a, ids.b])
  })

  test('a submitted corpus the caller cannot write is refused before anything runs (FE-62)', async () => {
    for (const route of ['/api/keyword-search/preview', '/api/keyword-search']) {
      for (const corpusId of [ids.viewed, ids.foreign]) {
        const res = await request(app).post(route).set(as('editor')).send({ query: 'labour', corpusId })
        expect(res.status).toBe(403)
      }
      const bad = await request(app).post(route).set(as('editor')).send({ query: 'labour', corpusId: 'x' })
      expect(bad.status).toBe(400)
    }
    expect(corpusArgs()).toEqual([])
  })

  test('the preview passes an unknown count as null and a genuine 0 as 0 (FE-55)', async () => {
    const preview = () => request(app).post('/api/keyword-search/preview').set(as('editor'))
      .send({ query: 'labour', corpusId: ids.a })
    for (const [json, count] of [
      ['{"count": null}', null],
      ['{}', null],
      ['{"count": "many"}', null],
      ['{"count": -3}', null],
      ['{"count": 0}', 0],
      ['{"count": 12}', 12],
    ]) {
      process.env.FAKE_COUNT_JSON = json
      const res = await preview()
      expect(res.status).toBe(200)
      expect(res.body.count).toBe(count)
    }
  })
})
