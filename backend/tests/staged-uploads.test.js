import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import Database from 'better-sqlite3'
import bcrypt from 'bcryptjs'
import request from 'supertest'
import { jest } from '@jest/globals'

// A staged seed file is usable only by the user who uploaded it (the admin
// is exempt), and the import and the extraction run in the corpus the user
// submitted, with the write check on that corpus (FE-65b, FE-68, spec D4).
describe('staged uploads: uploader binding and the submitted corpus', () => {
  let app
  let tmpDir = ''
  let dbPath = ''
  let argvLog = ''
  const lines = []
  const password = 'staged-password'
  const tokens = {}
  const ids = {}
  const saved = {}
  const silenced = []

  const IMPORT_MISSING = 'Uploaded file not found. Please re-upload.'
  const EXTRACT_MISSING = 'Uploaded PDF not found (and no extracted pages exist). Please re-upload the PDF.'
  const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/

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
  // --corpus-id of every fake-python call, in order (null for a child without one)
  const corpusArgs = () =>
    fs.existsSync(argvLog)
      ? fs.readFileSync(argvLog, 'utf8').trim().split('\n').filter(Boolean).map((line) => {
        const argv = JSON.parse(line)
        const i = argv.indexOf('--corpus-id')
        return i >= 0 ? Number(argv[i + 1]) : null
      })
      : []

  const waitFor = async (predicate, what) => {
    const deadline = Date.now() + 10_000
    while (!predicate()) {
      if (Date.now() > deadline) throw new Error(`${what} did not happen; lines: ${JSON.stringify(lines)}`)
      await new Promise((resolve) => setTimeout(resolve, 25))
    }
  }
  const doneLines = (filename) => lines.filter((l) => l.startsWith('[extract-status]') && l.includes(`[file=${filename}] done`)).length

  const upload = async (who, name, content) => {
    const res = await request(app).post('/api/process_pdf').set(as(who)).attach('file', Buffer.from(content), name)
    expect(res.status).toBe(200)
    return res.body.filename
  }
  const importSeed = (who, body) => request(app).post('/api/ingest/import-seed').set(as(who)).send(body)
  // Any 202 (expected or not) drains: the route's terminal line for that file,
  // then a timer round, so its finally (cleanup, queue release) has run before
  // the test asserts, resets the argv log or removes the temp dir.
  const extract = async (who, filename, body = {}) => {
    const before = doneLines(filename)
    const res = await request(app).post(`/api/extract-bibliography/${encodeURIComponent(filename)}`).set(as(who)).send(body)
    if (res.status === 202) {
      await waitFor(() => doneLines(filename) > before, `extraction of ${filename}`)
      await new Promise((resolve) => setTimeout(resolve, 25))
    }
    return res
  }

  beforeAll(async () => {
    for (const method of ['log', 'warn', 'error']) silenced.push(jest.spyOn(console, method).mockImplementation(() => {}))
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'dt-staged-uploads-'))
    dbPath = path.join(tmpDir, 'staged.db')
    argvLog = path.join(tmpDir, 'argv.log')
    fs.mkdirSync(path.join(tmpDir, 'uploads'))
    const fakePython = path.join(tmpDir, 'fake-python')
    // The import answers a summary; every extraction child fails (and the page
    // counter prints no usable count, so the shared upload path is not used).
    fs.writeFileSync(fakePython, `#!/usr/bin/env node
const fs = require('fs')
const path = require('path')
fs.appendFileSync(${JSON.stringify(argvLog)}, JSON.stringify(process.argv.slice(2)) + '\\n')
const script = path.basename(process.argv[2] || '')
if (script === 'ingest_import_seed.py') {
  console.log(JSON.stringify({ stopped: false, error: null, total: 0, processed: 0, added: 0, skipped: 0, corpus_linked: 0 }))
  process.exit(0)
}
process.exit(1)
`)
    fs.chmodSync(fakePython, 0o755)
    setEnv('RAG_FEEDER_DB_PATH', dbPath)
    setEnv('RAG_FEEDER_PYTHON', fakePython)
    setEnv('RAG_FEEDER_UPLOADS_DIR', path.join(tmpDir, 'uploads'))
    setEnv('RAG_FEEDER_ARTIFACTS_DIR', path.join(tmpDir, 'artifacts'))
    setEnv('RAG_FEEDER_JWT_SECRET', 'staged-secret')
    setEnv('RAG_ADMIN_USER', 'staged-admin')
    setEnv('RAG_ADMIN_PASSWORD', 'staged-admin-password')
    delete process.env.RAG_FEEDER_STUB

    const { createApp } = await import('../src/app.js')
    app = createApp({ broadcast: (line) => lines.push(line) })
    withDb((db) => {
      const insertUser = (name) => Number(db.prepare(
        `INSERT INTO users (username, password_hash, email, account_status, password_set_at)
         VALUES (?, ?, ?, 'active', CURRENT_TIMESTAMP)`
      ).run(name, bcrypt.hashSync(password, 4), `${name}@example.test`).lastInsertRowid)
      const insertCorpus = (name, ownerId) => Number(
        db.prepare('INSERT INTO corpora (name, owner_user_id) VALUES (?, ?)').run(name, ownerId).lastInsertRowid
      )
      ids.alice = insertUser('staged-alice')
      ids.bob = insertUser('staged-bob')
      ids.carol = insertUser('staged-carol')
      ids.a = insertCorpus('A', ids.carol)
      ids.b = insertCorpus('B', ids.bob)
      ids.viewed = insertCorpus('Viewed', ids.carol)
      ids.foreign = insertCorpus('Foreign', ids.carol)
      const member = db.prepare('INSERT INTO user_corpora (user_id, corpus_id, role) VALUES (?, ?, ?)')
      member.run(ids.carol, ids.a, 'owner')
      member.run(ids.carol, ids.viewed, 'owner')
      member.run(ids.carol, ids.foreign, 'owner')
      member.run(ids.bob, ids.b, 'owner')
      member.run(ids.bob, ids.a, 'editor')
      member.run(ids.alice, ids.a, 'editor')
      member.run(ids.alice, ids.viewed, 'viewer')
      // B is granted before it is saved as alice's corpus: ensureLastCorpus
      // replaces a saved corpus she cannot use.
      member.run(ids.alice, ids.b, 'editor')
      db.prepare('UPDATE users SET last_corpus_id = ? WHERE id = ?').run(ids.b, ids.alice)
      db.prepare('UPDATE users SET last_corpus_id = ? WHERE id = ?').run(ids.a, ids.bob)
    })
    for (const [who, username, pw] of [
      ['alice', 'staged-alice', password],
      ['bob', 'staged-bob', password],
      ['admin', 'staged-admin', 'staged-admin-password'],
    ]) {
      const login = await request(app).post('/api/auth/login').send({ username, password: pw })
      expect(login.status).toBe(200)
      tokens[who] = login.body.token
    }
    expect(withDb((db) => db.prepare('SELECT last_corpus_id FROM users WHERE id = ?').get(ids.alice).last_corpus_id)).toBe(ids.b)
  })

  afterAll(() => {
    for (const spy of silenced) spy.mockRestore()
    for (const [key, value] of Object.entries(saved)) {
      if (value === undefined) delete process.env[key]
      else process.env[key] = value
    }
    if (tmpDir) fs.rmSync(tmpDir, { recursive: true, force: true })
  })

  beforeEach(() => {
    fs.rmSync(argvLog, { force: true })
  })

  test("another user's staged file is refused even into a writable corpus", async () => {
    const seed = await upload('alice', 'seed.json', '[]')
    const pdf = await upload('alice', 'paper.pdf', '%PDF-1.4\n%%EOF\n')
    const warnSpy = console.warn
    warnSpy.mockClear()
    const bobImport = await importSeed('bob', { filename: seed, kind: 'json', corpusId: ids.b })
    expect([bobImport.status, bobImport.body]).toEqual([404, { error: IMPORT_MISSING }])
    const bobExtract = await extract('bob', pdf, { corpusId: ids.b })
    expect([bobExtract.status, bobExtract.body]).toEqual([404, { error: EXTRACT_MISSING }])
    expect(corpusArgs()).toEqual([])
    // The 404 does not tell bob; the log tells the operator, once per refusal.
    const refusals = warnSpy.mock.calls.filter(([tag]) => String(tag).endsWith('staged upload not owned:'))
    expect(refusals).toEqual([
      ['[/api/ingest/import-seed] staged upload not owned:', ids.bob, seed],
      ['[/api/extract-bibliography] staged upload not owned:', ids.bob, pdf],
    ])

    const own = await importSeed('alice', { filename: seed, kind: 'json' })
    expect(own.status).toBe(200)
    const admin = await importSeed('admin', { filename: seed, kind: 'json', corpusId: ids.a })
    expect(admin.status).toBe(200)
    expect(corpusArgs()).toEqual([ids.b, ids.a])
  })

  test('input errors stay 400 before any ownership check', async () => {
    const missing = await importSeed('bob', { filename: 'nobody.json' })
    expect(missing.status).toBe(400)
    for (const filename of ['a/b.json', 'a..b.json']) {
      const res = await importSeed('bob', { filename, kind: 'json' })
      expect(res.status).toBe(400)
    }
    const slash = await extract('bob', 'a\\b.pdf')
    expect(slash.status).toBe(400)
    expect(corpusArgs()).toEqual([])

    const seed = await upload('alice', 'seed.json', '[]')
    const padded = await importSeed('alice', { filename: `  ${seed}  `, kind: 'json' })
    expect(padded.status).toBe(200)
  })

  test('the import runs in the submitted corpus, with its write check', async () => {
    const seed = await upload('alice', 'seed.json', '[]')
    expect((await importSeed('alice', { filename: seed, kind: 'json', corpusId: ids.a })).status).toBe(200)
    expect(corpusArgs()).toEqual([ids.a])
    expect((await importSeed('alice', { filename: seed, kind: 'json' })).status).toBe(200)
    expect(corpusArgs()).toEqual([ids.a, ids.b])
    for (const corpusId of [ids.viewed, ids.foreign]) {
      expect((await importSeed('alice', { filename: seed, kind: 'json', corpusId })).status).toBe(403)
    }
    const bad = await importSeed('alice', { filename: seed, kind: 'json', corpusId: 'x' })
    expect([bad.status, bad.body]).toEqual([400, { error: 'Invalid corpusId' }])
    expect(corpusArgs()).toEqual([ids.a, ids.b])
  })

  test('the extraction starts under the submitted corpus, with its write check', async () => {
    const pdf = await upload('alice', 'paper.pdf', '%PDF-1.4\n%%EOF\n')
    const res = await extract('alice', pdf, { corpusId: ids.a })
    expect(res.status).toBe(202)
    expect(lines.some((l) => l.includes(`[extract-status][corpus=${ids.a}]`) && l.includes(`[file=${pdf}]`) && l.includes('started'))).toBe(true)
    expect(lines.some((l) => l.includes(`[extract-status][corpus=${ids.b}]`) && l.includes(`[file=${pdf}]`))).toBe(false)
    fs.rmSync(argvLog, { force: true })
    for (const corpusId of [ids.viewed, ids.foreign]) {
      expect((await extract('alice', pdf, { corpusId })).status).toBe(403)
    }
    expect(corpusArgs()).toEqual([])
  })

  test('a failing ownership lookup answers the JSON fallback', async () => {
    const seed = await upload('alice', 'seed.json', '[]')
    const pdf = await upload('alice', 'paper.pdf', '%PDF-1.4\n%%EOF\n')
    const db = new Database(dbPath)
    const exists = Boolean(db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'staged_uploads'").get())
    if (exists) db.exec('ALTER TABLE staged_uploads RENAME TO staged_uploads_x')
    try {
      const imported = await importSeed('alice', { filename: seed, kind: 'json' })
      expect(imported.status).toBe(500)
      expect(imported.headers['content-type']).toMatch(/application\/json/)
      expect(imported.body).toEqual({ error: 'Failed to import seed file' })
      const extracted = await extract('alice', pdf)
      expect(extracted.status).toBe(500)
      expect(extracted.headers['content-type']).toMatch(/application\/json/)
      expect(extracted.body).toEqual({ error: 'Failed to start bibliography extraction' })
      expect(corpusArgs()).toEqual([])
    } finally {
      if (exists) db.exec('ALTER TABLE staged_uploads_x RENAME TO staged_uploads')
      db.close()
    }
  })

  test('staged names carry a UUID', async () => {
    const seed = await upload('alice', 'seed.json', '[]')
    expect(seed).toMatch(/^[0-9a-f-]+-seed\.json$/)
    expect(seed.slice(0, 36)).toMatch(UUID)
    expect(seed[36]).toBe('-')
  })

  test('a long original name is cut to 150 UTF-8 bytes, keeping its extension and whole characters', async () => {
    const ascii = await upload('alice', `${'x'.repeat(300)}.pdf`, '%PDF-1.4\n%%EOF\n')
    expect(ascii.slice(0, 36)).toMatch(UUID)
    const asciiRest = ascii.slice(37)
    expect(Buffer.byteLength(asciiRest)).toBe(150)
    expect(asciiRest).toBe(`${'x'.repeat(146)}.pdf`)
    // The page extractor's derived names still fit a 255-byte path component.
    const derived = `${path.basename(ascii, '.pdf')}_refs_physical_p10000.pdf`
    expect(Buffer.byteLength(derived)).toBeLessThanOrEqual(255)

    const wide = await upload('alice', `${'ä'.repeat(150)}.pdf`, '%PDF-1.4\n%%EOF\n')
    expect(wide.slice(0, 36)).toMatch(UUID)
    const wideRest = wide.slice(37)
    expect(Buffer.byteLength(wideRest)).toBeLessThanOrEqual(150)
    expect(Buffer.byteLength(wideRest)).toBeGreaterThan(140)
    expect(wideRest.endsWith('.pdf')).toBe(true)
    expect(wideRest).not.toContain('\uFFFD')
    expect(Buffer.from(wideRest, 'utf8').toString('utf8')).toBe(wideRest)
    expect(fs.existsSync(path.join(tmpDir, 'uploads', wide))).toBe(true)

    // An extension that exactly fits the cap is kept whole; one longer is cut.
    const fitting = `.${'e'.repeat(149)}`
    const exact = await upload('alice', `a${fitting}`, '%PDF-1.4\n%%EOF\n')
    expect(exact.slice(37)).toBe(fitting)
    const longer = await upload('alice', `a.${'e'.repeat(200)}`, '%PDF-1.4\n%%EOF\n')
    expect(longer.slice(37)).toBe(`a.${'e'.repeat(148)}`)

    // A cut that ends the stem on a dot drops the dot, so the import's path
    // check (no '..') still accepts the staged name.
    const dotted = await upload('alice', `${'x'.repeat(144)}.v.json`, '[]')
    expect(dotted.slice(37)).toBe(`${'x'.repeat(144)}.json`)
    expect((await importSeed('alice', { filename: dotted, kind: 'json', corpusId: ids.a })).status).toBe(200)
  })
})
