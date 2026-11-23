import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import Database from 'better-sqlite3'
import bcrypt from 'bcryptjs'
import request from 'supertest'
import { jest } from '@jest/globals'
import * as seed from '../src/seed.js'

// app.js reads RAG_FEEDER_DB_PATH at load, so it is imported once, in
// beforeAll, after the environment is set (as graph-access.test.js does).
let appModule
let app
let tmpDir = ''
let dbPath = ''
const fx = {}
const tokens = {}
const all = () => true

// Copied from seed-state.test.js (a test file exports nothing).
function createSeedDb() {
  const db = new Database(':memory:')
  db.exec(`
    CREATE TABLE works (
      id INTEGER PRIMARY KEY,
      title TEXT,
      authors TEXT,
      year TEXT,
      doi TEXT,
      openalex_id TEXT,
      metadata_status TEXT,
      download_status TEXT,
      file_path TEXT,
      origin_key TEXT
    );
    CREATE TABLE corpus_works (
      corpus_id INTEGER NOT NULL,
      work_id INTEGER NOT NULL,
      added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (corpus_id, work_id)
    );
    CREATE TABLE work_aliases (
      work_table TEXT NOT NULL,
      work_id INTEGER NOT NULL,
      normalized_alias_title TEXT NOT NULL,
      alias_year INTEGER
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
  `)
  seed.ensureSeedSchema(db)
  return db
}

function addWork(db, id, title, year, corpora, { authors = '["X Author"]', status = 'downloaded', filePath = null } = {}) {
  db.prepare(
    `INSERT INTO works (id, title, authors, year, metadata_status, download_status, file_path)
     VALUES (?, ?, ?, ?, 'matched', ?, ?)`
  ).run(id, title, authors, year, status, filePath)
  corpora.forEach((corpusId) => db.prepare('INSERT INTO corpus_works (corpus_id, work_id) VALUES (?, ?)').run(corpusId, id))
}

function addEntry(db, id, corpusId, source, title, year, { authors = '["X Author"]', publisher = null } = {}) {
  db.prepare(
    `INSERT INTO ingest_entries (id, corpus_id, ingest_source, title, authors, year, publisher)
     VALUES (?, ?, ?, ?, ?, ?, ?)`
  ).run(id, corpusId, source, title, authors, year, publisher)
}

function dismiss(db, corpusId, sourceType, sourceKey, candidateKey) {
  db.prepare(
    `INSERT INTO seed_candidates_dismissed (corpus_id, source_type, source_key, candidate_key)
     VALUES (?, ?, ?, ?)`
  ).run(corpusId, sourceType, sourceKey, candidateKey)
}

// corpus 1: pdf seed paperA (5 entries) and search run 7 (3 results);
// corpus 2: pdf seed paperB.
function createFixtureDb() {
  const db = createSeedDb()
  db.exec(`
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
  addWork(db, 1, 'Shared Work', '2020', [1, 2], { filePath: '/lib/shared.pdf' })
  addWork(db, 2, 'Elsewhere Work', '2019', [2], { filePath: '/lib/elsewhere.pdf' })
  addWork(db, 3, 'Search Hit Work', '2021', [1], { authors: null, filePath: '/lib/search.pdf' })

  addEntry(db, 11, 1, 'paperA', 'Shared Work', '2020')
  addEntry(db, 12, 1, 'paperA', 'Elsewhere Work', '2019')
  addEntry(db, 13, 1, 'paperA', 'Plain Entry', '2018', { authors: '["First Person", "Second Author"]' })
  addEntry(db, 14, 1, 'paperA', 'Shared Work', '2020')
  addEntry(db, 15, 1, 'paperA', 'Fifth Entry', '2017', { publisher: 'Big Publisher' })
  dismiss(db, 1, 'pdf', 'paperA', 'ingest:14')
  addEntry(db, 21, 2, 'paperB', 'Shared Work', '2020')

  db.prepare(`INSERT INTO search_runs (id, query, status) VALUES (7, 'run seven', 'completed')`).run()
  db.prepare('INSERT INTO search_run_corpora (search_run_id, corpus_id) VALUES (7, 1)').run()
  const result = db.prepare('INSERT INTO search_results (id, search_run_id, title, year, raw_json) VALUES (?, 7, ?, ?, ?)')
  result.run(31, 'Search Hit Work', '2021', '{}')
  result.run(32, 'Search Pending Work', '2016', '{}')
  result.run(33, 'Search Hit Work', '2021', '{}')
  dismiss(db, 1, 'search', '7', 'search:33')
  return db
}

function recordSql(db) {
  const seen = []
  const orig = db.prepare.bind(db)
  db.prepare = (sql) => {
    seen.push(sql)
    return orig(sql)
  }
  return seen
}

function recordGets(db) {
  const calls = []
  const orig = db.prepare.bind(db)
  db.prepare = (sql) => {
    const statement = orig(sql)
    const get = statement.get.bind(statement)
    statement.get = (...args) => {
      calls.push({ sql, args })
      return get(...args)
    }
    return statement
  }
  return calls
}

const expectedDownload = (candidate) =>
  candidate && ['downloaded', 'downloaded_elsewhere'].includes(candidate.state) && candidate.downloaded_work_id > 0
    ? candidate.downloaded_work_id
    : null

const inCorpora = (db, corpora) => (workId) =>
  corpora.some((corpusId) => Boolean(db.prepare('SELECT 1 FROM corpus_works WHERE corpus_id = ? AND work_id = ?').get(corpusId, workId)))

describe('seed read path (unit)', () => {
  let db

  beforeEach(() => {
    db = createFixtureDb()
  })

  afterEach(() => {
    db?.close()
    db = null
  })

  test('the light seed list reads no works row and probes no file', () => {
    const probe = jest.fn(() => '/x.pdf')
    const seen = recordSql(db)
    const sources = seed.listSeedSources(db, 1, { withStateCounts: false, resolveDownloadedFilePath: probe, q: '' })
    expect(probe).not.toHaveBeenCalled()
    expect(seen.some((sql) => /\bFROM works\b|JOIN works\b/.test(sql))).toBe(false)
    expect(sources.find((s) => s.source_key === 'paperA').candidate_count).toBe(4)
  })

  test('light counts equal the resolved candidate list for every q', () => {
    for (const q of ['', 'second author', 'nomatch', 'PUBLISHER']) {
      const light = seed.listSeedSources(db, 1, { withStateCounts: false, q })
      const full = seed.listSeedSources(db, 1, { withStateCounts: true, q })
      expect(light.map((s) => [s.id, s.candidate_count])).toEqual(full.map((s) => [s.id, s.candidate_count]))
      expect(light.every((s) => s.state_counts === null)).toBe(true)
    }
    expect(seed.listSeedSources(db, 1, { withStateCounts: false, q: 'second author' }).map((s) => [s.id, s.candidate_count]))
      .toEqual([['pdf:paperA', 1]])
  })

  test('countSeedCandidates pdf equals listSeedCandidates length', () => {
    for (const q of ['', 'second author']) {
      expect(seed.countSeedCandidates(db, 1, 'pdf', 'paperA', { q }))
        .toBe(seed.listSeedCandidates(db, 1, 'pdf', 'paperA', { q }).length)
    }
    expect(seed.countSeedCandidates(db, 1, 'pdf', 'paperA', {})).toBe(4)
    expect(seed.countSeedCandidates(db, 1, 'pdf', 'paperA', { q: 'second author' })).toBe(1)
  })

  test('findSeedCandidateDownload decides as the full resolver does for a reader of every work', () => {
    const cases = [
      ['pdf', 'paperA', seed.listSeedCandidates(db, 1, 'pdf', 'paperA')],
      ['search', '7', seed.listSeedCandidates(db, 1, 'search', '7')],
    ]
    let downloaded = 0
    for (const [sourceType, sourceKey, list] of cases) {
      const extraKeys = ['ingest:21', 'search:999', 'bogus', 'ingest:0', 'ingest:07', sourceType === 'pdf' ? 'search:11' : 'ingest:31']
      for (const key of [...list.map((c) => c.candidate_key), ...extraKeys]) {
        const expected = expectedDownload(list.find((c) => c.candidate_key === key))
        if (expected !== null) downloaded += 1
        expect([key, seed.findSeedCandidateDownload(db, 1, sourceType, sourceKey, key, all)]).toEqual([key, expected])
      }
    }
    // paperA: W1 (local) and W2 (elsewhere); run 7: W3.
    expect(downloaded).toBe(3)
    expect(seed.findSeedCandidateDownload(db, 1, 'pdf', 'paperA', 'ingest:12', all)).toBe(2)
  })

  test('findSeedCandidateDownload serves only readable works and prefers the current corpus', () => {
    addEntry(db, 16, 1, 'paperA', 'Preferred Work', '2018')
    addEntry(db, 41, 3, 'paperC', 'Preferred Work', '2018')
    addWork(db, 20, 'Preferred Work', '2018', [3])
    addWork(db, 21, 'Preferred Work', '2018', [1])

    const reader13 = inCorpora(db, [1, 3])
    expect(seed.findSeedCandidateDownload(db, 1, 'pdf', 'paperA', 'ingest:16', reader13)).toBe(21)
    expect(seed.findSeedCandidateDownload(db, 3, 'pdf', 'paperC', 'ingest:41', reader13)).toBe(20)

    const reader1 = inCorpora(db, [1])
    expect(seed.findSeedCandidateDownload(db, 1, 'pdf', 'paperA', 'ingest:12', reader1)).toBeNull()
    expect(seed.findSeedCandidateDownload(db, 1, 'pdf', 'paperA', 'ingest:16', reader1)).toBe(21)

    const none = () => false
    for (const key of ['ingest:11', 'ingest:12', 'ingest:13', 'ingest:15', 'ingest:16']) {
      expect(seed.findSeedCandidateDownload(db, 1, 'pdf', 'paperA', key, none)).toBeNull()
    }
    expect(seed.findSeedCandidateDownload(db, 3, 'pdf', 'paperC', 'ingest:41', none)).toBeNull()
    expect(seed.findSeedCandidateDownload(db, 1, 'search', '7', 'search:31', none)).toBeNull()
  })

  test('a search candidate needs a canonical run id', () => {
    expect(seed.findSeedCandidateDownload(db, 1, 'search', '07', 'search:33', all)).toBeNull()
    expect(seed.findSeedCandidateDownload(db, 1, 'search', '7', 'search:33', all)).toBeNull()
    expect(seed.findSeedCandidateDownload(db, 1, 'search', '7', 'search:31', all)).toBe(3)
    expect(seed.findSeedCandidateDownload(db, 1, 'search', '07', 'search:31', all)).toBeNull()
  })

  test('findSeedCandidateDownload matches once, never builds the corpus-local resolver, probes the dismissal by its primary key', () => {
    const seen = recordSql(db)
    expect(seed.findSeedCandidateDownload(db, 1, 'pdf', 'paperA', 'ingest:11', all)).toBe(1)
    expect(seed.findSeedCandidateDownload(db, 1, 'search', '7', 'search:31', all)).toBe(3)
    expect(seen.some((sql) => sql.includes('JOIN corpus_works cw ON cw.work_id = t.id'))).toBe(false)
    const dismissal = seen.filter((sql) => sql.includes('seed_candidates_dismissed'))
    expect(dismissal.some((sql) => ['corpus_id', 'source_type', 'source_key', 'candidate_key'].every((col) => sql.includes(col))
      && sql.includes("source_type = 'pdf'"))).toBe(true)
    expect(seen.some((sql) => sql.includes('JOIN search_run_corpora src ON src.search_run_id = sr.search_run_id') && sql.includes('sr.id = ?'))).toBe(true)
    expect(seen.some((sql) => sql.includes('SELECT file_path, origin_key FROM works'))).toBe(false)
  })

  test('a dismissed candidate and another corpus\'s seed answer null', () => {
    // Both match a downloaded work, so null comes from the scoping, not a missing match.
    expect(seed.findSeedCandidateDownload(db, 1, 'pdf', 'paperA', 'ingest:14', all)).toBeNull()
    expect(seed.findSeedCandidateDownload(db, 1, 'search', '7', 'search:33', all)).toBeNull()
    expect(seed.findSeedCandidateDownload(db, 1, 'pdf', 'paperB', 'ingest:21', all)).toBeNull()
    expect(seed.findSeedCandidateDownload(db, 2, 'pdf', 'paperA', 'ingest:11', all)).toBeNull()
    expect(seed.findSeedCandidateDownload(db, 2, 'search', '7', 'search:31', all)).toBeNull()
    // The same rows in their own corpus do resolve.
    expect(seed.findSeedCandidateDownload(db, 2, 'pdf', 'paperB', 'ingest:21', all)).toBe(1)
  })

  test('a cached match that is no longer downloaded does not hide another downloaded local match', () => {
    addEntry(db, 17, 1, 'paperA', 'Twin Work', '2010')
    addWork(db, 60, 'Twin Work', '2010', [1])
    addWork(db, 61, 'Twin Work', '2010', [1])
    addWork(db, 62, 'Unrelated Work', '2009', [1], { status: 'queued' })
    expect(seed.findSeedCandidateDownload(db, 1, 'pdf', 'paperA', 'ingest:17', all)).toBe(60)
    // Requeue W60 while W62 finishes: the works fingerprint stays the same,
    // so the cached downloaded index still lists W60.
    db.prepare("UPDATE works SET download_status = 'queued' WHERE id = 60").run()
    db.prepare("UPDATE works SET download_status = 'downloaded' WHERE id = 62").run()
    const full = seed.listSeedCandidates(db, 1, 'pdf', 'paperA').find((c) => c.candidate_key === 'ingest:17')
    expect(expectedDownload(full)).toBe(61)
    expect(seed.findSeedCandidateDownload(db, 1, 'pdf', 'paperA', 'ingest:17', all)).toBe(61)
    // With no downloaded match left, nothing is served.
    db.prepare("UPDATE works SET download_status = 'queued' WHERE id = 61").run()
    expect(seed.findSeedCandidateDownload(db, 1, 'pdf', 'paperA', 'ingest:17', all)).toBeNull()
  })

  test('an id beyond the safe integer range answers null instead of rounding to another row', () => {
    const big = 9007199254740992n // 2^53; 2^53 + 1 rounds to it as a Number
    db.prepare(`INSERT INTO search_runs (id, query, status) VALUES (?, 'big run', 'completed')`).run(big)
    db.prepare('INSERT INTO search_run_corpora (search_run_id, corpus_id) VALUES (?, 1)').run(big)
    db.prepare(`INSERT INTO search_results (id, search_run_id, title, year, raw_json) VALUES (34, ?, 'Search Hit Work', '2021', '{}')`).run(big)
    expect(seed.findSeedCandidateDownload(db, 1, 'search', '9007199254740993', 'search:34', all)).toBeNull()
    expect(seed.findSeedCandidateDownload(db, 1, 'search', '9007199254740992', 'search:34', all)).toBeNull()
    db.prepare(`INSERT INTO ingest_entries (id, corpus_id, ingest_source, title, year) VALUES (?, 1, 'paperA', 'Shared Work', '2020')`).run(big)
    expect(seed.findSeedCandidateDownload(db, 1, 'pdf', 'paperA', 'ingest:9007199254740993', all)).toBeNull()
    expect(seed.findSeedCandidateDownload(db, 1, 'pdf', 'paperA', 'ingest:9007199254740992', all)).toBeNull()
  })

  test('createStateResolver probes sqlite_master for works once, whatever the candidate count', () => {
    addWork(db, 5, 'Fifth Download', '2015', [1])
    addWork(db, 6, 'Sixth Download', '2014', [1])
    addEntry(db, 51, 1, 'paperD', 'Shared Work', '2020')
    const probe = () => '/x.pdf'
    // Warm the cached global index so both measurements read it from the cache.
    seed.listSeedCandidates(db, 1, 'pdf', 'paperD', { resolveDownloadedFilePath: probe })
    const worksProbes = () => {
      const calls = recordGets(db)
      const list = seed.listSeedCandidates(db, 1, 'pdf', 'paperD', { resolveDownloadedFilePath: probe })
      delete db.prepare
      return {
        downloaded: list.filter((c) => c.downloaded_work_id).length,
        probes: calls.filter(({ sql, args }) => /sqlite_master/.test(sql) && args.includes('works')).length,
      }
    }
    const one = worksProbes()
    addEntry(db, 52, 1, 'paperD', 'Fifth Download', '2015')
    addEntry(db, 53, 1, 'paperD', 'Sixth Download', '2014')
    const three = worksProbes()
    expect(one.downloaded).toBe(1)
    expect(three.downloaded).toBe(3)
    expect(three.probes).toBe(one.probes)
  })
})

describe('seed read path (app)', () => {
  const insertUser = (db, username, password) =>
    Number(
      db
        .prepare(
          `INSERT INTO users (username, password_hash, email, account_status, password_set_at)
           VALUES (?, ?, ?, 'active', CURRENT_TIMESTAMP)`
        )
        .run(username, bcrypt.hashSync(password, 10), `${username}@example.test`).lastInsertRowid
    )

  const login = async (username, password) => {
    const res = await request(app).post('/api/auth/login').send({ username, password })
    expect(res.status).toBe(200)
    return res.body.token
  }

  const as = (name) => ({ Authorization: `Bearer ${tokens[name]}` })

  beforeAll(async () => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'dt-seed-read-path-'))
    dbPath = path.join(tmpDir, 'seed-read-path.db')
    process.env.RAG_FEEDER_DB_PATH = dbPath
    process.env.RAG_FEEDER_JWT_SECRET = 'seed-read-path-secret'
    process.env.RAG_ADMIN_USER = 'seed-read-path-admin'
    process.env.RAG_ADMIN_PASSWORD = 'seed-read-path-admin-password'

    appModule = await import('../src/app.js')
    app = appModule.createApp({ broadcast: () => {} })

    fx.pdfPath = path.join(tmpDir, 'corpus2-only.pdf')
    fs.writeFileSync(fx.pdfPath, Buffer.from('%PDF-1.4\n% seed read path fixture\n%%EOF\n'))

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
        CREATE TABLE IF NOT EXISTS search_runs (
          id INTEGER PRIMARY KEY, query TEXT, filters_json TEXT, status TEXT,
          fetched_count INTEGER, expected_count INTEGER, error TEXT, finished_at TIMESTAMP,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS search_results (
          id INTEGER PRIMARY KEY, search_run_id INTEGER NOT NULL, title TEXT,
          doi TEXT, openalex_id TEXT, year TEXT, raw_json TEXT
        );
      `)
      const u1 = insertUser(db, 'seed-u1', 'seed-u1-password')
      const u2 = insertUser(db, 'seed-u2', 'seed-u2-password')
      const corpus = (name) => Number(db.prepare('INSERT INTO corpora (name, owner_user_id) VALUES (?, ?)').run(name, u2).lastInsertRowid)
      fx.corpus1 = corpus('Seed corpus 1')
      fx.corpus2 = corpus('Seed corpus 2')
      const member = db.prepare("INSERT INTO user_corpora (user_id, corpus_id, role) VALUES (?, ?, 'owner')")
      member.run(u1, fx.corpus1)
      member.run(u2, fx.corpus1)
      member.run(u2, fx.corpus2)
      db.prepare('UPDATE users SET last_corpus_id = ? WHERE id IN (?, ?)').run(fx.corpus1, u1, u2)
      // The admin is a member of no fixture corpus; it reads through the admin rule.
      db.prepare('UPDATE users SET last_corpus_id = ? WHERE username = ?').run(fx.corpus1, process.env.RAG_ADMIN_USER)

      const workId = Number(
        db
          .prepare(
            `INSERT INTO works (title, normalized_title, authors, year, metadata_status, download_status, file_path)
             VALUES ('Corpus Two Only Work', 'corpus two only work', '["X Author"]', 2019, 'matched', 'downloaded', ?)`
          )
          .run(fx.pdfPath).lastInsertRowid
      )
      db.prepare('INSERT INTO corpus_works (corpus_id, work_id) VALUES (?, ?)').run(fx.corpus2, workId)
      fx.entryInCorpus2Only = Number(
        db
          .prepare(
            `INSERT INTO ingest_entries (corpus_id, ingest_source, title, authors, year)
             VALUES (?, 'paperA', 'Corpus Two Only Work', '["X Author"]', '2019')`
          )
          .run(fx.corpus1).lastInsertRowid
      )
      // D2/D5: two downloaded copies u2 can read; the current-corpus copy's
      // file is gone, the corpus-2 copy's file is on disk.
      fx.twoCopiesPdf = path.join(tmpDir, 'two-copies.pdf')
      fs.writeFileSync(fx.twoCopiesPdf, Buffer.from('%PDF-1.4\n% two copies fixture\n%%EOF\n'))
      const addCopy = (filePath, corpusId) => {
        const id = Number(
          db
            .prepare(
              `INSERT INTO works (title, normalized_title, authors, year, metadata_status, download_status, file_path)
               VALUES ('Two Copies Work', 'two copies work', '["X Author"]', 2018, 'matched', 'downloaded', ?)`
            )
            .run(filePath).lastInsertRowid
        )
        db.prepare('INSERT INTO corpus_works (corpus_id, work_id) VALUES (?, ?)').run(corpusId, id)
        return id
      }
      fx.missingCopy = addCopy(path.join(tmpDir, 'gone.pdf'), fx.corpus1)
      fx.presentCopy = addCopy(fx.twoCopiesPdf, fx.corpus2)
      fx.entryTwoCopies = Number(
        db
          .prepare(
            `INSERT INTO ingest_entries (corpus_id, ingest_source, title, authors, year)
             VALUES (?, 'paperA', 'Two Copies Work', '["X Author"]', '2018')`
          )
          .run(fx.corpus1).lastInsertRowid
      )
    } finally {
      db.close()
    }

    tokens.u1 = await login('seed-u1', 'seed-u1-password')
    tokens.u2 = await login('seed-u2', 'seed-u2-password')
    tokens.admin = await login(process.env.RAG_ADMIN_USER, process.env.RAG_ADMIN_PASSWORD)
  }, 30_000)

  afterAll(() => {
    delete process.env.RAG_FEEDER_DB_PATH
    delete process.env.RAG_FEEDER_JWT_SECRET
    delete process.env.RAG_ADMIN_USER
    delete process.env.RAG_ADMIN_PASSWORD
    if (tmpDir && fs.existsSync(tmpDir)) fs.rmSync(tmpDir, { recursive: true, force: true })
  })

  test('findDownloadedFilePath probes an existing absolute path once', () => {
    const file = path.join(tmpDir, 'probe-once.pdf')
    fs.writeFileSync(file, 'x')
    const spy = jest.spyOn(fs, 'existsSync')
    try {
      expect(appModule.findDownloadedFilePath(file)).toBe(file)
      expect(spy.mock.calls.filter(([arg]) => arg === file).length).toBe(1)
    } finally {
      spy.mockRestore()
    }
  })

  test('index.js schedules no seed warm-up and createApp exposes none', () => {
    expect(fs.readFileSync(new URL('../src/index.js', import.meta.url), 'utf8')).not.toMatch(/warmSeedState/)
    process.env.RAG_FEEDER_STUB = '1'
    try {
      expect(appModule.createApp({ broadcast: () => {} }).warmSeedState).toBeUndefined()
    } finally {
      delete process.env.RAG_FEEDER_STUB
    }
  })

  test('the candidate file route never serves a PDF from a corpus the caller cannot read', async () => {
    const url = `/api/seed/sources/pdf/paperA/candidates/ingest:${fx.entryInCorpus2Only}/file`
    const denied = await request(app).get(url).set(as('u1'))
    expect(denied.status).toBe(404)
    expect(denied.body).toEqual({ error: 'Downloaded file not found for this seed candidate' })
    const allowed = await request(app)
      .get(url)
      .set(as('u2'))
      .buffer(true)
      .parse((res, cb) => {
        const chunks = []
        res.on('data', (chunk) => chunks.push(chunk))
        res.on('end', () => cb(null, Buffer.concat(chunks)))
      })
    expect(allowed.status).toBe(200)
    expect(Buffer.compare(allowed.body, fs.readFileSync(fx.pdfPath))).toBe(0)
    // The admin reads every work (the row query has no membership clause for it).
    const admin = await request(app).get(url).set(as('admin'))
    expect(admin.status).toBe(200)
  })

  const candidateRow = async (who, entryId) => {
    const res = await request(app).get('/api/seed/sources/pdf/paperA/candidates').set(as(who))
    expect(res.status).toBe(200)
    return { body: res.body, row: res.body.candidates.find((c) => c.candidate_key === `ingest:${entryId}`) }
  }

  test('the candidate list shows a download only where the file route serves one; non-admins get no paths', async () => {
    // u1 cannot read the corpus-2 work: the match counts as not downloaded.
    const u1 = await candidateRow('u1', fx.entryInCorpus2Only)
    expect(u1.row.state).not.toBe('downloaded_elsewhere')
    expect(u1.row.state).not.toBe('downloaded')
    expect(u1.row.downloaded_work_id).toBeNull()
    expect(u1.row.file_available).toBe(false)
    expect(u1.row).not.toHaveProperty('downloaded_file_path')
    expect(u1.body.source_summary.state_counts.downloaded_elsewhere_available).toBe(0)
    expect(u1.body.candidates.every((c) => !('downloaded_file_path' in c))).toBe(true)

    // u2 reads it: downloadable, but the absolute path stays server-side.
    const u2 = await candidateRow('u2', fx.entryInCorpus2Only)
    expect(u2.row.state).toBe('downloaded_elsewhere')
    expect(u2.row.file_available).toBe(true)
    expect(u2.row.downloaded_work_id).toBeGreaterThan(0)
    expect(u2.row).not.toHaveProperty('downloaded_file_path')

    // The admin keeps the id and the path.
    const admin = await candidateRow('admin', fx.entryInCorpus2Only)
    expect(admin.row.downloaded_work_id).toBe(u2.row.downloaded_work_id)
    expect(admin.row.downloaded_file_path).toBe(fx.pdfPath)
  })

  test('list and file route both pick the first readable match whose file is on disk', async () => {
    const { row } = await candidateRow('u2', fx.entryTwoCopies)
    expect(row.downloaded_work_id).toBe(fx.presentCopy)
    expect(row.file_available).toBe(true)
    const res = await request(app)
      .get(`/api/seed/sources/pdf/paperA/candidates/ingest:${fx.entryTwoCopies}/file`)
      .set(as('u2'))
      .buffer(true)
      .parse((r, cb) => {
        const chunks = []
        r.on('data', (chunk) => chunks.push(chunk))
        r.on('end', () => cb(null, Buffer.concat(chunks)))
      })
    expect(res.status).toBe(200)
    expect(Buffer.compare(res.body, fs.readFileSync(fx.twoCopiesPdf))).toBe(0)
    // u1 reads only the corpus-1 copy, whose file is gone.
    const u1 = await candidateRow('u1', fx.entryTwoCopies)
    expect(u1.row.downloaded_work_id).toBe(fx.missingCopy)
    expect(u1.row.file_available).toBe(false)
    const missing = await request(app).get(`/api/seed/sources/pdf/paperA/candidates/ingest:${fx.entryTwoCopies}/file`).set(as('u1'))
    expect(missing.status).toBe(404)
  })
})
