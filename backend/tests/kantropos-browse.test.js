import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import Database from 'better-sqlite3'
import request from 'supertest'
import { jest } from '@jest/globals'

// app.js reads RAG_FEEDER_DB_PATH and RAG_FEEDER_KANTROPOS_CORPORA at load, so it
// is imported once, in beforeAll, after the environment is set (setup copied from
// corpus-sharing.test.js).
describe('Kantropos browse read path', () => {
  let app
  let appModule
  let dbPath = ''
  let tmpDir = ''
  let adminToken = ''
  const adminUsername = 'browse-admin'
  const adminPassword = 'browse-admin-password'
  const fx = {}

  const withDb = (fn) => {
    const db = new Database(dbPath)
    try {
      return fn(db)
    } finally {
      db.close()
    }
  }

  const bibEntry = (key) => `@article{${key},\n  title = {${key}},\n}\n`

  beforeAll(async () => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'dt-kantropos-browse-'))
    dbPath = path.join(tmpDir, 'browse.db')
    fx.targetA = path.join(tmpDir, 'target-a')
    fx.targetB = path.join(tmpDir, 'target-b')
    fx.bibA = path.join(fx.targetA, 'metadata.bib')
    fx.bibB = path.join(fx.targetB, 'metadata.bib')
    fs.mkdirSync(fx.targetA, { recursive: true })
    fs.mkdirSync(fx.targetB, { recursive: true })
    fs.writeFileSync(fx.bibA, ['a1', 'a2', 'a3'].map(bibEntry).join('\n'))
    fs.writeFileSync(fx.bibB, ['b1', 'b2'].map(bibEntry).join('\n'))

    process.env.RAG_FEEDER_DB_PATH = dbPath
    process.env.RAG_FEEDER_JWT_SECRET = 'browse-secret'
    process.env.RAG_ADMIN_USER = adminUsername
    process.env.RAG_ADMIN_PASSWORD = adminPassword
    process.env.RAG_FEEDER_KANTROPOS_CORPORA = JSON.stringify([
      { id: 'target-a', name: 'Target A', path: fx.targetA },
      { id: 'target-b', name: 'Target B', path: fx.targetB },
    ])

    appModule = await import('../src/app.js')
    app = appModule.createApp({ broadcast: () => {} })

    const login = await request(app).post('/api/auth/login').send({ username: adminUsername, password: adminPassword })
    expect(login.status).toBe(200)
    adminToken = login.body.token

    const filesDir = path.join(tmpDir, 'files')
    fs.mkdirSync(filesDir, { recursive: true })
    withDb((db) => {
      const adminId = Number(db.prepare('SELECT id FROM users WHERE username = ?').get(adminUsername).id)
      const insertCorpus = (name) => {
        const id = Number(db.prepare('INSERT INTO corpora (name, owner_user_id) VALUES (?, ?)').run(name, adminId).lastInsertRowid)
        db.prepare("INSERT INTO user_corpora (user_id, corpus_id, role) VALUES (?, ?, 'owner')").run(adminId, id)
        db.prepare(
          `INSERT INTO corpus_kantropos_assignments (corpus_id, target_id, target_name, target_path, updated_by_user_id)
           VALUES (?, 'target-a', 'Target A', ?, ?)`
        ).run(id, fx.targetA, adminId)
        return id
      }
      // LOWER(name) order is c3 ('alpha'), c1 ('Middle'), c2 ('Zeta'): not the id order.
      fx.c1 = insertCorpus('Middle corpus')
      fx.c2 = insertCorpus('Zeta corpus')
      fx.c3 = insertCorpus('alpha corpus')

      const insertWork = ({ id, title, year, openalex, status = 'downloaded', file = 'present', imported = false }) => {
        let filePath = null
        if (file === 'present') {
          filePath = path.join(filesDir, `${id}.pdf`)
          fs.writeFileSync(filePath, `pdf ${id}`)
        } else if (file === 'missing') {
          filePath = path.join(filesDir, `${id}-missing.pdf`)
        }
        db.prepare(
          `INSERT INTO works (id, title, normalized_title, authors, year, type, openalex_id,
             metadata_status, download_status, file_path, origin_type, origin_key)
           VALUES (?, ?, ?, 'Browse Author', ?, 'journal-article', ?, 'matched', ?, ?, ?, ?)`
        ).run(
          id,
          title,
          title.toLowerCase(),
          year,
          openalex,
          status,
          filePath,
          imported ? 'bibtex_import' : null,
          imported ? fx.bibA : null
        )
        return filePath
      }
      const member = (corpusId, workId) =>
        db.prepare('INSERT INTO corpus_works (corpus_id, work_id) VALUES (?, ?)').run(corpusId, workId)

      // Two imported works (current items), downloaded and in c1: they count as
      // downloaded but never as pending.
      insertWork({ id: 101, title: 'Imported one', year: 2020, openalex: 'W2001', imported: true })
      insertWork({ id: 102, title: 'Imported two', year: 2019, openalex: 'W2002', imported: true })
      member(fx.c1, 101)
      member(fx.c1, 102)

      // Seven downloaded pending works; order by year DESC, id DESC is
      // 201, 202, 205 (W5, in c2 and c3), 203, 204, 206, 207.
      insertWork({ id: 201, title: 'Pending one', year: 2025, openalex: 'W1001' })
      insertWork({ id: 202, title: 'Pending two', year: 2024, openalex: 'W1002', file: 'missing' })
      insertWork({ id: 205, title: 'Pending five', year: 2023, openalex: 'W1005' })
      insertWork({ id: 203, title: 'Pending three', year: 2022, openalex: 'W1003' })
      insertWork({ id: 204, title: 'Pending four', year: 2021, openalex: 'W1004' })
      insertWork({ id: 206, title: 'Pending six', year: 2018, openalex: 'W1006', file: 'missing' })
      insertWork({ id: 207, title: 'Pending seven', year: 2017, openalex: 'W1007' })
      fx.w5Path = path.join(filesDir, '205.pdf')
      member(fx.c1, 201)
      member(fx.c1, 202)
      member(fx.c2, 205)
      member(fx.c3, 205)
      member(fx.c1, 203)
      member(fx.c2, 204)
      member(fx.c3, 206)
      member(fx.c1, 207)
      // Not downloaded: counted nowhere.
      insertWork({ id: 301, title: 'Not downloaded', year: 2026, openalex: 'W3001', status: 'not_requested', file: 'none' })
      member(fx.c1, 301)

      fx.pageWorkIds = [201, 202, 205]
      fx.pendingFilePaths = new Set(
        db
          .prepare("SELECT file_path FROM works WHERE id BETWEEN 201 AND 299 AND file_path IS NOT NULL")
          .all()
          .map((row) => row.file_path)
      )

      db.exec(`
        CREATE TABLE IF NOT EXISTS citation_edges (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          source_id TEXT NOT NULL,
          target_id TEXT NOT NULL,
          relationship_type TEXT NOT NULL,
          source_table TEXT,
          target_table TEXT,
          source_row_id INTEGER,
          target_row_id INTEGER,
          run_id INTEGER,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          UNIQUE(source_id, target_id, relationship_type)
        )
      `)
      const edge = db.prepare('INSERT INTO citation_edges (source_id, target_id, relationship_type) VALUES (?, ?, ?)')
      // Three among page nodes (current W2001/W2002, pending W1001/W1002/W1005).
      edge.run('W2001', 'W1001', 'references')
      edge.run('W1001', 'W1005', 'references')
      edge.run('W1002', 'W2002', 'cited_by')
      // Two to off-page works.
      edge.run('W1001', 'W1003', 'references')
      edge.run('W1006', 'W2001', 'references')
      // A stored endpoint padded with spaces (D5: drops out).
      edge.run('  W1005 ', 'W2001', 'references')
    })
  }, 30_000)

  afterAll(() => {
    delete process.env.RAG_FEEDER_DB_PATH
    delete process.env.RAG_FEEDER_JWT_SECRET
    delete process.env.RAG_ADMIN_USER
    delete process.env.RAG_ADMIN_PASSWORD
    delete process.env.RAG_FEEDER_KANTROPOS_CORPORA
    if (tmpDir && fs.existsSync(tmpDir)) {
      fs.chmodSync(tmpDir, 0o755)
      fs.rmSync(tmpDir, { recursive: true, force: true })
    }
  })

  afterEach(() => {
    jest.restoreAllMocks()
  })

  const get = (url) => request(app).get(url).set('Authorization', `Bearer ${adminToken}`)
  const browse = () => get('/api/kantropos/corpora/target-a/browse?pending_limit=3')
  const edgeSet = (edges) => new Set(edges.map((e) => `${e.source}|${e.target}|${e.relationship_type}`))
  const paddedEdge = 'W1005|W2001|references'

  test('browse payload is unchanged (golden)', async () => {
    const res = await browse()
    expect(res.status).toBe(200)
    const body = res.body

    expect(body.source).toBe('api')
    expect(body.target).toMatchObject({
      id: 'target-a',
      name: 'Target A',
      directory_exists: true,
      metadata_bib_exists: true,
      metadata_bib_path: fx.bibA,
      metadata_entry_count: 3,
      assignment_count: 3,
    })
    expect(body.summary).toEqual({
      current_items_count: 2,
      pending_items_count: 5,
      pending_skipped_missing_file: 2,
      assigned_corpora_count: 3,
      metadata_bib_modified_at: fs.statSync(fx.bibA).mtime.toISOString(),
      metadata_bib_exists: true,
      metadata_bib_path: fx.bibA,
      metadata_entry_count: 3,
      has_imported_current: true,
      pending_is_provisional: false,
      current_items_loaded: 2,
      pending_items_loaded: 2,
      current_items_has_more: false,
      pending_items_has_more: true,
    })
    expect(body.assigned_corpora.map((c) => [c.id, c.name, c.downloaded_count, c.pending_count])).toEqual([
      [fx.c3, 'alpha corpus', 2, 2],
      [fx.c1, 'Middle corpus', 6, 4],
      [fx.c2, 'Zeta corpus', 2, 2],
    ])
    expect(body.current_items.map((item) => item.work_id)).toEqual([101, 102])
    expect(
      body.pending_items.map((item) => [
        item.work_id,
        item.graph_node_id,
        item.source_corpora.map((c) => c.name),
        item.source_corpus_ids,
        item.source_corpora_count,
        item.provisional,
        item.file_missing,
      ])
    ).toEqual([
      [201, 'W1001', ['Middle corpus'], [fx.c1], 1, false, false],
      [202, 'W1002', ['Middle corpus'], [fx.c1], 1, false, true],
      [205, 'W1005', ['alpha corpus', 'Zeta corpus'], [fx.c3, fx.c2], 2, false, false],
    ])
    expect(body.pending_items[2]).toMatchObject({
      id: 205,
      title: 'Pending five',
      authors: 'Browse Author',
      year: 2023,
      type: 'journal-article',
      status: 'downloaded',
      metadata_status: 'matched',
      download_status: 'downloaded',
      doi: null,
      openalex_id: 'W1005',
      file_path: fx.w5Path,
      source_work_id: null,
      relationship_type: null,
      source_corpora: [
        { id: fx.c3, name: 'alpha corpus', owner_username: adminUsername },
        { id: fx.c2, name: 'Zeta corpus', owner_username: adminUsername },
      ],
    })
    expect(body.graph.nodes.map((node) => [node.id, node.upstream_state])).toEqual([
      ['W2001', 'current'],
      ['W2002', 'current'],
      ['W1001', 'pending'],
      ['W1002', 'pending'],
      ['W1005', 'pending'],
    ])
    const edges = edgeSet(body.graph.edges)
    edges.delete(paddedEdge)
    expect(edges).toEqual(new Set(['W2001|W1001|references', 'W1001|W1005|references', 'W1002|W2002|cited_by']))
  })

  test('D5: a stored endpoint padded with spaces no longer matches', async () => {
    const res = await browse()
    expect(edgeSet(res.body.graph.edges).has(paddedEdge)).toBe(false)
    expect(res.body.graph.stats).toEqual({
      node_count: 5,
      edge_count: 3,
      relationship_counts: { references: 2, cited_by: 1 },
    })
  })

  test('browse reads full rows only for the page, in one snapshot, and edges through the UNIQUE index', async () => {
    const seen = []
    const db = app.authDb
    const orig = db.prepare.bind(db)
    jest.spyOn(db, 'prepare').mockImplementation((sql) => {
      const stmt = orig(sql)
      const entry = { sql }
      seen.push(entry)
      const all = stmt.all.bind(stmt)
      stmt.all = (...args) => {
        entry.args = args
        entry.inTransaction = db.inTransaction
        const rows = all(...args)
        // A worker commit between the passes (second connection): the page work
        // 201 stops being downloaded. One read snapshot keeps it in pass 2.
        if (/SELECT w\.id, w\.file_path, cw\.corpus_id/.test(sql)) {
          withDb((other) => other.prepare("UPDATE works SET download_status = 'failed' WHERE id = 201").run())
        }
        return rows
      }
      return stmt
    })
    const probes = []
    const statSync = fs.statSync.bind(fs)
    jest.spyOn(fs, 'statSync').mockImplementation((...args) => {
      probes.push({ path: args[0], inTransaction: db.inTransaction })
      return statSync(...args)
    })

    let res
    try {
      res = await browse()
    } finally {
      withDb((other) => other.prepare("UPDATE works SET download_status = 'downloaded' WHERE id = 201").run())
    }
    expect(res.status).toBe(200)
    expect(res.body.pending_items.map((item) => item.work_id)).toEqual(fx.pageWorkIds)
    expect(res.body.summary.pending_items_count).toBe(5)

    const pass1 = seen.find((s) => /SELECT w\.id, w\.file_path, cw\.corpus_id/.test(s.sql))
    expect(pass1).toBeDefined()
    const full = seen.filter((s) => /SELECT w\.\*/.test(s.sql))
    expect(full.length).toBe(1)
    expect(full[0].sql).toContain('json_each')
    expect(JSON.parse(full[0].args.at(-1))).toEqual(fx.pageWorkIds)
    expect(pass1.inTransaction && full[0].inTransaction).toBe(true)
    expect(probes.length).toBeGreaterThan(0)
    expect(probes.every((p) => p.inTransaction === false)).toBe(true)
    expect(seen.some((s) => /COUNT\(DISTINCT w\.id\) AS downloaded_count/.test(s.sql))).toBe(false)
    const edgeSql = seen.find((s) => s.sql.includes('FROM citation_edges')).sql
    expect(edgeSql).toContain('json_each')
    const plan = orig(`EXPLAIN QUERY PLAN ${edgeSql}`)
      .all('[]', '[]')
      .map((r) => r.detail)
      .join(' | ')
    expect(plan).toContain('sqlite_autoindex_citation_edges_1')
    expect(plan).not.toMatch(/SCAN citation_edges/)
  })

  test('missing upstream files are probed once per work without throwing', async () => {
    const spy = jest.spyOn(fs, 'statSync')
    const res = await browse()
    expect(res.status).toBe(200)
    const pendingCalls = spy.mock.calls.filter(([arg]) => fx.pendingFilePaths.has(arg))
    expect(pendingCalls.length).toBe(fx.pendingFilePaths.size)
    for (const call of pendingCalls) {
      expect(call[1]).toEqual({ throwIfNoEntry: false })
    }
    expect(pendingCalls.filter(([arg]) => arg === fx.w5Path).length).toBe(1)
    expect(spy.mock.results.every((result) => result.type === 'return')).toBe(true)
  })

  test('metadata.bib is read once until it changes, asynchronously, and only for the browsed target', async () => {
    // Earlier tests may have filled the memo: change both sizes so this test starts cold.
    fs.appendFileSync(fx.bibA, '\n')
    fs.appendFileSync(fx.bibB, '\n')
    const readSync = jest.spyOn(fs, 'readFileSync')
    // The count reads through one handle (fs.promises.open), never fs.readFileSync.
    const readAsync = jest.spyOn(fs.promises, 'open')
    const bibCalls = (spy, file) => spy.mock.calls.filter(([arg]) => String(arg) === file).length

    const first = await get('/api/kantropos/corpora')
    expect(first.status).toBe(200)
    const second = await get('/api/kantropos/corpora')
    expect(second.status).toBe(200)
    expect(bibCalls(readAsync, fx.bibA)).toBe(1)
    expect(bibCalls(readAsync, fx.bibB)).toBe(1)
    expect(readSync.mock.calls.some(([arg]) => String(arg).endsWith('metadata.bib'))).toBe(false)
    const countOf = (body, id) => body.corpora.find((entry) => entry.id === id).metadata_entry_count
    expect([countOf(second.body, 'target-a'), countOf(second.body, 'target-b')]).toEqual([3, 2])

    fs.appendFileSync(fx.bibA, bibEntry('a4'))
    const third = await get('/api/kantropos/corpora')
    expect(bibCalls(readAsync, fx.bibA)).toBe(2)
    expect(bibCalls(readAsync, fx.bibB)).toBe(1)
    expect(countOf(third.body, 'target-a')).toBe(4)

    fs.appendFileSync(fx.bibB, '\n')
    readAsync.mockClear()
    readSync.mockClear()
    const browsed = await browse()
    expect(browsed.body.summary.metadata_entry_count).toBe(4)
    expect(bibCalls(readAsync, fx.bibB)).toBe(0)
    expect(bibCalls(readSync, fx.bibB)).toBe(0)

    // A failed read answers null, is not remembered, and is retried.
    fs.rmSync(fx.bibB)
    fs.mkdirSync(fx.bibB)
    readAsync.mockClear()
    const warn = jest.spyOn(console, 'warn').mockImplementation(() => {})
    const failed = await get('/api/kantropos/corpora')
    expect(countOf(failed.body, 'target-b')).toBeNull()
    const retried = await get('/api/kantropos/corpora')
    expect(countOf(retried.body, 'target-b')).toBeNull()
    expect(bibCalls(readAsync, fx.bibB)).toBe(2)
    // A non-ENOENT read error is logged once per file version, not per request.
    const bibWarnings = warn.mock.calls.filter((args) => args.some((arg) => String(arg).includes(fx.bibB)))
    expect(bibWarnings).toHaveLength(1)
    expect(String(bibWarnings[0][0])).toContain('[kantropos] metadata.bib count failed')
  })

  test('an atomic replace between the stat and the read never caches the new count under the old fingerprint', async () => {
    // Old version: 3 entries, fixed whole-second mtime so it can be restored exactly.
    const oldContent = ['a1', 'a2', 'a3'].map(bibEntry).join('\n')
    const newContent = ['a1', 'a2', 'a3', 'a4', 'a5'].map(bibEntry).join('\n')
    const oldTime = 1_700_000_000
    const writeVersion = (content, seconds) => {
      const tmp = `${fx.bibA}.tmp`
      fs.writeFileSync(tmp, content)
      fs.utimesSync(tmp, seconds, seconds)
      fs.renameSync(tmp, fx.bibA)
    }
    writeVersion(oldContent, oldTime)

    const open = fs.promises.open.bind(fs.promises)
    let replaced = false
    jest.spyOn(fs.promises, 'open').mockImplementation(async (file, ...rest) => {
      if (String(file) === fx.bibA && !replaced) {
        replaced = true
        writeVersion(newContent, oldTime + 100)
      }
      return open(file, ...rest)
    })
    const countOf = (body) => body.corpora.find((entry) => entry.id === 'target-a').metadata_entry_count

    const raced = await get('/api/kantropos/corpora')
    expect(raced.status).toBe(200)
    expect(replaced).toBe(true)
    expect(countOf(raced.body)).toBe(5)

    // The old version comes back with its old size and mtime: its count must be
    // read again, not answered from a count cached under its fingerprint.
    writeVersion(oldContent, oldTime)
    const restored = await get('/api/kantropos/corpora')
    expect(countOf(restored.body)).toBe(3)
  })

  test('concurrent cold requests read metadata.bib once', async () => {
    fs.appendFileSync(fx.bibA, '\n')
    const open = fs.promises.open.bind(fs.promises)
    let release
    const gate = new Promise((resolve) => {
      release = resolve
    })
    const spy = jest.spyOn(fs.promises, 'open').mockImplementation(async (file, ...rest) => {
      if (String(file) === fx.bibA) await gate
      return open(file, ...rest)
    })
    const first = get('/api/kantropos/corpora').then((res) => res)
    const second = get('/api/kantropos/corpora').then((res) => res)
    // Let both requests reach the count before the first read may finish.
    await new Promise((resolve) => setTimeout(resolve, 200))
    release()
    const [a, b] = await Promise.all([first, second])
    const countOf = (body) => body.corpora.find((entry) => entry.id === 'target-a').metadata_entry_count
    expect([countOf(a.body), countOf(b.body)]).toEqual([3, 3])
    expect(spy.mock.calls.filter(([file]) => String(file) === fx.bibA).length).toBe(1)
  })
})
