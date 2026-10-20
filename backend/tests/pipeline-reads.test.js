import Database from 'better-sqlite3'
import { readIngestStats, listDownloads, listIngestRuns, tableExists } from '../src/pipelineReads.js'

let db
beforeEach(() => {
  db = new Database(':memory:')
  db.exec(`
    CREATE TABLE works (id INTEGER PRIMARY KEY, title TEXT, metadata_status TEXT, download_status TEXT, download_attempt_count INTEGER, downloaded_at TEXT);
    CREATE TABLE corpus_works (corpus_id INTEGER, work_id INTEGER);
  `)
})
afterEach(() => db.close())

function addWork(id, corpusId, meta, dl, extra = {}) {
  db.prepare('INSERT INTO works (id, title, metadata_status, download_status, download_attempt_count, downloaded_at) VALUES (?,?,?,?,?,?)')
    .run(id, `Work ${id}`, meta, dl, extra.attempts ?? 0, extra.at ?? null)
  db.prepare('INSERT INTO corpus_works VALUES (?,?)').run(corpusId, id)
}

function seedStats() {
  addWork(1, 1, 'pending', null)
  addWork(2, 1, 'in_progress', null)
  addWork(3, 1, 'matched', 'not_requested')
  addWork(4, 1, 'matched', 'queued')
  addWork(5, 1, 'matched', 'in_progress')
  addWork(6, 1, 'matched', 'downloaded')
  addWork(7, 1, 'failed', null)
}

test('tableExists reports presence', () => {
  expect(tableExists(db, 'works')).toBe(true)
  expect(tableExists(db, 'nope')).toBe(false)
})

test("ingest stats count each bucket with the script's predicates", () => {
  seedStats()
  expect(readIngestStats(db, 1)).toEqual({ raw_pending: 2, matched: 1, queued_download: 2, enriching: 1, downloading: 1, downloaded: 1 })
})

test('ingest stats for an empty corpus and for global scope return zeros / all works', () => {
  seedStats()
  addWork(8, 2, 'matched', 'downloaded')
  expect(readIngestStats(db, 999)).toEqual({ raw_pending: 0, matched: 0, queued_download: 0, enriching: 0, downloading: 0, downloaded: 0 })
  expect(readIngestStats(db, null).downloaded).toBe(2)
  expect(readIngestStats(db, 1).downloaded).toBe(1)
})

function seedDownloads() {
  addWork(10, 1, 'matched', 'queued')
  addWork(11, 1, 'matched', 'in_progress', { attempts: 2 })
  addWork(12, 1, 'matched', 'downloaded', { at: '2026-01-01T10:00:00' })
  addWork(13, 1, 'matched', 'downloaded', { at: '2026-03-01T10:00:00' })
  addWork(14, 1, 'matched', 'not_requested')
  addWork(15, 2, 'matched', 'queued')
}

test('downloads: in_progress first, then queued, then downloaded newest first; id desc ties; paged with total', () => {
  seedDownloads()
  const all = listDownloads(db, { corpusId: 1, limit: 10, offset: 0 })
  expect(all.items.map((i) => i.id)).toEqual([11, 10, 13, 12])
  const page = listDownloads(db, { corpusId: 1, limit: 2, offset: 1 })
  expect(page.total).toBe(4)
  expect(page.items.map((i) => i.id)).toEqual([10, 13])
  expect(page.items[0]).toEqual({ id: 10, title: 'Work 10', status: 'queued', attempts: 0, downloaded_at: null })
  // id desc breaks ties between equal downloaded_at
  addWork(16, 1, 'matched', 'downloaded', { at: '2026-03-01T10:00:00' })
  expect(listDownloads(db, { corpusId: 1, limit: 10, offset: 0 }).items.map((i) => i.id)).toEqual([11, 10, 16, 13, 12])
  // global scope sees other corpora
  expect(listDownloads(db, { corpusId: null, limit: 50, offset: 0 }).total).toBe(6)
})

test('downloads: a blank download_status is excluded, limit 0 returns no items but the total, negatives clamp', () => {
  seedDownloads()
  addWork(20, 1, 'matched', '')
  addWork(21, 1, 'matched', null)
  const none = listDownloads(db, { corpusId: 1, limit: 0, offset: 0 })
  expect(none.items).toEqual([])
  expect(none.total).toBe(4)
  const clamped = listDownloads(db, { corpusId: 1, limit: 2, offset: -5 })
  expect(clamped.items.map((i) => i.id)).toEqual([11, 10])
  expect(listDownloads(db, { corpusId: 1, limit: 10, offset: 10 })).toEqual({ items: [], total: 4 })
})

function addRunTables() {
  db.exec(`
    CREATE TABLE ingest_entries (ingest_source TEXT, corpus_id INTEGER, created_at TEXT, source_pdf TEXT);
    CREATE TABLE ingest_source_metadata (ingest_source TEXT, corpus_id INTEGER, title TEXT, authors TEXT, year INTEGER, doi TEXT, source TEXT, publisher TEXT);
  `)
}
const entry = (src, corpus, at, pdf = null) =>
  db.prepare('INSERT INTO ingest_entries VALUES (?,?,?,?)').run(src, corpus, at, pdf)

test('ingest runs group by source, newest first, limited, with seed_authors parsed', () => {
  addRunTables()
  entry('old.pdf', 1, '2026-01-01', 'old.pdf')
  entry('new.pdf', 1, '2026-02-01', 'new.pdf')
  entry('new.pdf', 1, '2026-02-02', 'new.pdf')
  entry('other.pdf', 2, '2026-03-01')
  db.prepare('INSERT INTO ingest_source_metadata VALUES (?,?,?,?,?,?,?,?)')
    .run('new.pdf', 1, 'Seed', JSON.stringify(['A. Author', 'B. Author']), 2020, '10.1/x', 'upload', 'Pub')
  db.prepare('INSERT INTO ingest_source_metadata VALUES (?,?,?,?,?,?,?,?)')
    .run('old.pdf', 1, 'Old', 'not json', 2019, null, null, null)
  const out = listIngestRuns(db, { corpusId: 1, limit: 5 })
  expect(out.runs.map((r) => r.ingest_source)).toEqual(['new.pdf', 'old.pdf'])
  expect(out.runs[0].seed_authors).toEqual(['A. Author', 'B. Author'])
  expect(out.runs[0]).toMatchObject({ entry_count: 2, last_created_at: '2026-02-02', seed_title: 'Seed', seed_year: 2020 })
  expect(out.runs[1].seed_authors).toBe('not json')
  expect(out.total).toBe(out.runs.length)
  expect(listIngestRuns(db, { corpusId: 1, limit: 1 }).runs).toHaveLength(1)
  expect(listIngestRuns(db, { corpusId: null, limit: 10 }).total).toBe(3)
})

test('ingest runs work without the metadata table', () => {
  db.exec('CREATE TABLE ingest_entries (ingest_source TEXT, corpus_id INTEGER, created_at TEXT, source_pdf TEXT)')
  entry('a.pdf', 1, '2026-01-01', 'a.pdf')
  const out = listIngestRuns(db, { corpusId: 1, limit: 5 })
  expect(out.runs).toEqual([{ ingest_source: 'a.pdf', entry_count: 1, last_created_at: '2026-01-01', source_pdf: 'a.pdf' }])
})

test('ingest runs without the ingest tables returns an empty list', () => {
  const bare = new Database(':memory:')
  expect(listIngestRuns(bare, { corpusId: 1, limit: 5 })).toEqual({ runs: [], total: 0 })
  bare.close()
})

test('listDownloads reads count and page inside one transaction', () => {
  seedStats()
  const seen = []
  const original = db.prepare.bind(db)
  db.prepare = (sql) => {
    seen.push(db.inTransaction)
    return original(sql)
  }
  const result = listDownloads(db, { corpusId: 1 })
  db.prepare = original
  expect(result.total).toBe(result.items.length)
  expect(seen.length).toBeGreaterThan(1)
  expect(seen.every(Boolean)).toBe(true)
  expect(db.inTransaction).toBe(false)
})
