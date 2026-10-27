import { jest } from '@jest/globals'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import Database from 'better-sqlite3'
import { readIngestStats, readWorkStateCounts, workStatus, bucketCounts, resetWorkStateMemo, listDownloads, listIngestRuns, tableExists } from '../src/pipelineReads.js'

let db
beforeEach(() => {
  resetWorkStateMemo()
  db = new Database(':memory:')
  db.exec(`
    CREATE TABLE works (id INTEGER PRIMARY KEY, title TEXT, metadata_status TEXT, download_status TEXT, download_attempt_count INTEGER, downloaded_at TEXT,
                        metadata_lease_expires_at INTEGER, download_lease_expires_at INTEGER);
    CREATE TABLE corpus_works (corpus_id INTEGER, work_id INTEGER);
  `)
})
afterEach(() => db.close())

function addWork(id, corpusId, meta, dl, extra = {}) {
  db.prepare('INSERT INTO works (id, title, metadata_status, download_status, download_attempt_count, downloaded_at, metadata_lease_expires_at, download_lease_expires_at) VALUES (?,?,?,?,?,?,?,?)')
    .run(id, `Work ${id}`, meta, dl, extra.attempts ?? 0, extra.at ?? null, extra.metaLease ?? null, extra.dlLease ?? null)
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
  expect(readIngestStats(db, 1)).toEqual({ raw_pending: 2, matched: 1, queued_download: 2, enriching: 1, downloading: 1, downloaded: 1, enriching_live: 0, downloading_live: 0 })
})

test('ingest stats for an empty corpus and for global scope return zeros / all works', () => {
  seedStats()
  addWork(8, 2, 'matched', 'downloaded')
  expect(readIngestStats(db, 999)).toEqual({ raw_pending: 0, matched: 0, queued_download: 0, enriching: 0, downloading: 0, downloaded: 0, enriching_live: 0, downloading_live: 0 })
  expect(readIngestStats(db, null).downloaded).toBe(2)
  expect(readIngestStats(db, 1).downloaded).toBe(1)
})

test('readWorkStateCounts groups by raw status pair with live-lease sums', () => {
  seedStats()
  const now = Math.floor(Date.now() / 1000)
  addWork(8, 1, 'in_progress', null, { metaLease: now + 600 })
  addWork(9, 1, 'matched', 'in_progress', { dlLease: now + 600 })
  addWork(10, 2, 'matched', 'downloaded')
  const byPair = (rows) => Object.fromEntries(rows.map((r) => [`${r.metadata_status}|${r.download_status}`, [r.n, r.live_meta, r.live_dl]]))
  expect(byPair(readWorkStateCounts(db, 1, { nowSec: now }))).toEqual({
    'pending|null': [1, 0, 0],
    'in_progress|null': [2, 1, 0],
    'matched|not_requested': [1, 0, 0],
    'matched|queued': [1, 0, 0],
    'matched|in_progress': [2, 0, 1],
    'matched|downloaded': [1, 0, 0],
    'failed|null': [1, 0, 0],
  })
  // Global scope has no corpus filter; a corpus without members has no rows.
  expect(byPair(readWorkStateCounts(db, null, { nowSec: now }))['matched|downloaded']).toEqual([2, 0, 0])
  expect(readWorkStateCounts(db, 999, { nowSec: now })).toEqual([])
})

test('bucketCounts derives every consumer\'s numbers from the grouped rows', () => {
  seedStats()
  addWork(8, 1, 'matched', 'failed')
  expect(bucketCounts(readWorkStateCounts(db, 1))).toEqual({
    status_counts: { raw: 1, enriching: 1, matched: 1, queued_download: 2, downloaded: 1, failed_enrichment: 1, failed_download: 1 },
    raw_pending_total: 2, pending_only: 1, in_progress: 1, matched_not_requested: 1, queued: 1, downloading: 1,
    downloaded: 1, failed_meta: 1, failed_dl: 1, enriching_live: 0, downloading_live: 0,
  })
})

test('readIngestStats equals the old numbers, derived from the grouped read', () => {
  seedStats()
  expect(readIngestStats(db, 1)).toEqual({ raw_pending: 2, matched: 1, queued_download: 2, enriching: 1, downloading: 1, downloaded: 1, enriching_live: 0, downloading_live: 0 })
})

test('workStatus buckets like STATUS_SQL, including padded/odd-case values', () => {
  expect(workStatus(' Matched ', 'not_requested')).toBe('matched')
  expect(workStatus('x', ' downloaded ')).toBe('downloaded')
  expect(workStatus('matched', '\u3000QUEUED\t')).toBe('queued_download')
  expect(workStatus('matched', 'In_Progress')).toBe('queued_download')
  expect(workStatus('matched', ' failed')).toBe('failed_download')
  expect(workStatus('\xa0in_progress\n', null)).toBe('enriching')
  expect(workStatus('FAILED', '')).toBe('failed_enrichment')
  expect(workStatus(null, null)).toBe('raw')
  expect(workStatus('pending', 'not_requested')).toBe('raw')
  // U+FEFF is not in Python's whitespace set, so STATUS_SQL keeps it.
  expect(workStatus('\ufeffmatched', 'not_requested')).toBe('raw')
})

test('the memo is per corpus, hits while nothing changed and misses right after a same-connection write', () => {
  seedStats()
  const grouped = []
  const original = db.prepare.bind(db)
  db.prepare = (sql) => {
    if (sql.includes('GROUP BY 1, 2')) grouped.push(sql)
    return original(sql)
  }
  expect(readIngestStats(db, 1).raw_pending).toBe(2)
  expect(readIngestStats(db, 1).raw_pending).toBe(2)
  expect(grouped).toHaveLength(1)
  addWork(8, 2, 'pending', null)
  expect(readIngestStats(db, 2).raw_pending).toBe(1)
  // A write on this same connection leaves data_version alone but bumps total_changes().
  addWork(9, 1, 'pending', null)
  expect(readIngestStats(db, 1).raw_pending).toBe(3)
  db.prepare("DELETE FROM corpus_works WHERE work_id = 9").run()
  expect(readIngestStats(db, 1).raw_pending).toBe(2)
  db.prepare = original
})

test('the memo expires after 1 s even without writes (lease expiry changes the live counts)', () => {
  const realNow = Date.now()
  addWork(1, 1, 'in_progress', null, { metaLease: Math.floor(realNow / 1000) + 2 })
  const clock = jest.spyOn(Date, 'now').mockReturnValue(realNow)
  try {
    expect(readIngestStats(db, 1).enriching_live).toBe(1)
    clock.mockReturnValue(realNow + 999)
    expect(readIngestStats(db, 1).enriching_live).toBe(1)
    clock.mockReturnValue(realNow + 5000)
    expect(readIngestStats(db, 1).enriching_live).toBe(0)
  } finally {
    clock.mockRestore()
  }
})

test('the memo misses as soon as another connection commits', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dt-state-memo-'))
  const file = path.join(dir, 'w.db')
  const a = new Database(file)
  const b = new Database(file)
  try {
    a.exec('CREATE TABLE works (id INTEGER PRIMARY KEY, metadata_status TEXT, download_status TEXT); CREATE TABLE corpus_works (corpus_id INTEGER, work_id INTEGER);')
    a.prepare("INSERT INTO works VALUES (1, 'pending', 'not_requested')").run()
    expect(readIngestStats(a, null).raw_pending).toBe(1)
    b.prepare("INSERT INTO works VALUES (2, 'pending', 'not_requested')").run()
    expect(readIngestStats(a, null).raw_pending).toBe(2)
  } finally {
    a.close()
    b.close()
    fs.rmSync(dir, { recursive: true, force: true })
  }
})

function seedDownloads() {
  addWork(10, 1, 'matched', 'queued')
  addWork(11, 1, 'matched', 'in_progress', { attempts: 2 })
  addWork(12, 1, 'matched', 'downloaded', { at: '2026-01-01T10:00:00' })
  addWork(13, 1, 'matched', 'downloaded', { at: '2026-03-01T10:00:00' })
  addWork(14, 1, 'matched', 'not_requested')
  addWork(15, 2, 'matched', 'queued')
}

test('ingest stats: *_live counts only in_progress rows whose lease has not expired', () => {
  const now = Math.floor(Date.now() / 1000)
  addWork(1, 1, 'in_progress', null, { metaLease: now + 600 })        // being enriched right now
  addWork(2, 1, 'in_progress', null, { metaLease: now - 5 })          // abandoned claim, lease expired
  addWork(3, 1, 'in_progress', null)                                   // legacy row without a lease
  addWork(4, 1, 'matched', 'in_progress', { dlLease: now + 600 })
  addWork(5, 1, 'matched', 'in_progress', { dlLease: now - 5 })
  const stats = readIngestStats(db, 1)
  expect(stats.enriching).toBe(3)
  expect(stats.downloading).toBe(2)
  expect(stats.enriching_live).toBe(1)
  expect(stats.downloading_live).toBe(1)
})

test('ingest stats without lease columns report zero live activity', () => {
  db.exec('DROP TABLE works; CREATE TABLE works (id INTEGER PRIMARY KEY, title TEXT, metadata_status TEXT, download_status TEXT, download_attempt_count INTEGER, downloaded_at TEXT)')
  db.prepare("INSERT INTO works (id, title, metadata_status, download_status) VALUES (1, 'W', 'in_progress', NULL)").run()
  db.prepare('INSERT INTO corpus_works VALUES (1, 1)').run()
  expect(readIngestStats(db, 1)).toMatchObject({ enriching: 1, enriching_live: 0, downloading_live: 0 })
})

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
