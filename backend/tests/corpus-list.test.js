import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { execFileSync } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import Database from 'better-sqlite3'
import { listCorpus, CORPUS_SORT_KEYS } from '../src/corpusList.js'

const HERE = path.dirname(fileURLToPath(import.meta.url))
const SCRIPT = path.join(HERE, '..', 'scripts', 'corpus_list.py')
const FIXTURE = path.join(HERE, 'fixtures', 'corpus-list.sql')
const PY = process.env.RAG_FEEDER_PYTHON || '/home/jay/DT/venv/bin/python'
// A bare name like `python3` (CI) resolves through PATH, so stat-ing it would
// skip every golden comparison; probe the interpreter instead.
function pythonRuns(exe) {
  try {
    execFileSync(exe, ['-c', 'import sqlite3'], { stdio: 'ignore' })
    return true
  } catch {
    return false
  }
}
const HAS_PY = pythonRuns(PY)

let tempDir
let dbPath
let db

beforeAll(() => {
  tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'dt-corpus-list-'))
  dbPath = path.join(tempDir, 'corpus.db')
  db = new Database(dbPath)
  db.exec(fs.readFileSync(FIXTURE, 'utf8'))
})

afterAll(() => {
  db?.close()
  if (tempDir) fs.rmSync(tempDir, { recursive: true, force: true })
})

function viaScript(args) {
  // corpus_list.py answers with stub data whenever RAG_FEEDER_STUB is set at all.
  const env = { ...process.env }
  delete env.RAG_FEEDER_STUB
  return JSON.parse(execFileSync(PY, [SCRIPT, '--db-path', dbPath, ...args], { encoding: 'utf8', env }))
}

// Maps the CLI-style argument list of corpus_list.py onto listCorpus options.
function parseArgs(args, { corpusId }) {
  const opts = { corpusId, limit: 200, offset: 0, q: '', sort: '' }
  for (let i = 0; i < args.length; i += 2) {
    const value = args[i + 1]
    if (args[i] === '--sort') opts.sort = value
    else if (args[i] === '--q') opts.q = value
    else if (args[i] === '--limit') opts.limit = Number(value)
    else if (args[i] === '--offset') opts.offset = Number(value)
    else throw new Error(`unknown arg ${args[i]}`)
  }
  return opts
}

const CASES = [
  [], ['--sort', 'title:asc'], ['--sort', 'authors:asc'], ['--sort', 'authors:desc'], ['--sort', 'year:desc'],
  ['--sort', 'added:desc'], ['--sort', 'source:asc'], ['--sort', 'seed:asc'], ['--sort', 'metadata:asc'],
  ['--sort', 'download:desc'], ['--q', 'labour'], ['--q', '100%'], ['--q', 'a_b'], ['--limit', '5', '--offset', '5'],
  ['--limit', '0'], ['--offset', '999'], ['--q', 'LABOUR', '--sort', 'year:asc', '--limit', '3'],
  // Beyond the brief: the remaining directions, a bare column (desc), an
  // unknown column, non-ASCII case folding and a needle spanning two fields.
  ['--sort', 'title:desc'], ['--sort', 'year:asc'], ['--sort', 'added:asc'], ['--sort', 'source:desc'],
  ['--sort', 'seed:desc'], ['--sort', 'metadata:desc'], ['--sort', 'download:asc'], ['--sort', 'title'],
  ['--sort', 'bogus:asc'], ['--q', 'ÅNGSTRÖM'], ['--q', 'éclair'], ['--q', 'employment ["smith'],
  ['--q', 'labour', '--sort', 'seed:asc', '--limit', '2', '--offset', '1'], ['--q', 'no such needle'],
]

const goldenTest = HAS_PY ? test : test.skip
const GOLDEN = CASES.map((args) => [args])
const goldenEach = HAS_PY ? test.each(GOLDEN) : test.skip.each(GOLDEN)

goldenEach('matches corpus_list.py for %j', (args) => {
  const expected = viaScript(['--corpus-id', '1', ...args])
  const actual = listCorpus(db, parseArgs(args, { corpusId: 1 }))
  expect(actual).toEqual(expected)
})

goldenTest('no corpus selected lists every work and uses created_at for added_at', () => {
  const actual = listCorpus(db, { corpusId: null, limit: 200, offset: 0, q: '', sort: '' })
  expect(actual).toEqual(viaScript([]))
  expect(actual.total).toBe(18)
  expect(actual.items.find((i) => i.id === 1).added_at).toBe('2025-01-01 00:00:01')
})

goldenTest('no corpus selected, sorted by seed, matches the script', () => {
  expect(listCorpus(db, { corpusId: null, limit: 200, offset: 0, q: '', sort: 'seed:asc' }))
    .toEqual(viaScript(['--sort', 'seed:asc']))
})

test('treats % and _ in q literally', () => {
  expect(listCorpus(db, { corpusId: 1, limit: 200, offset: 0, q: '100%', sort: '' }).items.map((i) => i.id)).toEqual([1])
  expect(listCorpus(db, { corpusId: 1, limit: 200, offset: 0, q: 'a_b', sort: '' }).items.map((i) => i.id)).toEqual([2])
})

test('unknown sort falls back to the default order', () => {
  const fallback = listCorpus(db, { corpusId: 1, limit: 200, offset: 0, q: '', sort: 'bogus:asc' })
  const plain = listCorpus(db, { corpusId: 1, limit: 200, offset: 0, q: '', sort: '' })
  expect(fallback).toEqual(plain)
  // Default: year desc (non-numeric/NULL count as 0), then id desc.
  expect(plain.items.map((i) => i.id)).toEqual([6, 5, 8, 7, 1, 2, 10, 9, 13, 12, 14, 11, 4, 3])
})

test('counts and labels for the fixture corpus', () => {
  const result = listCorpus(db, { corpusId: 1, limit: 200, offset: 0, q: '', sort: '' })
  expect(result.total).toBe(14)
  expect(result.status_counts).toEqual({
    matched: 2, downloaded: 1, queued_download: 2, enriching: 1, failed_download: 1, raw: 6, failed_enrichment: 1,
  })
  expect(result.stage_totals).toEqual({ raw: 6, metadata: 5, downloaded: 1, failed_enrichment: 1, failed_download: 1 })
  const byId = Object.fromEntries(result.items.map((i) => [i.id, i]))
  expect(byId[1].source_label).toBe('Seed Doc A — Alice Author')
  expect(byId[9].source_label).toBe('seedB — X One et al.')
  // pdfC only has metadata in corpus 2, so corpus 1 falls back to the raw key.
  expect(byId[10]).toMatchObject({ source_label: 'pdfC', source_type: 'source', source_key: 'pdfC' })
  expect(byId[12].source_label).toBe('Some Paper.v2')
  expect(byId[4].added_at).toBeNull()
  expect(CORPUS_SORT_KEYS).toEqual(['title', 'authors', 'year', 'added', 'source', 'seed', 'metadata', 'download'])
})

// --- Consistency and large pages -------------------------------------------

// A fresh copy of the fixture, so mutating tests leave the golden DB alone.
function freshDb(file = ':memory:') {
  const copy = new Database(file)
  if (file !== ':memory:') copy.pragma('journal_mode = WAL')
  copy.exec(fs.readFileSync(FIXTURE, 'utf8'))
  return copy
}

// Runs `hook` right before the page query (the only one selecting file_path)
// is prepared, and records db.inTransaction for every prepared statement.
function instrument(target, hook) {
  const seen = []
  const original = target.prepare.bind(target)
  target.prepare = (sql) => {
    seen.push(target.inTransaction)
    if (hook && sql.includes('w.file_path')) hook()
    return original(sql)
  }
  return seen
}

test('every statement of a listing runs inside one read transaction', () => {
  const copy = freshDb()
  try {
    for (const sort of ['', 'seed:asc']) {
      const seen = instrument(copy)
      listCorpus(copy, { corpusId: 1, limit: 200, offset: 0, q: '', sort })
      expect(seen.length).toBeGreaterThan(3)
      expect(seen.every(Boolean)).toBe(true)
    }
    expect(copy.inTransaction).toBe(false)
  } finally {
    copy.close()
  }
})

test.each(['', 'seed:asc'])('a removal committed mid-listing does not leak in (sort %j)', (sort) => {
  const file = path.join(tempDir, `snapshot-${sort || 'default'}.db`)
  const copy = freshDb(file)
  const other = new Database(file)
  try {
    instrument(copy, () => other.prepare('DELETE FROM corpus_works WHERE corpus_id = 1 AND work_id = 1').run())
    const result = listCorpus(copy, { corpusId: 1, limit: 200, offset: 0, q: '', sort })
    // The removal committed (another connection sees it) but this listing
    // read one snapshot: counts and rows agree and still include work 1.
    expect(other.prepare('SELECT COUNT(*) AS n FROM corpus_works WHERE corpus_id = 1').get().n).toBe(13)
    expect(result.total).toBe(14)
    expect(result.items).toHaveLength(14)
    expect(result.items.map((i) => i.id)).toContain(1)
    expect(listCorpus(copy, { corpusId: 1, limit: 200, offset: 0, q: '', sort }).total).toBe(13)
  } finally {
    other.close()
    copy.close()
  }
})

test('seed sort skips a row that vanished before the page fetch instead of throwing', () => {
  const copy = freshDb()
  try {
    // Same connection, so the delete is visible inside the transaction: this
    // exercises the filter(Boolean) fallback.
    instrument(copy, () => copy.exec('DELETE FROM corpus_works WHERE corpus_id = 1 AND work_id = 1'))
    const result = listCorpus(copy, { corpusId: 1, limit: 200, offset: 0, q: '', sort: 'seed:asc' })
    expect(result.items).toHaveLength(13)
    expect(result.items.every((i) => i && i.id !== 1)).toBe(true)
  } finally {
    copy.close()
  }
})

describe('a corpus larger than the bound-parameter limit', () => {
  // SQLite's default SQLITE_MAX_VARIABLE_NUMBER is 32766; a page this size
  // would overflow a one-placeholder-per-id IN list.
  const N = 33000
  const PAGE = 32900
  let bigPath
  let big
  beforeAll(() => {
    bigPath = path.join(tempDir, 'big.db')
    big = new Database(bigPath)
    big.exec(fs.readFileSync(FIXTURE, 'utf8'))
    big.exec('DELETE FROM corpus_works; DELETE FROM seed_candidates_in_corpus;')
    const insertRun = big.prepare('INSERT INTO search_runs (id, query) VALUES (?, ?)')
    for (let r = 100; r < 140; r += 1) insertRun.run(r, `query ${String((r * 7) % 40).padStart(2, '0')}`)
    const insertWork = big.prepare(`INSERT INTO works (id, title, year, metadata_status, download_status, origin_key, run_id, created_at)
      VALUES (?, ?, ?, 'pending', 'not_requested', ?, ?, '2025-01-01')`)
    const insertMember = big.prepare('INSERT INTO corpus_works (corpus_id, work_id, added_at) VALUES (1, ?, ?)')
    big.transaction(() => {
      for (let i = 0; i < N; i += 1) {
        const id = 1000 + i
        insertWork.run(id, `Work ${id}`, 1950 + (i % 70), i % 3 === 0 ? `search:${100 + (i % 40)}` : null, i % 3 === 1 ? 100 + (i % 40) : null)
        insertMember.run(id, '2026-01-01')
      }
    })()
  })
  afterAll(() => big?.close())

  test('seed sort returns a 32,900-row page in label order', () => {
    const result = listCorpus(big, { corpusId: 1, limit: PAGE, offset: 0, q: '', sort: 'seed:asc' })
    expect(result.total).toBe(N)
    expect(result.items).toHaveLength(PAGE)
    for (let i = 1; i < result.items.length; i += 1) {
      const a = result.items[i - 1]
      const b = result.items[i]
      const ka = a.source_label.toLowerCase()
      const kb = b.source_label.toLowerCase()
      if (!(ka < kb || (ka === kb && a.id > b.id))) throw new Error(`out of order at ${i}: ${a.id} ${b.id}`)
    }
  }, 30_000)

  goldenTest('seed sort on the large corpus matches corpus_list.py', () => {
    const env = { ...process.env }
    delete env.RAG_FEEDER_STUB
    const expected = JSON.parse(execFileSync(PY, [SCRIPT, '--db-path', bigPath, '--corpus-id', '1', '--sort', 'seed:asc', '--limit', String(PAGE)], { encoding: 'utf8', env, maxBuffer: 1 << 28 }))
    expect(listCorpus(big, { corpusId: 1, limit: PAGE, offset: 0, q: '', sort: 'seed:asc' })).toEqual(expected)
  }, 60_000)
})
