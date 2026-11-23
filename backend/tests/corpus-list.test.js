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

// corpus_list.py has no stage_total; with no stage it equals total.
const withStageTotal = (expected) => ({ ...expected, stage_total: expected.total })

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
// `extra` passes listCorpus-only options (smallCorpusLimit).
function parseArgs(args, { corpusId, ...extra }) {
  const opts = { corpusId, limit: 200, offset: 0, q: '', sort: '', ...extra }
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
  expect(actual).toEqual(withStageTotal(expected))
})

goldenTest('no corpus selected lists every work and uses created_at for added_at', () => {
  const actual = listCorpus(db, { corpusId: null, limit: 200, offset: 0, q: '', sort: '' })
  expect(actual).toEqual(withStageTotal(viaScript([])))
  expect(actual.total).toBe(18)
  expect(actual.items.find((i) => i.id === 1).added_at).toBe('2025-01-01 00:00:01')
})

goldenTest('no corpus selected, sorted by seed, matches the script', () => {
  expect(listCorpus(db, { corpusId: null, limit: 200, offset: 0, q: '', sort: 'seed:asc' }))
    .toEqual(withStageTotal(viaScript(['--sort', 'seed:asc'])))
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

test('status counts come from the grouped work-state read, not a per-row STATUS_SQL GROUP BY', () => {
  const copy = freshDb()
  try {
    const seen = []
    const original = copy.prepare.bind(copy)
    copy.prepare = (sql) => {
      seen.push(sql)
      return original(sql)
    }
    const result = listCorpus(copy, { corpusId: 1, limit: 200, offset: 0, q: '', sort: '' })
    expect(result.total).toBe(14)
    expect(seen.some((sql) => /GROUP BY/.test(sql) && sql.includes('lower(trim('))).toBe(false)
    // A q filter needs the search UDF, so it keeps the filtered GROUP BY.
    seen.length = 0
    listCorpus(copy, { corpusId: 1, limit: 200, offset: 0, q: 'labour', sort: '' })
    expect(seen.some((sql) => /GROUP BY/.test(sql) && sql.includes('lower(trim('))).toBe(true)
  } finally {
    copy.close()
  }
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
// is prepared, and records db.inTransaction for every prepared statement but
// the memo's committed-state note, which runs just before the transaction.
function instrument(target, hook) {
  const seen = []
  const original = target.prepare.bind(target)
  target.prepare = (sql) => {
    if (sql !== 'SELECT total_changes() AS n') seen.push(target.inTransaction)
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
    expect(listCorpus(big, { corpusId: 1, limit: PAGE, offset: 0, q: '', sort: 'seed:asc' })).toEqual(withStageTotal(expected))
  }, 60_000)
})

// --- Index-served sorts ------------------------------------------------------

const INT_FIXTURE = path.join(HERE, 'fixtures', 'corpus-list-int-years.sql')

// Records the page query (the only one selecting file_path) and its bound
// parameters, so the test can EXPLAIN exactly what listCorpus executed.
function capturePageQuery(target) {
  const captured = {}
  const original = target.prepare.bind(target)
  target.prepare = (sql) => {
    const stmt = original(sql)
    if (sql.includes('w.file_path')) {
      const all = stmt.all.bind(stmt)
      stmt.all = (...args) => {
        Object.assign(captured, { sql, args })
        return all(...args)
      }
    }
    return stmt
  }
  return captured
}

function pagePlan(target, page) {
  const explain = target.prepare.call(target, `EXPLAIN QUERY PLAN ${page.sql}`)
  return explain.all(...page.args).map((row) => row.detail).join('\n')
}

describe('index-served default and added sorts', () => {
  let intPath
  let intDb
  beforeAll(() => {
    intPath = path.join(tempDir, 'int-years.db')
    intDb = new Database(intPath)
    intDb.exec(fs.readFileSync(INT_FIXTURE, 'utf8'))
  })
  afterAll(() => intDb?.close())

  const viaScriptOn = (file, args) => {
    const env = { ...process.env }
    delete env.RAG_FEEDER_STUB
    return JSON.parse(execFileSync(PY, [SCRIPT, '--db-path', file, ...args], { encoding: 'utf8', env }))
  }
  const PAGES = [[], ['--q', 'labour'], ['--limit', '5'], ['--limit', '5', '--offset', '5'], ['--limit', '4', '--offset', '11'], ['--offset', '999']]

  // smallCorpusLimit: 0 classes the 14-member fixture corpus as large.
  goldenTest('with integer/NULL years a large corpus\'s default sort is served by idx_works_year_id and matches the Python script', () => {
    const page = capturePageQuery(intDb)
    for (const args of PAGES) {
      expect(listCorpus(intDb, parseArgs(args, { corpusId: 1, smallCorpusLimit: 0 }))).toEqual(withStageTotal(viaScriptOn(intPath, ['--corpus-id', '1', ...args])))
      expect(page.sql).not.toContain('dt_corpus_int')
      expect(page.sql).toContain('WHERE EXISTS')
      const plan = pagePlan(intDb, page)
      expect(plan).toContain('idx_works_year_id')
      expect(plan).not.toContain('TEMP B-TREE')
    }
    // NULL year (work 4) sorts last, as normalize_year's 0 does.
    expect(listCorpus(intDb, { corpusId: 1, smallCorpusLimit: 0 }).items.map((i) => i.id)).toEqual([6, 5, 8, 7, 1, 2, 10, 9, 11, 13, 12, 14, 4, 3])
    // No corpus selected walks the index regardless of corpus sizes.
    expect(listCorpus(intDb, { corpusId: null })).toEqual(withStageTotal(viaScriptOn(intPath, [])))
    expect(page.sql).not.toContain('dt_corpus_int')
    delete intDb.prepare
  })

  goldenTest('with integer/NULL years a small corpus\'s default sort takes the JOIN + YEAR_SQL path and matches the Python script', () => {
    const page = capturePageQuery(intDb)
    for (const args of PAGES) {
      expect(listCorpus(intDb, parseArgs(args, { corpusId: 1 }))).toEqual(withStageTotal(viaScriptOn(intPath, ['--corpus-id', '1', ...args])))
      expect(page.sql).toContain('dt_corpus_int')
      expect(page.sql).toContain('JOIN corpus_works cw ON cw.work_id = w.id')
      expect(page.sql).not.toContain('EXISTS')
    }
    expect(listCorpus(intDb, { corpusId: 1 }).items.map((i) => i.id)).toEqual([6, 5, 8, 7, 1, 2, 10, 9, 11, 13, 12, 14, 4, 3])
    delete intDb.prepare
  })

  goldenTest('a year of 0 or below takes the CASE path even for a large corpus, and matches the Python script', () => {
    // Raw `ORDER BY w.year DESC` puts 0 and -5 above NULL; normalize_year ties
    // NULL with 0 and puts -5 below both.
    const zeroPath = path.join(tempDir, 'zero-year.db')
    const zero = new Database(zeroPath)
    try {
      zero.exec(fs.readFileSync(INT_FIXTURE, 'utf8'))
      zero.prepare('UPDATE works SET year = 0 WHERE id = 13').run()
      zero.prepare('UPDATE works SET year = -5 WHERE id = 12').run()
      const page = capturePageQuery(zero)
      for (const args of PAGES) {
        expect(listCorpus(zero, parseArgs(args, { corpusId: 1, smallCorpusLimit: 0 }))).toEqual(withStageTotal(viaScriptOn(zeroPath, ['--corpus-id', '1', ...args])))
        expect(page.sql).toContain('dt_corpus_int')
      }
      expect(listCorpus(zero, { corpusId: null })).toEqual(withStageTotal(viaScriptOn(zeroPath, [])))
      const ids = listCorpus(zero, { corpusId: 1, smallCorpusLimit: 0 }).items.map((i) => i.id)
      // 13 (year 0) ties with NULL-year 4 and 3, id desc; -5 (work 12) is last.
      expect(ids.slice(-4)).toEqual([13, 4, 3, 12])
    } finally {
      zero.close()
    }
  })

  goldenTest('with text years the CASE path is used and the golden output is unchanged', () => {
    const copy = freshDb()
    try {
      const page = capturePageQuery(copy)
      for (const args of PAGES) {
        expect(listCorpus(copy, parseArgs(args, { corpusId: 1 }))).toEqual(withStageTotal(viaScript(['--corpus-id', '1', ...args])))
        expect(page.sql).toContain('dt_corpus_int')
      }
    } finally {
      copy.close()
    }
  })

  goldenTest('a REAL year takes the CASE path even for a large corpus, and matches the Python script', () => {
    // int(2005.5) is 2005, so work 7 ties with work 8 and the id tie-break puts
    // 8 first; a raw `ORDER BY w.year` would put 7 first. smallCorpusLimit: 0
    // skips the small-corpus JOIN branch so the year probe itself is what
    // keeps the raw order off.
    const realPath = path.join(tempDir, 'real-year.db')
    const real = new Database(realPath)
    try {
      real.exec(fs.readFileSync(INT_FIXTURE, 'utf8'))
      real.prepare('UPDATE works SET year = 2005.5 WHERE id = 7').run()
      expect(real.prepare('SELECT typeof(year) AS t FROM works WHERE id = 7').get().t).toBe('real')
      const page = capturePageQuery(real)
      for (const args of PAGES) {
        expect(listCorpus(real, parseArgs(args, { corpusId: 1, smallCorpusLimit: 0 }))).toEqual(withStageTotal(viaScriptOn(realPath, ['--corpus-id', '1', ...args])))
        expect(page.sql).toContain('dt_corpus_int')
      }
      expect(listCorpus(real, { corpusId: null })).toEqual(withStageTotal(viaScriptOn(realPath, [])))
      const ids = listCorpus(real, { corpusId: 1, smallCorpusLimit: 0 }).items.map((i) => i.id)
      expect(ids.indexOf(8)).toBeLessThan(ids.indexOf(7))
    } finally {
      real.close()
    }
  })

  goldenTest('a member with added_at = \'\' sends the added sort to the CASE path, tied with NULL as the script ties them', () => {
    const blankPath = path.join(tempDir, 'blank-added.db')
    const blank = new Database(blankPath)
    try {
      blank.exec(fs.readFileSync(INT_FIXTURE, 'utf8'))
      blank.prepare("UPDATE corpus_works SET added_at = '' WHERE corpus_id = 1 AND work_id = 7").run()
      const page = capturePageQuery(blank)
      for (const sort of ['added:asc', 'added:desc']) {
        for (const args of PAGES) {
          const full = ['--sort', sort, ...args]
          expect(listCorpus(blank, parseArgs(full, { corpusId: 1 }))).toEqual(withStageTotal(viaScriptOn(blankPath, ['--corpus-id', '1', ...full])))
          expect(page.sql).toContain("cw.added_at = ''")
        }
      }
      // '' (work 7) and NULL (work 4) tie, so id desc orders them in both directions.
      const asc = listCorpus(blank, { corpusId: 1, sort: 'added:asc' }).items.map((i) => i.id)
      const desc = listCorpus(blank, { corpusId: 1, sort: 'added:desc' }).items.map((i) => i.id)
      expect(asc.slice(0, 2)).toEqual([7, 4])
      expect(desc.slice(-2)).toEqual([7, 4])
      // Corpus 2 has no blank member, so it keeps the index path.
      listCorpus(blank, { corpusId: 2, sort: 'added:desc' })
      expect(pagePlan(blank, page)).toContain('idx_corpus_works_added')
    } finally {
      blank.close()
    }
  })

  goldenTest('added sort uses idx_corpus_works_added and keeps NULL ordering', () => {
    for (const [target, file] of [[intDb, intPath], [db, dbPath]]) {
      const page = capturePageQuery(target)
      for (const sort of ['added:asc', 'added:desc', 'added']) {
        for (const args of PAGES) {
          const full = ['--sort', sort, ...args]
          expect(listCorpus(target, parseArgs(full, { corpusId: 1 }))).toEqual(withStageTotal(viaScriptOn(file, ['--corpus-id', '1', ...full])))
          const plan = pagePlan(target, page)
          expect(plan).toContain('idx_corpus_works_added')
          // Rows stream in index order: no full sort of the membership, and
          // none at all when the tie-break runs the index's way (desc).
          expect(plan).not.toContain('TEMP B-TREE FOR ORDER BY')
          if (sort !== 'added:asc') expect(plan).not.toContain('TEMP B-TREE')
        }
      }
      // Work 4 has a NULL added_at: first ascending, last descending.
      const asc = listCorpus(target, { corpusId: 1, sort: 'added:asc' }).items.map((i) => i.id)
      const desc = listCorpus(target, { corpusId: 1, sort: 'added:desc' }).items.map((i) => i.id)
      expect(asc[0]).toBe(4)
      expect(desc[desc.length - 1]).toBe(4)
      delete target.prepare
    }
  })
})

// --- Stage filter (D13) --------------------------------------------------------

const STAGES = {
  raw: ['raw'],
  metadata: ['matched', 'queued_download', 'enriching'],
  downloaded: ['downloaded'],
  failed_enrichment: ['failed_enrichment'],
  failed_download: ['failed_download'],
}
const STAGE_TERM = (stage) => `IN (${STAGES[stage].map((s) => `'${s}'`).join(', ')})`

describe('the stage filter', () => {
  let copy
  beforeAll(() => { copy = freshDb() })
  afterAll(() => copy?.close())

  test('a stage returns exactly the unfiltered rows of that stage, paged, with its total', () => {
    const base = listCorpus(copy, { corpusId: 1, limit: 1000 })
    for (const [stage, statuses] of Object.entries(STAGES)) {
      const want = base.items.filter((i) => statuses.includes(i.status)).map((i) => i.id)
      expect(want.length).toBeGreaterThan(0)
      const got = listCorpus(copy, { corpusId: 1, limit: 1000, stage })
      expect(got.items.map((i) => i.id)).toEqual(want)
      expect(got.stage_total).toBe(want.length)
      expect(got.total).toBe(base.total)
      expect(got.stage_totals).toEqual(base.stage_totals)
      expect(got.status_counts).toEqual(base.status_counts)
      expect(listCorpus(copy, { corpusId: 1, limit: 2, offset: 1, stage }).items.map((i) => i.id)).toEqual(want.slice(1, 3))
    }
  })

  test('with q and a stage, the counts are the q counts and only the page is staged', () => {
    const qOnly = listCorpus(copy, { corpusId: 1, limit: 1000, q: 'labour' })
    expect(qOnly.total).toBeGreaterThan(0)
    for (const stage of Object.keys(STAGES)) {
      const got = listCorpus(copy, { corpusId: 1, limit: 1000, q: 'labour', stage })
      expect(got.total).toBe(qOnly.total)
      expect(got.stage_totals).toEqual(qOnly.stage_totals)
      expect(got.status_counts).toEqual(qOnly.status_counts)
      expect(got.stage_total).toBe(qOnly.stage_totals[stage])
      expect(got.items.map((i) => i.id)).toEqual(qOnly.items.filter((i) => STAGES[stage].includes(i.status)).map((i) => i.id))
    }
  })

  test('stage combines with q and with every sort', () => {
    for (const corpusId of [1, null]) {
      for (const column of CORPUS_SORT_KEYS) {
        for (const sort of [`${column}:asc`, `${column}:desc`]) {
          for (const q of ['', 'labour']) {
            const base = listCorpus(copy, { corpusId, limit: 1000, q, sort })
            for (const [stage, statuses] of Object.entries(STAGES)) {
              const want = base.items.filter((i) => statuses.includes(i.status)).map((i) => i.id)
              const got = listCorpus(copy, { corpusId, limit: 1000, q, sort, stage })
              expect([corpusId, sort, q, stage, got.items.map((i) => i.id)]).toEqual([corpusId, sort, q, stage, want])
              expect(got.stage_total).toBe(want.length)
            }
          }
        }
      }
    }
  })

  test('an unknown stage means all stages', () => {
    const none = listCorpus(copy, { corpusId: 1, limit: 1000 })
    for (const stage of ['bogus', 'all', '', 'RAW ', null, undefined, 'constructor', '__proto__']) {
      const got = listCorpus(copy, { corpusId: 1, limit: 1000, stage })
      expect(got).toEqual(none)
      expect(got.stage_total).toBe(got.total)
    }
  })

  test('the stage term binds no parameter', () => {
    const page = capturePageQuery(copy)
    listCorpus(copy, { corpusId: 1, stage: 'metadata' })
    expect(page.sql).toContain(STAGE_TERM('metadata'))
    expect(page.args).toEqual([1, 200, 0])
    delete copy.prepare
  })
})

test('the empty schema answers stage_total 0', () => {
  const empty = new Database(':memory:')
  try {
    expect(listCorpus(empty, { stage: 'raw' })).toEqual({
      items: [], total: 0, stage_total: 0,
      stage_totals: { raw: 0, metadata: 0, downloaded: 0, failed_enrichment: 0, failed_download: 0 },
      status_counts: {},
    })
  } finally {
    empty.close()
  }
})

test('the large-corpus year-index path keeps both q and stage', () => {
  const intDb = new Database(':memory:')
  try {
    intDb.exec(fs.readFileSync(INT_FIXTURE, 'utf8'))
    for (const q of ['', 'labour']) {
      // The reference: the same listing unstaged, filtered in JS.
      const base = listCorpus(intDb, { corpusId: 1, limit: 1000, q, smallCorpusLimit: 0 })
      const page = capturePageQuery(intDb)
      for (const [stage, statuses] of Object.entries(STAGES)) {
        const want = base.items.filter((i) => statuses.includes(i.status)).map((i) => i.id)
        const got = listCorpus(intDb, { corpusId: 1, limit: 1000, q, stage, smallCorpusLimit: 0 })
        expect(got.items.map((i) => i.id)).toEqual(want)
        expect(page.sql).toContain('WHERE EXISTS')
        expect(page.sql).toContain('ORDER BY w.year DESC, w.id DESC')
        expect(page.sql).toContain(STAGE_TERM(stage))
        if (q) expect(page.sql).toContain('dt_corpus_search_text')
        else expect(page.sql).not.toContain('dt_corpus_search_text')
        expect(pagePlan(intDb, page)).toContain('idx_works_year_id')
      }
      delete intDb.prepare
    }
  } finally {
    intDb.close()
  }
})
