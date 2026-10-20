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
const HAS_PY = fs.existsSync(PY)

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
  expect(actual.total).toBe(17)
  expect(actual.items.find((i) => i.id === 1).added_at).toBe('2025-01-01 00:00:01')
})

goldenTest('no corpus selected, sorted by seed, matches the script', () => {
  expect(listCorpus(db, { corpusId: null, limit: 200, offset: 0, q: '', sort: 'seed:asc' }))
    .toEqual(viaScript(['--sort', 'seed:asc']))
})

test('escapes LIKE metacharacters in q', () => {
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
