import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import Database from 'better-sqlite3'
import request from 'supertest'
import { ensureAuthSchema, createApp } from '../src/app.js'
import { ensureSeedSchema } from '../src/seed.js'

const NEW = ['idx_works_state', 'idx_works_year_id', 'idx_corpus_works_added']
// Created alongside the drops, so dropping a duplicate never leaves its table unindexed.
const RETAINED = ['idx_work_aliases_title', 'idx_work_aliases_work', 'idx_search_results_run_openalex', 'idx_search_results_work']
const DROPPED = [
  'idx_corpus_works_lookup',
  'idx_corpus_works_corpus',
  'idx_work_aliases_norm_title',
  'idx_work_aliases_owner',
  'idx_search_results_run',
]
const HERE = path.dirname(fileURLToPath(import.meta.url))

function withDb(fn) {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'dt-schema-indexes-'))
  const db = new Database(path.join(tmpDir, 'idx.db'))
  try {
    fn(db)
  } finally {
    db.close()
    fs.rmSync(tmpDir, { recursive: true, force: true })
  }
}

const indexNames = (db) => new Set(db.prepare("SELECT name FROM sqlite_master WHERE type='index'").all().map((r) => r.name))

test('ensureAuthSchema creates the state/year/added and retained indexes and drops the duplicates, twice', () => {
  withDb((db) => {
    db.exec(`
      CREATE TABLE corpus_works (corpus_id INTEGER, work_id INTEGER, added_at TIMESTAMP, PRIMARY KEY (corpus_id, work_id));
      CREATE INDEX idx_corpus_works_lookup ON corpus_works(corpus_id, work_id);
      CREATE INDEX idx_corpus_works_corpus ON corpus_works(corpus_id);
      CREATE TABLE work_aliases (id INTEGER PRIMARY KEY, work_table TEXT, work_id INTEGER, normalized_alias_title TEXT);
      CREATE INDEX idx_work_aliases_norm_title ON work_aliases(normalized_alias_title);
      CREATE INDEX idx_work_aliases_owner ON work_aliases(work_table, work_id);
      CREATE TABLE search_results (id INTEGER PRIMARY KEY, search_run_id INTEGER, openalex_id TEXT);
      CREATE INDEX idx_search_results_run ON search_results(search_run_id);
    `)
    ensureAuthSchema(db)
    ensureAuthSchema(db)
    const names = indexNames(db)
    for (const n of [...NEW, ...RETAINED]) expect(names.has(n)).toBe(true)
    for (const n of DROPPED) expect(names.has(n)).toBe(false)
  })
})

test('ensureAuthSchema adds search_results.work_id before indexing it on an old table', () => {
  withDb((db) => {
    db.exec('CREATE TABLE search_results (id INTEGER PRIMARY KEY, search_run_id INTEGER, openalex_id TEXT)')
    ensureAuthSchema(db)
    expect(db.prepare('PRAGMA table_info(search_results)').all().map((c) => c.name)).toContain('work_id')
    expect(indexNames(db).has('idx_search_results_work')).toBe(true)
  })
})

test('ensureSeedSchema indexes seed_candidates_in_corpus.work_id and never the legacy lookup, twice', () => {
  withDb((db) => {
    db.exec(`
      CREATE TABLE seed_candidates_in_corpus (corpus_id INTEGER NOT NULL, source_type TEXT NOT NULL, source_key TEXT NOT NULL,
        candidate_key TEXT NOT NULL, work_id INTEGER, PRIMARY KEY (corpus_id, source_type, source_key, candidate_key));
      CREATE INDEX idx_seed_candidates_in_corpus_lookup ON seed_candidates_in_corpus(corpus_id, source_type, source_key);
    `)
    ensureSeedSchema(db)
    ensureSeedSchema(db)
    const names = indexNames(db)
    expect(names.has('idx_seed_candidates_in_corpus_work')).toBe(true)
    expect(names.has('idx_seed_candidates_in_corpus_lookup')).toBe(false)
  })
})

test('the seed-marker index DDL is textually identical in seed.js and seed_promote.py', () => {
  const seed = fs.readFileSync(path.join(HERE, '..', 'src', 'seed.js'), 'utf8')
  const promote = fs.readFileSync(path.join(HERE, '..', 'scripts', 'seed_promote.py'), 'utf8')
  const ddl = seed.match(/CREATE INDEX IF NOT EXISTS idx_seed_candidates_in_corpus_work ON [^;]+/)
  expect(ddl).not.toBeNull()
  expect(promote).toContain(ddl[0])
})

test('ensureAuthSchema skips the retained indexes of absent tables', () => {
  withDb((db) => {
    ensureAuthSchema(db)
    const names = indexNames(db)
    expect(names.has('idx_work_aliases_title')).toBe(false)
    expect(names.has('idx_search_results_run_openalex')).toBe(false)
  })
})

test('ensureAuthSchema adds the lease columns before indexing them on an old works table', () => {
  withDb((db) => {
    db.exec('CREATE TABLE works (id INTEGER PRIMARY KEY, title TEXT, year INTEGER, metadata_status TEXT, download_status TEXT)')
    ensureAuthSchema(db)
    const cols = db.prepare('PRAGMA table_info(works)').all().map((c) => c.name)
    expect(cols).toEqual(expect.arrayContaining(['metadata_lease_expires_at', 'download_lease_expires_at']))
    expect(indexNames(db).has('idx_works_state')).toBe(true)
  })
})

test('the retained-index DDL is textually identical in app.js and db_manager.py', () => {
  const app = fs.readFileSync(path.join(HERE, '..', 'src', 'app.js'), 'utf8')
  const py = fs.readFileSync(path.join(HERE, '..', '..', 'dl_lit_project', 'dl_lit', 'db_manager.py'), 'utf8')
  for (const name of [...RETAINED, ...NEW]) {
    const ddl = app.match(new RegExp(`CREATE INDEX IF NOT EXISTS ${name} ON [^;\`]+`))
    expect(ddl).not.toBeNull()
    expect(py).toContain(`"${ddl[0]}"`)
  }
})

test('ensureAuthSchema indexes pipeline_jobs by status and finished_at, idempotently', () => {
  withDb((db) => {
    db.exec(`CREATE TABLE pipeline_jobs (id INTEGER PRIMARY KEY AUTOINCREMENT, corpus_id INTEGER, job_type TEXT NOT NULL,
      status TEXT DEFAULT 'pending', parameters_json TEXT, result_json TEXT, created_at TIMESTAMP, started_at TIMESTAMP, finished_at TIMESTAMP)`)
    ensureAuthSchema(db)
    ensureAuthSchema(db)
    expect(indexNames(db).has('idx_pipeline_jobs_status_finished')).toBe(true)
    expect(db.prepare("PRAGMA index_info('idx_pipeline_jobs_status_finished')").all().map((r) => r.name)).toEqual(['status', 'finished_at'])
    const plan = (sql, ...args) => db.prepare(`EXPLAIN QUERY PLAN ${sql}`).all(...args).map((r) => r.detail).join(' ')
    expect(plan(
      `SELECT id, status FROM pipeline_jobs WHERE job_type = ? AND corpus_id IS ? AND status IN (?, ?) ORDER BY created_at ASC, id ASC LIMIT 1`,
      'enrich', 1, 'pending', 'running'
    )).toContain('idx_pipeline_jobs_status_finished')
    expect(plan(
      `DELETE FROM pipeline_jobs WHERE status IN ('completed','failed','cancelled') AND finished_at < ?`, '2026-01-01'
    )).toContain('idx_pipeline_jobs_status_finished')
  })
})

test('the pipeline_jobs index DDL is textually identical in app.js and db_manager.py', () => {
  const app = fs.readFileSync(path.join(HERE, '..', 'src', 'app.js'), 'utf8')
  const py = fs.readFileSync(path.join(HERE, '..', '..', 'dl_lit_project', 'dl_lit', 'db_manager.py'), 'utf8')
  const ddl = app.match(/CREATE INDEX IF NOT EXISTS idx_pipeline_jobs_status_finished ON [^;`'"]+/)
  expect(ddl).not.toBeNull()
  expect(py).toContain(`"${ddl[0]}"`)
})

test('the ingest/latest route is gone', async () => {
  process.env.RAG_FEEDER_STUB = '1'
  try {
    const res = await request(createApp({ broadcast: () => {} })).get('/api/ingest/latest')
    expect(res.status).toBe(404)
  } finally {
    delete process.env.RAG_FEEDER_STUB
  }
  const app = fs.readFileSync(path.join(HERE, '..', 'src', 'app.js'), 'utf8')
  expect(app).not.toContain('INGEST_LATEST_SCRIPT')
  expect(app).not.toContain('/api/ingest/latest')
})
