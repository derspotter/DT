import Database from 'better-sqlite3'
import { ensureSeedSchema } from '../src/seed.js'
import { pruneOrphanedCorpusRows } from '../src/app.js'

// The DELETE /api/corpora/:id transaction is inline in createApp, so this test
// exercises the same statements against a scratch DB: every corpus-scoped
// table must be emptied for the deleted corpus, and search runs left with no
// corpus must not survive.
function createDb() {
  const db = new Database(':memory:')
  db.exec(`
    CREATE TABLE corpora (id INTEGER PRIMARY KEY, name TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
    CREATE TABLE user_corpora (user_id INTEGER, corpus_id INTEGER, role TEXT);
    CREATE TABLE corpus_works (corpus_id INTEGER, work_id INTEGER);
    CREATE TABLE corpus_items (corpus_id INTEGER, item_id INTEGER);
    CREATE TABLE ingest_entries (id INTEGER PRIMARY KEY, corpus_id INTEGER, ingest_source TEXT);
    CREATE TABLE ingest_source_metadata (corpus_id INTEGER, ingest_source TEXT);
    CREATE TABLE pipeline_jobs (id INTEGER PRIMARY KEY, corpus_id INTEGER);
    CREATE TABLE corpus_kantropos_assignments (corpus_id INTEGER, target_id TEXT);
    CREATE TABLE search_runs (id INTEGER PRIMARY KEY, query TEXT, filters_json TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
    CREATE TABLE search_results (id INTEGER PRIMARY KEY, search_run_id INTEGER, title TEXT);
  `)
  ensureSeedSchema(db)
  return db
}

// Mirrors the delete transaction's cleanup half.
function deleteCorpusRows(db, corpusId) {
  for (const table of [
    'pipeline_jobs', 'ingest_entries', 'ingest_source_metadata',
    'corpus_kantropos_assignments', 'corpus_works', 'corpus_items',
    'seed_sources_hidden', 'seed_candidates_dismissed', 'seed_candidates_in_corpus',
    'search_run_corpora',
  ]) {
    db.prepare(`DELETE FROM ${table} WHERE corpus_id = ?`).run(corpusId)
  }
  db.prepare(
    `DELETE FROM search_results WHERE search_run_id IN (
       SELECT sr.id FROM search_runs sr
       LEFT JOIN search_run_corpora src ON src.search_run_id = sr.id
       WHERE src.search_run_id IS NULL)`
  ).run()
  db.prepare(
    `DELETE FROM search_runs WHERE id IN (
       SELECT sr.id FROM search_runs sr
       LEFT JOIN search_run_corpora src ON src.search_run_id = sr.id
       WHERE src.search_run_id IS NULL)`
  ).run()
  db.prepare('DELETE FROM user_corpora WHERE corpus_id = ?').run(corpusId)
  db.prepare('DELETE FROM corpora WHERE id = ?').run(corpusId)
}

describe('corpus deletion cleans every corpus-scoped table', () => {
  let db

  afterEach(() => {
    db?.close()
    db = null
  })

  test('leaves no rows behind for the deleted corpus', () => {
    db = createDb()
    db.prepare('INSERT INTO corpora (id, name) VALUES (7, ?)').run('doomed')
    db.prepare('INSERT INTO user_corpora VALUES (1, 7, ?)').run('owner')
    db.prepare('INSERT INTO corpus_works VALUES (7, 1)').run()
    db.prepare('INSERT INTO corpus_items VALUES (7, 1)').run()
    db.prepare("INSERT INTO ingest_entries (id, corpus_id, ingest_source) VALUES (1, 7, 'doc')").run()
    db.prepare("INSERT INTO ingest_source_metadata VALUES (7, 'doc')").run()
    db.prepare('INSERT INTO pipeline_jobs (id, corpus_id) VALUES (1, 7)').run()
    db.prepare("INSERT INTO corpus_kantropos_assignments VALUES (7, 'x')").run()
    db.prepare("INSERT INTO seed_sources_hidden (corpus_id, source_type, source_key) VALUES (7, 'search', '1')").run()
    db.prepare("INSERT INTO seed_candidates_dismissed (corpus_id, source_type, source_key, candidate_key) VALUES (7, 'search', '1', 'c')").run()
    db.prepare("INSERT INTO seed_candidates_in_corpus (corpus_id, source_type, source_key, candidate_key) VALUES (7, 'search', '1', 'c')").run()
    db.prepare("INSERT INTO search_runs (id, query) VALUES (1, 'q')").run()
    db.prepare('INSERT INTO search_run_corpora (search_run_id, corpus_id) VALUES (1, 7)').run()
    db.prepare("INSERT INTO search_results (id, search_run_id, title) VALUES (1, 1, 't')").run()

    deleteCorpusRows(db, 7)

    for (const table of [
      'corpora', 'user_corpora', 'corpus_works', 'corpus_items', 'ingest_entries',
      'ingest_source_metadata', 'pipeline_jobs', 'corpus_kantropos_assignments',
      'seed_sources_hidden', 'seed_candidates_dismissed', 'seed_candidates_in_corpus',
      'search_run_corpora', 'search_runs', 'search_results',
    ]) {
      expect({ table, rows: db.prepare(`SELECT COUNT(*) AS n FROM ${table}`).get().n }).toEqual({ table, rows: 0 })
    }
  })

  test('leaves another corpus\u2019s search run untouched', () => {
    // search_run_corpora keys on search_run_id alone, so a run belongs to
    // exactly one corpus; deleting one corpus must not reach into another.
    db = createDb()
    db.prepare('INSERT INTO corpora (id, name) VALUES (7, ?)').run('doomed')
    db.prepare('INSERT INTO corpora (id, name) VALUES (8, ?)').run('keeper')
    db.prepare("INSERT INTO search_runs (id, query) VALUES (1, 'doomed run')").run()
    db.prepare('INSERT INTO search_run_corpora (search_run_id, corpus_id) VALUES (1, 7)').run()
    db.prepare("INSERT INTO search_results (id, search_run_id, title) VALUES (1, 1, 'gone')").run()
    db.prepare("INSERT INTO search_runs (id, query) VALUES (2, 'kept run')").run()
    db.prepare('INSERT INTO search_run_corpora (search_run_id, corpus_id) VALUES (2, 8)').run()
    db.prepare("INSERT INTO search_results (id, search_run_id, title) VALUES (2, 2, 'kept')").run()

    deleteCorpusRows(db, 7)

    expect(db.prepare('SELECT id FROM search_runs').all()).toEqual([{ id: 2 }])
    expect(db.prepare('SELECT title FROM search_results').all()).toEqual([{ title: 'kept' }])
    expect(db.prepare('SELECT corpus_id FROM search_run_corpora').all()).toEqual([{ corpus_id: 8 }])
  })
})

describe('pruneOrphanedCorpusRows', () => {
  let db

  afterEach(() => {
    db?.close()
    db = null
  })

  test('removes rows whose corpus is gone and keeps rows whose corpus survives', () => {
    db = createDb()
    db.prepare('INSERT INTO corpora (id, name) VALUES (8, ?)').run('alive')
    // Orphans: a background extraction that landed after its corpus was
    // deleted, plus rows from corpora deleted before the route cleaned up.
    db.prepare("INSERT INTO ingest_source_metadata VALUES (99, 'late-extraction')").run()
    db.prepare('INSERT INTO pipeline_jobs (id, corpus_id) VALUES (1, 99)').run()
    db.prepare('INSERT INTO corpus_works VALUES (99, 1)').run()
    db.prepare("INSERT INTO seed_sources_hidden (corpus_id, source_type, source_key) VALUES (99, 'search', '1')").run()
    db.prepare("INSERT INTO search_runs (id, query) VALUES (1, 'orphan')").run()
    db.prepare('INSERT INTO search_run_corpora (search_run_id, corpus_id) VALUES (1, 99)').run()
    db.prepare("INSERT INTO search_results (id, search_run_id, title) VALUES (1, 1, 'orphan')").run()
    // Survivors.
    db.prepare("INSERT INTO ingest_source_metadata VALUES (8, 'kept')").run()
    db.prepare('INSERT INTO pipeline_jobs (id, corpus_id) VALUES (2, 8)').run()
    db.prepare("INSERT INTO search_runs (id, query) VALUES (2, 'kept')").run()
    db.prepare('INSERT INTO search_run_corpora (search_run_id, corpus_id) VALUES (2, 8)').run()
    db.prepare("INSERT INTO search_results (id, search_run_id, title) VALUES (2, 2, 'kept')").run()

    const removed = pruneOrphanedCorpusRows(db)

    expect(removed).toMatchObject({
      ingest_source_metadata: 1,
      pipeline_jobs: 1,
      corpus_works: 1,
      seed_sources_hidden: 1,
      search_run_corpora: 1,
      search_runs: 1,
      search_results: 1,
    })
    expect(db.prepare('SELECT ingest_source FROM ingest_source_metadata').all()).toEqual([{ ingest_source: 'kept' }])
    expect(db.prepare('SELECT id FROM pipeline_jobs').all()).toEqual([{ id: 2 }])
    expect(db.prepare('SELECT query FROM search_runs').all()).toEqual([{ query: 'kept' }])
    expect(db.prepare('SELECT title FROM search_results').all()).toEqual([{ title: 'kept' }])
  })

  test('is a no-op on a clean database', () => {
    db = createDb()
    db.prepare('INSERT INTO corpora (id, name) VALUES (8, ?)').run('alive')
    db.prepare("INSERT INTO ingest_source_metadata VALUES (8, 'kept')").run()
    expect(pruneOrphanedCorpusRows(db)).toEqual({})
  })
})
