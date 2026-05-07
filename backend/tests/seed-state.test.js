import Database from 'better-sqlite3'
import { ensureSeedSchema, listSeedCandidates } from '../src/seed.js'

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
  ensureSeedSchema(db)
  return db
}

describe('seed candidate state resolution', () => {
  let db

  afterEach(() => {
    db?.close()
    db = null
  })

  test('does not classify aliases for failed works as downloaded elsewhere', () => {
    db = createSeedDb()
    db.prepare(
      `INSERT INTO works (id, title, authors, year, metadata_status, download_status)
       VALUES (101, 'Failed Alias Work', '["A. Author"]', '2014', 'matched', 'failed')`
    ).run()
    db.prepare(
      `INSERT INTO work_aliases (work_table, work_id, normalized_alias_title, alias_year)
       VALUES ('works', 101, 'from shuttle trader to businesswomen the informal bazaar econommy in kyrgyzstan', 2014)`
    ).run()
    db.prepare(
      `INSERT INTO ingest_entries (id, corpus_id, ingest_source, title, authors, year)
       VALUES (2453, 130, 'basare', 'From Shuttle Trader to Businesswomen: The Informal Bazaar Econommy in Kyrgyzstan.', '["A. Author"]', '2014')`
    ).run()

    const [candidate] = listSeedCandidates(db, 130, 'pdf', 'basare')

    expect(candidate.state).toBe('pending')
    expect(candidate.in_corpus).toBe(false)
    expect(candidate.downloaded_work_id).toBeNull()
  })

  test('uses explicit seed-candidate corpus markers when matching is incomplete', () => {
    db = createSeedDb()
    db.prepare(
      `INSERT INTO ingest_entries (id, corpus_id, ingest_source, title, authors, year)
       VALUES (2495, 130, 'basare', 'Of Basti and Bazaar', '["Unknown"]', '2001')`
    ).run()
    db.prepare(
      `INSERT INTO seed_candidates_in_corpus (corpus_id, source_type, source_key, candidate_key, work_id)
       VALUES (130, 'pdf', 'basare', 'ingest:2495', 999)`
    ).run()

    const [candidate] = listSeedCandidates(db, 130, 'pdf', 'basare')

    expect(candidate.in_corpus).toBe(true)
    expect(candidate.state).toBe('added')
  })

  test('checks downloaded elsewhere files relative to their metadata.bib origin', () => {
    db = createSeedDb()
    db.prepare(
      `INSERT INTO works (id, title, authors, year, metadata_status, download_status, file_path, origin_key)
       VALUES (201, 'Economic Anthropology', '["A. Author"]', '2021', 'matched', 'downloaded', '/source/example.pdf', '/upstream/corpus/metadata.bib')`
    ).run()
    db.prepare(
      `INSERT INTO ingest_entries (id, corpus_id, ingest_source, title, authors, year)
       VALUES (3001, 130, 'basare', 'Economic Anthropology.', '["A. Author"]', '2021')`
    ).run()

    const resolverCalls = []
    const [candidate] = listSeedCandidates(db, 130, 'pdf', 'basare', {
      resolveDownloadedFilePath: (filePath, context) => {
        resolverCalls.push({ filePath, context })
        return '/upstream/corpus/source/example.pdf'
      },
    })

    expect(candidate.state).toBe('downloaded_elsewhere')
    expect(candidate.file_available).toBe(true)
    expect(candidate.downloaded_file_path).toBe('/source/example.pdf')
    expect(resolverCalls).toEqual([
      {
        filePath: '/source/example.pdf',
        context: {
          originKey: '/upstream/corpus/metadata.bib',
          origin_key: '/upstream/corpus/metadata.bib',
        },
      },
    ])
  })
})
