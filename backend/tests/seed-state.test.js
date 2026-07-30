import Database from 'better-sqlite3'
import { ensureSeedSchema, listSeedCandidates, listSeedSources } from '../src/seed.js'

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

function createSearchSeedDb() {
  const db = createSeedDb()
  db.exec(`
    CREATE TABLE search_runs (
      id INTEGER PRIMARY KEY,
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
  db.prepare(`INSERT INTO search_runs (id) VALUES (7)`).run()
  db.prepare(`INSERT INTO search_run_corpora (search_run_id, corpus_id) VALUES (7, 130)`).run()
  return db
}

function insertSearchResult(db, rawJson) {
  db.prepare(
    `INSERT INTO search_results (id, search_run_id, title, year, raw_json)
     VALUES (1, 7, 'A Work', '2020', ?)`
  ).run(JSON.stringify(rawJson))
}

describe('seed candidate venue fallback', () => {
  let db

  afterEach(() => {
    db?.close()
    db = null
  })

  test('returns null instead of a landing page URL when the venue has no display_name', () => {
    db = createSearchSeedDb()
    insertSearchResult(db, {
      primary_location: {
        source: { display_name: null },
        landing_page_url: 'https://doi.org/10.1234/abcd',
      },
    })

    const [candidate] = listSeedCandidates(db, 130, 'search', '7')

    expect(candidate.source).toBeNull()
  })

  test('still returns the venue display_name when present', () => {
    db = createSearchSeedDb()
    insertSearchResult(db, {
      primary_location: {
        source: { display_name: 'Journal of Labour Studies' },
        landing_page_url: 'https://doi.org/10.1234/abcd',
      },
    })

    const [candidate] = listSeedCandidates(db, 130, 'search', '7')

    expect(candidate.source).toBe('Journal of Labour Studies')
  })
})

describe('seed candidate text filter', () => {
  let db

  afterEach(() => {
    db?.close()
    db = null
  })

  function seedTwoEntries() {
    db = createSeedDb()
    // listSeedSources joins the seed-document metadata table; the shared
    // createSeedDb helper only sets up what listSeedCandidates needs.
    db.exec(`
      CREATE TABLE ingest_source_metadata (
        corpus_id INTEGER NOT NULL,
        ingest_source TEXT NOT NULL,
        title TEXT,
        authors TEXT,
        year TEXT,
        doi TEXT,
        source TEXT,
        publisher TEXT,
        source_pdf TEXT
      );
    `)
    // ensureSeedSchema creates search_run_corpora, so listSeedSources takes the
    // search branch and needs these two present even when empty.
    db.exec(`
      CREATE TABLE search_runs (
        id INTEGER PRIMARY KEY,
        query TEXT,
        filters_json TEXT,
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
    db.prepare(
      `INSERT INTO ingest_entries (id, corpus_id, ingest_source, title, authors, year, source)
       VALUES (1, 130, 'basare', 'Bazaar Economies', '["Anna Author"]', '2001', 'Journal of Labour Studies')`
    ).run()
    db.prepare(
      `INSERT INTO ingest_entries (id, corpus_id, ingest_source, title, authors, year, source)
       VALUES (2, 130, 'basare', 'Shuttle Trade', '["Bert Writer"]', '2015', 'Economic Review')`
    ).run()
  }

  test('returns every candidate when no query is given', () => {
    seedTwoEntries()
    expect(listSeedCandidates(db, 130, 'pdf', 'basare')).toHaveLength(2)
  })

  test('matches on title case-insensitively', () => {
    seedTwoEntries()
    const results = listSeedCandidates(db, 130, 'pdf', 'basare', { q: 'BAZAAR' })
    expect(results.map((row) => row.title)).toEqual(['Bazaar Economies'])
  })

  test('matches on author', () => {
    seedTwoEntries()
    const results = listSeedCandidates(db, 130, 'pdf', 'basare', { q: 'bert' })
    expect(results.map((row) => row.title)).toEqual(['Shuttle Trade'])
  })

  test('matches on publication', () => {
    seedTwoEntries()
    const results = listSeedCandidates(db, 130, 'pdf', 'basare', { q: 'economic review' })
    expect(results.map((row) => row.title)).toEqual(['Shuttle Trade'])
  })

  test('hides seeds whose candidates all fail to match', () => {
    seedTwoEntries()
    const sources = listSeedSources(db, 130, { q: 'nothing matches this' })
    expect(sources).toHaveLength(0)
  })

  test('keeps a seed that still has one matching candidate', () => {
    seedTwoEntries()
    const sources = listSeedSources(db, 130, { q: 'bazaar' })
    expect(sources).toHaveLength(1)
    expect(sources[0].candidate_count).toBe(1)
  })
})
