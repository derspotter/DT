import Database from 'better-sqlite3'
import { ensureSeedSchema, listSeedCandidates, listSeedSources, countSeedCandidates, dismissAllSeedCandidates } from '../src/seed.js'

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

describe('seed candidate reference counts', () => {
  let db

  afterEach(() => {
    db?.close()
    db = null
  })

  test('exposes referenced_works_count and cited_by_count from raw_json', () => {
    db = createSearchSeedDb()
    insertSearchResult(db, { referenced_works_count: 42, cited_by_count: 1234 })
    const [candidate] = listSeedCandidates(db, 130, 'search', '7')
    expect(candidate.refs_count).toBe(42)
    expect(candidate.cited_by_count).toBe(1234)
  })

  test('is null when the run predates the select change', () => {
    db = createSearchSeedDb()
    insertSearchResult(db, { display_name: 'Old run' })
    const [candidate] = listSeedCandidates(db, 130, 'search', '7')
    expect(candidate.refs_count).toBeNull()
    expect(candidate.cited_by_count).toBeNull()
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

  test('marks expansion runs as snowball seeds', () => {
    seedTwoEntries() // assigns `db` with two pdf entries and empty search tables
    db.prepare(
      `INSERT INTO search_runs (id, query, filters_json) VALUES (9, 'Downstream of «Bazaar Economies»', ?)`
    ).run(JSON.stringify({
      expansion_direction: 'downstream',
      expansion_of_openalex_id: 'https://openalex.org/W1',
      expansion_of_title: 'Bazaar Economies',
    }))
    db.prepare(`INSERT INTO search_run_corpora (search_run_id, corpus_id) VALUES (9, 130)`).run()
    db.prepare(
      `INSERT INTO search_results (id, search_run_id, title, year, raw_json) VALUES (50, 9, 'Cited Work', '2010', '{}')`
    ).run()
    db.prepare(
      `INSERT INTO search_runs (id, query, filters_json) VALUES (10, 'plain query', ?)`
    ).run(JSON.stringify({ mode: 'query' }))
    db.prepare(`INSERT INTO search_run_corpora (search_run_id, corpus_id) VALUES (10, 130)`).run()
    db.prepare(
      `INSERT INTO search_results (id, search_run_id, title, year, raw_json) VALUES (51, 10, 'Plain Work', '2011', '{}')`
    ).run()

    const sources = listSeedSources(db, 130)
    const snowball = sources.find((s) => s.source_key === '9')
    const plain = sources.find((s) => s.source_key === '10')
    const pdf = sources.find((s) => s.source_type === 'pdf')
    expect(snowball.seed_kind).toBe('snowball')
    expect(snowball.snowball).toEqual({
      direction: 'downstream',
      of_title: 'Bazaar Economies',
      of_openalex_id: 'https://openalex.org/W1',
    })
    expect(plain.seed_kind).toBe('search')
    expect(plain.snowball).toBeUndefined()
    expect(pdf.seed_kind).toBe('pdf')
  })
})

describe('seed state resolver cache', () => {
  let db

  afterEach(() => {
    db?.close()
    db = null
  })

  test('picks up a newly downloaded work on the next call', () => {
    db = createSearchSeedDb()
    insertSearchResult(db, { doi: 'https://doi.org/10.1000/cached', display_name: 'Cached Work' })
    db.prepare(`UPDATE search_results SET doi = '10.1000/cached' WHERE id = 1`).run()
    expect(listSeedCandidates(db, 130, 'search', '7')[0].state).toBe('pending')

    // Another corpus downloads the same work; the fingerprint (count + max id
    // + downloaded count) changes, so the cached global lookup is rebuilt.
    db.prepare(
      `INSERT INTO works (id, title, doi, metadata_status, download_status, file_path)
       VALUES (900, 'Cached Work', '10.1000/cached', 'matched', 'downloaded', '/tmp/cached.pdf')`
    ).run()
    db.prepare(`INSERT INTO corpus_works (corpus_id, work_id) VALUES (999, 900)`).run()
    expect(listSeedCandidates(db, 130, 'search', '7')[0].state).toBe('downloaded_elsewhere')
  })
})

describe('listSeedSources with only + q', () => {
  let db

  afterEach(() => {
    db?.close()
    db = null
  })

  test('summary counts reflect the text filter, matching the filtered candidate list', () => {
    db = createSeedDb()
    db.exec(`
      CREATE TABLE search_runs (id INTEGER PRIMARY KEY, query TEXT, filters_json TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
      CREATE TABLE search_results (id INTEGER PRIMARY KEY, search_run_id INTEGER NOT NULL, title TEXT, doi TEXT, openalex_id TEXT, year TEXT, raw_json TEXT);
      CREATE TABLE ingest_source_metadata (
        corpus_id INTEGER NOT NULL DEFAULT 0, ingest_source TEXT NOT NULL, source_pdf TEXT,
        title TEXT, authors TEXT, year INTEGER, doi TEXT, source TEXT, publisher TEXT
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
    const only = { sourceType: 'pdf', sourceKey: 'basare' }
    expect(listSeedSources(db, 130, { only })[0].candidate_count).toBe(2)
    const [filtered] = listSeedSources(db, 130, { only, q: 'shuttle' })
    expect(filtered.candidate_count).toBe(1)
    expect(listSeedCandidates(db, 130, 'pdf', 'basare', { q: 'shuttle' })).toHaveLength(1)
  })
})

describe('search seed paging, sorting and run status', () => {
  let db

  function seedRun(n) {
    db = createSearchSeedDb()
    db.exec(`ALTER TABLE search_runs ADD COLUMN query TEXT; ALTER TABLE search_runs ADD COLUMN filters_json TEXT;
             ALTER TABLE search_runs ADD COLUMN status TEXT; ALTER TABLE search_runs ADD COLUMN fetched_count INTEGER;
             ALTER TABLE search_runs ADD COLUMN expected_count INTEGER; ALTER TABLE search_runs ADD COLUMN error TEXT;`)
    db.prepare(`UPDATE search_runs SET query = 'big', status = 'running', fetched_count = ?, expected_count = 500 WHERE id = 7`).run(n)
    const ins = db.prepare(`INSERT INTO search_results (id, search_run_id, title, year, raw_json) VALUES (?, 7, ?, ?, ?)`)
    for (let i = 1; i <= n; i += 1) {
      ins.run(i, `Title ${String(i).padStart(3, '0')}`, String(1900 + i), JSON.stringify({
        referenced_works_count: n - i, cited_by_count: i * 10,
        authorships: [{ author: { display_name: i % 2 ? 'Zed Author' : 'Anna Author' } }],
        primary_location: { source: { display_name: i % 3 ? 'Journal A' : 'Journal B' } },
      }))
    }
  }

  afterEach(() => { db?.close(); db = null })

  test('pages in SQL and reports the total', () => {
    seedRun(25)
    const page = listSeedCandidates(db, 130, 'search', '7', { limit: 10, offset: 10, sort: 'title', dir: 'asc' })
    expect(page).toHaveLength(10)
    expect(page[0].title).toBe('Title 011')
    expect(countSeedCandidates(db, 130, 'search', '7', {})).toBe(25)
  })

  test('sorts by refs descending in SQL with blanks last', () => {
    seedRun(5)
    db.prepare(`UPDATE search_results SET raw_json = '{}' WHERE id = 3`).run()
    const rows = listSeedCandidates(db, 130, 'search', '7', { limit: 5, offset: 0, sort: 'refs', dir: 'desc' })
    expect(rows.map((r) => r.refs_count)).toEqual([4, 3, 1, 0, null])
  })

  test('filter and dismissals apply before paging', () => {
    seedRun(6)
    db.prepare(`INSERT INTO seed_candidates_dismissed (corpus_id, source_type, source_key, candidate_key) VALUES (130, 'search', '7', 'search:1')`).run()
    expect(countSeedCandidates(db, 130, 'search', '7', { q: 'title 00' })).toBe(5)
    const rows = listSeedCandidates(db, 130, 'search', '7', { q: 'title 00', limit: 2, offset: 0, sort: 'title', dir: 'asc' })
    expect(rows.map((r) => r.title)).toEqual(['Title 002', 'Title 003'])
  })

  test('seed sources carry the run status and skip state counts above the limit', () => {
    process.env.RAG_FEEDER_SEED_STATE_COUNT_LIMIT = '10'
    try {
      seedRun(12)
      const [source] = listSeedSources(db, 130)
      expect(source.run).toEqual({ status: 'running', fetched_count: 12, expected_count: 500, error: null })
      expect(source.candidate_count).toBe(12)
      expect(source.state_counts).toBeNull()
    } finally {
      delete process.env.RAG_FEEDER_SEED_STATE_COUNT_LIMIT
    }
  })

  test('a running run with no results yet is still listed', () => {
    seedRun(0)
    expect(listSeedSources(db, 130).map((s) => s.source_key)).toEqual(['7'])
  })

  test('dismissAllSeedCandidates dismisses the filtered set', () => {
    seedRun(4)
    expect(dismissAllSeedCandidates(db, 130, 'search', '7', { q: 'title 00' })).toBe(4)
    expect(countSeedCandidates(db, 130, 'search', '7', {})).toBe(0)
  })

  test('dismissAllSeedCandidates dismisses only the filtered rows, leaving the rest untouched', () => {
    seedRun(5)
    // i % 2 is truthy for odd i => 'Zed Author'; that's ids 1, 3, 5 (3 of 5).
    expect(dismissAllSeedCandidates(db, 130, 'search', '7', { q: 'zed author' })).toBe(3)
    expect(countSeedCandidates(db, 130, 'search', '7', {})).toBe(2)
    const remainingIds = listSeedCandidates(db, 130, 'search', '7', {}).map((c) => c.id).sort((a, b) => a - b)
    expect(remainingIds).toEqual([2, 4])
  })

  test('two consecutive pages with no sort are contiguous by id', () => {
    seedRun(10)
    const page1 = listSeedCandidates(db, 130, 'search', '7', { limit: 5, offset: 0 })
    const page2 = listSeedCandidates(db, 130, 'search', '7', { limit: 5, offset: 5 })
    expect(page1.map((c) => c.id)).toEqual([10, 9, 8, 7, 6])
    expect(page2.map((c) => c.id)).toEqual([5, 4, 3, 2, 1])
  })

  test('sorts by refs ascending in SQL with blanks last', () => {
    seedRun(5)
    db.prepare(`UPDATE search_results SET raw_json = '{}' WHERE id = 3`).run()
    const rows = listSeedCandidates(db, 130, 'search', '7', { limit: 5, offset: 0, sort: 'refs', dir: 'asc' })
    expect(rows.map((r) => r.refs_count)).toEqual([0, 1, 3, 4, null])
  })

  test('sorts by year ascending in SQL with blanks last', () => {
    seedRun(5)
    db.prepare(`UPDATE search_results SET year = '' WHERE id = 3`).run()
    const rows = listSeedCandidates(db, 130, 'search', '7', { limit: 5, offset: 0, sort: 'year', dir: 'asc' })
    expect(rows.map((r) => r.year)).toEqual(['1901', '1902', '1904', '1905', null])
  })

  function stubResolver(stateByTitle, fileAvailableByTitle = {}) {
    return {
      resolveState: (candidate) => stateByTitle[candidate.title] || 'pending',
      resolveDownloadedAvailability: (candidate) => ({
        downloaded_work_id: null,
        file_available: fileAvailableByTitle[candidate.title] ?? true,
        file_path: null,
      }),
      isInCorpus: () => false,
    }
  }

  test('metadata sort ranks failed_enrichment, then pending/staged_raw, then queued_enrichment, then the rest', () => {
    seedRun(5)
    const stateResolver = stubResolver({
      'Title 001': 'downloaded',
      'Title 002': 'failed_enrichment',
      'Title 003': 'queued_enrichment',
      'Title 004': 'pending',
      'Title 005': 'staged_raw',
    })
    const asc = listSeedCandidates(db, 130, 'search', '7', { stateResolver, sort: 'metadata', dir: 'asc' })
    // rank0: id2; rank1 tie {id4, id5} broken by id DESC -> 5 then 4; rank2: id3; rank3 (everything else): id1
    expect(asc.map((c) => c.id)).toEqual([2, 5, 4, 3, 1])
    const desc = listSeedCandidates(db, 130, 'search', '7', { stateResolver, sort: 'metadata', dir: 'desc' })
    expect(desc.map((c) => c.id)).toEqual([1, 3, 5, 4, 2])
  })

  test('download sort ranks failed_download, not-downloaded, queued_download, downloaded_elsewhere (unavailable below available), then downloaded', () => {
    seedRun(6)
    const stateResolver = stubResolver(
      {
        'Title 001': 'queued_download',
        'Title 002': 'failed_download',
        'Title 003': 'downloaded_elsewhere',
        'Title 004': 'downloaded_elsewhere',
        'Title 005': 'downloaded',
        'Title 006': 'pending',
      },
      { 'Title 003': false, 'Title 004': true, 'Title 005': true }
    )
    const asc = listSeedCandidates(db, 130, 'search', '7', { stateResolver, sort: 'download', dir: 'asc' })
    expect(asc.map((c) => c.id)).toEqual([2, 6, 1, 4, 3, 5])
    const desc = listSeedCandidates(db, 130, 'search', '7', { stateResolver, sort: 'download', dir: 'desc' })
    expect(desc.map((c) => c.id)).toEqual([5, 3, 4, 1, 6, 2])
  })
})
