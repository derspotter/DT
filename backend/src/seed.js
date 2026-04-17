function tableExists(db, tableName) {
  const row = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name = ?").get(tableName)
  return Boolean(row)
}

function parseJson(value, fallback = null) {
  if (value === undefined || value === null || value === '') return fallback
  if (typeof value === 'object') return value
  try {
    return JSON.parse(value)
  } catch {
    return fallback
  }
}

function parseNameList(value) {
  if (value === undefined || value === null || value === '') return []
  if (Array.isArray(value)) {
    return value.map((item) => String(item || '').trim()).filter(Boolean)
  }
  if (typeof value === 'string') {
    const trimmed = value.trim()
    if (!trimmed) return []
    const parsed = parseJson(trimmed, null)
    if (Array.isArray(parsed)) {
      return parsed.map((item) => String(item || '').trim()).filter(Boolean)
    }
    return [trimmed]
  }
  return [String(value).trim()].filter(Boolean)
}

function normalizeText(text) {
  if (!text) return null
  return String(text).toLowerCase().replace(/[^a-z0-9\s]/g, '').trim() || null
}

function normalizeDoi(doi) {
  if (!doi) return null
  const match = String(doi).match(/(10\.\d{4,9}\/[\-._;()/:A-Z0-9]+)/i)
  if (match) return match[1].toUpperCase()
  return String(doi).trim().toUpperCase() || null
}

function normalizeOpenAlexId(value) {
  if (!value) return null
  const raw = String(value).trim()
  if (!raw) return null
  const match = raw.match(/(https?:\/\/openalex\.org\/W\d+|W\d+)/i)
  return match ? match[1].toUpperCase() : raw.toUpperCase()
}

function normalizeYear(value) {
  if (value === undefined || value === null || value === '') return null
  const raw = String(value).trim()
  return raw || null
}

function normalizeContributors(authors) {
  const names = parseNameList(authors)
  if (names.length === 0) return null
  return normalizeText(names.join(' '))
}

function tupleKey(...parts) {
  return parts.map((part) => (part === undefined || part === null ? '' : String(part))).join('||')
}

const SEED_STATE_PRIORITY = new Map([
  ['downloaded', 0],
  ['downloaded_elsewhere', 1],
  ['queued_download', 2],
  ['enriched', 3],
  ['queued_enrichment', 4],
  ['staged_raw', 5],
  ['failed_download', 6],
  ['failed_enrichment', 7],
  ['added', 8],
  ['pending', 9],
])

function seedStateSortRank(state) {
  return SEED_STATE_PRIORITY.has(state) ? SEED_STATE_PRIORITY.get(state) : 99
}

function sortSeedCandidates(candidates) {
  return [...candidates].sort((a, b) => {
    const stateCmp = seedStateSortRank(a?.state) - seedStateSortRank(b?.state)
    if (stateCmp !== 0) return stateCmp
    const yearA = Number(a?.year) || 0
    const yearB = Number(b?.year) || 0
    if (yearA !== yearB) return yearB - yearA
    const createdA = String(a?.created_at || '')
    const createdB = String(b?.created_at || '')
    const createdCmp = createdB.localeCompare(createdA)
    if (createdCmp !== 0) return createdCmp
    return String(a?.title || '').localeCompare(String(b?.title || ''))
  })
}

function loadMatchRows(db, table, { corpusId = null, requireSelected = false, onlyQueued = false } = {}) {
  if (!tableExists(db, table)) return []

  const joins = []
  const where = []
  const params = []
  if (corpusId !== null && table === 'works') {
    joins.push('JOIN corpus_works cw ON cw.work_id = t.id')
    where.push('cw.corpus_id = ?')
    params.push(corpusId)
  }
  if (table === 'works' && requireSelected) {
    where.push(`COALESCE(t.metadata_status, 'pending') = 'pending'`)
  }
  if (table === 'works' && onlyQueued) {
    where.push(`COALESCE(t.download_status, 'not_requested') IN ('queued', 'in_progress')`)
  }

  const columns = ['t.id', 't.title', 't.authors', 't.year', 't.doi']
  if (tableExists(db, table)) {
    const cols = db.prepare(`PRAGMA table_info(${table})`).all().map((row) => row.name)
    if (cols.includes('openalex_id')) columns.push('t.openalex_id')
    if (cols.includes('download_state')) columns.push('t.download_state')
    if (cols.includes('download_last_error')) columns.push('t.download_last_error')
    if (cols.includes('metadata_status')) columns.push('t.metadata_status')
    if (cols.includes('download_status')) columns.push('t.download_status')
  }

  const query = `
    SELECT ${columns.join(', ')}
    FROM ${table} t
    ${joins.join(' ')}
    ${where.length ? `WHERE ${where.join(' AND ')}` : ''}
  `
  return db.prepare(query).all(...params)
}

function buildMatchIndex(rows) {
  const index = {
    doi: new Set(),
    openalex: new Set(),
    titleAuthorsYear: new Set(),
    titleAuthors: new Set(),
    titleYear: new Set(),
  }
  rows.forEach((row) => {
    const doi = normalizeDoi(row?.doi)
    if (doi) index.doi.add(doi)
    const openalexId = normalizeOpenAlexId(row?.openalex_id)
    if (openalexId) index.openalex.add(openalexId)
    const title = normalizeText(row?.title)
    const authors = normalizeContributors(row?.authors)
    const year = normalizeYear(row?.year)
    if (title && authors && year) index.titleAuthorsYear.add(tupleKey(title, authors, year))
    if (title && authors) index.titleAuthors.add(tupleKey(title, authors))
    if (title && year) index.titleYear.add(tupleKey(title, year))
  })
  return index
}

function addLookupValue(map, key, rowId) {
  if (!key || !Number.isFinite(Number(rowId))) return
  const normalizedId = Number(rowId)
  const existing = map.get(key)
  if (existing) {
    existing.add(normalizedId)
    return
  }
  map.set(key, new Set([normalizedId]))
}

function buildMatchLookup(rows) {
  const lookup = {
    doi: new Map(),
    openalex: new Map(),
    titleAuthorsYear: new Map(),
    titleAuthors: new Map(),
    titleYear: new Map(),
  }
  rows.forEach((row) => {
    const rowId = Number(row?.id)
    const doi = normalizeDoi(row?.doi)
    if (doi) addLookupValue(lookup.doi, doi, rowId)
    const openalexId = normalizeOpenAlexId(row?.openalex_id)
    if (openalexId) addLookupValue(lookup.openalex, openalexId, rowId)
    const title = normalizeText(row?.title)
    const authors = normalizeContributors(row?.authors)
    const year = normalizeYear(row?.year)
    if (title && authors && year) addLookupValue(lookup.titleAuthorsYear, tupleKey(title, authors, year), rowId)
    if (title && authors) addLookupValue(lookup.titleAuthors, tupleKey(title, authors), rowId)
    if (title && year) addLookupValue(lookup.titleYear, tupleKey(title, year), rowId)
  })
  return lookup
}

function matchesIndex(row, index) {
  const doi = normalizeDoi(row?.doi)
  if (doi && index.doi.has(doi)) return true
  const openalexId = normalizeOpenAlexId(row?.openalex_id)
  if (openalexId && index.openalex.has(openalexId)) return true
  const title = normalizeText(row?.title)
  const authors = normalizeContributors(row?.authors)
  const year = normalizeYear(row?.year)
  if (title && authors && year && index.titleAuthorsYear.has(tupleKey(title, authors, year))) return true
  if (title && authors && index.titleAuthors.has(tupleKey(title, authors))) return true
  if (title && year && index.titleYear.has(tupleKey(title, year))) return true
  return false
}

function loadAliasIndex(db, workTable, allowedIds = null) {
  if (!tableExists(db, 'work_aliases')) {
    return { byTitle: new Map(), byTitleYear: new Map() }
  }
  const rows = db.prepare(
    `SELECT normalized_alias_title, alias_year, work_id
     FROM work_aliases
     WHERE work_table = ?`
  ).all(String(workTable || ''))
  const byTitle = new Map()
  const byTitleYear = new Map()
  const allowed = allowedIds instanceof Set ? allowedIds : null
  rows.forEach((row) => {
    const workId = Number(row?.work_id)
    if (allowed && !allowed.has(workId)) return
    const title = String(row?.normalized_alias_title || '').trim()
    if (!title) return
    addLookupValue(byTitle, title, workId)
    const year = row?.alias_year
    if (year !== undefined && year !== null && year !== '') {
      addLookupValue(byTitleYear, tupleKey(title, Number(year)), workId)
    }
  })
  return { byTitle, byTitleYear }
}

function addIdsToSet(target, ids) {
  if (!ids) return
  ids.forEach((id) => {
    if (Number.isFinite(Number(id))) {
      target.add(Number(id))
    }
  })
}

function findMatchingIds(row, lookup) {
  const matched = new Set()
  const doi = normalizeDoi(row?.doi)
  if (doi) addIdsToSet(matched, lookup.doi.get(doi))
  const openalexId = normalizeOpenAlexId(row?.openalex_id)
  if (openalexId) addIdsToSet(matched, lookup.openalex.get(openalexId))
  const title = normalizeText(row?.title)
  const authors = normalizeContributors(row?.authors)
  const year = normalizeYear(row?.year)
  if (title && authors && year) addIdsToSet(matched, lookup.titleAuthorsYear.get(tupleKey(title, authors, year)))
  if (title && authors) addIdsToSet(matched, lookup.titleAuthors.get(tupleKey(title, authors)))
  if (title && year) addIdsToSet(matched, lookup.titleYear.get(tupleKey(title, year)))
  return matched
}

function findIdsWithAliases(row, lookup, aliasIndex) {
  const matched = findMatchingIds(row, lookup)
  const title = normalizeText(row?.title)
  if (!title) return matched
  const yearRaw = normalizeYear(row?.year)
  const year = yearRaw && /^\d+$/.test(yearRaw) ? Number(yearRaw) : null
  if (year !== null) {
    for (let offset = -1; offset <= 1; offset += 1) {
      addIdsToSet(matched, aliasIndex.byTitleYear.get(tupleKey(title, year + offset)))
    }
  }
  addIdsToSet(matched, aliasIndex.byTitle.get(title))
  return matched
}

function createStateResolver(db, corpusId) {
  const allRows = loadMatchRows(db, 'works')
  const localRows = loadMatchRows(db, 'works', { corpusId })
  const localDownloadedRows = localRows.filter((row) => String(row?.download_status || '').trim().toLowerCase() === 'downloaded')
  const localRawRows = localRows.filter((row) => String(row?.metadata_status || '').trim().toLowerCase() === 'pending')
  const localWithMetadataRows = localRows.filter((row) => {
    const metadata = String(row?.metadata_status || '').trim().toLowerCase()
    const download = String(row?.download_status || '').trim().toLowerCase()
    return metadata === 'matched' && !['queued', 'in_progress', 'downloaded', 'failed'].includes(download)
  })
  const localFailedEnrichmentRows = localRows.filter((row) => String(row?.metadata_status || '').trim().toLowerCase() === 'failed')
  const localFailedDownloadRows = localRows.filter((row) => String(row?.download_status || '').trim().toLowerCase() === 'failed')
  const localQueuedEnrichmentRows = localRows.filter((row) => String(row?.metadata_status || '').trim().toLowerCase() === 'in_progress')
  const localQueuedDownloadRows = localRows.filter((row) => ['queued', 'in_progress'].includes(String(row?.download_status || '').trim().toLowerCase()))
  const indexes = {
    raw: buildMatchIndex(localRawRows),
    queuedEnrichment: buildMatchIndex(localQueuedEnrichmentRows),
    withMetadata: buildMatchIndex(localWithMetadataRows),
    queuedDownload: buildMatchIndex(localQueuedDownloadRows),
    downloaded: buildMatchLookup(allRows.filter((row) => String(row?.download_status || '').trim().toLowerCase() === 'downloaded')),
    failedDownloadLookup: buildMatchLookup(localFailedDownloadRows),
    failedEnrichment: buildMatchIndex(localFailedEnrichmentRows),
    failedDownload: buildMatchIndex(localFailedDownloadRows),
  }
  const localDownloadedIds = new Set(localDownloadedRows.map((row) => Number(row?.id)).filter((id) => Number.isFinite(id)))
  const localFailedDownloadIds = new Set(localFailedDownloadRows.map((row) => Number(row?.id)).filter((id) => Number.isFinite(id)))
  const downloadedAliasIndex = loadAliasIndex(db, 'works')
  const failedDownloadAliasIndex = loadAliasIndex(db, 'works', localFailedDownloadIds)
  const isInCorpus = (row) => {
    const downloadedIds = findIdsWithAliases(row, indexes.downloaded, downloadedAliasIndex)
    for (const matchedId of downloadedIds) {
      if (localDownloadedIds.has(matchedId)) return true
    }
    const failedDownloadIds = findIdsWithAliases(row, indexes.failedDownloadLookup, failedDownloadAliasIndex)
    if (failedDownloadIds.size > 0) return true
    if (matchesIndex(row, indexes.failedDownload)) return true
    if (matchesIndex(row, indexes.queuedDownload)) return true
    if (matchesIndex(row, indexes.withMetadata)) return true
    if (matchesIndex(row, indexes.failedEnrichment)) return true
    if (matchesIndex(row, indexes.queuedEnrichment)) return true
    if (matchesIndex(row, indexes.raw)) return true
    return false
  }
  const resolveState = (row) => {
    const downloadedIds = findIdsWithAliases(row, indexes.downloaded, downloadedAliasIndex)
    if (downloadedIds.size > 0) {
      for (const matchedId of downloadedIds) {
        if (localDownloadedIds.has(matchedId)) return 'downloaded'
      }
      return 'downloaded_elsewhere'
    }
    const failedDownloadIds = findIdsWithAliases(row, indexes.failedDownloadLookup, failedDownloadAliasIndex)
    if (failedDownloadIds.size > 0) return 'failed_download'
    if (matchesIndex(row, indexes.failedDownload)) return 'failed_download'
    if (matchesIndex(row, indexes.queuedDownload)) return 'queued_download'
    if (matchesIndex(row, indexes.withMetadata)) return 'enriched'
    if (matchesIndex(row, indexes.failedEnrichment)) return 'failed_enrichment'
    if (matchesIndex(row, indexes.queuedEnrichment)) return 'queued_enrichment'
    if (matchesIndex(row, indexes.raw)) return 'staged_raw'
    return 'pending'
  }
  return {
    resolveState,
    isInCorpus,
  }
}

function formatPdfSubtitle(meta = {}) {
  const bits = []
  const authors = parseNameList(meta.authors)
  if (authors.length > 0) bits.push(authors.join(', '))
  if (meta.year) bits.push(String(meta.year))
  if (meta.source) bits.push(String(meta.source))
  if (meta.publisher) bits.push(String(meta.publisher))
  return bits.join(' · ')
}

function formatSearchSubtitle(run) {
  const filters = parseJson(run?.filters_json, {}) || {}
  const bits = []
  if (filters.field && filters.field !== 'default') bits.push(`field: ${filters.field}`)
  if (filters.year_from || filters.year_to) bits.push(`years: ${filters.year_from || '…'}-${filters.year_to || '…'}`)
  const directions = []
  if (filters.include_downstream) directions.push(`down ${filters.related_depth_downstream || 0}`)
  if (filters.include_upstream) directions.push(`up ${filters.related_depth_upstream || 0}`)
  if (directions.length > 0) bits.push(directions.join(' / '))
  return bits.join(' · ')
}

function buildSourceId(sourceType, sourceKey) {
  return `${sourceType}:${String(sourceKey)}`
}

function buildCandidateKey(sourceType, candidateId) {
  return sourceType === 'pdf' ? `ingest:${candidateId}` : `search:${candidateId}`
}

function loadDismissedCandidateKeySet(db, corpusId, sourceType, sourceKey) {
  if (!tableExists(db, 'seed_candidates_dismissed')) return new Set()
  const rows = db.prepare(
    `SELECT candidate_key
     FROM seed_candidates_dismissed
     WHERE corpus_id = ? AND source_type = ? AND source_key = ?`
  ).all(corpusId, sourceType, String(sourceKey))
  return new Set(rows.map((row) => String(row.candidate_key || '')))
}

function normalizePdfCandidate(row, sourceKey, resolverBundle) {
  const authors = parseNameList(row.authors)
  const candidate = {
    candidate_key: buildCandidateKey('pdf', row.id),
    source_type: 'pdf',
    source_key: String(sourceKey),
    ingest_source: String(sourceKey),
    run_id: null,
    id: Number(row.id),
    title: row.title || '',
    authors,
    year: row.year || null,
    doi: row.doi || null,
    source: row.source || null,
    publisher: row.publisher || null,
    url: row.url || null,
    openalex_id: null,
    created_at: row.created_at || null,
    source_pdf: row.source_pdf || null,
  }
  candidate.state = resolverBundle.resolveState(candidate)
  candidate.in_corpus = resolverBundle.isInCorpus(candidate)
  return candidate
}

function extractAuthorsFromRawJson(raw) {
  if (!raw || typeof raw !== 'object') return []
  if (Array.isArray(raw.authors)) return parseNameList(raw.authors)
  if (Array.isArray(raw.authorships)) {
    return raw.authorships
      .map((item) => item?.author?.display_name || item?.raw_author_name || '')
      .map((item) => String(item || '').trim())
      .filter(Boolean)
  }
  return []
}

function normalizeSearchCandidate(row, resolverBundle) {
  const raw = parseJson(row.raw_json, {}) || {}
  const authors = extractAuthorsFromRawJson(raw)
  const primarySource = raw?.primary_location?.source || raw?.host_venue || {}
  const candidate = {
    candidate_key: buildCandidateKey('search', row.id),
    source_type: 'search',
    source_key: String(row.search_run_id),
    ingest_source: `search:${row.search_run_id}`,
    run_id: Number(row.search_run_id),
    id: Number(row.id),
    title: row.title || raw.display_name || '',
    authors,
    year: row.year || raw.publication_year || null,
    doi: row.doi || raw.doi || null,
    source: primarySource.display_name || raw?.primary_location?.landing_page_url || null,
    publisher: primarySource.host_organization_name || raw?.host_organization_name || null,
    url: raw?.primary_location?.pdf_url || raw?.primary_location?.landing_page_url || raw?.id || null,
    openalex_id: row.openalex_id || raw.id || null,
    type: raw?.type || null,
    created_at: row.created_at || null,
  }
  candidate.state = resolverBundle.resolveState(candidate)
  candidate.in_corpus = resolverBundle.isInCorpus(candidate)
  return candidate
}

export function ensureSeedSchema(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS search_run_corpora (
      search_run_id INTEGER PRIMARY KEY,
      corpus_id INTEGER NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS seed_sources_hidden (
      corpus_id INTEGER NOT NULL,
      source_type TEXT NOT NULL,
      source_key TEXT NOT NULL,
      hidden_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (corpus_id, source_type, source_key)
    );
    CREATE TABLE IF NOT EXISTS seed_candidates_dismissed (
      corpus_id INTEGER NOT NULL,
      source_type TEXT NOT NULL,
      source_key TEXT NOT NULL,
      candidate_key TEXT NOT NULL,
      dismissed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (corpus_id, source_type, source_key, candidate_key)
    );
    CREATE INDEX IF NOT EXISTS idx_search_run_corpora_corpus ON search_run_corpora(corpus_id, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_seed_sources_hidden_lookup ON seed_sources_hidden(corpus_id, source_type, source_key);
    CREATE INDEX IF NOT EXISTS idx_seed_candidates_dismissed_lookup ON seed_candidates_dismissed(corpus_id, source_type, source_key);
  `)
}

function loadInCorpusCandidateKeySet(db, corpusId, sourceType, sourceKey) {
  return new Set()
}

export function upsertSearchRunCorpus(db, { searchRunId, corpusId }) {
  const runId = Number(searchRunId)
  const scopedCorpusId = Number(corpusId)
  if (!Number.isFinite(runId) || runId <= 0 || !Number.isFinite(scopedCorpusId) || scopedCorpusId <= 0) return false
  db.prepare(
    `INSERT INTO search_run_corpora (search_run_id, corpus_id)
     VALUES (?, ?)
     ON CONFLICT(search_run_id) DO UPDATE SET corpus_id = excluded.corpus_id`
  ).run(runId, scopedCorpusId)
  return true
}

export function listSeedCandidates(db, corpusId, sourceType, sourceKey, { stateResolver = null } = {}) {
  const sourceKind = String(sourceType || '').trim().toLowerCase()
  const sourceRef = String(sourceKey || '').trim()
  if (!sourceRef || !['pdf', 'search'].includes(sourceKind)) return []

  const resolverBundle = stateResolver || createStateResolver(db, corpusId)
  const dismissed = loadDismissedCandidateKeySet(db, corpusId, sourceKind, sourceRef)
  const inCorpusMarked = loadInCorpusCandidateKeySet(db, corpusId, sourceKind, sourceRef)

  if (sourceKind === 'pdf') {
    const rows = db.prepare(
      `SELECT id, title, authors, year, doi, source, publisher, url, source_pdf, created_at
       FROM ingest_entries
       WHERE corpus_id = ? AND ingest_source = ?
       ORDER BY created_at DESC, id DESC`
    ).all(corpusId, sourceRef)
    return sortSeedCandidates(rows
      .map((row) => {
        const candidate = normalizePdfCandidate(row, sourceRef, resolverBundle)
        if (inCorpusMarked.has(candidate.candidate_key)) candidate.in_corpus = true
        if (candidate.in_corpus && candidate.state === 'pending') candidate.state = 'added'
        return candidate
      })
      .filter((candidate) => !dismissed.has(candidate.candidate_key)))
  }

  const runId = Number(sourceRef)
  if (!Number.isFinite(runId) || runId <= 0) return []
  const rows = db.prepare(
    `SELECT sr.id, sr.search_run_id, sr.title, sr.doi, sr.openalex_id, sr.year, sr.raw_json, s.created_at
     FROM search_results sr
     JOIN search_run_corpora src ON src.search_run_id = sr.search_run_id
     JOIN search_runs s ON s.id = sr.search_run_id
     WHERE src.corpus_id = ? AND sr.search_run_id = ?
     ORDER BY sr.id DESC`
  ).all(corpusId, runId)
  return sortSeedCandidates(rows
    .map((row) => {
      const candidate = normalizeSearchCandidate(row, resolverBundle)
      if (inCorpusMarked.has(candidate.candidate_key)) candidate.in_corpus = true
      if (candidate.in_corpus && candidate.state === 'pending') candidate.state = 'added'
      return candidate
    })
    .filter((candidate) => !dismissed.has(candidate.candidate_key)))
}

function summarizeStates(candidates) {
  const stateCounts = {
    in_corpus: 0,
    added: 0,
    pending: 0,
    staged_raw: 0,
    queued_enrichment: 0,
    enriched: 0,
    queued_download: 0,
    downloaded: 0,
    downloaded_elsewhere: 0,
    failed_enrichment: 0,
    failed_download: 0,
  }
  candidates.forEach((candidate) => {
    if (candidate?.in_corpus) {
      stateCounts.in_corpus += 1
    }
    const key = candidate?.state
    if (Object.prototype.hasOwnProperty.call(stateCounts, key)) {
      stateCounts[key] += 1
    }
  })
  return stateCounts
}

export function listSeedSources(db, corpusId, { limit = 200 } = {}) {
  const resolver = createStateResolver(db, corpusId)
  const pdfSources = db.prepare(
    `SELECT ie.ingest_source AS source_key,
            MAX(ie.created_at) AS created_at,
            COUNT(*) AS entry_count,
            MAX(ism.title) AS seed_title,
            MAX(ism.authors) AS seed_authors,
            MAX(ism.year) AS seed_year,
            MAX(ism.doi) AS seed_doi,
            MAX(ism.source) AS seed_source,
            MAX(ism.publisher) AS seed_publisher,
            MAX(ism.source_pdf) AS source_pdf
     FROM ingest_entries ie
     LEFT JOIN ingest_source_metadata ism
       ON ism.corpus_id = ie.corpus_id AND ism.ingest_source = ie.ingest_source
     LEFT JOIN seed_sources_hidden ssh
       ON ssh.corpus_id = ie.corpus_id AND ssh.source_type = 'pdf' AND ssh.source_key = ie.ingest_source
     WHERE ie.corpus_id = ?
       AND COALESCE(ie.ingest_source, '') <> ''
       AND ssh.source_key IS NULL
     GROUP BY ie.ingest_source
     ORDER BY MAX(ie.created_at) DESC`
  ).all(corpusId)

  const searchSources = tableExists(db, 'search_run_corpora')
    ? db.prepare(
      `SELECT sr.id AS source_key,
              COALESCE(src.created_at, sr.created_at) AS created_at,
              sr.query,
              sr.filters_json,
              COUNT(sres.id) AS entry_count
       FROM search_runs sr
       JOIN search_run_corpora src ON src.search_run_id = sr.id
       JOIN search_results sres ON sres.search_run_id = sr.id
       LEFT JOIN seed_sources_hidden ssh
         ON ssh.corpus_id = src.corpus_id AND ssh.source_type = 'search' AND ssh.source_key = CAST(sr.id AS TEXT)
       WHERE src.corpus_id = ?
         AND ssh.source_key IS NULL
       GROUP BY sr.id
       ORDER BY COALESCE(src.created_at, sr.created_at) DESC`
    ).all(corpusId)
    : []

  const sources = []
  pdfSources.forEach((row) => {
    const sourceKey = String(row.source_key || '').trim()
    if (!sourceKey) return
    const meta = {
      title: row.seed_title,
      authors: parseNameList(row.seed_authors),
      year: row.seed_year,
      doi: row.seed_doi,
      source: row.seed_source,
      publisher: row.seed_publisher,
    }
    const candidates = listSeedCandidates(db, corpusId, 'pdf', sourceKey, { stateResolver: resolver })
    if (candidates.length === 0) return
    sources.push({
      id: buildSourceId('pdf', sourceKey),
      source_type: 'pdf',
      source_key: sourceKey,
      label: row.seed_title || sourceKey,
      subtitle: formatPdfSubtitle(meta) || (row.source_pdf || ''),
      created_at: row.created_at,
      candidate_count: candidates.length,
      state_counts: summarizeStates(candidates),
      removable: true,
      meta,
    })
  })

  searchSources.forEach((row) => {
    const sourceKey = String(row.source_key || '').trim()
    if (!sourceKey) return
    const candidates = listSeedCandidates(db, corpusId, 'search', sourceKey, { stateResolver: resolver })
    if (candidates.length === 0) return
    sources.push({
      id: buildSourceId('search', sourceKey),
      source_type: 'search',
      source_key: sourceKey,
      label: row.query || `Search #${sourceKey}`,
      subtitle: formatSearchSubtitle(row),
      created_at: row.created_at,
      candidate_count: candidates.length,
      state_counts: summarizeStates(candidates),
      removable: true,
      meta: {
        query: row.query,
        filters: parseJson(row.filters_json, {}) || {},
      },
    })
  })

  return sources
    .sort((a, b) => String(b.created_at || '').localeCompare(String(a.created_at || '')))
    .slice(0, Math.max(1, Number(limit) || 200))
}

export function hideSeedSource(db, corpusId, sourceType, sourceKey) {
  const type = String(sourceType || '').trim().toLowerCase()
  const key = String(sourceKey || '').trim()
  if (!key || !['pdf', 'search'].includes(type)) return false
  const result = db.prepare(
    `INSERT OR REPLACE INTO seed_sources_hidden (corpus_id, source_type, source_key, hidden_at)
     VALUES (?, ?, ?, CURRENT_TIMESTAMP)`
  ).run(corpusId, type, key)
  return Number(result?.changes || 0) > 0
}

export function dismissSeedCandidates(db, corpusId, sourceType, sourceKey, candidateKeys = []) {
  const type = String(sourceType || '').trim().toLowerCase()
  const key = String(sourceKey || '').trim()
  const normalized = [...new Set((candidateKeys || []).map((value) => String(value || '').trim()).filter(Boolean))]
  if (!key || !['pdf', 'search'].includes(type) || normalized.length === 0) return 0
  const insert = db.prepare(
    `INSERT OR REPLACE INTO seed_candidates_dismissed (corpus_id, source_type, source_key, candidate_key, dismissed_at)
     VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)`
  )
  const tx = db.transaction((values) => {
    values.forEach((candidateKey) => insert.run(corpusId, type, key, candidateKey))
  })
  tx(normalized)
  return normalized.length
}
