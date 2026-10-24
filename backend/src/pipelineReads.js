export function tableExists(db, name) {
  return Boolean(db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name = ?").get(name))
}

const nonNegInt = (value, fallback) => {
  const n = Math.trunc(Number(value))
  return Number.isFinite(n) ? Math.max(n, 0) : fallback
}

function hasColumn(db, table, column) {
  return db.prepare(`PRAGMA table_info(${table})`).all().some((col) => col.name === column)
}

// Counts mirror ingest_stats.py. The *_live counts are extra: in_progress rows
// whose worker lease has not expired, i.e. work that is being done right now.
// A claim abandoned by a killed worker stays in_progress until it is requeued,
// so the plain enriching/downloading counts cannot tell activity from debris.
export function readIngestStats(db, corpusId) {
  const scoped = corpusId !== null && corpusId !== undefined
  const leased = hasColumn(db, 'works', 'metadata_lease_expires_at') && hasColumn(db, 'works', 'download_lease_expires_at')
  const now = Math.floor(Date.now() / 1000)
  const row = db.prepare(`
    SELECT
      COALESCE(SUM(CASE WHEN w.metadata_status IN ('pending', 'in_progress') THEN 1 ELSE 0 END), 0) AS raw_pending,
      COALESCE(SUM(CASE WHEN w.metadata_status = 'matched'
                         AND COALESCE(w.download_status, 'not_requested') = 'not_requested' THEN 1 ELSE 0 END), 0) AS matched,
      COALESCE(SUM(CASE WHEN w.download_status IN ('queued', 'in_progress') THEN 1 ELSE 0 END), 0) AS queued_download,
      COALESCE(SUM(CASE WHEN w.metadata_status = 'in_progress' THEN 1 ELSE 0 END), 0) AS enriching,
      COALESCE(SUM(CASE WHEN w.download_status = 'in_progress' THEN 1 ELSE 0 END), 0) AS downloading,
      COALESCE(SUM(CASE WHEN w.download_status = 'downloaded' THEN 1 ELSE 0 END), 0) AS downloaded,
      ${leased
        ? `COALESCE(SUM(CASE WHEN w.metadata_status = 'in_progress' AND w.metadata_lease_expires_at > ? THEN 1 ELSE 0 END), 0) AS enriching_live,
           COALESCE(SUM(CASE WHEN w.download_status = 'in_progress' AND w.download_lease_expires_at > ? THEN 1 ELSE 0 END), 0) AS downloading_live`
        : '0 AS enriching_live, 0 AS downloading_live'}
    FROM works w
    ${scoped ? 'JOIN corpus_works cw ON cw.work_id = w.id WHERE cw.corpus_id = ?' : ''}
  `).get(...(leased ? [now, now] : []), ...(scoped ? [corpusId] : []))
  return {
    raw_pending: Number(row.raw_pending),
    matched: Number(row.matched),
    queued_download: Number(row.queued_download),
    enriching: Number(row.enriching),
    downloading: Number(row.downloading),
    downloaded: Number(row.downloaded),
    enriching_live: Number(row.enriching_live),
    downloading_live: Number(row.downloading_live)
  }
}

// Count and page in one read transaction (one WAL snapshot), so total matches the rows.
export function listDownloads(db, options = {}) {
  return db.transaction(readDownloads)(db, options)
}

function readDownloads(db, { corpusId = null, limit = 200, offset = 0 } = {}) {
  if (!tableExists(db, 'works')) return { items: [], total: 0 }
  const scoped = corpusId !== null && corpusId !== undefined
  const join = scoped ? 'JOIN corpus_works cw ON cw.work_id = w.id' : ''
  const where = `WHERE ${scoped ? 'cw.corpus_id = ? AND ' : ''}w.download_status IN ('queued', 'in_progress', 'downloaded')`
  const params = scoped ? [corpusId] : []
  const total = db.prepare(`SELECT COUNT(*) AS n FROM works w ${join} ${where}`).get(...params).n
  const rows = db.prepare(`
    SELECT w.id, w.title, w.download_status, w.download_attempt_count, w.downloaded_at
    FROM works w
    ${join}
    ${where}
    ORDER BY
      CASE w.download_status
        WHEN 'in_progress' THEN 0
        WHEN 'queued' THEN 1
        WHEN 'downloaded' THEN 2
        ELSE 9
      END,
      CASE WHEN w.download_status = 'downloaded' THEN datetime(w.downloaded_at) END DESC,
      w.id DESC
    LIMIT ? OFFSET ?
  `).all(...params, nonNegInt(limit, 200), nonNegInt(offset, 0))
  const items = rows.map((row) => ({
    id: row.id,
    title: row.title,
    status: String(row.download_status || '').trim() || 'queued',
    attempts: Number(row.download_attempt_count || 0),
    downloaded_at: row.downloaded_at ?? null
  }))
  return { items, total }
}

export function listIngestRuns(db, { corpusId = null, limit = 20 } = {}) {
  if (!tableExists(db, 'ingest_entries')) return { runs: [], total: 0 }
  const hasMeta = tableExists(db, 'ingest_source_metadata')
  const scoped = corpusId !== null && corpusId !== undefined
  const metaSelect = hasMeta
    ? ', MAX(ism.title) AS seed_title, MAX(ism.authors) AS seed_authors, MAX(ism.year) AS seed_year, MAX(ism.doi) AS seed_doi, MAX(ism.source) AS seed_source, MAX(ism.publisher) AS seed_publisher'
    : ''
  const metaJoin = hasMeta
    ? `LEFT JOIN ingest_source_metadata ism
         ON ism.ingest_source = ie.ingest_source
        AND ism.corpus_id = COALESCE(ie.corpus_id, 0)`
    : ''
  const runs = db.prepare(`
    SELECT ie.ingest_source,
           COUNT(*) AS entry_count,
           MAX(ie.created_at) AS last_created_at,
           MAX(ie.source_pdf) AS source_pdf
           ${metaSelect}
    FROM ingest_entries ie
    ${metaJoin}
    ${scoped ? 'WHERE ie.corpus_id = ?' : ''}
    GROUP BY ie.ingest_source
    ORDER BY last_created_at DESC
    LIMIT ?
  `).all(...(scoped ? [corpusId] : []), Math.trunc(Number(limit)) || 0)
  for (const run of runs) {
    if (typeof run.seed_authors === 'string') {
      try {
        const parsed = JSON.parse(run.seed_authors)
        if (Array.isArray(parsed)) run.seed_authors = parsed
      } catch { /* keep raw string */ }
    }
  }
  return { runs, total: runs.length }
}
