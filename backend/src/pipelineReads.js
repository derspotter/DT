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

// Python's str.isspace() set, as corpusList's STATUS_SQL trims it (that file's
// WS list). Unlike String#trim it keeps U+FEFF and strips U+001C..U+001F, U+0085.
const STATUS_WS = '\\t\\n\\x0b\\x0c\\r\\x1c-\\x1f \\x85\\xa0\\u1680\\u2000-\\u200a\\u2028\\u2029\\u202f\\u205f\\u3000'
const STATUS_TRIM_RE = new RegExp(`^[${STATUS_WS}]+|[${STATUS_WS}]+$`, 'g')
// SQLite's lower() folds ASCII only; so does this.
const normStatus = (value) => (value === null || value === undefined ? '' : String(value))
  .replace(STATUS_TRIM_RE, '')
  .replace(/[A-Z]/g, (ch) => ch.toLowerCase())

// The browser bucket of a work, download state first (STATUS_SQL in JS).
export function workStatus(meta, dl) {
  const download = normStatus(dl)
  if (download === 'downloaded') return 'downloaded'
  if (download === 'queued' || download === 'in_progress') return 'queued_download'
  if (download === 'failed') return 'failed_download'
  const metadata = normStatus(meta)
  if (metadata === 'in_progress') return 'enriching'
  if (metadata === 'failed') return 'failed_enrichment'
  if (metadata === 'matched') return 'matched'
  return 'raw'
}

// Per-connection memo for whole-scope reads (grouped state counts, corpus
// size class, year-type probe). Entries are stamped with data_version, which
// catches commits by other connections (the workers), and total_changes(),
// which catches writes on this same connection (e.g. a corpus removal on
// authDb). Neither moves when a worker lease merely expires, so the grouped
// counts also carry a 1 s TTL that keeps the live_* counts current.
//
// A ROLLBACK moves neither stamp, so a value read inside a transaction that
// has written must not be stored. Inside a transaction an entry is stored only
// when total_changes() still equals the value last seen outside one, i.e. the
// transaction has written nothing (listCorpus's read transaction).
let memo = new WeakMap()
const MEMO_TTL_MS = 1000

export function resetWorkStateMemo() {
  memo = new WeakMap()
}

export function memoised(db, key, compute, { ttlMs = null } = {}) {
  const { dataVersion, totalChanges } = db
    .prepare('SELECT (SELECT data_version FROM pragma_data_version) AS dataVersion, total_changes() AS totalChanges')
    .get()
  let perDb = memo.get(db)
  if (!perDb) {
    perDb = { committedChanges: null, entries: new Map() }
    memo.set(db, perDb)
  }
  if (!db.inTransaction) perDb.committedChanges = totalChanges
  const hit = perDb.entries.get(key)
  if (hit && hit.dataVersion === dataVersion && hit.totalChanges === totalChanges &&
      (ttlMs === null || Date.now() - hit.at < ttlMs)) return hit.value
  const value = compute()
  if (!db.inTransaction || perDb.committedChanges === totalChanges) {
    perDb.entries.set(key, { dataVersion, totalChanges, at: Date.now(), value })
  }
  return value
}

// A connection's last out-of-transaction total_changes(), recorded before a
// read transaction opens so that memoised reads inside it can be stored.
export function noteCommittedState(db) {
  if (db.inTransaction) return
  let perDb = memo.get(db)
  if (!perDb) {
    perDb = { committedChanges: null, entries: new Map() }
    memo.set(db, perDb)
  }
  perDb.committedChanges = db.prepare('SELECT total_changes() AS n').get().n
}

// Corpora up to this many members are read through corpus_works (JOIN-driven,
// cost proportional to the corpus); larger ones through the works-wide
// EXISTS / index-ordered forms, whose cost is bounded by the works table.
export const SMALL_CORPUS_LIMIT = 5000

// Size class of a corpus: a LIMIT-ed count over its corpus_works range.
export function corpusIsSmall(db, corpusId, limit = SMALL_CORPUS_LIMIT) {
  return memoised(db, `small\u0000${corpusId}\u0000${limit}`, () => db
    .prepare('SELECT COUNT(*) AS n FROM (SELECT 1 FROM corpus_works WHERE corpus_id = ? LIMIT ?)')
    .get(corpusId, limit + 1).n <= limit)
}

// One grouped read of (metadata_status, download_status) pairs, the only full
// pass over works that the count consumers need. Bare status columns plus the
// lease columns inside the CASE sums keep it answerable from idx_works_state.
// Small corpora are driven from corpus_works (JOIN); large ones and the global
// scope walk works with an EXISTS membership probe. An explicit nowSec bypasses
// the memo (a memoised entry was computed for an earlier now).
export function readWorkStateCounts(db, corpusId, { nowSec, smallCorpusLimit = SMALL_CORPUS_LIMIT } = {}) {
  const scoped = corpusId !== null && corpusId !== undefined
  const compute = () => {
    const now = nowSec ?? Math.floor(Date.now() / 1000)
    const leased = hasColumn(db, 'works', 'metadata_lease_expires_at') && hasColumn(db, 'works', 'download_lease_expires_at')
    let from = 'FROM works w'
    if (scoped) {
      from = corpusIsSmall(db, corpusId, smallCorpusLimit)
        ? 'FROM corpus_works cw JOIN works w ON w.id = cw.work_id WHERE cw.corpus_id = ?'
        : 'FROM works w WHERE EXISTS (SELECT 1 FROM corpus_works cw WHERE cw.corpus_id = ? AND cw.work_id = w.id)'
    }
    return db.prepare(`
      SELECT w.metadata_status, w.download_status, COUNT(*) AS n,
        ${leased
          ? `SUM(CASE WHEN w.metadata_status = 'in_progress' AND w.metadata_lease_expires_at > ? THEN 1 ELSE 0 END) AS live_meta,
             SUM(CASE WHEN w.download_status = 'in_progress' AND w.download_lease_expires_at > ? THEN 1 ELSE 0 END) AS live_dl`
          : '0 AS live_meta, 0 AS live_dl'}
      ${from}
      GROUP BY 1, 2
    `).all(...(leased ? [now, now] : []), ...(scoped ? [corpusId] : []))
  }
  if (nowSec !== undefined) return compute()
  return memoised(db, `counts\u0000${scoped ? corpusId : 'all'}`, compute, { ttlMs: MEMO_TTL_MS })
}

// Every count consumer's numbers from the grouped rows. The worker-facing
// counts compare raw status values, as the SQL they replace did; status_counts
// uses the normalised browser buckets. A NULL download_status (old DBs) counts
// as not_requested.
export function bucketCounts(rows) {
  const out = {
    status_counts: {}, raw_pending_total: 0, pending_only: 0, in_progress: 0, matched_not_requested: 0,
    queued: 0, downloading: 0, downloaded: 0, failed_meta: 0, failed_dl: 0, enriching_live: 0, downloading_live: 0
  }
  for (const row of rows) {
    const n = Number(row.n)
    const meta = row.metadata_status
    const dl = row.download_status ?? 'not_requested'
    const bucket = workStatus(row.metadata_status, row.download_status)
    out.status_counts[bucket] = (out.status_counts[bucket] || 0) + n
    if (meta === 'pending') out.pending_only += n
    if (meta === 'in_progress') out.in_progress += n
    if (meta === 'matched' && dl === 'not_requested') out.matched_not_requested += n
    if (meta === 'failed') out.failed_meta += n
    if (dl === 'queued') out.queued += n
    if (dl === 'in_progress') out.downloading += n
    if (dl === 'downloaded') out.downloaded += n
    if (dl === 'failed') out.failed_dl += n
    out.enriching_live += Number(row.live_meta || 0)
    out.downloading_live += Number(row.live_dl || 0)
  }
  out.raw_pending_total = out.pending_only + out.in_progress
  return out
}

// Counts mirror ingest_stats.py. The *_live counts are extra: in_progress rows
// whose worker lease has not expired, i.e. work that is being done right now.
// A claim abandoned by a killed worker stays in_progress until it is requeued,
// so the plain enriching/downloading counts cannot tell activity from debris.
export function readIngestStats(db, corpusId) {
  const c = bucketCounts(readWorkStateCounts(db, corpusId))
  return {
    // Ingest stats: raw_pending = pending + in_progress (the backlog summary counts pending only).
    raw_pending: c.raw_pending_total,
    matched: c.matched_not_requested,
    queued_download: c.queued + c.downloading,
    enriching: c.in_progress,
    downloading: c.downloading,
    downloaded: c.downloaded,
    enriching_live: c.enriching_live,
    downloading_live: c.downloading_live
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
