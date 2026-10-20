// Corpus listing (GET /api/corpus) in SQL on the shared connection.
//
// Ported from backend/scripts/corpus_list.py, which stays in the repo as the
// oracle for tests/corpus-list.test.js: the item shape, filter, sort orders,
// status buckets and provenance labels must stay byte-for-byte equal to what
// the script prints. Filtering, counting, sorting and paging happen in SQL;
// labels are computed in JS for the returned page only, except for the `seed`
// sort, whose key is the label itself.
import { tableExists } from './pipelineReads.js'

export const CORPUS_SORT_KEYS = ['title', 'authors', 'year', 'added', 'source', 'seed', 'metadata', 'download']

// --- Python semantics helpers ------------------------------------------------

function pyTruthy(value) {
  if (value === null || value === undefined || value === false || value === 0 || value === '') return false
  if (Array.isArray(value) || Buffer.isBuffer(value)) return value.length > 0
  if (typeof value === 'object') return Object.keys(value).length > 0
  return true
}

function pyStr(value) {
  if (typeof value === 'string') return value
  if (value === null || value === undefined) return 'None'
  if (value === true) return 'True'
  if (value === false) return 'False'
  if (typeof value === 'object' && !Buffer.isBuffer(value)) return JSON.stringify(value)
  return String(value)
}

// str.strip(): Python's whitespace set, which unlike String#trim keeps U+FEFF
// (seen as a leading BOM in live author lists) and strips U+001C..U+001F, U+0085.
const PY_WS = '\\t\\n\\x0b\\x0c\\r\\x1c-\\x1f \\x85\\xa0\\u1680\\u2000-\\u200a\\u2028\\u2029\\u202f\\u205f\\u3000'
const PY_STRIP_RE = new RegExp(`^[${PY_WS}]+|[${PY_WS}]+$`, 'g')
const pyStrip = (text) => text.replace(PY_STRIP_RE, '')

// `a or b or c` returns the first truthy operand, else the last one.
function pyOr(...values) {
  for (const value of values) if (pyTruthy(value)) return value
  return values[values.length - 1]
}

// `str(x or "")`
const strOrEmpty = (value) => (pyTruthy(value) ? pyStr(value) : '')

// int(value) as normalize_year/normalize_id use it: non-numbers become 0.
function pyInt(value) {
  if (value === null || value === undefined) return 0
  if (typeof value === 'number') return Number.isFinite(value) ? Math.trunc(value) : 0
  if (typeof value === 'bigint') return Number(value)
  if (typeof value === 'string') {
    const text = pyStrip(value)
    // int() takes any Unicode decimal digit (U+FF11, U+0661, ...), not just ASCII.
    if (!/^[+-]?\p{Nd}+(?:_\p{Nd}+)*$/u.test(text)) return 0
    return Number(Array.from(text.replace(/_/g, ''), (ch) => (/\p{Nd}/u.test(ch) ? String(digitValue(ch)) : ch)).join(''))
  }
  return 0
}

// Decimal digits come in aligned runs of ten (0..9), so a digit's value is its
// distance from the start of its contiguous \p{Nd} run, modulo 10.
function digitValue(ch) {
  const cp = ch.codePointAt(0)
  let start = cp
  while (/\p{Nd}/u.test(String.fromCodePoint(start - 1))) start -= 1
  return (cp - start) % 10
}

// Python orders strings by code point; JS `<` by UTF-16 unit. They disagree
// only when an astral character (surrogate pair) meets U+E000..U+FFFF.
function comparePyStrings(a, b) {
  const n = Math.min(a.length, b.length)
  for (let i = 0; i < n; i += 1) {
    const x = a.charCodeAt(i)
    const y = b.charCodeAt(i)
    if (x !== y) {
      const sx = x >= 0xd800 && x <= 0xdfff
      const sy = y >= 0xd800 && y <= 0xdfff
      if (sx !== sy) return sx ? 1 : -1
      return x - y
    }
  }
  return a.length - b.length
}

function parseJson(raw) {
  try {
    return { ok: true, value: JSON.parse(raw) }
  } catch {
    return { ok: false }
  }
}

// Path(value).stem
function pathStem(value) {
  const parts = String(value).split('/').filter((part) => part !== '' && part !== '.')
  const name = parts.length ? parts[parts.length - 1] : ''
  const dot = name.lastIndexOf('.')
  return dot > 0 && dot < name.length - 1 ? name.slice(0, dot) : name
}

// --- Sort / filter keys (registered as SQL functions) ------------------------

// _authors_sort_key: first non-blank author of a JSON list, else the string.
function authorsSortKey(raw) {
  let value = raw
  if (typeof value === 'string') {
    const parsed = parseJson(value)
    if (!parsed.ok) return pyStrip(value).toLowerCase()
    value = parsed.value
  }
  if (Array.isArray(value)) {
    for (const name of value) {
      const text = pyStrip(strOrEmpty(name))
      if (text) return text.toLowerCase()
    }
    return ''
  }
  return pyStrip(strOrEmpty(value)).toLowerCase()
}

// _searchable_text: title, authors and venue joined by spaces, lowercased.
function searchableText(title, authors, source) {
  return [title, authors, source].filter(pyTruthy).map(pyStr).join(' ').toLowerCase()
}

const registeredDbs = new WeakSet()

// SQLite's lower() and LIKE only fold ASCII; the script lowercases with
// Python's str.lower(), so the keys that need it run as JS functions.
function registerFunctions(db) {
  if (registeredDbs.has(db)) return
  const opts = { deterministic: true }
  db.function('dt_corpus_lower', opts, (value) => strOrEmpty(value).toLowerCase())
  db.function('dt_corpus_authors_key', opts, (value) => authorsSortKey(value))
  db.function('dt_corpus_int', opts, (value) => pyInt(value))
  db.function('dt_corpus_search_text', opts, (title, authors, source) => searchableText(title, authors, source))
  registeredDbs.add(db)
}

// status_from_row, download state first.
// trim() set = Python's str.isspace() characters. lower() folding only ASCII is
// exact here: no non-ASCII character lowercases into these status words.
const WS = 'char(9, 10, 11, 12, 13, 28, 29, 30, 31, 32, 133, 160, 5760, 8192, 8193, 8194, 8195, 8196, 8197, 8198, 8199, 8200, 8201, 8202, 8232, 8233, 8239, 8287, 12288)'
const META = `lower(trim(COALESCE(w.metadata_status, ''), ${WS}))`
const DOWNLOAD = `lower(trim(COALESCE(w.download_status, ''), ${WS}))`
const STATUS_SQL = `CASE
  WHEN ${DOWNLOAD} = 'downloaded' THEN 'downloaded'
  WHEN ${DOWNLOAD} IN ('queued', 'in_progress') THEN 'queued_download'
  WHEN ${DOWNLOAD} = 'failed' THEN 'failed_download'
  WHEN ${META} = 'in_progress' THEN 'enriching'
  WHEN ${META} = 'failed' THEN 'failed_enrichment'
  WHEN ${META} = 'matched' THEN 'matched'
  ELSE 'raw' END`

// normalize_year without a JS call for the common integer/NULL cases.
const YEAR_SQL = "CASE WHEN typeof(w.year) = 'integer' THEN w.year WHEN w.year IS NULL THEN 0 ELSE dt_corpus_int(w.year) END"

function sortExpression(column, addedSql) {
  switch (column) {
    case 'title': return 'dt_corpus_lower(w.title)'
    case 'authors': return 'dt_corpus_authors_key(w.authors)'
    case 'year': return YEAR_SQL
    // str(added_at or ""): ISO timestamps sort lexically.
    case 'added': return `CASE WHEN ${addedSql} IS NULL OR ${addedSql} = '' THEN '' ELSE CAST(${addedSql} AS TEXT) END`
    case 'source': return 'dt_corpus_lower(w.source)'
    case 'metadata': return 'dt_corpus_lower(w.metadata_status)'
    case 'download': return 'dt_corpus_lower(w.download_status)'
    default: return null
  }
}

// --- Provenance labels (one-to-one ports of the script's loaders) -------------

const labelKey = (corpusId, key) => `${corpusId === null || corpusId === undefined ? 'None' : corpusId}\u0000${key}`

function firstAuthor(raw) {
  if (!pyTruthy(raw)) return ''
  const parsed = parseJson(raw)
  if (!parsed.ok) return pyStrip(pyStr(raw))
  if (Array.isArray(parsed.value)) {
    const names = parsed.value.filter((name) => pyStrip(strOrEmpty(name))).map((name) => pyStrip(pyStr(name)))
    if (!names.length) return ''
    return names.length === 1 ? names[0] : `${names[0]} et al.`
  }
  return pyStrip(pyStr(parsed.value))
}

function loadPdfSourceLabels(db, corpusId) {
  const labels = new Map()
  if (!tableExists(db, 'ingest_source_metadata')) return labels
  const rows = db.prepare(`
        SELECT corpus_id, ingest_source, title, authors, source_pdf
        FROM ingest_source_metadata
        WHERE (? IS NULL OR corpus_id = ?)
  `).all(corpusId, corpusId)
  for (const row of rows) {
    const key = pyStrip(strOrEmpty(row.ingest_source))
    if (!key) continue
    let label = pyOr(row.title, pyTruthy(row.source_pdf) ? pathStem(pyStr(row.source_pdf)) : '', key)
    const author = firstAuthor(row.authors)
    if (author) label = `${label} — ${author}`
    labels.set(labelKey(row.corpus_id, key), label)
    labels.set(labelKey(null, key), label)
  }
  return labels
}

function loadSearchSourceLabels(db) {
  const labels = new Map()
  if (!tableExists(db, 'search_runs')) return labels
  for (const row of db.prepare('SELECT id, query FROM search_runs').all()) {
    labels.set(pyStr(row.id), pyOr(row.query, `Search #${pyStr(row.id)}`))
  }
  return labels
}

const pdfLabel = (labels, corpusId, key) => pyOr(labels.get(labelKey(corpusId, key)), labels.get(labelKey(null, key)))

function loadSeedProvenance(db, corpusId, pdfLabels, searchLabels) {
  const provenance = new Map()
  if (!tableExists(db, 'seed_candidates_in_corpus')) return provenance
  const rows = db.prepare(`
        SELECT work_id, source_type, source_key, marked_at
        FROM seed_candidates_in_corpus
        WHERE corpus_id = ?
          AND work_id IS NOT NULL
        ORDER BY marked_at DESC
  `).all(corpusId)
  for (const row of rows) {
    const workId = pyInt(row.work_id)
    if (!workId || provenance.has(workId)) continue
    const sourceType = pyStrip(strOrEmpty(row.source_type)).toLowerCase()
    const sourceKey = pyStrip(strOrEmpty(row.source_key))
    let label
    if (sourceType === 'pdf') label = pyOr(pdfLabel(pdfLabels, corpusId, sourceKey), sourceKey)
    else if (sourceType === 'search') label = pyOr(searchLabels.get(sourceKey), `Search #${sourceKey}`)
    else label = sourceKey
    provenance.set(workId, { source_type: sourceType, source_key: sourceKey, source_label: label })
  }
  return provenance
}

function sourceLabelForWork(row, corpusId, { pdfLabels, searchLabels, provenance }) {
  const workId = pyInt(row.id)
  if (provenance.has(workId)) return provenance.get(workId)

  const originKey = pyStrip(strOrEmpty(row.origin_key))
  const originType = pyStrip(strOrEmpty(row.origin_type)).toLowerCase()
  const runId = row.run_id

  if (originType === 'bibtex_import' || originKey.endsWith('/metadata.bib')) {
    return { source_type: '', source_key: '', source_label: '' }
  }
  if (originKey.startsWith('search:')) {
    const searchKey = originKey.slice('search:'.length)
    return { source_type: 'search', source_key: searchKey, source_label: pyOr(searchLabels.get(searchKey), `Search #${searchKey}`) }
  }
  if (runId !== null && runId !== undefined) {
    const searchKey = pyStr(runId)
    return { source_type: 'search', source_key: searchKey, source_label: pyOr(searchLabels.get(searchKey), `Search #${searchKey}`) }
  }
  if (originKey) {
    const label = pdfLabel(pdfLabels, corpusId, originKey)
    if (pyTruthy(label)) return { source_type: 'pdf', source_key: originKey, source_label: label }
    return { source_type: originType || 'source', source_key: originKey, source_label: originKey }
  }
  if (pyTruthy(row.source_pdf)) {
    return { source_type: 'pdf', source_key: pyStr(row.source_pdf), source_label: pathStem(pyStr(row.source_pdf)) }
  }
  return { source_type: '', source_key: '', source_label: pyOr(row.metadata_source, row.source, '') }
}

// --- Listing -------------------------------------------------------------------

const nonNegInt = (value, fallback) => {
  const n = Math.trunc(Number(value))
  return Number.isFinite(n) ? Math.max(n, 0) : fallback
}

function stageTotals(counts) {
  const get = (status) => counts[status] || 0
  return {
    raw: get('raw') + get('extract_references_from_pdf') + get('pending'),
    metadata: get('matched') + get('queued_download') + get('enriching'),
    downloaded: get('downloaded'),
    failed_enrichment: get('failed_enrichment'),
    failed_download: get('failed_download')
  }
}

const readTransactions = new WeakMap()

// One read transaction per listing: in WAL mode every statement inside sees the
// same snapshot, so counts, ids and page rows cannot disagree when a worker or
// a membership removal commits in between.
export function listCorpus(db, options = {}) {
  registerFunctions(db)
  let read = readTransactions.get(db)
  if (!read) {
    read = db.transaction(readCorpus)
    readTransactions.set(db, read)
  }
  return read(db, options)
}

function readCorpus(db, { corpusId = null, limit = 200, offset = 0, q = '', sort = '' } = {}) {
  const scoped = corpusId !== null && corpusId !== undefined
  const emptyCounts = {}
  if (!tableExists(db, 'works')) {
    return { items: [], total: 0, stage_totals: stageTotals(emptyCounts), status_counts: emptyCounts }
  }

  const pageLimit = nonNegInt(limit, 200)
  const pageOffset = nonNegInt(offset, 0)
  const needle = pyStrip(strOrEmpty(q)).toLowerCase()
  const addedSql = scoped ? 'cw.added_at' : 'w.created_at'

  const where = []
  const params = []
  if (scoped) {
    where.push('cw.corpus_id = ?')
    params.push(corpusId)
  }
  if (needle) {
    where.push('instr(dt_corpus_search_text(w.title, w.authors, w.source), ?) > 0')
    params.push(needle)
  }
  const from = `FROM works w ${scoped ? 'JOIN corpus_works cw ON cw.work_id = w.id' : ''} ${where.length ? `WHERE ${where.join(' AND ')}` : ''}`

  const statusCounts = {}
  let total = 0
  for (const row of db.prepare(`SELECT ${STATUS_SQL} AS status, COUNT(*) AS n ${from} GROUP BY 1`).all(...params)) {
    statusCounts[row.status] = row.n
    total += row.n
  }

  const rawSort = pyStrip(strOrEmpty(sort)).toLowerCase()
  const colon = rawSort.indexOf(':')
  const column = colon === -1 ? rawSort : rawSort.slice(0, colon)
  const direction = colon === -1 ? '' : rawSort.slice(colon + 1)
  const reverse = direction !== 'asc'

  const pdfLabels = loadPdfSourceLabels(db, scoped ? corpusId : null)
  const searchLabels = loadSearchSourceLabels(db)
  const provenance = loadSeedProvenance(db, scoped ? corpusId : null, pdfLabels, searchLabels)
  const lookups = { pdfLabels, searchLabels, provenance }

  const columns = `w.id, w.title, w.authors, w.year, ${addedSql} AS added_at, w.source, w.publisher, w.type,
    w.origin_type, w.origin_key, w.run_id, w.source_pdf, w.metadata_source, ${STATUS_SQL} AS status,
    w.metadata_status, w.download_status, w.doi, w.openalex_id, w.file_path`

  let rows
  if (column === 'seed') {
    // The label comes from four tables with precedence, so it is computed in
    // JS over the filtered set's provenance columns and only the page is
    // fetched in full.
    const light = db.prepare(`SELECT w.id, w.origin_type, w.origin_key, w.run_id, w.source_pdf, w.metadata_source, w.source ${from}`).all(...params)
    const keyed = light.map((row) => ({ id: row.id, key: strOrEmpty(sourceLabelForWork(row, corpusId, lookups).source_label).toLowerCase() }))
    keyed.sort((a, b) => {
      const byKey = comparePyStrings(a.key, b.key)
      if (byKey !== 0) return reverse ? -byKey : byKey
      return pyInt(b.id) - pyInt(a.id)
    })
    const ids = keyed.slice(pageOffset, pageOffset + pageLimit).map((row) => row.id)
    if (!ids.length) {
      rows = []
    } else {
      // One JSON parameter instead of one per id: pages may exceed SQLite's
      // bound-variable limit.
      const fetched = db.prepare(`SELECT ${columns} ${from} ${where.length ? 'AND' : 'WHERE'} w.id IN (SELECT value FROM json_each(?))`)
        .all(...params, JSON.stringify(ids))
      const byId = new Map(fetched.map((row) => [row.id, row]))
      // filter(Boolean): never throw on a row gone missing (the transaction
      // already prevents that on a WAL database).
      rows = ids.map((id) => byId.get(id)).filter(Boolean)
    }
  } else {
    const key = sortExpression(column, addedSql)
    // Unknown/absent sort: newest year first, then newest id. Explicit sorts
    // tie-break on id descending, as the script's two-pass stable sort does.
    const orderBy = key ? `${key} ${reverse ? 'DESC' : 'ASC'}, w.id DESC` : `${YEAR_SQL} DESC, w.id DESC`
    rows = db.prepare(`SELECT ${columns} ${from} ORDER BY ${orderBy} LIMIT ? OFFSET ?`).all(...params, pageLimit, pageOffset)
  }

  const items = rows.map((row) => {
    const info = sourceLabelForWork(row, corpusId, lookups)
    return {
      id: row.id,
      work_id: row.id,
      title: row.title,
      authors: row.authors,
      year: row.year,
      added_at: row.added_at,
      source: row.source,
      origin: pyOr(row.origin_key, row.source_pdf, row.metadata_source, row.source),
      publisher: row.publisher,
      type: row.type,
      source_label: info.source_label,
      source_type: info.source_type,
      source_key: info.source_key,
      status: row.status,
      metadata_status: row.metadata_status,
      download_status: row.download_status,
      doi: row.doi,
      openalex_id: row.openalex_id,
      file_path: row.file_path
    }
  })

  return { items, total, stage_totals: stageTotals(statusCounts), status_counts: statusCounts }
}
