// One home for the work-row rules both tables (seed candidates in App.svelte,
// corpus rows in Corpus.svelte) and the upstream browser share: resolved
// state, status buckets, the metadata/download labels, author parsing, cell
// text and the sort toggle encoding.
//
// Two row shapes reach these helpers:
//   - seed candidates carry a resolved `state` (one of WORK_STATES) and
//     `file_available`;
//   - corpus/upstream rows carry the collapsed `status` (see the backend's
//     workStatus) plus `metadata_status`/`download_status` and `file_path`.

export const WORK_STATES = [
  'added',
  'pending',
  'staged_raw',
  'queued_enrichment',
  'enriched',
  'queued_download',
  'downloaded',
  'downloaded_elsewhere',
  'failed_enrichment',
  'failed_download',
]

export function workState(value) {
  const state = String(value || '').trim().toLowerCase()
  return WORK_STATES.includes(state) ? state : 'pending'
}

export const RAW_STATUSES = new Set(['raw', 'extract_references_from_pdf', 'pending'])
export const METADATA_STATUSES = new Set(['enriching', 'matched', 'queued_download'])
export const FAILED_ENRICHMENT_STATUSES = new Set(['failed_enrichment'])
export const FAILED_DOWNLOAD_STATUSES = new Set(['failed_download'])
export const DOWNLOADED_STATUSES = new Set(['downloaded'])

export function bucketForStatus(status) {
  if (!status || RAW_STATUSES.has(status)) return 'raw'
  if (METADATA_STATUSES.has(status)) return 'metadata'
  if (FAILED_ENRICHMENT_STATUSES.has(status) || FAILED_DOWNLOAD_STATUSES.has(status)) return 'failed'
  if (DOWNLOADED_STATUSES.has(status)) return 'downloaded'
  return 'raw'
}

// Spec line 16: show the metadata and download axes separately. The derived
// state/status still drives every behaviour — only the display splits.
export const METADATA_LABELS = {
  pending: 'Pending',
  enriching: 'Enriching',
  matched: 'Confirmed',
  failed_enrichment: 'Metadata not confirmed',
}
export const DOWNLOAD_LABELS = {
  not_requested: 'Not requested',
  queued: 'Queued',
  in_progress: 'Downloading',
  downloaded: 'Downloaded',
  failed: 'Not retrievable',
  failed_download: 'Not retrievable',
}

// Search candidates carry no metadata_status of their own, so the column
// falls back to the resolved state (what the old row tag used to say). Only
// rows that carry a `state` (seed candidates) fall back; corpus rows show '-'.
const METADATA_BY_STATE = {
  enriched: 'Confirmed',
  added: 'Confirmed',
  downloaded: 'Confirmed',
  downloaded_elsewhere: 'Confirmed',
  queued_download: 'Confirmed',
  failed_download: 'Confirmed',
  queued_enrichment: 'Enriching',
  staged_raw: 'Pending',
  pending: 'Pending',
}

function resolvedState(item) {
  return item?.state === undefined || item?.state === null ? null : workState(item.state)
}

export function metadataLabel(item) {
  const state = resolvedState(item)
  if (state === 'failed_enrichment' || FAILED_ENRICHMENT_STATUSES.has(item?.status)) {
    return METADATA_LABELS.failed_enrichment
  }
  const raw = String(item?.metadata_status || '').trim().toLowerCase()
  if (METADATA_LABELS[raw]) return METADATA_LABELS[raw]
  if (raw) return raw.replace(/_/g, ' ')
  if (state) return METADATA_BY_STATE[state] || '-'
  return '-'
}

export function downloadLabel(item) {
  const state = resolvedState(item)
  if (state === 'failed_download' || FAILED_DOWNLOAD_STATUSES.has(item?.status)) {
    return DOWNLOAD_LABELS.failed_download
  }
  const raw = String(item?.download_status || '').trim().toLowerCase()
  if (DOWNLOAD_LABELS[raw]) return DOWNLOAD_LABELS[raw]
  if (raw) return raw.replace(/_/g, ' ')
  if (state === 'downloaded') return item?.file_available === false ? 'File missing' : 'Downloaded'
  if (state === 'downloaded_elsewhere') return item?.file_available === true ? 'Downloaded' : 'Stale record'
  if (state === 'queued_download') return DOWNLOAD_LABELS.queued
  return '-'
}

function cleanList(values) {
  return values.map((v) => String(v ?? '').trim()).filter(Boolean)
}

// The one author rule: arrays as-is; strings that parse as a JSON array
// likewise; other strings split on ';' when one is present, else on ','.
export function parseAuthors(value) {
  if (Array.isArray(value)) return cleanList(value)
  if (typeof value !== 'string') return []
  const raw = value.trim()
  if (!raw) return []
  try {
    const parsed = JSON.parse(raw)
    if (Array.isArray(parsed)) return cleanList(parsed)
  } catch (error) {
    // not JSON — fall through to the delimiter rule
  }
  return cleanList(raw.split(raw.includes(';') ? ';' : ','))
}

function authorsOf(entry) {
  if (!entry) return []
  if (Array.isArray(entry.authors_list)) return entry.authors_list
  const list = parseAuthors(entry.authors)
  return list.length > 0 ? list : parseAuthors(entry.author)
}

// The full list joined with ', ' — the corpus search filter matches against it.
export function formatAuthors(entry) {
  return authorsOf(entry).join(', ')
}

// Table cells show at most `cap` authors; the full list stays in the expanded
// detail card and on hover.
export function formatAuthorsShort(entry, cap = 3) {
  const list = authorsOf(entry)
  if (list.length <= cap) return list.join(', ')
  return `${list.slice(0, cap).join(', ')} et al.`
}

// Parse authors once at load. Tables read authors_list/authors_short and
// never re-split the joined authors_display ("Last, First" names contain ',').
export function normalizeWorkItem(entry) {
  if (!entry || typeof entry !== 'object') return entry
  const list = parseAuthors(entry.authors)
  const authorsList = list.length > 0 ? list : parseAuthors(entry.author)
  return {
    ...entry,
    authors_list: authorsList,
    authors_display: authorsList.join(', '),
    authors_short: formatAuthorsShort({ authors_list: authorsList }),
  }
}

export function formatTitle(entry) {
  return entry?.title || entry?.source || entry?.url || 'Untitled'
}

// SQLite CURRENT_TIMESTAMP is UTC "YYYY-MM-DD HH:MM:SS"; show the day only.
export function formatDateAdded(value) {
  const raw = String(value || '').trim()
  if (!raw) return '-'
  const match = raw.match(/^(\d{4}-\d{2}-\d{2})/)
  if (match) return match[1]
  const parsed = new Date(raw)
  return Number.isNaN(parsed.getTime()) ? raw : parsed.toISOString().slice(0, 10)
}

// Spec line 19: "Source" is the article's venue in both tables.
export function workSource(item) {
  return String(item?.source || '').trim()
}

// Spec C2 + line 19: where a corpus row came from — the search query, or the
// seed document's title and author.
export function workSeed(item) {
  const label = String(item?.source_label || '').trim()
  if (label) return label
  const raw = String(item?.origin || '').trim()
  if (!raw) return ''
  if (raw.endsWith('/metadata.bib')) return 'Upstream metadata.bib'
  if (raw.startsWith('search:')) return `Search #${raw.slice('search:'.length)}`
  return raw
}

// Union of the old seedCellText (App.svelte, blank '') and Corpus cellText
// (blank '-'); the blank is passed by the caller. Where the two switches
// differed on the same key beyond the blank, both behaviours are kept:
//   - title: the seed chain (title → source → url → 'Untitled'); the corpus
//     table renders its own title cell and never read this key;
//   - file: seed candidates answer from `file_available`, corpus rows (which
//     have no `file_available`) from `file_path`;
//   - refs / cited_by: seed-only, keep the en-dash placeholder whatever the
//     blank is ('added' and 'seed' are corpus-only).
export function cellText(item, key, { blank = '-' } = {}) {
  switch (key) {
    case 'metadata': return metadataLabel(item)
    case 'download': return downloadLabel(item)
    case 'title': return formatTitle(item)
    case 'authors': return typeof item?.authors_short === 'string' ? item.authors_short : formatAuthorsShort(item)
    case 'year': return item?.year || blank
    case 'added': return formatDateAdded(item?.added_at)
    case 'refs': return item?.refs_count ?? '–'
    case 'cited_by': return item?.cited_by_count ?? '–'
    case 'source': return workSource(item) || blank
    case 'seed': return workSeed(item) || blank
    case 'doi': return item?.doi || blank
    case 'publisher': return item?.publisher || blank
    case 'type': return item?.type || blank
    case 'openalex': return item?.openalex_id || blank
    case 'pages': return item?.pages || blank
    case 'open_access': return item?.open_access_url ? 'Yes' : 'No'
    case 'file': {
      const present = item?.file_available !== undefined ? item.file_available : item?.file_path
      return present ? 'Yes' : 'No'
    }
    default: return blank
  }
}

// Sorts are encoded as "column:direction" (direction 'asc' | 'desc'); '' is
// unsorted. Clicking a column sorts it ascending, clicking it again flips.
export function parseSort(sort) {
  const [column, direction] = String(sort || '').split(':')
  if (!column) return null
  return { column, direction: direction === 'desc' ? 'desc' : 'asc' }
}

export function nextSort(current, column) {
  const parsed = parseSort(current)
  const direction = parsed && parsed.column === column && parsed.direction === 'asc' ? 'desc' : 'asc'
  return `${column}:${direction}`
}

export function sortIndicator(current, column) {
  const parsed = parseSort(current)
  if (!parsed || parsed.column !== column) return ''
  return parsed.direction === 'asc' ? ' ▲' : ' ▼'
}
