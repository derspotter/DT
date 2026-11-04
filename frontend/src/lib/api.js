const DEFAULT_TIMEOUT = 8_000
// Uploads can be large over SSH port-forwarding; keep this generous.
const UPLOAD_TIMEOUT = 600_000
// Enrichment / ingestion steps can take a while (OpenAlex + DB IO).
const PIPELINE_TIMEOUT = 600_000
const API_BASE = (import.meta.env.VITE_API_BASE || '').replace(/\/$/, '')
let authToken = localStorage.getItem('rag_feeder_token') || ''
let unauthorizedHandler = null

export function setAuthToken(token) {
  authToken = token || ''
  if (authToken) {
    localStorage.setItem('rag_feeder_token', authToken)
  } else {
    localStorage.removeItem('rag_feeder_token')
  }
}

export function getAuthToken() {
  return authToken
}

// The app registers one handler (its sign-out path); every request that
// answers 401 runs it before throwing.
export function onUnauthorized(handler) {
  unauthorizedHandler = typeof handler === 'function' ? handler : null
}

// `fetch` rejects with text meant for developers ("signal is aborted without
// reason", "Failed to fetch"). HTTP errors (they carry a status) and anything
// else, such as a body that is not JSON, pass through.
function transportError(error, timedOut) {
  if (error?.status !== undefined) return error
  if (timedOut) return new Error('The server did not answer in time.', { cause: error })
  if (error instanceof TypeError) return new Error('Could not reach the server.', { cause: error })
  return error
}

// `read` runs while the timer is armed, so a body that stalls after the
// headers is aborted too. A `raw` caller reads its own body after this returns.
async function fetchWithTimeout(url, options = {}, timeoutMs = DEFAULT_TIMEOUT, read = (response) => response) {
  const controller = new AbortController()
  const timeout = setTimeout(() => controller.abort(), timeoutMs)
  try {
    const headers = { ...(options.headers || {}) }
    if (authToken) {
      headers.Authorization = `Bearer ${authToken}`
    }
    const response = await fetch(url, { cache: 'no-store', ...options, headers, signal: controller.signal })
    return await read(response)
  } catch (error) {
    throw transportError(error, controller.signal.aborted)
  } finally {
    clearTimeout(timeout)
  }
}

function apiUrl(pathOrUrl) {
  const value = String(pathOrUrl || '')
  if (/^https?:\/\//i.test(value)) return value
  return `${API_BASE}${value.startsWith('/') ? value : `/${value}`}`
}

function isPlainObject(value) {
  if (!value || typeof value !== 'object') return false
  const proto = Object.getPrototypeOf(value)
  return proto === Object.prototype || proto === null || Array.isArray(value)
}

// The one request path: bearer header, timeout, JSON body, 401 handling and
// error messages. `notifyUnauthorized: false` is for the sign-in endpoints,
// whose 401 means "wrong credentials", not "session expired".
async function requestJson(path, {
  method = 'GET',
  body,
  headers = {},
  timeout = DEFAULT_TIMEOUT,
  errorMessage = 'Request failed',
  raw = false,
  notifyUnauthorized = true,
} = {}) {
  const options = { method, headers: { ...headers } }
  if (body !== undefined) {
    if (isPlainObject(body)) {
      options.headers['Content-Type'] = 'application/json'
      options.body = JSON.stringify(body)
    } else {
      options.body = body
    }
  }
  const sentToken = authToken
  return fetchWithTimeout(apiUrl(path), options, timeout, async (response) => {
    if (response.status === 401) {
      const text = await response.text().catch(() => '')
      // A 401 for a token that is no longer current (signed out, or signed in
      // again meanwhile) says nothing about the session in use now.
      if (notifyUnauthorized && sentToken === authToken) unauthorizedHandler?.()
      throw Object.assign(new Error(errorText(text) || 'Unauthorized'), { status: 401 })
    }
    if (!response.ok) {
      const text = await response.text().catch(() => '')
      throw Object.assign(new Error(errorText(text) || errorMessage), { status: response.status })
    }
    return raw ? response : response.json()
  })
}

// The backend answers errors as { error: '...' }; show that string, not the
// raw JSON body.
function errorText(text) {
  if (!text) return ''
  try {
    const parsed = JSON.parse(text)
    if (parsed && typeof parsed.error === 'string' && parsed.error) return parsed.error
  } catch {
    // not JSON; use the text as is
  }
  return text
}

function dispositionFilename(response) {
  const disposition = response.headers.get('content-disposition') || ''
  const star = disposition.match(/filename\*=UTF-8''([^;]+)/i)
  const plain = disposition.match(/filename=\"?([^\";]+)\"?/i)
  if (star && star[1]) return decodeURIComponent(star[1])
  if (plain && plain[1]) return plain[1]
  return ''
}

// The sign-in calls return the payload and leave the token to the caller: only
// the attempt that still owns the session may store it.
export async function login({ username, password }) {
  return requestJson('/api/auth/login', {
    method: 'POST',
    body: { username, password },
    errorMessage: 'Login failed',
    notifyUnauthorized: false,
  })
}

export async function fetchMe() {
  return requestJson('/api/auth/me', { errorMessage: 'Failed to load user' })
}

export async function requestPasswordReset(email) {
  return requestJson('/api/auth/forgot-password', {
    method: 'POST',
    body: { email },
    errorMessage: 'Failed to send password reset email',
    notifyUnauthorized: false,
  })
}

export async function acceptInvite({ token, password }) {
  return requestJson('/api/auth/accept-invite', {
    method: 'POST',
    body: { token, password },
    errorMessage: 'Failed to accept invite',
    notifyUnauthorized: false,
  })
}

export async function resetPassword({ token, password }) {
  return requestJson('/api/auth/reset-password', {
    method: 'POST',
    body: { token, password },
    errorMessage: 'Failed to reset password',
    notifyUnauthorized: false,
  })
}

export async function createUserInvitation({ username, email }) {
  return requestJson('/api/auth/invitations', {
    method: 'POST',
    body: { username, email },
    errorMessage: 'Failed to create invitation',
  })
}

export async function fetchCorpora() {
  return requestJson('/api/corpora', { errorMessage: 'Failed to load corpora' })
}

export async function selectCorpus(corpusId) {
  return requestJson(`/api/corpora/${corpusId}/select`, { method: 'POST', errorMessage: 'Failed to select corpus' })
}

export async function createCorpus(name) {
  return requestJson('/api/corpora', { method: 'POST', body: { name }, errorMessage: 'Failed to create corpus' })
}

export async function deleteCorpus(corpusId) {
  return requestJson(`/api/corpora/${corpusId}`, { method: 'DELETE', errorMessage: 'Failed to delete corpus' })
}

export async function shareCorpus(corpusId, { username, role }) {
  return requestJson(`/api/corpora/${corpusId}/share`, {
    method: 'POST',
    body: { username, role },
    errorMessage: 'Failed to share corpus',
  })
}

export async function fetchKantroposCorpora() {
  return requestJson('/api/kantropos/corpora', { errorMessage: 'Failed to load Kantropos corpora' })
}

export async function fetchKantroposCorpusBrowse(targetId, options = {}) {
  const params = new URLSearchParams()
  if (Number.isFinite(Number(options.currentLimit)) && Number(options.currentLimit) > 0) {
    params.set('current_limit', String(Number(options.currentLimit)))
  }
  if (Number.isFinite(Number(options.pendingLimit)) && Number(options.pendingLimit) > 0) {
    params.set('pending_limit', String(Number(options.pendingLimit)))
  }
  const suffix = params.size > 0 ? `?${params.toString()}` : ''
  return requestJson(`/api/kantropos/corpora/${encodeURIComponent(String(targetId || ''))}/browse${suffix}`, {
    errorMessage: 'Failed to load Kantropos corpus browser data',
  })
}

export async function fetchCorpusKantroposAssignment(corpusId) {
  return requestJson(`/api/corpora/${corpusId}/kantropos-assignment`, { errorMessage: 'Failed to load Kantropos assignment' })
}

export async function saveCorpusKantroposAssignment(corpusId, targetId) {
  return requestJson(`/api/corpora/${corpusId}/kantropos-assignment`, {
    method: 'PUT',
    body: { targetId },
    errorMessage: 'Failed to save Kantropos assignment',
  })
}

export async function createScraperRun({ sourceType, query }) {
  return requestJson('/api/scraper/runs', {
    method: 'POST',
    body: { sourceType, query },
    timeout: PIPELINE_TIMEOUT,
    errorMessage: 'Failed to create scraper run',
  })
}

export async function fetchScraperRuns(limit = 50) {
  return requestJson(`/api/scraper/runs?limit=${encodeURIComponent(String(limit))}`, { errorMessage: 'Failed to load scraper runs' })
}

export async function fetchScraperRun(runId) {
  return requestJson(`/api/scraper/runs/${encodeURIComponent(String(runId || ''))}`, { errorMessage: 'Failed to load scraper run' })
}

export async function applyScraperArtifacts({ targetId, artifactIds }) {
  return requestJson('/api/scraper/apply', {
    method: 'POST',
    body: { targetId, artifactIds },
    timeout: PIPELINE_TIMEOUT,
    errorMessage: 'Failed to apply scraper artifacts',
  })
}

export async function downloadScraperMetadataDraft(eventId) {
  const response = await requestJson(`/api/scraper/apply-events/${encodeURIComponent(String(eventId || ''))}/metadata-bib`, {
    timeout: PIPELINE_TIMEOUT,
    errorMessage: 'Failed to download generated metadata.bib',
    raw: true,
  })
  const blob = await response.blob()
  return { blob, filename: 'metadata.bib' }
}

export async function uploadPdf(file) {
  const formData = new FormData()
  formData.append('file', file)
  return requestJson('/api/process_pdf', {
    method: 'POST',
    body: formData,
    timeout: UPLOAD_TIMEOUT,
    errorMessage: 'Upload failed',
  })
}

export async function importSeed(filename, kind) {
  return requestJson('/api/ingest/import-seed', {
    method: 'POST',
    body: { filename, kind },
    timeout: UPLOAD_TIMEOUT,
    errorMessage: 'Import failed',
  })
}

export async function extractBibliography(filename) {
  return requestJson(`/api/extract-bibliography/${encodeURIComponent(filename)}`, {
    method: 'POST',
    errorMessage: 'Extraction failed',
  })
}

export async function fetchBibliographyEntries(baseName) {
  return requestJson(`/api/ingest/latest?baseName=${encodeURIComponent(baseName)}&limit=5000`, {
    errorMessage: 'Failed to load ingest entries',
  })
}

export async function fetchIngestStats({ global = false } = {}) {
  return requestJson(`/api/ingest/stats${global ? '?global=1' : ''}`, { errorMessage: 'Failed to load ingest stats' })
}

export async function fetchIngestRuns(limit = 20) {
  return requestJson(`/api/ingest/runs?limit=${encodeURIComponent(String(limit))}`, { errorMessage: 'Failed to load ingest runs' })
}

export async function fetchSeedSources(limit = 100, { q = '' } = {}) {
  const params = new URLSearchParams()
  params.set('limit', String(limit))
  if (q) params.set('q', q)
  return requestJson(`/api/seed/sources?${params.toString()}`, { errorMessage: 'Failed to load seed sources' })
}

// `lightSummary` asks the route to skip the source_summary state counts,
// which cost a full-seed state resolution. Background polls do not read them
// (the expanded seed's pills come from reconcileSeedSourceCandidates over the
// rows already on screen), so they pay for nothing.
export async function fetchSeedCandidates(sourceType, sourceKey, { q = '', limit = 200, offset = 0, sort = '', dir = 'asc', lightSummary = false } = {}) {
  const params = new URLSearchParams()
  if (q) params.set('q', q)
  params.set('limit', String(limit))
  params.set('offset', String(offset))
  if (sort) { params.set('sort', sort); params.set('dir', dir) }
  if (lightSummary) params.set('summary', 'light')
  return requestJson(
    `/api/seed/sources/${encodeURIComponent(String(sourceType || ''))}/${encodeURIComponent(String(sourceKey || ''))}/candidates?${params}`,
    { errorMessage: 'Failed to load seed candidates' }
  )
}

export async function downloadSeedCandidateFile(sourceType, sourceKey, candidateKey) {
  const response = await requestJson(
    `/api/seed/sources/${encodeURIComponent(String(sourceType || ''))}/${encodeURIComponent(String(sourceKey || ''))}/candidates/${encodeURIComponent(String(candidateKey || ''))}/file`,
    { timeout: PIPELINE_TIMEOUT, errorMessage: 'Failed to download seed candidate file', raw: true }
  )
  const blob = await response.blob()
  const filename = dispositionFilename(response) || 'seed-candidate.pdf'
  return { blob, filename }
}

export async function promoteSeedCandidates(sourceType, sourceKey, {
  candidateKeys = [],
  q = '',
  includeDownstream = false,
  includeUpstream = false,
  relatedDepthDownstream = 0,
  relatedDepthUpstream = 0,
  maxRelated = 30,
  relatedSort = 'most_cited',
  promotionMode = 'new_seed',
  enqueueDownload = true,
  downloadBatchSize = 25,
  workers = 6,
} = {}) {
  return requestJson(
    `/api/seed/sources/${encodeURIComponent(String(sourceType || ''))}/${encodeURIComponent(String(sourceKey || ''))}/promote`,
    {
      method: 'POST',
      body: {
        candidateKeys,
        q,
        includeDownstream,
        includeUpstream,
        relatedDepthDownstream,
        relatedDepthUpstream,
        maxRelated,
        relatedSort,
        promotionMode,
        enqueueDownload,
        downloadBatchSize,
        workers,
      },
      timeout: PIPELINE_TIMEOUT,
      errorMessage: 'Failed to promote seed candidates',
    }
  )
}

export async function dismissSeedCandidates(sourceType, sourceKey, candidateKeys, { all = false, q = '' } = {}) {
  return requestJson('/api/seed/candidates/dismiss', {
    method: 'POST',
    body: { sourceType, sourceKey, ...(all ? { all: true, q } : { candidateKeys }) },
    timeout: PIPELINE_TIMEOUT,
    errorMessage: 'Failed to dismiss seed candidates',
  })
}

export async function removeSeedSource(sourceType, sourceKey) {
  return requestJson(
    `/api/seed/sources/${encodeURIComponent(String(sourceType || ''))}/${encodeURIComponent(String(sourceKey || ''))}`,
    { method: 'DELETE', timeout: PIPELINE_TIMEOUT, errorMessage: 'Failed to remove seed source' }
  )
}

export async function enqueueIngestEntries(entryIds) {
  return requestJson('/api/ingest/enqueue', {
    method: 'POST',
    body: { entryIds },
    timeout: PIPELINE_TIMEOUT,
    errorMessage: 'Failed to enqueue entries',
  })
}

export async function processMarkedIngestEntries({
  limit = 10,
  includeDownstream = false,
  includeUpstream = false,
  relatedDepthDownstream = 0,
  relatedDepthUpstream = 0,
  maxRelated = 30,
  enqueueDownload = false,
  downloadBatchSize = 25,
} = {}) {
  return requestJson('/api/ingest/process-marked', {
    method: 'POST',
    body: {
      limit,
      includeDownstream,
      includeUpstream,
      relatedDepthDownstream,
      relatedDepthUpstream,
      maxRelated,
      enqueueDownload,
      downloadBatchSize,
    },
    timeout: PIPELINE_TIMEOUT,
    errorMessage: 'Failed to process marked entries',
  })
}

// Type-ahead for the Topic field; the backend proxies OpenAlex autocomplete
// (free of credits) and returns [{ id, label, hint, works_count }].
export async function fetchTopicSuggestions(q) {
  const payload = await requestJson(`/api/openalex/topics?q=${encodeURIComponent(String(q || ''))}`, {
    timeout: 15_000,
    errorMessage: 'Topic lookup failed',
  })
  return Array.isArray(payload?.topics) ? payload.topics : []
}

export async function previewKeywordSearch(body) {
  return requestJson('/api/keyword-search/preview', {
    method: 'POST',
    body,
    timeout: 60_000,
    errorMessage: 'Preview failed',
  })
}

export async function cancelKeywordSearch(runId) {
  return requestJson(`/api/keyword-search/${encodeURIComponent(String(runId))}/cancel`, { method: 'POST', errorMessage: 'Cancel failed' })
}

export async function runKeywordSearch({
  query,
  seedJson,
  field,
  author,
  yearFrom,
  yearTo,
  maxResults = 0,
  sort = '',
  includeDownstream = false,
  includeUpstream = false,
  relatedDepthDownstream = 0,
  relatedDepthUpstream = 0,
  maxRelated = 30,
  enqueue = false,
  topics = [],
}) {
  const response = await requestJson('/api/keyword-search', {
    method: 'POST',
    body: {
      query,
      seedJson,
      field,
      author,
      yearFrom,
      yearTo,
      maxResults,
      sort,
      includeDownstream,
      includeUpstream,
      relatedDepthDownstream,
      relatedDepthUpstream,
      maxRelated,
      enqueue,
      topics,
    },
    timeout: PIPELINE_TIMEOUT,
    errorMessage: 'Search failed',
    raw: true,
  })
  const payload = await response.json()
  const running = response.status === 202
  const data = running ? [] : (Array.isArray(payload) ? payload : payload.results || [])
  return {
    data,
    source: payload.source || 'api',
    runId: payload.runId,
    expansion: payload.expansion,
    running,
  }
}

export async function fetchRecursionConfig() {
  return requestJson('/api/recursion-config', { errorMessage: 'Failed to load recursion config' })
}

export async function fetchOpenAlexQuota() {
  return requestJson('/api/openalex/quota', { errorMessage: 'Failed to load OpenAlex quota' })
}

export async function fetchCorpus({ limit = 200, offset = 0, q = '', sort = '' } = {}) {
  const params = new URLSearchParams()
  params.set('limit', String(limit))
  params.set('offset', String(offset))
  if (q) params.set('q', q)
  if (sort) params.set('sort', sort)
  const payload = await requestJson(`/api/corpus?${params.toString()}`, { errorMessage: 'Failed to load corpus' })
  const data = Array.isArray(payload) ? payload : payload.items || []
  const total = Number.isFinite(Number(payload?.total)) ? Number(payload.total) : data.length
  const stageTotals = payload?.stage_totals || null
  const statusCounts = payload?.status_counts || null
  return { data, total, source: payload.source || 'api', stageTotals, statusCounts }
}

// Fetched as a blob rather than linked directly: this route is behind the
// normal auth header, and a plain <a href> would not carry it.
// A Content-Disposition filename may be percent-encoded (RFC 5987) or plain;
// a plain name containing "%" would make decodeURIComponent throw, so fall
// back to the raw value, and never let a path separator through.
function safeDownloadFilename(raw) {
  let name = String(raw || '')
  try {
    name = decodeURIComponent(name)
  } catch {
    // keep the raw name
  }
  name = name.split(/[\\/]/).pop() || ''
  return name || 'download.pdf'
}

export async function fetchSeedSourceDocument(sourceKey) {
  const response = await requestJson(`/api/seed/sources/pdf/${encodeURIComponent(String(sourceKey || ''))}/file`, {
    errorMessage: 'Failed to download the seed document',
    raw: true,
  })
  const disposition = response.headers.get('content-disposition') || ''
  const match = disposition.match(/filename\*?=(?:UTF-8'')?"?([^";]+)"?/i)
  const filename = match ? safeDownloadFilename(match[1]) : `${sourceKey || 'seed'}.pdf`
  return { blob: await response.blob(), filename }
}

export async function removeCorpusWorks(workIds) {
  return requestJson('/api/corpus/works/remove', {
    method: 'POST',
    body: { workIds },
    errorMessage: 'Failed to remove items from corpus',
  })
}

export async function fetchAppSettings() {
  return requestJson('/api/admin/settings', { errorMessage: 'Failed to load settings' })
}

export async function saveAppSettings(settings) {
  return requestJson('/api/admin/settings', {
    method: 'PUT',
    body: { settings },
    errorMessage: 'Failed to save settings',
  })
}

export async function removeCorpusWork(workId) {
  return requestJson(`/api/corpus/works/${encodeURIComponent(String(workId))}`, {
    method: 'DELETE',
    errorMessage: 'Failed to remove item from corpus',
  })
}

export async function fetchDownloadQueue() {
  const payload = await requestJson('/api/downloads', { errorMessage: 'Failed to load download activity' })
  const data = Array.isArray(payload) ? payload : payload.items || []
  return { data, source: payload.source || 'api' }
}

export async function fetchDownloadWorkerStatus() {
  return requestJson('/api/downloads/worker/status', { errorMessage: 'Failed to load download worker status' })
}

export async function startDownloadWorker() {
  return requestJson('/api/downloads/worker/start', {
    method: 'POST',
    body: {},
    timeout: PIPELINE_TIMEOUT,
    errorMessage: 'Failed to start download worker',
  })
}

export async function stopDownloadWorker({ force = false } = {}) {
  return requestJson('/api/downloads/worker/stop', {
    method: 'POST',
    body: { force },
    timeout: PIPELINE_TIMEOUT,
    errorMessage: 'Failed to stop download worker',
  })
}

export async function runDownloadWorkerOnce() {
  return requestJson('/api/downloads/worker/run-once', {
    method: 'POST',
    body: {},
    timeout: PIPELINE_TIMEOUT,
    errorMessage: 'Failed to run download worker',
  })
}

export async function fetchPipelineWorkerStatus() {
  return requestJson('/api/pipeline/worker/status', { errorMessage: 'Failed to load pipeline worker status' })
}

export async function fetchPipelineDaemonStatus({ global = false } = {}) {
  return requestJson(`/api/pipeline/daemon/status${global ? '?global=1' : ''}`, { errorMessage: 'Failed to load daemon status' })
}

export async function fetchPipelineJobsSummary({ jobIds = [], types = [], recentLimit = 50 } = {}) {
  const params = new URLSearchParams()
  if (Array.isArray(jobIds) && jobIds.length > 0) {
    const normalized = [...new Set(jobIds.map((v) => Number(v)).filter((v) => Number.isFinite(v) && v > 0))]
    if (normalized.length > 0) {
      params.set('job_ids', normalized.join(','))
    }
  }
  if (Array.isArray(types) && types.length > 0) {
    const normalizedTypes = [...new Set(types.map((v) => String(v || '').trim().toLowerCase()).filter(Boolean))]
    if (normalizedTypes.length > 0) {
      params.set('types', normalizedTypes.join(','))
    }
  }
  const parsedLimit = Number(recentLimit)
  if (Number.isFinite(parsedLimit) && parsedLimit > 0) {
    params.set('recent_limit', String(parsedLimit))
  }

  const query = params.toString()
  return requestJson(`/api/pipeline/jobs/summary${query ? `?${query}` : ''}`, { errorMessage: 'Failed to load pipeline jobs summary' })
}

export async function fetchLogsTail({
  type = 'pipeline',
  lines = 200,
  includeRotated = true,
  cursor = 0,
  corpusId = null,
} = {}) {
  const params = new URLSearchParams()
  params.set('type', String(type || 'pipeline'))
  params.set('lines', String(lines || 200))
  params.set('cursor', String(cursor || 0))
  params.set('include_rotated', includeRotated ? '1' : '0')
  if (type === 'pipeline' && corpusId !== undefined && corpusId !== null) {
    params.set('corpus_id', String(corpusId))
  }
  return requestJson(`/api/logs/tail?${params.toString()}`, { errorMessage: 'Failed to fetch log tail' })
}

export async function startPipelineWorker({
  intervalSeconds = 15,
  promoteBatchSize = 25,
  promoteWorkers = 6,
  downloadBatchSize = 5,
  downloadWorkers = 0,
} = {}) {
  return requestJson('/api/pipeline/worker/start', {
    method: 'POST',
    body: { intervalSeconds, promoteBatchSize, promoteWorkers, downloadBatchSize, downloadWorkers },
    timeout: PIPELINE_TIMEOUT,
    errorMessage: 'Failed to start pipeline worker',
  })
}

export async function pausePipelineWorker({ force = false } = {}) {
  return requestJson('/api/pipeline/worker/pause', {
    method: 'POST',
    body: { force },
    timeout: PIPELINE_TIMEOUT,
    errorMessage: 'Failed to pause pipeline worker',
  })
}

export async function downloadCorpusExport(format, { status = '', yearFrom = '', yearTo = '', source = '' } = {}) {
  const params = new URLSearchParams()
  if (status) params.set('status', String(status))
  if (yearFrom !== '' && yearFrom !== null && yearFrom !== undefined) params.set('year_from', String(yearFrom))
  if (yearTo !== '' && yearTo !== null && yearTo !== undefined) params.set('year_to', String(yearTo))
  if (source) params.set('source', String(source))
  const query = params.toString()
  const endpoint = `/api/exports/${encodeURIComponent(String(format || ''))}${query ? `?${query}` : ''}`

  const response = await requestJson(endpoint, {
    timeout: PIPELINE_TIMEOUT,
    errorMessage: 'Failed to export corpus data',
    raw: true,
  })

  const blob = await response.blob()
  let filename = dispositionFilename(response)

  if (!filename) {
    const fallback = {
      json: 'references.json',
      bibtex: 'references.bib',
      pdfs: 'references.zip',
      bundle: 'references-bundle.zip',
    }
    filename = fallback[String(format)] || 'references-export.bin'
  }

  return { blob, filename }
}

export async function createCorpusItemDownloadUrl(itemId) {
  const payload = await requestJson(`/api/corpus/downloaded/${encodeURIComponent(String(itemId || ''))}/download-ticket`, {
    method: 'POST',
    timeout: PIPELINE_TIMEOUT,
    errorMessage: 'Failed to prepare file download',
  })
  const url = String(payload?.url || '').trim()
  if (!url) throw new Error('Download ticket response did not include a URL')
  return API_BASE && url.startsWith('/') ? `${API_BASE}${url}` : url
}

export async function createUpstreamCorpusItemDownloadUrl(targetId, itemId) {
  const payload = await requestJson(
    `/api/kantropos/corpora/${encodeURIComponent(String(targetId || ''))}/works/${encodeURIComponent(String(itemId || ''))}/download-ticket`,
    { method: 'POST', timeout: PIPELINE_TIMEOUT, errorMessage: 'Failed to prepare upstream file download' }
  )
  const url = String(payload?.url || '').trim()
  if (!url) throw new Error('Download ticket response did not include a URL')
  return API_BASE && url.startsWith('/') ? `${API_BASE}${url}` : url
}

function graph3DParams({
  maxNodes = 10000,
  relationship = 'both',
  yearFrom = null,
  yearTo = null,
  groupBy = 'field',
  refresh = false,
  corpusId = 'all',
} = {}) {
  // status/scope are fixed server-side (downloaded works); the backend ignores
  // them, so they are not sent. corpus_id scopes the graph: 'all' keeps the
  // historical view that merges every corpus.
  const params = new URLSearchParams()
  if (corpusId !== null && corpusId !== undefined && corpusId !== '') {
    params.set('corpus_id', String(corpusId))
  }
  params.set('max_nodes', String(maxNodes))
  if (relationship) params.set('relationship', relationship)
  if (yearFrom !== null && yearFrom !== undefined && yearFrom !== '') {
    params.set('year_from', String(yearFrom))
  }
  if (yearTo !== null && yearTo !== undefined && yearTo !== '') {
    params.set('year_to', String(yearTo))
  }
  if (groupBy) params.set('group_by', groupBy)
  if (refresh) params.set('refresh', '1')
  return params
}

async function fetchGraph3DResource(pathOrUrl, type = 'json') {
  const response = await requestJson(pathOrUrl, { timeout: PIPELINE_TIMEOUT, errorMessage: 'Failed to load graph data', raw: true })
  return type === 'arrayBuffer' ? response.arrayBuffer() : response.json()
}

export async function fetchGraph3DSnapshot(options = {}) {
  const params = graph3DParams(options)
  return requestJson(`/api/graph/3d/snapshot?${params.toString()}`, { timeout: PIPELINE_TIMEOUT, errorMessage: 'Failed to load graph snapshot' })
}

export async function fetchGraph3DSnapshotResources(manifest) {
  const files = manifest?.files || {}
  const inlineNodes = manifest?.nodes_meta || manifest?.nodes || null
  const inlineEdges = manifest?.edge_triplets || manifest?.edges || null
  const inlineClusters = manifest?.clusters || null
  const [nodesBuffer, edgesBuffer, nodesMeta, clusters] = await Promise.all([
    files.nodes ? fetchGraph3DResource(files.nodes, 'arrayBuffer') : null,
    files.edges ? fetchGraph3DResource(files.edges, 'arrayBuffer') : null,
    files.nodes_meta ? fetchGraph3DResource(files.nodes_meta, 'json') : inlineNodes,
    files.clusters ? fetchGraph3DResource(files.clusters, 'json') : inlineClusters,
  ])
  const decodedInlineEdges = Array.isArray(inlineEdges) && typeof inlineEdges[0] === 'number'
    ? new Uint32Array(inlineEdges)
    : inlineEdges
  return {
    manifest,
    positions: nodesBuffer ? new Float32Array(nodesBuffer) : null,
    edgeTriplets: edgesBuffer ? new Uint32Array(edgesBuffer) : decodedInlineEdges,
    nodes: nodesMeta || [],
    clusters: clusters || [],
  }
}

export async function fetchGraph3DNodeDetail(nodeId) {
  return requestJson(`/api/graph/3d/node/${encodeURIComponent(String(nodeId || ''))}`, {
    timeout: PIPELINE_TIMEOUT,
    errorMessage: 'Failed to load graph node',
  })
}

export async function fetchGraph3DClusterDetail(snapshotKey, clusterId) {
  return requestJson(
    `/api/graph/3d/cluster/${encodeURIComponent(String(snapshotKey || ''))}/${encodeURIComponent(String(clusterId || ''))}`,
    { timeout: PIPELINE_TIMEOUT, errorMessage: 'Failed to load graph cluster' }
  )
}
