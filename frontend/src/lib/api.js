import {
  sampleSearchResults,
  sampleDownloads,
  sampleGraph,
} from './sample-data'

const DEFAULT_TIMEOUT = 8_000
// Uploads can be large over SSH port-forwarding; keep this generous.
const UPLOAD_TIMEOUT = 600_000
// Enrichment / ingestion steps can take a while (OpenAlex + DB IO).
const PIPELINE_TIMEOUT = 600_000
const API_BASE = (import.meta.env.VITE_API_BASE || '').replace(/\/$/, '')
let authToken = localStorage.getItem('rag_feeder_token') || ''

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

async function fetchWithTimeout(url, options = {}, timeoutMs = DEFAULT_TIMEOUT) {
  const controller = new AbortController()
  const timeout = setTimeout(() => controller.abort(), timeoutMs)
  try {
    const headers = { ...(options.headers || {}) }
    if (authToken) {
      headers.Authorization = `Bearer ${authToken}`
    }
    const response = await fetch(url, { cache: 'no-store', ...options, headers, signal: controller.signal })
    return response
  } finally {
    clearTimeout(timeout)
  }
}

async function throwIfUnauthorized(response) {
  if (response.status === 401) {
    const payload = await response.text()
    const error = new Error(payload || 'Unauthorized')
    error.status = 401
    throw error
  }
}

export async function login({ username, password }) {
  const response = await fetchWithTimeout(`${API_BASE}/api/auth/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ username, password }),
  })
  if (!response.ok) {
    const payload = await response.text()
    throw new Error(payload || 'Login failed')
  }
  const payload = await response.json()
  if (payload.token) {
    setAuthToken(payload.token)
  }
  return payload
}

export async function fetchMe() {
  const response = await fetchWithTimeout(`${API_BASE}/api/auth/me`)
  if (!response.ok) {
    const payload = await response.text()
    throw new Error(payload || 'Failed to load user')
  }
  return response.json()
}

export async function requestPasswordReset(email) {
  const response = await fetchWithTimeout(`${API_BASE}/api/auth/forgot-password`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ email }),
  })
  if (!response.ok) {
    const payload = await response.text()
    throw new Error(payload || 'Failed to send password reset email')
  }
  return response.json()
}

export async function acceptInvite({ token, password }) {
  const response = await fetchWithTimeout(`${API_BASE}/api/auth/accept-invite`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ token, password }),
  })
  if (!response.ok) {
    const payload = await response.text()
    throw new Error(payload || 'Failed to accept invite')
  }
  const payload = await response.json()
  if (payload.token) {
    setAuthToken(payload.token)
  }
  return payload
}

export async function resetPassword({ token, password }) {
  const response = await fetchWithTimeout(`${API_BASE}/api/auth/reset-password`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ token, password }),
  })
  if (!response.ok) {
    const payload = await response.text()
    throw new Error(payload || 'Failed to reset password')
  }
  const payload = await response.json()
  if (payload.token) {
    setAuthToken(payload.token)
  }
  return payload
}

export async function createUserInvitation({ username, email }) {
  const response = await fetchWithTimeout(`${API_BASE}/api/auth/invitations`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ username, email }),
  })
  await throwIfUnauthorized(response)
  if (!response.ok) {
    const payload = await response.text()
    throw new Error(payload || 'Failed to create invitation')
  }
  return response.json()
}

export async function fetchCorpora() {
  const response = await fetchWithTimeout(`${API_BASE}/api/corpora`)
  if (!response.ok) {
    const payload = await response.text()
    throw new Error(payload || 'Failed to load corpora')
  }
  return response.json()
}

export async function selectCorpus(corpusId) {
  const response = await fetchWithTimeout(`${API_BASE}/api/corpora/${corpusId}/select`, {
    method: 'POST',
  })
  if (!response.ok) {
    const payload = await response.text()
    throw new Error(payload || 'Failed to select corpus')
  }
  return response.json()
}

export async function createCorpus(name) {
  const response = await fetchWithTimeout(`${API_BASE}/api/corpora`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name }),
  })
  if (!response.ok) {
    const payload = await response.text()
    throw new Error(payload || 'Failed to create corpus')
  }
  return response.json()
}

export async function deleteCorpus(corpusId) {
  const response = await fetchWithTimeout(`${API_BASE}/api/corpora/${corpusId}`, {
    method: 'DELETE',
  })
  await throwIfUnauthorized(response)
  if (!response.ok) {
    const payload = await response.text()
    throw new Error(payload || 'Failed to delete corpus')
  }
  return response.json()
}

export async function shareCorpus(corpusId, { username, role }) {
  const response = await fetchWithTimeout(`${API_BASE}/api/corpora/${corpusId}/share`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ username, role }),
  })
  if (!response.ok) {
    const payload = await response.text()
    throw new Error(payload || 'Failed to share corpus')
  }
  return response.json()
}

export async function fetchKantroposCorpora() {
  const response = await fetchWithTimeout(`${API_BASE}/api/kantropos/corpora`)
  await throwIfUnauthorized(response)
  if (!response.ok) {
    const payload = await response.text()
    throw new Error(payload || 'Failed to load Kantropos corpora')
  }
  return response.json()
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
  const response = await fetchWithTimeout(`${API_BASE}/api/kantropos/corpora/${encodeURIComponent(String(targetId || ''))}/browse${suffix}`)
  await throwIfUnauthorized(response)
  if (!response.ok) {
    const payload = await response.text()
    throw new Error(payload || 'Failed to load Kantropos corpus browser data')
  }
  return response.json()
}

export async function fetchCorpusKantroposAssignment(corpusId) {
  const response = await fetchWithTimeout(`${API_BASE}/api/corpora/${corpusId}/kantropos-assignment`)
  await throwIfUnauthorized(response)
  if (!response.ok) {
    const payload = await response.text()
    throw new Error(payload || 'Failed to load Kantropos assignment')
  }
  return response.json()
}

export async function saveCorpusKantroposAssignment(corpusId, targetId) {
  const response = await fetchWithTimeout(`${API_BASE}/api/corpora/${corpusId}/kantropos-assignment`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ targetId }),
  })
  await throwIfUnauthorized(response)
  if (!response.ok) {
    const payload = await response.text()
    throw new Error(payload || 'Failed to save Kantropos assignment')
  }
  return response.json()
}

export async function uploadPdf(file) {
  const formData = new FormData()
  formData.append('file', file)
  const response = await fetchWithTimeout(
    `${API_BASE}/api/process_pdf`,
    {
      method: 'POST',
      body: formData,
    },
    UPLOAD_TIMEOUT
  )
  await throwIfUnauthorized(response)
  if (!response.ok) {
    const message = await response.text()
    throw new Error(message || 'Upload failed')
  }
  const payload = await response.json()
  return payload
}

export async function extractBibliography(filename) {
  const response = await fetchWithTimeout(
    `${API_BASE}/api/extract-bibliography/${encodeURIComponent(filename)}`,
    {
      method: 'POST',
    }
  )
  await throwIfUnauthorized(response)
  if (!response.ok) {
    const payload = await response.text()
    throw new Error(payload || 'Extraction failed')
  }
  return response.json()
}

export async function fetchBibliographyEntries(baseName) {
  const response = await fetchWithTimeout(
    `${API_BASE}/api/ingest/latest?baseName=${encodeURIComponent(baseName)}&limit=5000`
  )
  await throwIfUnauthorized(response)
  if (!response.ok) {
    const payload = await response.text()
    throw new Error(payload || 'Failed to load ingest entries')
  }
  return response.json()
}

export async function fetchIngestStats({ global = false } = {}) {
  const response = await fetchWithTimeout(`${API_BASE}/api/ingest/stats${global ? '?global=1' : ''}`)
  await throwIfUnauthorized(response)
  if (!response.ok) {
    const payload = await response.text()
    throw new Error(payload || 'Failed to load ingest stats')
  }
  return response.json()
}

export async function fetchIngestRuns(limit = 20) {
  const response = await fetchWithTimeout(`${API_BASE}/api/ingest/runs?limit=${encodeURIComponent(String(limit))}`)
  await throwIfUnauthorized(response)
  if (!response.ok) {
    const payload = await response.text()
    throw new Error(payload || 'Failed to load ingest runs')
  }
  return response.json()
}

export async function fetchSeedSources(limit = 100) {
  const response = await fetchWithTimeout(`${API_BASE}/api/seed/sources?limit=${encodeURIComponent(String(limit))}`)
  await throwIfUnauthorized(response)
  if (!response.ok) {
    const payload = await response.text()
    throw new Error(payload || 'Failed to load seed sources')
  }
  return response.json()
}

export async function fetchSeedCandidates(sourceType, sourceKey) {
  const response = await fetchWithTimeout(
    `${API_BASE}/api/seed/sources/${encodeURIComponent(String(sourceType || ''))}/${encodeURIComponent(String(sourceKey || ''))}/candidates`
  )
  await throwIfUnauthorized(response)
  if (!response.ok) {
    const payload = await response.text()
    throw new Error(payload || 'Failed to load seed candidates')
  }
  return response.json()
}

export async function promoteSeedCandidates(sourceType, sourceKey, {
  candidateKeys = [],
  includeDownstream = false,
  includeUpstream = false,
  relatedDepthDownstream = 0,
  relatedDepthUpstream = 0,
  maxRelated = 30,
  enqueueDownload = true,
  downloadBatchSize = 25,
  workers = 6,
} = {}) {
  const response = await fetchWithTimeout(
    `${API_BASE}/api/seed/sources/${encodeURIComponent(String(sourceType || ''))}/${encodeURIComponent(String(sourceKey || ''))}/promote`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        candidateKeys,
        includeDownstream,
        includeUpstream,
        relatedDepthDownstream,
        relatedDepthUpstream,
        maxRelated,
        enqueueDownload,
        downloadBatchSize,
        workers,
      }),
    },
    PIPELINE_TIMEOUT
  )
  await throwIfUnauthorized(response)
  if (!response.ok) {
    const payload = await response.text()
    throw new Error(payload || 'Failed to promote seed candidates')
  }
  return response.json()
}

export async function dismissSeedCandidates(sourceType, sourceKey, candidateKeys) {
  const response = await fetchWithTimeout(
    `${API_BASE}/api/seed/candidates/dismiss`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sourceType, sourceKey, candidateKeys }),
    },
    PIPELINE_TIMEOUT
  )
  await throwIfUnauthorized(response)
  if (!response.ok) {
    const payload = await response.text()
    throw new Error(payload || 'Failed to dismiss seed candidates')
  }
  return response.json()
}

export async function removeSeedSource(sourceType, sourceKey) {
  const response = await fetchWithTimeout(
    `${API_BASE}/api/seed/sources/${encodeURIComponent(String(sourceType || ''))}/${encodeURIComponent(String(sourceKey || ''))}`,
    {
      method: 'DELETE',
    },
    PIPELINE_TIMEOUT
  )
  await throwIfUnauthorized(response)
  if (!response.ok) {
    const payload = await response.text()
    throw new Error(payload || 'Failed to remove seed source')
  }
  return response.json()
}

export async function enqueueIngestEntries(entryIds) {
  const response = await fetchWithTimeout(
    `${API_BASE}/api/ingest/enqueue`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ entryIds }),
    },
    PIPELINE_TIMEOUT
  )
  await throwIfUnauthorized(response)
  if (!response.ok) {
    const payload = await response.text()
    throw new Error(payload || 'Failed to enqueue entries')
  }
  return response.json()
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
  const response = await fetchWithTimeout(
    `${API_BASE}/api/ingest/process-marked`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        limit,
        includeDownstream,
        includeUpstream,
        relatedDepthDownstream,
        relatedDepthUpstream,
        maxRelated,
        enqueueDownload,
        downloadBatchSize,
      }),
    },
    PIPELINE_TIMEOUT
  )
  await throwIfUnauthorized(response)
  if (!response.ok) {
    const payload = await response.text()
    throw new Error(payload || 'Failed to process marked entries')
  }
  return response.json()
}

export async function runKeywordSearch({
  query,
  seedJson,
  field,
  author,
  yearFrom,
  yearTo,
  includeDownstream = false,
  includeUpstream = false,
  relatedDepthDownstream = 0,
  relatedDepthUpstream = 0,
  maxRelated = 30,
  enqueue = false,
  fallbackToSample = true,
}) {
  try {
    const response = await fetchWithTimeout(`${API_BASE}/api/keyword-search`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        query,
        seedJson,
        field,
        author,
        yearFrom,
        yearTo,
        includeDownstream,
        includeUpstream,
        relatedDepthDownstream,
        relatedDepthUpstream,
        maxRelated,
        enqueue,
      }),
    })
    await throwIfUnauthorized(response)
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`)
    }
    const payload = await response.json()
    const data = Array.isArray(payload) ? payload : payload.results || []
    return {
      data,
      source: payload.source || 'api',
      runId: payload.runId,
      mode: payload.mode,
      expansion: payload.expansion,
    }
  } catch (error) {
    if (error?.status === 401) {
      throw error
    }
    if (!fallbackToSample) {
      throw error
    }
    return { data: sampleSearchResults, source: 'sample', error }
  }
}

export async function fetchRecursionConfig() {
  const response = await fetchWithTimeout(`${API_BASE}/api/recursion-config`)
  await throwIfUnauthorized(response)
  if (!response.ok) {
    const payload = await response.text()
    throw new Error(payload || 'Failed to load recursion config')
  }
  return response.json()
}

export async function fetchCorpus({ limit = 200, offset = 0 } = {}) {
  const params = new URLSearchParams()
  params.set('limit', String(limit))
  params.set('offset', String(offset))
  const response = await fetchWithTimeout(`${API_BASE}/api/corpus?${params.toString()}`)
  await throwIfUnauthorized(response)
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`)
  }
  const payload = await response.json()
  const data = Array.isArray(payload) ? payload : payload.items || []
  const total = Number.isFinite(Number(payload?.total)) ? Number(payload.total) : data.length
  const stageTotals = payload?.stage_totals || null
  const statusCounts = payload?.status_counts || null
  return { data, total, source: payload.source || 'api', stageTotals, statusCounts }
}

export async function fetchDownloadQueue() {
  try {
    const response = await fetchWithTimeout(`${API_BASE}/api/downloads`)
    await throwIfUnauthorized(response)
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`)
    }
    const payload = await response.json()
    const data = Array.isArray(payload) ? payload : payload.items || []
    return { data, source: payload.source || 'api' }
  } catch (error) {
    if (error?.status === 401) {
      throw error
    }
    return { data: sampleDownloads, source: 'sample', error }
  }
}

export async function fetchDownloadWorkerStatus() {
  const response = await fetchWithTimeout(`${API_BASE}/api/downloads/worker/status`)
  await throwIfUnauthorized(response)
  if (!response.ok) {
    const payload = await response.text()
    throw new Error(payload || 'Failed to load download worker status')
  }
  return response.json()
}

export async function startDownloadWorker() {
  const response = await fetchWithTimeout(
    `${API_BASE}/api/downloads/worker/start`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({}),
    },
    PIPELINE_TIMEOUT
  )
  await throwIfUnauthorized(response)
  if (!response.ok) {
    const payload = await response.text()
    throw new Error(payload || 'Failed to start download worker')
  }
  return response.json()
}

export async function stopDownloadWorker({ force = false } = {}) {
  const response = await fetchWithTimeout(
    `${API_BASE}/api/downloads/worker/stop`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ force }),
    },
    PIPELINE_TIMEOUT
  )
  await throwIfUnauthorized(response)
  if (!response.ok) {
    const payload = await response.text()
    throw new Error(payload || 'Failed to stop download worker')
  }
  return response.json()
}

export async function runDownloadWorkerOnce() {
  const response = await fetchWithTimeout(
    `${API_BASE}/api/downloads/worker/run-once`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({}),
    },
    PIPELINE_TIMEOUT
  )
  await throwIfUnauthorized(response)
  if (!response.ok) {
    const payload = await response.text()
    throw new Error(payload || 'Failed to run download worker')
  }
  return response.json()
}

export async function fetchPipelineWorkerStatus() {
  const response = await fetchWithTimeout(`${API_BASE}/api/pipeline/worker/status`)
  await throwIfUnauthorized(response)
  if (!response.ok) {
    const payload = await response.text()
    throw new Error(payload || 'Failed to load pipeline worker status')
  }
  return response.json()
}

export async function fetchPipelineDaemonStatus({ global = false } = {}) {
  const response = await fetchWithTimeout(`${API_BASE}/api/pipeline/daemon/status${global ? '?global=1' : ''}`)
  await throwIfUnauthorized(response)
  if (!response.ok) {
    const payload = await response.text()
    throw new Error(payload || 'Failed to load daemon status')
  }
  return response.json()
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
  const response = await fetchWithTimeout(`${API_BASE}/api/pipeline/jobs/summary${query ? `?${query}` : ''}`)
  await throwIfUnauthorized(response)
  if (!response.ok) {
    const payload = await response.text()
    throw new Error(payload || 'Failed to load pipeline jobs summary')
  }
  return response.json()
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
  const response = await fetchWithTimeout(`${API_BASE}/api/logs/tail?${params.toString()}`)
  await throwIfUnauthorized(response)
  if (!response.ok) {
    const payload = await response.text()
    throw new Error(payload || 'Failed to fetch log tail')
  }
  return response.json()
}

export async function startPipelineWorker({
  intervalSeconds = 15,
  promoteBatchSize = 25,
  promoteWorkers = 6,
  downloadBatchSize = 5,
  downloadWorkers = 0,
} = {}) {
  const response = await fetchWithTimeout(
    `${API_BASE}/api/pipeline/worker/start`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ intervalSeconds, promoteBatchSize, promoteWorkers, downloadBatchSize, downloadWorkers }),
    },
    PIPELINE_TIMEOUT
  )
  await throwIfUnauthorized(response)
  if (!response.ok) {
    const payload = await response.text()
    throw new Error(payload || 'Failed to start pipeline worker')
  }
  return response.json()
}

export async function pausePipelineWorker({ force = false } = {}) {
  const response = await fetchWithTimeout(
    `${API_BASE}/api/pipeline/worker/pause`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ force }),
    },
    PIPELINE_TIMEOUT
  )
  await throwIfUnauthorized(response)
  if (!response.ok) {
    const payload = await response.text()
    throw new Error(payload || 'Failed to pause pipeline worker')
  }
  return response.json()
}

export async function downloadCorpusExport(format, { status = '', yearFrom = '', yearTo = '', source = '' } = {}) {
  const params = new URLSearchParams()
  if (status) params.set('status', String(status))
  if (yearFrom !== '' && yearFrom !== null && yearFrom !== undefined) params.set('year_from', String(yearFrom))
  if (yearTo !== '' && yearTo !== null && yearTo !== undefined) params.set('year_to', String(yearTo))
  if (source) params.set('source', String(source))
  const query = params.toString()
  const endpoint = `${API_BASE}/api/exports/${encodeURIComponent(String(format || ''))}${query ? `?${query}` : ''}`

  const response = await fetchWithTimeout(
    endpoint,
    {
      method: 'GET',
    },
    PIPELINE_TIMEOUT
  )
  await throwIfUnauthorized(response)
  if (!response.ok) {
    const payload = await response.text()
    throw new Error(payload || 'Failed to export corpus data')
  }

  const blob = await response.blob()
  const disposition = response.headers.get('content-disposition') || ''
  let filename = ''
  const star = disposition.match(/filename\*=UTF-8''([^;]+)/i)
  const plain = disposition.match(/filename=\"?([^\";]+)\"?/i)
  if (star && star[1]) {
    filename = decodeURIComponent(star[1])
  } else if (plain && plain[1]) {
    filename = plain[1]
  }

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

export async function downloadCorpusItemFile(itemId) {
  const response = await fetchWithTimeout(
    `${API_BASE}/api/corpus/downloaded/${encodeURIComponent(String(itemId || ''))}/file`,
    { method: 'GET' },
    PIPELINE_TIMEOUT
  )
  await throwIfUnauthorized(response)
  if (!response.ok) {
    const payload = await response.text()
    throw new Error(payload || 'Failed to download file')
  }

  const blob = await response.blob()
  const disposition = response.headers.get('content-disposition') || ''
  let filename = ''
  const star = disposition.match(/filename\*=UTF-8''([^;]+)/i)
  const plain = disposition.match(/filename=\"?([^\";]+)\"?/i)
  if (star && star[1]) {
    filename = decodeURIComponent(star[1])
  } else if (plain && plain[1]) {
    filename = plain[1]
  }
  if (!filename) {
    filename = `corpus-item-${String(itemId || 'file')}.pdf`
  }
  return { blob, filename }
}

export async function fetchGraph({
  maxNodes = 200,
  relationship = 'both',
  status = 'all',
  yearFrom = null,
  yearTo = null,
  hideIsolates = true,
} = {}) {
  try {
    const params = new URLSearchParams()
    params.set('max_nodes', String(maxNodes))
    if (relationship) params.set('relationship', relationship)
    if (status) params.set('status', status)
    params.set('hide_isolates', hideIsolates ? '1' : '0')
    if (yearFrom !== null && yearFrom !== undefined && yearFrom !== '') {
      params.set('year_from', String(yearFrom))
    }
    if (yearTo !== null && yearTo !== undefined && yearTo !== '') {
      params.set('year_to', String(yearTo))
    }
    const response = await fetchWithTimeout(`${API_BASE}/api/graph?${params.toString()}`)
    await throwIfUnauthorized(response)
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`)
    }
    const payload = await response.json()
    const data = payload.nodes ? payload : payload.data || payload
    return { data, source: payload.source || 'api' }
  } catch (error) {
    if (error?.status === 401) {
      throw error
    }
    return { data: sampleGraph, source: 'sample', error }
  }
}
