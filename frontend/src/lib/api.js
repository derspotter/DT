import {
  sampleSearchResults,
  sampleCorpus,
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
    const response = await fetch(url, { ...options, headers, signal: controller.signal })
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
    `${API_BASE}/api/ingest/latest?baseName=${encodeURIComponent(baseName)}`
  )
  await throwIfUnauthorized(response)
  if (!response.ok) {
    const payload = await response.text()
    throw new Error(payload || 'Failed to load ingest entries')
  }
  return response.json()
}

export async function fetchIngestStats() {
  const response = await fetchWithTimeout(`${API_BASE}/api/ingest/stats`)
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

export async function processMarkedIngestEntries({ limit = 10 } = {}) {
  const response = await fetchWithTimeout(
    `${API_BASE}/api/ingest/process-marked`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ limit }),
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
  yearFrom,
  yearTo,
  relatedDepthDownstream = 1,
  relatedDepthUpstream = 1,
  maxRelated = 30,
}) {
  try {
    const response = await fetchWithTimeout(`${API_BASE}/api/keyword-search`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        query,
        seedJson,
        field,
        yearFrom,
        yearTo,
        relatedDepthDownstream,
        relatedDepthUpstream,
        maxRelated,
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
    return { data: sampleSearchResults, source: 'sample', error }
  }
}

export async function fetchCorpus() {
  try {
    const response = await fetchWithTimeout(`${API_BASE}/api/corpus`)
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
    return { data: sampleCorpus, source: 'sample', error }
  }
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

export async function startDownloadWorker({ intervalSeconds = 60, batchSize = 3 } = {}) {
  const response = await fetchWithTimeout(
    `${API_BASE}/api/downloads/worker/start`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ intervalSeconds, batchSize }),
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

export async function runDownloadWorkerOnce({ batchSize = 3 } = {}) {
  const response = await fetchWithTimeout(
    `${API_BASE}/api/downloads/worker/run-once`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ batchSize }),
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
