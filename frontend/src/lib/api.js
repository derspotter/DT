import {
  sampleBibliographies,
  sampleSearchResults,
  sampleCorpus,
  sampleDownloads,
  sampleGraph,
} from './sample-data'

const DEFAULT_TIMEOUT = 8_000
const UPLOAD_TIMEOUT = 120_000
const API_BASE = (import.meta.env.VITE_API_BASE || '').replace(/\/$/, '')

async function fetchWithTimeout(url, options = {}, timeoutMs = DEFAULT_TIMEOUT) {
  const controller = new AbortController()
  const timeout = setTimeout(() => controller.abort(), timeoutMs)
  try {
    const response = await fetch(url, { ...options, signal: controller.signal })
    return response
  } finally {
    clearTimeout(timeout)
  }
}

export async function fetchBibliographyList() {
  try {
    const response = await fetchWithTimeout(`${API_BASE}/api/bibliographies/all-current`)
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`)
    }
    const data = await response.json()
    return { data, source: 'api' }
  } catch (error) {
    return { data: sampleBibliographies, source: 'sample', error }
  }
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
  if (!response.ok) {
    const payload = await response.text()
    throw new Error(payload || 'Extraction failed')
  }
  return response.json()
}

export async function consolidateBibliographies() {
  const response = await fetchWithTimeout(`${API_BASE}/api/bibliographies/consolidate`, {
    method: 'POST',
  })
  if (!response.ok) {
    const payload = await response.text()
    throw new Error(payload || 'Consolidation failed')
  }
  return response.json()
}

export async function fetchBibliographyEntries(baseName) {
  const response = await fetchWithTimeout(
    `${API_BASE}/api/ingest/latest?baseName=${encodeURIComponent(baseName)}`
  )
  if (!response.ok) {
    const payload = await response.text()
    throw new Error(payload || 'Failed to load ingest entries')
  }
  return response.json()
}

export async function fetchIngestStats() {
  const response = await fetchWithTimeout(`${API_BASE}/api/ingest/stats`)
  if (!response.ok) {
    const payload = await response.text()
    throw new Error(payload || 'Failed to load ingest stats')
  }
  return response.json()
}

export async function runKeywordSearch({ query, field, yearFrom, yearTo }) {
  try {
    const response = await fetchWithTimeout(`${API_BASE}/api/keyword-search`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ query, field, yearFrom, yearTo }),
    })
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`)
    }
    const payload = await response.json()
    const data = Array.isArray(payload) ? payload : payload.results || []
    return { data, source: payload.source || 'api', runId: payload.runId }
  } catch (error) {
    return { data: sampleSearchResults, source: 'sample', error }
  }
}

export async function fetchCorpus() {
  try {
    const response = await fetchWithTimeout(`${API_BASE}/api/corpus`)
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`)
    }
    const payload = await response.json()
    const data = Array.isArray(payload) ? payload : payload.items || []
    return { data, source: payload.source || 'api' }
  } catch (error) {
    return { data: sampleCorpus, source: 'sample', error }
  }
}

export async function fetchDownloadQueue() {
  try {
    const response = await fetchWithTimeout(`${API_BASE}/api/downloads`)
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`)
    }
    const payload = await response.json()
    const data = Array.isArray(payload) ? payload : payload.items || []
    return { data, source: payload.source || 'api' }
  } catch (error) {
    return { data: sampleDownloads, source: 'sample', error }
  }
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
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`)
    }
    const payload = await response.json()
    const data = payload.nodes ? payload : payload.data || payload
    return { data, source: payload.source || 'api' }
  } catch (error) {
    return { data: sampleGraph, source: 'sample', error }
  }
}
