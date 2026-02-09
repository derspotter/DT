<script>
  import { onMount } from 'svelte'
  import {
    login,
    fetchMe,
    selectCorpus,
    createCorpus,
    shareCorpus,
    setAuthToken,
    uploadPdf,
    extractBibliography,
    fetchBibliographyEntries,
    fetchIngestStats,
    fetchIngestRuns,
    enqueueIngestEntries,
    processMarkedIngestEntries,
    runKeywordSearch,
    fetchCorpus,
    fetchDownloadQueue,
    fetchDownloadWorkerStatus,
    startDownloadWorker,
    stopDownloadWorker,
    runDownloadWorkerOnce,
    fetchGraph,
  } from './lib/api'
  import { sampleActivity } from './lib/sample-data'

  const tabs = [
    { id: 'dashboard', label: 'Dashboard' },
    { id: 'ingest', label: 'Ingest' },
    { id: 'search', label: 'Keyword Search' },
    { id: 'corpus', label: 'Corpus' },
    { id: 'downloads', label: 'Downloads' },
    { id: 'logs', label: 'Logs' },
    { id: 'graph', label: 'Graph' },
  ]

  let activeTab = 'dashboard'
  let apiStatus = 'unknown'
  let authStatus = 'loading'
  let authError = ''
  let authUser = null
  let corpora = []
  let currentCorpusId = null
  let loginUsername = ''
  let loginPassword = ''

  let uploads = []
  let ingestStats = {
    no_metadata: 0,
    with_metadata: 0,
    to_download_references: 0,
    downloaded_references: 0,
  }
  let ingestStatsStatus = ''
  let latestEntries = []
  let latestEntriesStatus = ''
  let latestEntriesBase = ''
  let latestSelection = new Set()
  let enqueueStatus = ''
  let processMarkedLimit = 10
  let processMarkedStatus = ''
  let processingMarked = false
  let ingestRuns = []
  let ingestRunsStatus = ''

  let searchQuery = ''
  let searchField = 'default'
  let yearFrom = ''
  let yearTo = ''
  let searchResults = []
  let searchStatus = ''
  let searchSource = ''

  let corpusItems = []
  let corpusSource = ''
  let downloads = []
  let downloadsSource = ''
  let downloadWorker = {
    running: false,
    in_flight: false,
    started_at: null,
    last_tick_at: null,
    last_result: null,
    last_error: null,
    config: { intervalSeconds: 60, batchSize: 3 },
  }
  let downloadWorkerStatus = ''
  let downloadWorkerIntervalSeconds = 60
  let downloadWorkerBatchSize = 3
  let downloadWorkerBusy = false

  let logs = []
  let logsStatus = 'connecting'
  let graphStatus = ''
  let graphSource = ''
  let graphNodes = []
  let graphEdges = []
  let graphStats = null
  let graphRelationship = 'both'
  let graphStatusFilter = 'all'
  let graphYearFrom = ''
  let graphYearTo = ''
  let graphMaxNodes = 200
  let graphHideIsolates = true
  let graphFocusConnected = true
  let hoveredNodeId = null
  let graphLayout = []
  let graphNodeMap = new Map()
  let graphDegreeMap = new Map()
  let graphCanvasSize = 440
  let graphViewBox = { x: 0, y: 0, size: 440 }
  let isGraphPanning = false
  let graphPanStart = { x: 0, y: 0, viewX: 0, viewY: 0 }
  let creatingCorpus = false
  let shareUsername = ''
  let shareRole = 'viewer'
  let shareStatus = ''

  const maxLogs = 200

  function formatBytes(bytes) {
    if (bytes === 0) return '0 B'
    const k = 1024
    const sizes = ['B', 'KB', 'MB', 'GB']
    const i = Math.floor(Math.log(bytes) / Math.log(k))
    return `${(bytes / Math.pow(k, i)).toFixed(1)} ${sizes[i]}`
  }

  function updateUpload(id, patch) {
    uploads = uploads.map((item) => (item.id === id ? { ...item, ...patch } : item))
  }

  function formatAuthors(entry) {
    if (!entry) return ''
    if (Array.isArray(entry.authors)) return entry.authors.join(', ')
    if (typeof entry.authors === 'string') {
      try {
        const parsed = JSON.parse(entry.authors)
        if (Array.isArray(parsed)) return parsed.join(', ')
      } catch (error) {
        // fall through
      }
    }
    if (Array.isArray(entry.author)) return entry.author.join(', ')
    return entry.authors || entry.author || ''
  }

  function formatTitle(entry) {
    return entry?.title || entry?.source || entry?.url || 'Untitled'
  }

  function formatPipelineStatus(statusRaw) {
    const status = String(statusRaw || '').trim()
    const map = {
      no_metadata: { label: 'Raw', tone: 'queued' },
      with_metadata: { label: 'Enriched', tone: 'completed' },
      to_download_references: { label: 'Queued', tone: 'queued' },
      downloaded_references: { label: 'Downloaded', tone: 'completed' },
      failed_enrichments: { label: 'Enrich failed', tone: 'in_progress' },
      failed_downloads: { label: 'Download failed', tone: 'in_progress' },
    }
    if (map[status]) return { ...map[status], raw: status }
    if (!status) return { label: 'Unknown', tone: 'in_progress', raw: '' }
    // Fall back to a shortened label.
    return { label: status.replace(/_/g, ' '), tone: 'in_progress', raw: status }
  }

  function pipelineStatusLabel(statusRaw) {
    return formatPipelineStatus(statusRaw).label
  }

  function pipelineStatusTone(statusRaw) {
    return formatPipelineStatus(statusRaw).tone
  }

  function pipelineStatusTitle(statusRaw) {
    const st = formatPipelineStatus(statusRaw)
    return st.raw || st.label
  }

  function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms))
  }

  async function bootstrapAuth() {
    authStatus = 'loading'
    authError = ''
    try {
      const payload = await fetchMe()
      authUser = payload.user
      corpora = payload.corpora || []
      currentCorpusId = authUser?.last_corpus_id || (corpora[0]?.id ?? null)
      authStatus = 'authenticated'
      await refreshAll()
      connectLogs()
    } catch (error) {
      authStatus = 'unauthenticated'
      authError = ''
      authUser = null
      corpora = []
      currentCorpusId = null
      setAuthToken('')
    }
  }

  async function handleLogin(event) {
    event?.preventDefault?.()
    authError = ''
    try {
      const payload = await login({ username: loginUsername.trim(), password: loginPassword })
      if (payload?.user) {
        authUser = payload.user
        currentCorpusId = payload.user.last_corpus_id || null
      }
      const me = await fetchMe()
      authUser = me.user
      corpora = me.corpora || []
      currentCorpusId = authUser?.last_corpus_id || currentCorpusId || corpora[0]?.id || null
      authStatus = 'authenticated'
      await refreshAll()
      connectLogs()
    } catch (error) {
      authError = error.message || 'Login failed'
      authStatus = 'unauthenticated'
      setAuthToken('')
    }
  }

  async function refreshAll() {
    await loadIngestStats()
    await loadIngestRuns()
    await loadCorpus()
    await loadDownloads()
    await loadDownloadWorkerStatus()
    await loadGraph()
  }

  async function handleSelectCorpus(event) {
    const nextId = Number(event.target.value)
    if (!nextId) return
    try {
      await selectCorpus(nextId)
      const me = await fetchMe()
      authUser = me.user
      corpora = me.corpora || []
      currentCorpusId = authUser?.last_corpus_id || nextId
      await refreshAll()
    } catch (error) {
      authError = error.message || 'Failed to select corpus'
    }
  }

  async function handleCreateCorpus() {
    if (creatingCorpus) return
    const name = window.prompt('New corpus name')
    if (!name) return
    creatingCorpus = true
    try {
      await createCorpus(name.trim())
      const me = await fetchMe()
      authUser = me.user
      corpora = me.corpora || []
      currentCorpusId = authUser?.last_corpus_id || currentCorpusId
      await refreshAll()
    } catch (error) {
      authError = error.message || 'Failed to create corpus'
    } finally {
      creatingCorpus = false
    }
  }

  async function handleShareCorpus(event) {
    event?.preventDefault?.()
    shareStatus = ''
    if (!currentCorpusId) {
      shareStatus = 'Select a corpus first.'
      return
    }
    if (!shareUsername.trim()) {
      shareStatus = 'Enter a username to share with.'
      return
    }
    try {
      await shareCorpus(currentCorpusId, { username: shareUsername.trim(), role: shareRole })
      shareStatus = `Shared with ${shareUsername.trim()} (${shareRole}).`
      shareUsername = ''
    } catch (error) {
      shareStatus = error.message || 'Failed to share corpus.'
    }
  }

  async function loadLatestEntries(baseName) {
    latestEntriesBase = baseName
    latestEntriesStatus = 'Waiting for extracted entries...'
    latestEntries = []
    latestSelection = new Set()
    // Extraction + parsing can easily take a couple of minutes for multi-page reference sections.
    const attempts = 36
    for (let i = 0; i < attempts; i += 1) {
      try {
        const payload = await fetchBibliographyEntries(baseName)
        const items = payload.items || []
        if (Array.isArray(items) && items.length > 0) {
          latestEntries = items
          latestEntriesStatus = `Loaded ${items.length} entries.`
          return
        }
      } catch (error) {
        if (error?.status === 401) {
          authStatus = 'unauthenticated'
          setAuthToken('')
          return
        }
        // Keep waiting; extraction can take time.
      }
      latestEntriesStatus = `Waiting for extracted entries... (${i + 1}/${attempts})`
      await sleep(5000)
    }
    latestEntriesStatus = 'No entries found yet.'
  }

  function isLatestSelected(id) {
    return latestSelection.has(id)
  }

  function toggleLatestSelection(id) {
    const next = new Set(latestSelection)
    if (next.has(id)) {
      next.delete(id)
    } else {
      next.add(id)
    }
    latestSelection = next
  }

  function clearLatestSelection() {
    latestSelection = new Set()
  }

  function selectAllLatest() {
    latestSelection = new Set((latestEntries || []).map((e) => e.id))
  }

  async function enqueueSelectedLatest() {
    enqueueStatus = ''
    const ids = [...latestSelection]
    if (ids.length === 0) {
      enqueueStatus = 'Select at least one entry.'
      return
    }
    enqueueStatus = `Marking ${ids.length} entries for enrichment...`
    try {
      const payload = await enqueueIngestEntries(ids)
      enqueueStatus = `Marked ${payload.marked} (staged: ${payload.staged}, already processed: ${payload.already_processed}, duplicates: ${payload.duplicates}).`
      await loadDownloads()
      await loadIngestStats()
    } catch (error) {
      let message = error?.message || 'Failed to enqueue entries.'
      try {
        // Avoid dumping raw JSON/tracebacks into the UI.
        const parsed = JSON.parse(message)
        if (parsed?.error) message = parsed.error
      } catch {
        // ignore
      }
      enqueueStatus = message
    }
  }

  async function processMarkedBatch() {
    processMarkedStatus = ''
    if (processingMarked) return
    const limit = Number(processMarkedLimit)
    if (!Number.isFinite(limit) || limit <= 0) {
      processMarkedStatus = 'Limit must be a positive number.'
      return
    }
    processingMarked = true
    processMarkedStatus = `Processing up to ${limit} marked entries (enrich -> queue)...`
    try {
      const payload = await processMarkedIngestEntries({ limit })
      processMarkedStatus = `Processed ${payload.processed} (promoted: ${payload.promoted}, queued: ${payload.queued}, failed: ${payload.failed}).`
      await loadDownloads()
      await loadIngestStats()
    } catch (error) {
      if (error?.status === 401) {
        authStatus = 'unauthenticated'
        setAuthToken('')
        processMarkedStatus = 'Session expired. Please sign in again.'
        return
      }
      let message = error?.message || 'Failed to process marked entries.'
      try {
        const parsed = JSON.parse(message)
        if (parsed?.error) message = parsed.error
      } catch {
        // ignore
      }
      processMarkedStatus = message
    } finally {
      processingMarked = false
    }
  }

  function handleFiles(event) {
    const files = Array.from(event.target.files || [])
    const newItems = files.map((file) => ({
      id: `${file.name}-${file.size}-${Date.now()}`,
      file,
      name: file.name,
      size: file.size,
      status: 'ready',
      backendFilename: null,
      message: '',
    }))
    uploads = [...uploads, ...newItems]
    event.target.value = ''
  }

  async function uploadItem(item) {
    updateUpload(item.id, { status: 'uploading', message: '' })
    try {
      const payload = await uploadPdf(item.file)
      updateUpload(item.id, {
        status: 'uploaded',
        backendFilename: payload.filename,
        message: 'Uploaded',
      })
      apiStatus = 'online'
    } catch (error) {
      if (error?.status === 401) {
        authStatus = 'unauthenticated'
        setAuthToken('')
        updateUpload(item.id, { status: 'failed', message: 'Session expired. Please sign in again.' })
        return
      }
      updateUpload(item.id, { status: 'failed', message: error.message || 'Upload failed' })
      apiStatus = 'offline'
    }
  }

  async function uploadAll() {
    for (const item of uploads) {
      if (item.status === 'ready') {
        await uploadItem(item)
      }
    }
  }

  async function extractForItem(item) {
    if (!item.backendFilename) return
    updateUpload(item.id, { status: 'extracting', message: 'Extracting bibliography' })
    try {
      await extractBibliography(item.backendFilename)
      updateUpload(item.id, { status: 'extracted', message: 'Extraction started' })
      const baseName = item.backendFilename.replace(/\.pdf$/i, '')
      await loadLatestEntries(baseName)
      await loadIngestStats()
      await loadIngestRuns()
    } catch (error) {
      if (error?.status === 401) {
        authStatus = 'unauthenticated'
        setAuthToken('')
        updateUpload(item.id, { status: 'failed', message: 'Session expired. Please sign in again.' })
        return
      }
      updateUpload(item.id, { status: 'failed', message: error.message || 'Extraction failed' })
    }
  }

  async function loadIngestRuns() {
    ingestRunsStatus = 'Loading ingest runs...'
    try {
      const payload = await fetchIngestRuns(20)
      ingestRuns = payload.runs || []
      ingestRunsStatus = ingestRuns.length ? `Loaded ${ingestRuns.length} runs.` : 'No ingest runs found.'
    } catch (error) {
      if (error?.status === 401) {
        authStatus = 'unauthenticated'
        setAuthToken('')
      }
      ingestRunsStatus = 'Failed to load ingest runs.'
    }
  }

  async function loadIngestStats() {
    ingestStatsStatus = 'Loading ingest stats...'
    try {
      const payload = await fetchIngestStats()
      ingestStats = payload.stats || ingestStats
      ingestStatsStatus = 'Loaded ingest stats.'
      apiStatus = 'online'
    } catch (error) {
      if (error?.status === 401) {
        authStatus = 'unauthenticated'
        setAuthToken('')
      }
      ingestStatsStatus = 'Failed to load ingest stats.'
      apiStatus = 'offline'
    }
  }

  async function runSearch() {
    searchStatus = 'Searching...'
    try {
      const { data, source } = await runKeywordSearch({
        query: searchQuery,
        field: searchField,
        yearFrom,
        yearTo,
      })
      searchResults = data
      searchSource = source
      searchStatus = source === 'api' ? 'Results loaded from API.' : 'Sample results loaded.'
    } catch (error) {
      if (error?.status === 401) {
        authStatus = 'unauthenticated'
        setAuthToken('')
      }
      searchStatus = 'Search failed.'
    }
  }

  async function loadCorpus() {
    try {
      const { data, source } = await fetchCorpus()
      corpusItems = data
      corpusSource = source
    } catch (error) {
      if (error?.status === 401) {
        authStatus = 'unauthenticated'
        setAuthToken('')
      }
    }
  }

  async function loadDownloads() {
    try {
      const { data, source } = await fetchDownloadQueue()
      downloads = data
      downloadsSource = source
    } catch (error) {
      if (error?.status === 401) {
        authStatus = 'unauthenticated'
        setAuthToken('')
      }
    }
  }

  async function loadDownloadWorkerStatus() {
    downloadWorkerStatus = 'Loading worker status...'
    try {
      const payload = await fetchDownloadWorkerStatus()
      downloadWorker = payload
      downloadWorkerIntervalSeconds = payload?.config?.intervalSeconds ?? downloadWorkerIntervalSeconds
      downloadWorkerBatchSize = payload?.config?.batchSize ?? downloadWorkerBatchSize
      downloadWorkerStatus = ''
    } catch (error) {
      if (error?.status === 401) {
        authStatus = 'unauthenticated'
        setAuthToken('')
        return
      }
      downloadWorkerStatus = error?.message || 'Failed to load worker status.'
    }
  }

  async function handleStartDownloadWorker() {
    if (downloadWorkerBusy) return
    downloadWorkerBusy = true
    downloadWorkerStatus = 'Starting worker...'
    try {
      await startDownloadWorker({
        intervalSeconds: Number(downloadWorkerIntervalSeconds) || 60,
        batchSize: Number(downloadWorkerBatchSize) || 3,
      })
      await loadDownloadWorkerStatus()
      downloadWorkerStatus = 'Worker started.'
    } catch (error) {
      if (error?.status === 401) {
        authStatus = 'unauthenticated'
        setAuthToken('')
        return
      }
      downloadWorkerStatus = error?.message || 'Failed to start worker.'
    } finally {
      downloadWorkerBusy = false
    }
  }

  async function handleStopDownloadWorker(force = false) {
    if (downloadWorkerBusy) return
    downloadWorkerBusy = true
    downloadWorkerStatus = 'Stopping worker...'
    try {
      await stopDownloadWorker({ force })
      await loadDownloadWorkerStatus()
      downloadWorkerStatus = 'Worker stopped.'
    } catch (error) {
      if (error?.status === 401) {
        authStatus = 'unauthenticated'
        setAuthToken('')
        return
      }
      downloadWorkerStatus = error?.message || 'Failed to stop worker.'
    } finally {
      downloadWorkerBusy = false
    }
  }

  async function handleRunDownloadOnce() {
    if (downloadWorkerBusy) return
    downloadWorkerBusy = true
    downloadWorkerStatus = 'Running one batch...'
    try {
      await runDownloadWorkerOnce({ batchSize: Number(downloadWorkerBatchSize) || 3 })
      await loadDownloadWorkerStatus()
      await loadDownloads()
      downloadWorkerStatus = 'Batch completed.'
    } catch (error) {
      if (error?.status === 401) {
        authStatus = 'unauthenticated'
        setAuthToken('')
        return
      }
      downloadWorkerStatus = error?.message || 'Batch failed.'
    } finally {
      downloadWorkerBusy = false
    }
  }

  async function loadGraph() {
    graphStatus = 'Loading graph...'
    try {
      const { data, source } = await fetchGraph({
        maxNodes: graphMaxNodes,
        relationship: graphRelationship,
        status: graphStatusFilter,
        yearFrom: graphYearFrom ? Number(graphYearFrom) : null,
        yearTo: graphYearTo ? Number(graphYearTo) : null,
        hideIsolates: graphHideIsolates,
      })
      graphNodes = data.nodes || []
      graphEdges = data.edges || []
      graphStats = data.stats || null
      graphSource = source
      graphStatus = source === 'api' ? 'Loaded from API.' : 'Sample graph loaded.'
      const degreeMap = buildDegreeMap(graphNodes, graphEdges)
      graphDegreeMap = degreeMap
      const { layoutEdges } = buildLayoutEdges(graphNodes, graphEdges, degreeMap)
      graphLayout = buildGraphLayout(graphNodes, graphEdges, layoutEdges, degreeMap, {
        focusConnected: graphFocusConnected,
      })
      graphNodeMap = new Map(graphLayout.map((node) => [node.id, node]))
      graphCanvasSize = graphLayout[0]?.canvasSize || 440
      graphViewBox = { x: 0, y: 0, size: graphCanvasSize }
    } catch (error) {
      if (error?.status === 401) {
        authStatus = 'unauthenticated'
        setAuthToken('')
      }
      graphStatus = 'Failed to load graph.'
    }
  }

  function connectLogs() {
    try {
      const rawBase = import.meta.env.VITE_API_BASE || window.location.origin
      let wsUrl = 'ws://localhost:4000'
      try {
        const u = new URL(rawBase, window.location.origin)
        wsUrl = `${u.protocol === 'https:' ? 'wss' : 'ws'}://${u.host}`
      } catch (error) {
        // fall back to localhost default
      }
      const ws = new WebSocket(wsUrl)
      ws.onopen = () => {
        logsStatus = 'connected'
      }
      ws.onmessage = (event) => {
        logs = [...logs, event.data].slice(-maxLogs)
      }
      ws.onclose = () => {
        logsStatus = 'disconnected'
      }
      ws.onerror = () => {
        logsStatus = 'error'
      }
    } catch (error) {
      logsStatus = 'unavailable'
    }
  }

  function buildDegreeMap(nodes, edges) {
    const map = new Map()
    for (const node of nodes) {
      map.set(node.id, 0)
    }
    for (const edge of edges) {
      map.set(edge.source, (map.get(edge.source) || 0) + 1)
      map.set(edge.target, (map.get(edge.target) || 0) + 1)
    }
    return map
  }

  function computeNodeSize(degree) {
    const base = 14
    const scaled = Math.sqrt(Math.max(0, degree)) * 5
    return Math.min(42, base + scaled)
  }

  function runForceLayout(nodes, edges, degreeMap, canvasSize, options = {}) {
    const {
      initialPositions,
      repulsion = 5200,
      springLength = Math.max(120, Math.min(200, Math.sqrt(nodes.length) * 18)),
      springStrength = 0.03,
      centerStrength = 0.06,
      damping = 0.85,
      iterations = Math.min(200, 100 + nodes.length * 0.45),
      collisionStrength = 0.7,
      radialRings,
      radialStrength = 0.06,
    } = options
    const center = canvasSize / 2
    const layout = nodes.map((node) => {
      const degree = degreeMap.get(node.id) || 0
      const seeded = initialPositions?.get(node.id)
      return {
        ...node,
        x: seeded?.x ?? center + (Math.random() - 0.5) * canvasSize * 0.6,
        y: seeded?.y ?? center + (Math.random() - 0.5) * canvasSize * 0.6,
        vx: 0,
        vy: 0,
        size: computeNodeSize(degree),
        canvasSize,
      }
    })

    const indexById = new Map(layout.map((node, idx) => [node.id, idx]))
    const springs = edges
      .map((edge) => {
        const source = indexById.get(edge.source)
        const target = indexById.get(edge.target)
        if (source === undefined || target === undefined) return null
        return {
          source,
          target,
          length: edge.length ?? 1,
          strength: edge.weight ?? 1,
        }
      })
      .filter(Boolean)

    for (let iter = 0; iter < iterations; iter += 1) {
      for (let i = 0; i < layout.length; i += 1) {
        let fx = 0
        let fy = 0
        const node = layout[i]
        for (let j = i + 1; j < layout.length; j += 1) {
          const other = layout[j]
          let dx = node.x - other.x
          let dy = node.y - other.y
          let distSq = dx * dx + dy * dy
          if (distSq < 1) distSq = 1
          const force = repulsion / distSq
          const dist = Math.sqrt(distSq)
          const nx = dx / dist
          const ny = dy / dist
          fx += nx * force
          fy += ny * force
          other.vx -= nx * force
          other.vy -= ny * force
          const minDist = node.size + other.size + 10
          if (dist < minDist) {
            const overlap = (minDist - dist) / minDist
            const push = overlap * collisionStrength * 12
            fx += nx * push
            fy += ny * push
            other.vx -= nx * push
            other.vy -= ny * push
          }
        }
        node.vx += fx
        node.vy += fy
      }

      for (const edge of springs) {
        const source = layout[edge.source]
        const target = layout[edge.target]
        let dx = target.x - source.x
        let dy = target.y - source.y
        let dist = Math.sqrt(dx * dx + dy * dy) || 1
        const desired = springLength * edge.length
        const displacement = dist - desired
        const force = displacement * springStrength * edge.strength
        const nx = dx / dist
        const ny = dy / dist
        source.vx += nx * force
        source.vy += ny * force
        target.vx -= nx * force
        target.vy -= ny * force
      }

      if (radialRings) {
        for (const node of layout) {
          const targetRadius = radialRings.get(node.id)
          if (targetRadius === undefined) continue
          const dx = node.x - center
          const dy = node.y - center
          const dist = Math.sqrt(dx * dx + dy * dy) || 1
          const diff = dist - targetRadius
          const nx = dx / dist
          const ny = dy / dist
          node.vx -= nx * diff * radialStrength
          node.vy -= ny * diff * radialStrength
        }
      }

      for (const node of layout) {
        const dx = center - node.x
        const dy = center - node.y
        node.vx += dx * centerStrength
        node.vy += dy * centerStrength
        node.vx *= damping
        node.vy *= damping
        const margin = Math.max(30, node.size + 10)
        node.x = Math.min(canvasSize - margin, Math.max(margin, node.x + node.vx))
        node.y = Math.min(canvasSize - margin, Math.max(margin, node.y + node.vy))
      }
    }

    return layout.map((node) => ({ ...node, canvasSize }))
  }

  function buildRadialLayout(nodes, edges, layoutEdges, degreeMap, canvasSize) {
    const center = canvasSize / 2
    if (nodes.length <= 2) {
      return runForceLayout(nodes, layoutEdges, degreeMap, canvasSize, { centerStrength: 0.12 })
    }

    const adjacency = new Map()
    for (const node of nodes) adjacency.set(node.id, [])
    for (const edge of edges) {
      if (adjacency.has(edge.source) && adjacency.has(edge.target)) {
        adjacency.get(edge.source).push(edge.target)
        adjacency.get(edge.target).push(edge.source)
      }
    }

    const anchors = [...nodes]
      .sort((a, b) => (degreeMap.get(b.id) || 0) - (degreeMap.get(a.id) || 0))
      .slice(0, Math.min(3, nodes.length))
    const anchorIds = anchors.map((node) => node.id)
    const anchorIndex = new Map(anchorIds.map((id, index) => [id, index]))

    const distance = new Map()
    const anchorFor = new Map()
    const queue = []
    for (const id of anchorIds) {
      distance.set(id, 0)
      anchorFor.set(id, id)
      queue.push(id)
    }
    while (queue.length) {
      const current = queue.shift()
      const nextDistance = (distance.get(current) || 0) + 1
      for (const neighbor of adjacency.get(current) || []) {
        if (!distance.has(neighbor)) {
          distance.set(neighbor, nextDistance)
          anchorFor.set(neighbor, anchorFor.get(current))
          queue.push(neighbor)
        }
      }
    }

    const maxDistance = Math.max(0, ...distance.values())
    const ringGap = Math.max(90, Math.min(160, 60 + Math.sqrt(nodes.length) * 7))
    const baseRadius = 40
    const ringGroups = new Map()
    for (const node of nodes) {
      const dist = distance.get(node.id) ?? maxDistance + 1
      if (!ringGroups.has(dist)) ringGroups.set(dist, [])
      ringGroups.get(dist).push(node)
    }

    const initialPositions = new Map()
    const radialRings = new Map()
    for (const [dist, group] of ringGroups.entries()) {
      const radius = baseRadius + dist * ringGap
      const anchorCount = Math.max(1, anchorIds.length)
      const segmentSpan = (Math.PI * 2) / anchorCount
      const ringRotation = (dist % 2) * (Math.PI / anchorCount)
      const groupedByAnchor = new Map()
      for (const node of group) {
        const anchor = anchorFor.get(node.id) || anchorIds[0]
        if (!groupedByAnchor.has(anchor)) groupedByAnchor.set(anchor, [])
        groupedByAnchor.get(anchor).push(node)
      }
      for (const [anchor, nodesForAnchor] of groupedByAnchor.entries()) {
        nodesForAnchor.sort((a, b) => (degreeMap.get(b.id) || 0) - (degreeMap.get(a.id) || 0))
        const anchorIdx = anchorIndex.get(anchor) ?? 0
        nodesForAnchor.forEach((node, idx) => {
          const slot = (idx + 1) / (nodesForAnchor.length + 1)
          const angle = ringRotation + anchorIdx * segmentSpan + slot * segmentSpan
          const jitter = (Math.random() - 0.5) * ringGap * 0.08
          const x = center + (radius + jitter) * Math.cos(angle)
          const y = center + (radius + jitter) * Math.sin(angle)
          initialPositions.set(node.id, { x, y })
          radialRings.set(node.id, radius)
        })
      }
    }

    return runForceLayout(nodes, layoutEdges, degreeMap, canvasSize, {
      initialPositions,
      radialRings,
      radialStrength: 0.08,
      repulsion: 6800,
      springStrength: 0.04,
      springLength: Math.max(120, ringGap * 0.9),
    })
  }

  function buildGraphLayout(nodes, edges, layoutEdges, degreeMap, { focusConnected = true } = {}) {
    const count = nodes.length
    if (count === 0) return []
    const orderedNodes = [...nodes].sort((a, b) => (degreeMap.get(b.id) || 0) - (degreeMap.get(a.id) || 0))

    if (count > 400) {
      const ringSize = 36
      const radiusBase = 120
      const ringGap = 80
      const maxRing = Math.ceil(count / ringSize)
      const canvasSize = Math.min(1800, Math.max(520, (radiusBase + maxRing * ringGap + 120) * 2))
      const center = canvasSize / 2
      return orderedNodes.map((node, index) => {
        const ringIndex = Math.floor(index / ringSize)
        const positionInRing = index % ringSize
        const countInRing = Math.min(ringSize, count - ringIndex * ringSize) || 1
        const angle = (positionInRing / countInRing) * Math.PI * 2
        const radius = radiusBase + ringIndex * ringGap
        const degree = degreeMap.get(node.id) || 0
        return {
          ...node,
          x: center + radius * Math.cos(angle),
          y: center + radius * Math.sin(angle),
          size: computeNodeSize(degree),
          canvasSize,
        }
      })
    }

    const adjacency = new Map()
    for (const node of orderedNodes) {
      adjacency.set(node.id, [])
    }
    for (const edge of edges) {
      if (adjacency.has(edge.source) && adjacency.has(edge.target)) {
        adjacency.get(edge.source).push(edge.target)
        adjacency.get(edge.target).push(edge.source)
      }
    }

    const components = []
    const visited = new Set()
    for (const node of orderedNodes) {
      if (visited.has(node.id)) continue
      const stack = [node.id]
      const component = []
      visited.add(node.id)
      while (stack.length) {
        const current = stack.pop()
        component.push(current)
        for (const next of adjacency.get(current) || []) {
          if (!visited.has(next)) {
            visited.add(next)
            stack.push(next)
          }
        }
      }
      components.push(component)
    }

    if (focusConnected) {
      components.sort((a, b) => b.length - a.length)
      const largest = components[0] || []
      const componentNodes = orderedNodes.filter((node) => largest.includes(node.id))
      const componentEdges = edges.filter((edge) => largest.includes(edge.source) && largest.includes(edge.target))
      const componentLayoutEdges = layoutEdges.filter(
        (edge) => largest.includes(edge.source) && largest.includes(edge.target)
      )
      const componentSize = Math.min(1200, Math.max(420, 320 + Math.sqrt(componentNodes.length) * 200))
      if (componentNodes.length >= 12) {
        return buildRadialLayout(componentNodes, componentEdges, componentLayoutEdges, degreeMap, componentSize)
      }
      return runForceLayout(componentNodes, componentLayoutEdges, degreeMap, componentSize, { centerStrength: 0.1 })
    }

    const componentLayouts = components.map((ids) => {
      const componentNodes = orderedNodes.filter((node) => ids.includes(node.id))
      const componentEdges = edges.filter((edge) => ids.includes(edge.source) && ids.includes(edge.target))
      const componentLayoutEdges = layoutEdges.filter(
        (edge) => ids.includes(edge.source) && ids.includes(edge.target)
      )
      const componentSize = Math.min(1000, Math.max(320, 260 + Math.sqrt(componentNodes.length) * 170))
      const layout =
        componentNodes.length >= 12
          ? buildRadialLayout(componentNodes, componentEdges, componentLayoutEdges, degreeMap, componentSize)
          : runForceLayout(componentNodes, componentLayoutEdges, degreeMap, componentSize, { centerStrength: 0.1 })
      return { ids, layout, size: componentSize }
    })

    const columns = Math.ceil(Math.sqrt(componentLayouts.length))
    const maxComponentSize = componentLayouts.reduce((max, comp) => Math.max(max, comp.size), 0)
    const cellSize = maxComponentSize + 140
    const rows = Math.ceil(componentLayouts.length / columns)
    const canvasSize = Math.max(cellSize * columns, cellSize * rows)

    const combined = []
    componentLayouts.forEach((component, index) => {
      const offsetX = (index % columns) * cellSize + (cellSize - component.size) / 2
      const offsetY = Math.floor(index / columns) * cellSize + (cellSize - component.size) / 2
      component.layout.forEach((node) => {
        combined.push({
          ...node,
          x: node.x + offsetX,
          y: node.y + offsetY,
          canvasSize,
        })
      })
    })

    return combined
  }

  function buildLayoutEdges(nodes, edges, degreeMap) {
    const similarityEdges = buildSimilarityEdges(nodes, edges, degreeMap)
    return { layoutEdges: [...edges, ...similarityEdges], similarityEdges }
  }

  function buildSimilarityEdges(nodes, edges, degreeMap) {
    const count = nodes.length
    if (count < 8 || count > 280) return []
    const adjacency = new Map()
    for (const node of nodes) adjacency.set(node.id, new Set())
    for (const edge of edges) {
      if (adjacency.has(edge.source) && adjacency.has(edge.target)) {
        adjacency.get(edge.source).add(edge.target)
        adjacency.get(edge.target).add(edge.source)
      }
    }

    const ids = nodes.map((node) => node.id)
    const candidates = new Map()
    for (const id of ids) candidates.set(id, [])
    const sharedThreshold = count > 140 ? 2 : 1
    for (let i = 0; i < ids.length; i += 1) {
      const a = ids[i]
      const setA = adjacency.get(a)
      if (!setA || setA.size === 0) continue
      for (let j = i + 1; j < ids.length; j += 1) {
        const b = ids[j]
        const setB = adjacency.get(b)
        if (!setB || setB.size === 0) continue
        const [small, large] = setA.size < setB.size ? [setA, setB] : [setB, setA]
        let shared = 0
        for (const neighbor of small) {
          if (large.has(neighbor)) shared += 1
        }
        if (shared < sharedThreshold) continue
        const denom = Math.sqrt((degreeMap.get(a) || 1) * (degreeMap.get(b) || 1))
        const score = shared / denom
        if (!Number.isFinite(score) || score <= 0) continue
        candidates.get(a).push({ target: b, score, shared })
        candidates.get(b).push({ target: a, score, shared })
      }
    }

    const perNode = count > 160 ? 3 : 4
    const selected = new Map()
    for (const [id, list] of candidates.entries()) {
      list.sort((a, b) => b.score - a.score)
      for (const item of list.slice(0, perNode)) {
        const key = id < item.target ? `${id}|${item.target}` : `${item.target}|${id}`
        const existing = selected.get(key)
        if (!existing || existing.score < item.score) {
          selected.set(key, { source: id, target: item.target, score: item.score })
        }
      }
    }

    return [...selected.values()].map((edge) => ({
      source: edge.source,
      target: edge.target,
      relationship_type: 'similarity',
      weight: Math.min(0.9, Math.max(0.4, edge.score * 1.4)),
      length: 0.7,
    }))
  }

  function clamp(value, min, max) {
    return Math.min(Math.max(value, min), max)
  }

  function handleGraphWheel(event) {
    event.preventDefault()
    if (!graphCanvasSize) return
    const rect = event.currentTarget.getBoundingClientRect()
    const pointerX = (event.clientX - rect.left) / rect.width
    const pointerY = (event.clientY - rect.top) / rect.height
    const currentSize = graphViewBox.size
    const zoomFactor = event.deltaY > 0 ? 1.12 : 0.88
    const minSize = Math.max(220, graphCanvasSize * 0.35)
    const maxSize = graphCanvasSize * 2.2
    const newSize = clamp(currentSize * zoomFactor, minSize, maxSize)
    const svgX = graphViewBox.x + pointerX * currentSize
    const svgY = graphViewBox.y + pointerY * currentSize
    let nextX = svgX - pointerX * newSize
    let nextY = svgY - pointerY * newSize
    const padding = graphCanvasSize * 0.2
    nextX = clamp(nextX, -padding, graphCanvasSize - newSize + padding)
    nextY = clamp(nextY, -padding, graphCanvasSize - newSize + padding)
    graphViewBox = { x: nextX, y: nextY, size: newSize }
  }

  function handleGraphPointerDown(event) {
    isGraphPanning = true
    graphPanStart = {
      x: event.clientX,
      y: event.clientY,
      viewX: graphViewBox.x,
      viewY: graphViewBox.y,
    }
    event.currentTarget.setPointerCapture(event.pointerId)
  }

  function handleGraphPointerMove(event) {
    if (!isGraphPanning) return
    const rect = event.currentTarget.getBoundingClientRect()
    const scaleX = graphViewBox.size / rect.width
    const scaleY = graphViewBox.size / rect.height
    const dx = (event.clientX - graphPanStart.x) * scaleX
    const dy = (event.clientY - graphPanStart.y) * scaleY
    graphViewBox = {
      ...graphViewBox,
      x: graphPanStart.viewX - dx,
      y: graphPanStart.viewY - dy,
    }
  }

  function handleGraphPointerUp(event) {
    isGraphPanning = false
    try {
      event.currentTarget.releasePointerCapture(event.pointerId)
    } catch (error) {
      // ignore
    }
  }

  function resetGraphView() {
    graphViewBox = { x: 0, y: 0, size: graphCanvasSize }
  }

  $: hoveredNode = graphLayout.find((node) => node.id === hoveredNodeId)
  $: graphNodeCount = graphStats?.node_count ?? graphNodes.length
  $: graphEdgeCount = graphStats?.edge_count ?? graphEdges.length
  $: graphRelationshipCounts = graphStats?.relationship_counts || {
    references: graphEdges.filter((edge) => edge.relationship_type === 'references').length,
    cited_by: graphEdges.filter((edge) => edge.relationship_type === 'cited_by').length,
  }
  $: graphList = [...graphLayout].sort(
    (a, b) => (graphDegreeMap.get(b.id) || 0) - (graphDegreeMap.get(a.id) || 0)
  )
  $: graphDegreeMap = (() => {
    const map = new Map()
    for (const node of graphNodes) {
      map.set(node.id, 0)
    }
    for (const edge of graphEdges) {
      if (map.has(edge.source)) map.set(edge.source, map.get(edge.source) + 1)
      if (map.has(edge.target)) map.set(edge.target, map.get(edge.target) + 1)
    }
    return map
  })()

  function truncateLabel(label, max = 28) {
    if (!label) return ''
    return label.length > max ? `${label.slice(0, max - 1)}…` : label
  }

  const tabIds = new Set(tabs.map((tab) => tab.id))

  function readTabFromHash() {
    if (typeof window === 'undefined') return null
    const raw = window.location.hash || ''
    const cleaned = raw.replace(/^#\/?/, '').trim()
    return tabIds.has(cleaned) ? cleaned : null
  }

  function updateHash(tabId, replace = false) {
    if (typeof window === 'undefined') return
    const next = `#/${tabId}`
    if (window.location.hash === next) return
    if (replace && window.history?.replaceState) {
      window.history.replaceState(null, '', next)
    } else {
      window.location.hash = next
    }
  }

  function setActiveTab(tabId, { replace = false } = {}) {
    const safeTab = tabIds.has(tabId) ? tabId : 'dashboard'
    activeTab = safeTab
    updateHash(safeTab, replace)
  }

  onMount(() => {
    const initialTab = readTabFromHash() || activeTab
    setActiveTab(initialTab, { replace: true })

    const onHashChange = () => {
      const tab = readTabFromHash()
      if (tab && tab !== activeTab) {
        activeTab = tab
      }
    }
    window.addEventListener('hashchange', onHashChange)

    const boot = async () => {
      await bootstrapAuth()
    }
    boot()

    return () => window.removeEventListener('hashchange', onHashChange)
  })
</script>

{#if authStatus !== 'authenticated'}
  <div class="login-shell">
    <div class="login-card">
      <p class="eyebrow">Korpus Builder</p>
      <h1>Sign in</h1>
      <p class="subtitle">Use your username and password to access corpora.</p>
      <form class="login-form" on:submit={handleLogin}>
        <label>
          Username
          <input type="text" bind:value={loginUsername} autocomplete="username" required />
        </label>
        <label>
          Password
          <input type="password" bind:value={loginPassword} autocomplete="current-password" required />
        </label>
        {#if authError}
          <p class="error">{authError}</p>
        {/if}
        <button type="submit">Sign in</button>
      </form>
    </div>
  </div>
{:else}
  <div class="app-shell">
    <header class="app-header">
      <div>
        <p class="eyebrow">Korpus Builder</p>
        <h1>Corpus orchestration workspace</h1>
        <p class="subtitle">
          Build, enrich, and monitor literature pipelines end-to-end. API status: {apiStatus}.
        </p>
      </div>
      <div class="header-meta">
        <div class="header-badge">
          <span>Milestone 2 prep</span>
          <strong>Frontend · Svelte</strong>
        </div>
        <div class="header-panel">
          <div class="header-user">
            <span class="eyebrow">Signed in</span>
            <strong>{authUser?.username}</strong>
          </div>
          <div class="header-corpus">
            <label>
              Corpus
              <select on:change={handleSelectCorpus} bind:value={currentCorpusId}>
                {#each corpora as corpus}
                  <option value={corpus.id}>{corpus.name}</option>
                {/each}
              </select>
            </label>
            <button class="secondary" type="button" on:click={handleCreateCorpus} disabled={creatingCorpus}>
              New corpus
            </button>
          </div>
        </div>
      </div>
    </header>

  <div class="layout">
    <aside class="side-nav" data-testid="side-nav">
      {#each tabs as tab}
        <button
          class:active={activeTab === tab.id}
          on:click={() => setActiveTab(tab.id)}
          type="button"
          data-testid={`tab-${tab.id}`}
        >
          {tab.label}
        </button>
      {/each}
    </aside>

    <section class="content">
      {#if activeTab === 'dashboard'}
        <div class="grid">
          <div class="card highlight" data-testid="dashboard-overview">
            <h2>At a glance</h2>
            <p>Seed ingestion, keyword search, and corpus health in one view.</p>
            <div class="stat-row">
              <div>
                <span class="stat-label">Bibliographies</span>
                <strong>{ingestStats.no_metadata}</strong>
              </div>
              <div>
                <span class="stat-label">Corpus works</span>
                <strong>{corpusItems.length}</strong>
              </div>
              <div>
                <span class="stat-label">Queue items</span>
                <strong>{downloads.length}</strong>
              </div>
            </div>
          </div>
          <div class="card">
            <h2>Recent activity</h2>
            <ul class="activity">
              {#each sampleActivity as item}
                <li>
                  <div>
                    <strong>{item.label}</strong>
                    <span>{item.detail}</span>
                  </div>
                  <time>{item.timestamp}</time>
                </li>
              {/each}
            </ul>
          </div>
          <div class="card">
            <h2>Pipeline health</h2>
            <p>Extraction, enrichment, and downloads are staged and recoverable.</p>
            <div class="pill-row">
              <span class="pill">Extraction ready</span>
              <span class="pill">Enrichment queued</span>
              <span class="pill">Downloads active</span>
            </div>
          </div>
        </div>
      {/if}

      {#if activeTab === 'ingest'}
        <div class="card" data-testid="ingest-panel">
          <h2>Seed ingestion</h2>
          <p>Upload PDFs, extract bibliographies, and consolidate reference files.</p>
          <div class="uploader">
            <label class="upload-drop">
              <input type="file" multiple accept=".pdf" on:change={handleFiles} />
              <span>Drop PDFs or click to add files</span>
            </label>
            <button class="primary" type="button" on:click={uploadAll}>Upload selected</button>
          </div>
          <div class="upload-list">
            {#if uploads.length === 0}
              <p class="muted">No files selected yet.</p>
            {:else}
              {#each uploads as item}
                <div class="upload-item">
                  <div>
                    <strong>{item.name}</strong>
                    <span>{formatBytes(item.size)}</span>
                  </div>
                  <div class="upload-actions">
                    <span class={`status ${item.status}`}>{item.status}</span>
                    <button type="button" on:click={() => uploadItem(item)} disabled={item.status !== 'ready'}>
                      Upload
                    </button>
                    <button
                      type="button"
                      on:click={() => extractForItem(item)}
                      disabled={!item.backendFilename}
                    >
                      Extract
                    </button>
                  </div>
                </div>
              {/each}
            {/if}
          </div>
        </div>

        <div class="card">
          <h3>Extracted entries</h3>
          <p class="muted">{latestEntriesStatus}</p>
          {#if latestEntriesBase}
            <p class="muted">Source: {latestEntriesBase}</p>
          {/if}
          <div class="split">
            <button class="secondary" type="button" on:click={loadIngestStats}>Refresh stats</button>
            <button class="secondary" type="button" on:click={loadIngestRuns}>Refresh runs</button>
            <span class="muted">{ingestStatsStatus}</span>
          </div>
          <div class="pill-row">
            <span class="pill">Raw: {ingestStats.no_metadata}</span>
            <span class="pill">Enriched: {ingestStats.with_metadata}</span>
            <span class="pill">Queued: {ingestStats.to_download_references}</span>
            <span class="pill">Downloaded: {ingestStats.downloaded_references}</span>
          </div>
          <div class="split">
            <span class="muted">{ingestRunsStatus}</span>
          </div>
          {#if ingestRuns.length > 0}
            <div class="table">
              <div class="table-row header cols-4">
                <span>Run</span>
                <span>Entries</span>
                <span>Last seen</span>
                <span>Source PDF</span>
              </div>
              {#each ingestRuns as run}
                <div class="table-row cols-4">
                  <span>
                    <button class="link" type="button" on:click={() => loadLatestEntries(run.ingest_source)}>
                      {run.ingest_source}
                    </button>
                  </span>
                  <span>{run.entry_count}</span>
                  <span>{run.last_created_at || ''}</span>
                  <span>{run.source_pdf || ''}</span>
                </div>
              {/each}
            </div>
          {/if}
          {#if latestEntries.length === 0}
            <p class="muted">No extracted entries yet.</p>
          {:else}
            <div class="table-toolbar">
              <div class="table-toolbar-left">
                <span class="muted">Selected: {latestSelection.size}</span>
                <button class="secondary" type="button" on:click={selectAllLatest}>Select all</button>
                <button class="secondary" type="button" on:click={clearLatestSelection}>Clear</button>
              </div>
              <div class="table-toolbar-right">
                <div class="toolbar-actions">
                  <label class="inline-field">
                    <span class="muted">Batch</span>
                    <input type="number" min="1" max="200" step="1" bind:value={processMarkedLimit} />
                  </label>
                  <button class="secondary" type="button" on:click={processMarkedBatch} disabled={processingMarked}>
                    Enrich + queue
                  </button>
                  <button
                    class="primary"
                    type="button"
                    on:click={enqueueSelectedLatest}
                    disabled={latestSelection.size === 0}
                  >
                    Mark selected
                  </button>
                </div>
              </div>
            </div>
            {#if enqueueStatus}
              <p class="muted">{enqueueStatus}</p>
            {/if}
            {#if processMarkedStatus}
              <p class="muted">{processMarkedStatus}</p>
            {/if}
            <div class="table">
              <div class="table-row header cols-6">
                <span>
                  <input
                    type="checkbox"
                    aria-label="Select all"
                    checked={latestSelection.size > 0 && latestSelection.size === latestEntries.length}
                    on:change={(e) => (e.target.checked ? selectAllLatest() : clearLatestSelection())}
                  />
                </span>
                <span>Title</span>
                <span>Authors</span>
                <span>Year</span>
                <span>Source</span>
                <span>DOI</span>
              </div>
              {#each latestEntries as entry}
                <div class={`table-row cols-6 ${isLatestSelected(entry.id) ? 'selected' : ''}`}>
                  <span>
                    <input
                      type="checkbox"
                      checked={isLatestSelected(entry.id)}
                      aria-label={`Select ${formatTitle(entry)}`}
                      on:change={() => toggleLatestSelection(entry.id)}
                    />
                  </span>
                  <span>{formatTitle(entry)}</span>
                  <span>{formatAuthors(entry)}</span>
                  <span>{entry.year || ''}</span>
                  <span>{entry.source || entry.publisher || ''}</span>
                  <span>{entry.doi || ''}</span>
                </div>
              {/each}
            </div>
          {/if}
        </div>
      {/if}

      {#if activeTab === 'search'}
        <div class="card" data-testid="search-panel">
          <h2>Keyword search</h2>
          <p>Compose OpenAlex queries and queue results for downloads.</p>
          <form class="search-form" on:submit|preventDefault={runSearch}>
            <input
              type="text"
              placeholder="e.g., institutional economics AND governance"
              bind:value={searchQuery}
              data-testid="search-query"
            />
            <select bind:value={searchField}>
              <option value="default">All fields</option>
              <option value="title">Title</option>
              <option value="abstract">Abstract</option>
              <option value="title_and_abstract">Title + abstract</option>
              <option value="fulltext">Full text</option>
            </select>
            <input type="number" placeholder="From" bind:value={yearFrom} />
            <input type="number" placeholder="To" bind:value={yearTo} />
            <button class="primary" type="submit">Search</button>
          </form>
          <p class="muted">{searchStatus} ({searchSource})</p>
          <div class="table">
            <div class="table-row header cols-4">
              <span>Title</span>
              <span>Authors</span>
              <span>Year</span>
              <span>Type</span>
            </div>
            {#each searchResults as result}
              <div class="table-row cols-4">
                <span>{result.title}</span>
                <span>{result.authors}</span>
                <span>{result.year}</span>
                <span>{result.type}</span>
              </div>
            {/each}
          </div>
        </div>
      {/if}

      {#if activeTab === 'corpus'}
        <div class="card" data-testid="corpus-panel">
          <h2>Corpus</h2>
          <p class="muted">{corpusSource === 'api' ? 'Live corpus from API.' : 'Sample corpus data.'}</p>
          <form class="corpus-share" on:submit|preventDefault={handleShareCorpus}>
            <div>
              <label>
                Share with
                <input type="text" placeholder="username" bind:value={shareUsername} />
              </label>
            </div>
            <div>
              <label>
                Role
                <select bind:value={shareRole}>
                  <option value="viewer">Viewer</option>
                  <option value="editor">Editor</option>
                  <option value="owner">Owner</option>
                </select>
              </label>
            </div>
            <button class="secondary" type="submit">Share corpus</button>
            {#if shareStatus}
              <span class="muted">{shareStatus}</span>
            {/if}
          </form>
          <div class="table">
            <div class="table-row header cols-corpus">
              <span>Title</span>
              <span>Year</span>
              <span>Source</span>
              <span>Status</span>
            </div>
            {#each corpusItems as item}
              <div class="table-row cols-corpus">
                <span>{item.title}</span>
                <span>{item.year}</span>
                <span class="muted">{item.source || '—'}</span>
                <span
                  class={`tag pipeline ${pipelineStatusTone(item.status)}`}
                  title={pipelineStatusTitle(item.status)}
                >
                  {pipelineStatusLabel(item.status)}
                </span>
              </div>
            {/each}
          </div>
        </div>
      {/if}

      {#if activeTab === 'downloads'}
        <div class="card" data-testid="downloads-panel">
          <h2>Download queue</h2>
          <p class="muted">{downloadsSource === 'api' ? 'Live queue from API.' : 'Sample queue data.'}</p>

          <div class="worker-card">
            <div class="worker-card__header">
              <div>
                <span class="eyebrow">Download worker</span>
                <strong>{downloadWorker.running ? 'Running' : 'Stopped'}</strong>
                {#if downloadWorker.in_flight}
                  <span class="tag queued">in progress</span>
                {/if}
              </div>
              <div class="worker-card__actions">
                <label class="inline-field">
                  <span class="muted">Every</span>
                  <input type="number" min="10" step="10" bind:value={downloadWorkerIntervalSeconds} />
                  <span class="muted">s</span>
                </label>
                <label class="inline-field">
                  <span class="muted">Batch</span>
                  <input type="number" min="1" max="25" step="1" bind:value={downloadWorkerBatchSize} />
                </label>
                <button class="secondary" type="button" on:click={handleRunDownloadOnce} disabled={downloadWorkerBusy}>
                  Run once
                </button>
                {#if downloadWorker.running}
                  <button class="secondary" type="button" on:click={() => handleStopDownloadWorker(false)} disabled={downloadWorkerBusy}>
                    Stop
                  </button>
                  <button class="danger" type="button" on:click={() => handleStopDownloadWorker(true)} disabled={downloadWorkerBusy}>
                    Force stop
                  </button>
                {:else}
                  <button class="primary" type="button" on:click={handleStartDownloadWorker} disabled={downloadWorkerBusy}>
                    Start
                  </button>
                {/if}
              </div>
            </div>
            <div class="worker-card__meta">
              <span class="muted">{downloadWorkerStatus}</span>
              {#if downloadWorker.last_error}
                <span class="error">{downloadWorker.last_error}</span>
              {/if}
              {#if downloadWorker.last_result}
                <span class="muted">
                  Last batch: processed {downloadWorker.last_result.processed}, downloaded {downloadWorker.last_result.downloaded},
                  failed {downloadWorker.last_result.failed}, skipped {downloadWorker.last_result.skipped}.
                </span>
              {/if}
            </div>
          </div>

          <div class="table table-scroll">
            <div class="table-row header cols-3">
              <span>Work</span>
              <span>Status</span>
              <span>Attempts</span>
            </div>
            {#each downloads as item (item.id)}
              <div class="table-row cols-3">
                <span class="work-title" title={item.title}>{item.title}</span>
                <span class={`tag ${item.status}`}>{item.status}</span>
                <span
                  class={`attempts-badge ${item.attempts >= 4 ? 'danger' : item.attempts >= 2 ? 'warn' : ''}`}
                  title={item.attempts > 0 ? `Attempts: ${item.attempts}` : 'No attempts yet'}
                >
                  {item.attempts}
                </span>
              </div>
            {:else}
              <div class="empty-state">
                <strong>Queue is empty.</strong>
                <span class="muted">Add works to the download queue via ingest or keyword search.</span>
              </div>
            {/each}
          </div>
        </div>
      {/if}

      {#if activeTab === 'logs'}
        <div class="card" data-testid="logs-panel">
          <h2>Pipeline logs</h2>
          <p class="muted">WebSocket status: {logsStatus}</p>
          <div class="log-box">
            {#if logs.length === 0}
              <p class="muted">Waiting for pipeline output...</p>
            {:else}
              {#each logs as entry}
                <div class="log-line">{entry}</div>
              {/each}
            {/if}
          </div>
        </div>
      {/if}

      {#if activeTab === 'graph'}
        <div class="card" data-testid="graph-panel">
          <h2>Graph overview</h2>
          <p class="muted">{graphStatus} ({graphSource})</p>
          <div class="graph-controls">
            <label>
              <span>Relationship</span>
              <select bind:value={graphRelationship}>
                <option value="both">References + Cited by</option>
                <option value="references">References only</option>
                <option value="cited_by">Cited by only</option>
              </select>
            </label>
            <label>
              <span>Status</span>
              <select bind:value={graphStatusFilter}>
                <option value="all">All</option>
                <option value="with_metadata">With metadata</option>
                <option value="downloaded">Downloaded</option>
                <option value="queued">Queued</option>
              </select>
            </label>
            <label>
              <span>Year from</span>
              <input type="number" min="1600" max="2100" bind:value={graphYearFrom} placeholder="1900" />
            </label>
            <label>
              <span>Year to</span>
              <input type="number" min="1600" max="2100" bind:value={graphYearTo} placeholder="2026" />
            </label>
            <label>
              <span>Max nodes</span>
              <input type="number" min="20" max="2000" step="20" bind:value={graphMaxNodes} />
            </label>
            <label class="graph-toggle">
              <span>Hide isolates</span>
              <input type="checkbox" bind:checked={graphHideIsolates} />
            </label>
            <label class="graph-toggle">
              <span>Largest cluster only</span>
              <input type="checkbox" bind:checked={graphFocusConnected} />
            </label>
            <div class="graph-controls-actions">
              <button class="secondary" type="button" on:click={loadGraph}>Apply filters</button>
            </div>
          </div>
          <div class="graph-layout">
            <div class="graph-wrap graph-canvas">
              <svg
                viewBox={`${graphViewBox.x} ${graphViewBox.y} ${graphViewBox.size} ${graphViewBox.size}`}
                aria-label="Graph visualization"
                role="application"
                on:wheel={handleGraphWheel}
                on:pointerdown={handleGraphPointerDown}
                on:pointermove={handleGraphPointerMove}
                on:pointerup={handleGraphPointerUp}
                on:pointerleave={handleGraphPointerUp}
                style="touch-action: none;"
              >
                {#each graphEdges as edge}
                  {#if graphNodeMap.has(edge.source) && graphNodeMap.has(edge.target)}
                    <line
                      x1={graphNodeMap.get(edge.source).x}
                      y1={graphNodeMap.get(edge.source).y}
                      x2={graphNodeMap.get(edge.target).x}
                      y2={graphNodeMap.get(edge.target).y}
                      class={`edge ${edge.relationship_type === 'cited_by' ? 'edge--cited' : 'edge--ref'}`}
                    />
                  {/if}
                {/each}
                {#each graphLayout as node}
                  <g
                    class:active={hoveredNodeId === node.id}
                    on:mouseenter={() => (hoveredNodeId = node.id)}
                    on:mouseleave={() => (hoveredNodeId = null)}
                    on:focus={() => (hoveredNodeId = node.id)}
                    on:blur={() => (hoveredNodeId = null)}
                    role="button"
                    tabindex="0"
                    aria-label={node.title}
                  >
                    <circle
                      cx={node.x}
                      cy={node.y}
                      r={hoveredNodeId === node.id ? node.size + 4 : node.size}
                      class="node"
                    />
                    <title>{node.title}</title>
                  </g>
                {/each}
              </svg>
            </div>
            <div class="graph-details">
              <div class="graph-summary">
                <div>
                  <span class="eyebrow">Nodes</span>
                  <strong>{graphNodeCount}</strong>
                </div>
                <div>
                  <span class="eyebrow">Edges</span>
                  <strong>{graphEdgeCount}</strong>
                </div>
                <div>
                  <span class="eyebrow">References</span>
                  <strong>{graphRelationshipCounts.references}</strong>
                </div>
                <div>
                  <span class="eyebrow">Cited by</span>
                  <strong>{graphRelationshipCounts.cited_by}</strong>
                </div>
              </div>
              {#if hoveredNode}
                <div class="graph-hover">
                  <span class="eyebrow">Selected node</span>
                  <strong>{hoveredNode.title}</strong>
                </div>
              {/if}
              <div class="graph-actions">
                <button class="secondary" type="button" on:click={resetGraphView}>Reset view</button>
              </div>
              <div class="graph-list">
                {#each graphList as node}
                  <button
                    class:active={hoveredNodeId === node.id}
                    on:mouseenter={() => (hoveredNodeId = node.id)}
                    on:mouseleave={() => (hoveredNodeId = null)}
                    type="button"
                  >
                    <span>{truncateLabel(node.title)}</span>
                    <strong>{graphDegreeMap.get(node.id) ?? 0}</strong>
                  </button>
                {/each}
              </div>
            </div>
          </div>
        </div>
      {/if}
    </section>
  </div>
</div>
{/if}
