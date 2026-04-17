<script>
  import { onMount, tick } from 'svelte'
  import {
    login,
    fetchMe,
    requestPasswordReset,
    acceptInvite,
    resetPassword,
    createUserInvitation,
    selectCorpus,
    createCorpus,
    deleteCorpus as deleteCorpusApi,
    shareCorpus,
    fetchKantroposCorpora,
    fetchCorpusKantroposAssignment,
    saveCorpusKantroposAssignment,
    setAuthToken,
    uploadPdf,
    extractBibliography,
    fetchBibliographyEntries,
    fetchIngestStats,
    fetchIngestRuns,
    fetchSeedSources,
    fetchSeedCandidates,
    promoteSeedCandidates,
    dismissSeedCandidates as dismissSeedCandidatesApi,
    removeSeedSource,
    enqueueIngestEntries,
    processMarkedIngestEntries,
    runKeywordSearch,
    fetchRecursionConfig,
    fetchCorpus,
    fetchDownloadQueue,
    fetchDownloadWorkerStatus,
    startDownloadWorker,
    stopDownloadWorker,
    runDownloadWorkerOnce,
    fetchPipelineWorkerStatus,
    fetchPipelineDaemonStatus,
    fetchPipelineJobsSummary,
    fetchLogsTail,
    startPipelineWorker,
    pausePipelineWorker,
    pauseAllPipelineWorkers,
    fetchGraph,
    downloadCorpusExport,
    downloadCorpusItemFile,
  } from './lib/api'
  import Dashboard from './components/Dashboard.svelte'
  import Logs from './components/Logs.svelte'
  import Graph from './components/Graph.svelte'
  import Corpus from './components/Corpus.svelte'

  const userTabGroups = [
    {
      title: '',
      tabs: [
        { id: 'workspace', label: 'Workspace' },
      ]
    }
  ]
  const adminTabGroups = [
    {
      title: 'Diagnostics',
      tabs: [
        { id: 'downloads', label: 'Downloads' },
        { id: 'graph', label: 'Graph' },
        { id: 'dashboard', label: 'Pipeline Status' },
        { id: 'logs', label: 'Logs' },
      ]
    }
  ]

  let activeTab = 'workspace'
  let apiStatus = 'unknown'
  let authStatus = 'loading'
  let authError = ''
  let authUser = null
  $: isAdmin = Boolean(authUser?.is_admin || authUser?.username === 'admin')
  $: diagnosticsEnabled = isAdmin
  $: tabGroups = isAdmin ? [...userTabGroups, ...adminTabGroups] : userTabGroups
  $: showSideNav = tabGroups.some((group) => (group.tabs || []).some((tab) => tab.id !== 'workspace'))
  $: visibleCorpusNameCounts = corpora.reduce((map, corpus) => {
    const key = String(corpus?.name || '').trim()
    map.set(key, (map.get(key) || 0) + 1)
    return map
  }, new Map())
  $: visibleCorpusOwnerCounts = corpora.reduce((map, corpus) => {
    const key = `${String(corpus?.name || '').trim()}|${String(corpus?.owner_username || '')}`
    map.set(key, (map.get(key) || 0) + 1)
    return map
  }, new Map())

  function corpusOptionLabel(corpus) {
    if (!corpus) return ''
    const base = String(corpus.name || '').trim() || `Corpus ${corpus.id}`
    const owner = String(corpus.owner_username || '').trim()
    const role = String(corpus.role || '').trim()
    const duplicateName = (visibleCorpusNameCounts.get(base) || 0) > 1
    const duplicateOwnerName = (visibleCorpusOwnerCounts.get(`${base}|${owner}`) || 0) > 1
    const parts = []
    if (duplicateName && owner) parts.push(owner)
    if (role && role !== 'owner') parts.push(role)
    if ((duplicateName && !owner) || duplicateOwnerName) parts.push(`#${corpus.id}`)
    return parts.length > 0 ? `${base} · ${parts.join(' · ')}` : base
  }
  let corpora = []
  let currentCorpusId = null
  let loginUsername = ''
  let loginPassword = ''
  let authFlow = 'login'
  let authFlowToken = ''
  let authEmail = ''
  let authPassword = ''
  let authPasswordConfirm = ''
  let authInfo = ''

  let uploads = []
  let ingestStats = {
    raw_pending: 0,
    matched: 0,
    queued_download: 0,
    downloaded: 0,
  }
  let globalIngestStats = {
    raw_pending: 0,
    matched: 0,
    queued_download: 0,
    downloaded: 0,
  }
  $: pipelineRawCount = Number(ingestStats.raw_pending || 0)
  $: pipelineMetadataCount = Number(ingestStats.matched || 0)
  $: pipelineDownloadedCount = Number(ingestStats.downloaded || 0)
  let ingestStatsStatus = ''
  let latestEntries = []
  let latestEntriesStatus = ''
  $: isWaitingLatestEntries =
    latestEntriesStatus.startsWith('Waiting for extracted entries') ||
    latestEntriesStatus.startsWith('Loading extracted entries')
  let latestEntriesBase = ''
  let latestSeedDocument = null
  let extractionProgress = {
    active: false,
    runId: 0,
    baseName: '',
    backendFilename: '',
    phase: '',
    detail: '',
    percent: 0,
    indeterminate: false,
    backendDone: false,
    pageFilesTotal: 0,
    pageFilesProcessed: 0,
    startedAtMs: 0,
  }
  let extractionProgressRunId = 0
  let extractionCompletionCheckRunId = 0
  let latestSelection = []
  let selectableLatestCount = 0
  let selectedLatestCount = 0
  let selectableLatestKeysCurrent = []
  let allLatestSelected = false
  function selectableLatestKeys(entries = []) {
    return entries.reduce((keys, entry, index) => {
      if (!isLatestSelectable(entry)) return keys
      const key = latestSelectionKey(entry, index)
      if (key) keys.push(key)
      return keys
    }, [])
  }
  $: selectableLatestKeysCurrent = selectableLatestKeys(latestEntries || [])
  $: selectableLatestCount = selectableLatestKeysCurrent.length
  $: selectedLatestCount = selectableLatestKeysCurrent.reduce((count, key) => {
    return latestSelection.includes(key) ? count + 1 : count
  }, 0)
  $: allLatestSelected = selectableLatestCount > 0 && selectedLatestCount === selectableLatestCount
  let enqueueStatus = ''
  let processMarkedLimit = 10
  let processMarkedStatus = ''
  let processingMarked = false
  let ingestTrackedRequest = {
    active: false,
    requestId: '',
    jobIds: [],
    withDownload: false,
  }
  let ingestJobProgress = {
    pending: 0,
    running: 0,
    completed: 0,
    failed: 0,
    cancelled: 0,
    total: 0,
    updatedAt: '',
    daemonOnline: null,
    daemonLastSeenAt: '',
    error: '',
  }
  let ingestProgressPollTimer = null
  let ingestLastProgressSignature = ''
  const INGEST_PROGRESS_POLL_MS = 2000
  $: ingestProgressDone = ingestJobProgress.total > 0 &&
    ingestJobProgress.pending === 0 &&
    ingestJobProgress.running === 0
  $: ingestProgressRatio = ingestJobProgress.total > 0
    ? (ingestJobProgress.completed + ingestJobProgress.failed + ingestJobProgress.cancelled) / ingestJobProgress.total
    : 0
  let ingestRuns = []
  let ingestRunsStatus = ''
  let seedSources = []
  let seedSourcesStatus = ''
  let expandedSeedSourceId = ''
  let seedCandidatesBySource = {}
  let seedCandidatesLoading = {}
  let seedSelections = {}
  let seedSelectionVersions = {}
  let selectedSeedCandidateKeys = {}
  let seedActionStatus = ''
  let seedActionBusy = false
  let seedLastRunKey = ''

  let searchQuery = ''
  let searchMode = 'query'
  let searchSeedJson = ''
  let searchField = 'default'
  let searchAuthor = ''
  let yearFrom = ''
  let yearTo = ''
  let includeDownstream = false
  let includeUpstream = false
  let relatedDepthDownstream = 0
  let relatedDepthUpstream = 0
  let maxRelated = 30
  let keywordRecursionConfig = {
    includeDownstream: false,
    includeUpstream: false,
    relatedDepthDownstream: 0,
    relatedDepthUpstream: 0,
    maxRelated: 30,
  }
  let searchResults = []
  let searchStatus = ''
  let searchSource = ''
  let searchSelection = []
  let searchQueueConfigs = {}
  let searchQueueStatus = ''
  let searchQueueBusy = false
  let allSearchSelected = false
  let selectedSearchCount = 0
  $: allSearchSelected =
    searchResults.length > 0 &&
    searchResults.every((result, index) => {
      const key = searchResultKey(result, index)
      return key && searchSelection.includes(key)
    })
  $: selectedSearchCount = searchResults.reduce((count, result, index) => {
    const key = searchResultKey(result, index)
    return key && searchSelection.includes(key) ? count + 1 : count
  }, 0)

  const CORPUS_PAGE_SIZE = 120
  const RAW_STATUSES = new Set(['raw', 'extract_references_from_pdf', 'pending'])
  const FAILED_STATUSES = new Set(['failed_enrichment', 'failed_download'])
  let corpusItems = []
  let corpusTotal = 0
  let corpusHasMore = false
  let corpusLoading = false
  let corpusLoadingMore = false
  let corpusLoadRequestSeq = 0
  let corpusLoadStatus = ''
  let corpusSource = ''
  let corpusStageTotals = { raw: 0, metadata: 0, downloaded: 0, failed_enrichment: 0, failed_download: 0 }
  $: rawItems = corpusItems.filter(i => !i.status || RAW_STATUSES.has(i.status))
  $: metaItems = corpusItems.filter(i => i.status === 'enriching' || i.status === 'matched' || i.status === 'queued_download')
  $: downloadedItems = corpusItems.filter(i => i.status === 'downloaded')
  $: failedItems = corpusItems.filter(i => FAILED_STATUSES.has(i.status))
  $: rawTotal = Number(corpusStageTotals?.raw || 0)
  $: metadataTotal = Number(corpusStageTotals?.metadata || 0)
  $: downloadedTotal = Number(corpusStageTotals?.downloaded || 0)
  $: failedEnrichmentTotal = Number(corpusStageTotals?.failed_enrichment || 0)
  $: failedDownloadTotal = Number(corpusStageTotals?.failed_download || 0)
  let corpusRowElements = {
    raw: new Map(),
    metadata: new Map(),
    downloaded: new Map(),
    failed: new Map(),
  }
  let pendingPromotionEvents = []
  let promotionAnimationInFlight = false
  let promotionFlushTimer = null
  let promotionHighlights = new Set()
  let promotionHighlightVersion = 0
  let prefersReducedMotion = false
  let reducedMotionMediaQuery = null

  let downloads = []
  let downloadsSource = ''
  let downloadsQuery = ''
  let downloadsStatusFilter = 'all'
  $: recentDownloads = downloads.filter((item) => item.status === 'downloaded').slice(0, 10)
  $: filteredDownloads = downloads.filter((item) => {
    const statusOk = downloadsStatusFilter === 'all' || item.status === downloadsStatusFilter
    if (!statusOk) return false
    const q = downloadsQuery.trim().toLowerCase()
    if (!q) return true
    return String(item.title || '').toLowerCase().includes(q)
  })
  let downloadWorker = {
    running: false,
    in_flight: false,
    queued_jobs: 0,
    running_jobs: 0,
    queued_download_items: 0,
    started_at: null,
    last_tick_at: null,
    last_result: null,
    last_error: null,
    config: { intervalSeconds: 60, batchSize: 3 },
  }
  let downloadWorkerStatus = ''
  let downloadWorkerBusy = false
  let pipelineWorker = {
    running: false,
    in_flight: false,
    started_at: null,
    last_tick_at: null,
    last_result: null,
    last_error: null,
    resolved_download_workers: 1,
    config: { intervalSeconds: 15, promoteBatchSize: 25, promoteWorkers: 6, downloadBatchSize: 5, downloadWorkers: 0 },
  }
  let pipelineWorkerStatus = ''
  let pipelineWorkerBusy = false
  let daemonStatus = { online: false, workers: {}, last_seen_at: null, status: 'offline' }
  let workerBacklog = {
    raw_pending: 0,
    enriching: 0,
    matched: 0,
    queued_download: 0,
    downloading: 0,
    downloaded: 0,
    failed_enrichment: 0,
    failed_download: 0,
  }
  let exportBusy = false
  let exportStatus = ''
  let exportFilterStatus = 'all'
  let exportFilterYearFrom = ''
  let exportFilterYearTo = ''
  let exportFilterSource = ''

  let logs = []
  let logsType = 'pipeline'
  let logsFilterCurrentCorpus = true
  let logsStatus = 'connecting'
  let logsSocket = null
  let logsReconnectTimer = null
  let logsCursor = 0
  let logsHasMore = false
  let logsLoadingOlder = false
  let logsTailStatus = ''
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
  let graphColorMode = 'source_path'
  let graphHideIsolates = true
  let graphFocusConnected = true
  let hoveredNodeId = null
  let graphLayout = []
  let graphVisibleEdges = []
  let graphNodeMap = new Map()
  let graphDegreeMap = new Map()
  let graphComponentMap = new Map()
  let graphNodeColorMap = new Map()
  let graphColorLegend = []
  let graphTotalNodeCount = 0
  let graphTotalEdgeCount = 0
  let graphCanvasSize = 440
  let graphViewBox = { x: 0, y: 0, size: 440 }
  let isGraphPanning = false
  let graphPanStart = { x: 0, y: 0, viewX: 0, viewY: 0 }
  let pipelineRefreshTimer = null
  let pipelineRefreshInFlight = false
  let liveRefreshIntervalId = null
  let lastTabRefreshAt = 0
  let rawCorpusTableEl = null
  let metaCorpusTableEl = null
  let downloadedCorpusTableEl = null
  let creatingCorpus = false
  let deletingCorpus = false
  let corpusActionStatus = ''
  let corpusActionError = false
  $: currentCorpus = (corpora || []).find((c) => Number(c.id) === Number(currentCorpusId)) || null
  $: canDeleteCurrentCorpus = Boolean(currentCorpusId && currentCorpus?.role === 'owner')
  $: canConfigureCurrentCorpusKantropos = Boolean(currentCorpusId && (isAdmin || currentCorpus?.role === 'owner' || currentCorpus?.role === 'editor'))
  let shareUsername = ''
  let shareRole = 'viewer'
  let shareStatus = ''
  let inviteUsername = ''
  let inviteEmail = ''
  let inviteStatus = ''
  let inviteError = false
  let kantroposTargets = []
  let kantroposAssignment = null
  let selectedKantroposTargetId = ''
  let kantroposAssignmentStatus = ''
  let kantroposAssignmentError = false
  let kantroposAssignmentLoading = false
  let kantroposAssignmentSaving = false
  let kantroposAssignmentRequestSeq = 0

  const maxLogs = 200
  const maxLogHistory = 5000

  function resetLogState() {
    logs = []
    logsCursor = 0
    logsHasMore = false
    logsTailStatus = ''
  }

  function resetKantroposAssignmentState() {
    kantroposAssignment = null
    selectedKantroposTargetId = ''
    kantroposAssignmentStatus = ''
    kantroposAssignmentError = false
    kantroposAssignmentLoading = false
    kantroposAssignmentSaving = false
  }

  function formatBytes(bytes) {
    if (bytes === 0) return '0 B'
    const k = 1024
    const sizes = ['B', 'KB', 'MB', 'GB']
    const i = Math.floor(Math.log(bytes) / Math.log(k))
    return `${(bytes / Math.pow(k, i)).toFixed(1)} ${sizes[i]}`
  }

  function parseAuthFlowFromHash() {
    if (typeof window === 'undefined') return { flow: 'login', token: '' }
    const raw = String(window.location.hash || '').replace(/^#\/?/, '')
    const [pathPart = '', queryPart = ''] = raw.split('?')
    const path = pathPart.trim()
    const params = new URLSearchParams(queryPart)
    if (path === 'forgot-password') {
      return { flow: 'forgot-password', token: '' }
    }
    if (path === 'accept-invite') {
      return { flow: 'accept-invite', token: params.get('token') || '' }
    }
    if (path === 'reset-password') {
      return { flow: 'reset-password', token: params.get('token') || '' }
    }
    return { flow: 'login', token: '' }
  }

  function applyAuthFlowFromHash() {
    const next = parseAuthFlowFromHash()
    authFlow = next.flow
    authFlowToken = next.token
    authInfo = ''
    authError = ''
    authEmail = ''
    authPassword = ''
    authPasswordConfirm = ''
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

  function normalizeCorpusItem(entry) {
    if (!entry || typeof entry !== 'object') return entry
    return {
      ...entry,
      authors_display: formatAuthors(entry),
    }
  }

  function formatTitle(entry) {
    return entry?.title || entry?.source || entry?.url || 'Untitled'
  }

  function parseAuthorsValue(value) {
    if (Array.isArray(value)) return value.map((v) => String(v || '').trim()).filter(Boolean)
    if (typeof value === 'string') {
      const raw = value.trim()
      if (!raw) return []
      try {
        const parsed = JSON.parse(raw)
        if (Array.isArray(parsed)) return parsed.map((v) => String(v || '').trim()).filter(Boolean)
      } catch (error) {
        // fall through
      }
      return raw.split(';').map((v) => v.trim()).filter(Boolean)
    }
    return []
  }

  function formatSeedDocumentLabel(meta, fallback = '') {
    if (!meta || typeof meta !== 'object') return fallback || 'Unknown source'
    const title = String(meta.title || '').trim()
    const year = String(meta.year || '').trim()
    const doi = String(meta.doi || '').trim()
    const base = title || fallback || 'Unknown source'
    const yearPart = year ? ` (${year})` : ''
    const doiPart = doi ? ` · ${doi}` : ''
    return `${base}${yearPart}${doiPart}`
  }

  function formatSeedDocumentDetails(meta) {
    if (!meta || typeof meta !== 'object') return ''
    const authors = parseAuthorsValue(meta.authors)
    if (authors.length > 0) return authors.join(', ')
    const source = String(meta.source || '').trim()
    const publisher = String(meta.publisher || '').trim()
    if (source && publisher) return `${source} · ${publisher}`
    return source || publisher || ''
  }

  function formatPipelineStatus(statusRaw) {
    const status = String(statusRaw || '').trim()
    const map = {
      raw: { label: 'Raw', tone: 'queued' },
      enriching: { label: 'Enriching', tone: 'in_progress' },
      matched: { label: 'Matched', tone: 'completed' },
      queued_download: { label: 'Queued download', tone: 'in_progress' },
      downloaded: { label: 'Downloaded', tone: 'completed' },
      failed_enrichment: { label: 'Enrich failed', tone: 'in_progress' },
      failed_download: { label: 'Download failed', tone: 'in_progress' },
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

  function corpusMotionKey(stage, id) {
    if (!stage) return ''
    const numericId = Number(id)
    if (!Number.isFinite(numericId)) return ''
    return `${stage}:${numericId}`
  }

  function isPromotionHighlighted(stage, id) {
    promotionHighlightVersion
    const key = corpusMotionKey(stage, id)
    return key ? promotionHighlights.has(key) : false
  }

  function addPromotionHighlight(stage, id, ttlMs = 700) {
    const key = corpusMotionKey(stage, id)
    if (!key) return
    promotionHighlights.add(key)
    promotionHighlights = new Set(promotionHighlights)
    promotionHighlightVersion += 1
    window.setTimeout(() => {
      if (!promotionHighlights.has(key)) return
      promotionHighlights.delete(key)
      promotionHighlights = new Set(promotionHighlights)
      promotionHighlightVersion += 1
    }, ttlMs)
  }

  function stageFromTransitionSide(value) {
    const v = String(value || '').trim().toLowerCase()
    if (v === 'raw' || v === 'metadata' || v === 'downloaded' || v === 'failed') return v
    return ''
  }

  function parsePromotionEventMessage(raw) {
    if (typeof raw !== 'string' || !raw.startsWith('{')) return null
    try {
      const parsed = JSON.parse(raw)
      if (!parsed || parsed.kind !== 'promotion') return null
      const fromStage = stageFromTransitionSide(parsed.from_stage)
      const toStage = stageFromTransitionSide(parsed.to_stage)
      const itemId = Number(parsed.item_id)
      const destItemId = Number(parsed.dest_item_id)
      if (!fromStage || !toStage || !Number.isFinite(itemId)) return null
      return {
        corpusId: Number.isFinite(Number(parsed.corpus_id)) ? Number(parsed.corpus_id) : null,
        transition: String(parsed.transition || ''),
        fromStage,
        toStage,
        itemId,
        destItemId: Number.isFinite(destItemId) ? destItemId : null,
      }
    } catch (error) {
      return null
    }
  }

  function parseExtractionEventMessage(raw) {
    if (typeof raw !== 'string' || !raw.startsWith('{')) return null
    try {
      const parsed = JSON.parse(raw)
      if (!parsed || parsed.kind !== 'extraction' || parsed.event !== 'done') return null
      return {
        corpusId: Number.isFinite(Number(parsed.corpus_id)) ? Number(parsed.corpus_id) : null,
        baseName: String(parsed.base_name || '').trim(),
        filename: String(parsed.filename || '').trim(),
        status: String(parsed.status || 'success').trim().toLowerCase(),
        mode: String(parsed.mode || '').trim().toLowerCase(),
        reason: String(parsed.reason || '').trim(),
      }
    } catch (error) {
      return null
    }
  }

  function bindCorpusRow(node, params) {
    let currentStage = ''
    let currentKey = ''
    const apply = (next) => {
      const stage = stageFromTransitionSide(next?.stage)
      const key = String(next?.key || '')
      if (currentStage && currentKey && corpusRowElements[currentStage]) {
        corpusRowElements[currentStage].delete(currentKey)
      }
      currentStage = stage
      currentKey = key
      if (currentStage && currentKey && corpusRowElements[currentStage]) {
        corpusRowElements[currentStage].set(currentKey, node)
      }
    }
    apply(params)
    return {
      update(next) {
        apply(next)
      },
      destroy() {
        if (currentStage && currentKey && corpusRowElements[currentStage]) {
          corpusRowElements[currentStage].delete(currentKey)
        }
      },
    }
  }

  function queuePromotionAnimation(eventPayload) {
    if (activeTab !== 'corpus') return
    if (eventPayload?.corpusId !== null && Number(currentCorpusId) !== Number(eventPayload.corpusId)) return
    const sourceKey = corpusMotionKey(eventPayload.fromStage, eventPayload.itemId)
    const sourceEl = sourceKey ? corpusRowElements[eventPayload.fromStage]?.get(sourceKey) : null
    const sourceRect = sourceEl ? sourceEl.getBoundingClientRect() : null
    pendingPromotionEvents = [...pendingPromotionEvents, { ...eventPayload, sourceRect }]
    if (pendingPromotionEvents.length > 200) {
      pendingPromotionEvents = pendingPromotionEvents.slice(-200)
    }
    if (promotionFlushTimer || promotionAnimationInFlight) return
    promotionFlushTimer = window.setTimeout(() => {
      promotionFlushTimer = null
      flushPromotionAnimations()
    }, 110)
  }

  function waitForNextFrame() {
    return new Promise((resolve) => {
      requestAnimationFrame(() => resolve())
    })
  }

  async function playPromotionAnimation(eventPayload) {
    const destinationId = eventPayload.destItemId
    if (!Number.isFinite(destinationId)) return
    const destinationKey = corpusMotionKey(eventPayload.toStage, destinationId)
    const destinationEl = destinationKey ? corpusRowElements[eventPayload.toStage]?.get(destinationKey) : null
    if (!destinationEl) {
      addPromotionHighlight(eventPayload.toStage, destinationId)
      return
    }

    if (prefersReducedMotion || !eventPayload.sourceRect) {
      addPromotionHighlight(eventPayload.toStage, destinationId)
      return
    }

    const destinationRect = destinationEl.getBoundingClientRect()
    const dx = eventPayload.sourceRect.left - destinationRect.left
    const dy = eventPayload.sourceRect.top - destinationRect.top
    if (!Number.isFinite(dx) || !Number.isFinite(dy)) {
      addPromotionHighlight(eventPayload.toStage, destinationId)
      return
    }

    destinationEl.style.setProperty('--promotion-dx', `${dx}px`)
    destinationEl.style.setProperty('--promotion-dy', `${dy}px`)
    destinationEl.classList.add('promotion-slide')
    await waitForNextFrame()
    await new Promise((resolve) => setTimeout(resolve, 440))
    destinationEl.classList.remove('promotion-slide')
    destinationEl.style.removeProperty('--promotion-dx')
    destinationEl.style.removeProperty('--promotion-dy')
    addPromotionHighlight(eventPayload.toStage, destinationId)
  }

  async function flushPromotionAnimations() {
    if (promotionAnimationInFlight) return
    if (activeTab !== 'corpus') return
    if (pendingPromotionEvents.length === 0) return
    promotionAnimationInFlight = true
    try {
      const batch = pendingPromotionEvents
      pendingPromotionEvents = []
      await loadCorpus({ preserveSelection: true, quiet: true })
      await tick()
      await waitForNextFrame()
      for (const eventPayload of batch) {
        await playPromotionAnimation(eventPayload)
      }
    } finally {
      promotionAnimationInFlight = false
      if (pendingPromotionEvents.length > 0 && !promotionFlushTimer) {
        promotionFlushTimer = window.setTimeout(() => {
          promotionFlushTimer = null
          flushPromotionAnimations()
        }, 110)
      }
    }
  }

  function bucketLabel(bucket) {
    if (bucket === 'raw') return 'Raw'
    if (bucket === 'metadata') return 'Metadata'
    if (bucket === 'downloaded') return 'Downloaded'
    if (bucket === 'failed') return 'Failed'
    return 'Unknown'
  }

  function doiHref(doi) {
    const value = String(doi || '').trim()
    if (!value) return ''
    return value.match(/^https?:\/\//i) ? value : `https://doi.org/${value}`
  }

  function openAlexHref(openalexId) {
    const value = String(openalexId || '').trim()
    if (!value) return ''
    return value.match(/^https?:\/\//i) ? value : `https://openalex.org/${value}`
  }

  function coerceRecursionInt(value, fallback) {
    const parsed = Number(value)
    return Number.isFinite(parsed) ? parsed : fallback
  }

  function applyKeywordRecursionDefaults(config = {}) {
    const next = {
      includeDownstream: config?.includeDownstream ?? keywordRecursionConfig.includeDownstream,
      includeUpstream: config?.includeUpstream ?? keywordRecursionConfig.includeUpstream,
      relatedDepthDownstream: coerceRecursionInt(config?.relatedDepthDownstream, keywordRecursionConfig.relatedDepthDownstream),
      relatedDepthUpstream: coerceRecursionInt(config?.relatedDepthUpstream, keywordRecursionConfig.relatedDepthUpstream),
      maxRelated: coerceRecursionInt(config?.maxRelated, keywordRecursionConfig.maxRelated),
    }

    includeDownstream = Boolean(next.includeDownstream)
    includeUpstream = Boolean(next.includeUpstream)
    relatedDepthDownstream = Math.max(0, Number(next.relatedDepthDownstream))
    relatedDepthUpstream = Math.max(0, Number(next.relatedDepthUpstream))
    maxRelated = Math.max(1, Number(next.maxRelated))

    keywordRecursionConfig = {
      ...keywordRecursionConfig,
      ...next,
    }
  }

  async function loadRecursionConfig() {
    try {
      const payload = await fetchRecursionConfig()
      const cfg = payload?.keyword || payload?.uploadedDocs || null
      if (cfg && typeof cfg === 'object') {
        applyKeywordRecursionDefaults(cfg)
      }
      apiStatus = 'online'
    } catch (error) {
      if (error?.status === 401) {
        throw error
      }
      // keep defaults if fetch fails
    }
  }

  function handleCorpusColumnScroll(event) {
    if (!corpusHasMore || corpusLoadingMore || corpusLoading) return
    const target = event?.currentTarget
    if (!target) return
    const remaining = target.scrollHeight - target.scrollTop - target.clientHeight
    if (remaining <= 120) {
      loadCorpus({ append: true })
    }
  }

  function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms))
  }

  function stopIngestProgressPolling() {
    if (ingestProgressPollTimer) {
      clearInterval(ingestProgressPollTimer)
      ingestProgressPollTimer = null
    }
  }

  function resetIngestProgressTracking() {
    stopIngestProgressPolling()
    ingestTrackedRequest = {
      active: false,
      requestId: '',
      jobIds: [],
      withDownload: false,
    }
    ingestJobProgress = {
      pending: 0,
      running: 0,
      completed: 0,
      failed: 0,
      cancelled: 0,
      total: 0,
      updatedAt: '',
      daemonOnline: null,
      daemonLastSeenAt: '',
      error: '',
    }
    ingestLastProgressSignature = ''
  }

  function applyIngestJobsSummary(summary) {
    const counts = summary?.counts || {}
    const daemon = summary?.daemon || {}
    const next = {
      pending: Number(counts.pending || 0),
      running: Number(counts.running || 0),
      completed: Number(counts.completed || 0),
      failed: Number(counts.failed || 0),
      cancelled: Number(counts.cancelled || 0),
      total: Number(counts.total || 0),
      updatedAt: new Date().toISOString(),
      daemonOnline: daemon?.online === true ? true : daemon?.online === false ? false : null,
      daemonLastSeenAt: String(daemon?.last_seen_at || '').trim(),
      error: '',
    }
    ingestJobProgress = next

    if (next.running > 0) {
      processMarkedStatus = `Background processing... running ${next.running}, queued ${next.pending}.`
      seedActionStatus = `Background processing... running ${next.running}, queued ${next.pending}.`
    } else if (next.pending > 0) {
      processMarkedStatus = `Queued for background processing... ${next.pending} job${next.pending === 1 ? '' : 's'} waiting.`
      seedActionStatus = `Queued for background processing... ${next.pending} job${next.pending === 1 ? '' : 's'} waiting.`
    } else if (next.total > 0) {
      if (next.failed > 0 || next.cancelled > 0) {
        processMarkedStatus = `Finished with issues: ${next.completed} completed, ${next.failed} failed, ${next.cancelled} cancelled.`
        seedActionStatus = `Finished with issues: ${next.completed} completed, ${next.failed} failed, ${next.cancelled} cancelled.`
      } else {
        processMarkedStatus = `Finished: ${next.completed} job${next.completed === 1 ? '' : 's'} completed.`
        seedActionStatus = `Finished: ${next.completed} job${next.completed === 1 ? '' : 's'} completed.`
      }
    }
  }

  async function refreshIngestJobProgress() {
    if (!ingestTrackedRequest.active || !Array.isArray(ingestTrackedRequest.jobIds) || ingestTrackedRequest.jobIds.length === 0) {
      return
    }
    try {
      const summary = await fetchPipelineJobsSummary({
        jobIds: ingestTrackedRequest.jobIds,
        recentLimit: Math.max(50, ingestTrackedRequest.jobIds.length),
      })
      applyIngestJobsSummary(summary)

      const signature = [
        ingestJobProgress.pending,
        ingestJobProgress.running,
        ingestJobProgress.completed,
        ingestJobProgress.failed,
        ingestJobProgress.cancelled,
      ].join(':')
      if (signature !== ingestLastProgressSignature) {
        ingestLastProgressSignature = signature
        const refreshTasks = [
          loadIngestStats({ quiet: true }),
        ...(diagnosticsEnabled ? [loadIngestStats({ quiet: true, global: true }), loadPipelineDaemonStatus({ quiet: true, global: true })] : []),
          ...(diagnosticsEnabled ? [loadIngestStats({ quiet: true, global: true }), loadPipelineDaemonStatus({ quiet: true, global: true })] : []),
          loadCorpus({ preserveSelection: true, quiet: true }),
        ]
        if (diagnosticsEnabled) {
          refreshTasks.push(loadDownloads())
        }
        await Promise.all(refreshTasks)
      }

      if (ingestProgressDone) {
        stopIngestProgressPolling()
        ingestTrackedRequest = { ...ingestTrackedRequest, active: false }
      }
    } catch (error) {
      if (error?.status === 401) {
        authStatus = 'unauthenticated'
        setAuthToken('')
        return
      }
      ingestJobProgress = {
        ...ingestJobProgress,
        error: error?.message || 'Failed to load queued job progress.',
      }
    }
  }

  function startIngestProgressPolling() {
    stopIngestProgressPolling()
    ingestProgressPollTimer = setInterval(() => {
      refreshIngestJobProgress()
    }, INGEST_PROGRESS_POLL_MS)
  }

  async function trackIngestQueuedJobs(payload, { withDownload = false } = {}) {
    const jobIdsFromPayload = Array.isArray(payload?.jobs)
      ? payload.jobs.map((job) => Number(job?.id)).filter((v) => Number.isFinite(v) && v > 0)
      : []
    const fallbackJobIds = [payload?.enrich_job_id, payload?.download_job_id]
      .map((v) => Number(v))
      .filter((v) => Number.isFinite(v) && v > 0)
    const jobIds = [...new Set([...jobIdsFromPayload, ...fallbackJobIds])]

    if (jobIds.length === 0) {
      resetIngestProgressTracking()
      return
    }

    ingestTrackedRequest = {
      active: true,
      requestId: String(payload?.request_id || ''),
      jobIds,
      withDownload: Boolean(withDownload),
    }
    ingestJobProgress = {
      ...ingestJobProgress,
      pending: jobIds.length,
      total: jobIds.length,
      updatedAt: new Date().toISOString(),
      error: '',
    }
    await refreshIngestJobProgress()
    if (ingestTrackedRequest.active) {
      startIngestProgressPolling()
    }
  }

  function beginExtractionProgress(baseName, backendFilename = '') {
    extractionProgressRunId += 1
    extractionCompletionCheckRunId = 0
    extractionProgress = {
      active: true,
      runId: extractionProgressRunId,
      baseName: String(baseName || '').trim(),
      backendFilename: String(backendFilename || '').trim(),
      phase: 'queued',
      detail: 'Extraction queued...',
      percent: 2,
      indeterminate: true,
      backendDone: false,
      pageFilesTotal: 0,
      pageFilesProcessed: 0,
      startedAtMs: Date.now(),
    }
  }

  function finishExtractionProgress() {
    extractionCompletionCheckRunId = 0
    extractionProgress = {
      active: false,
      runId: 0,
      baseName: '',
      backendFilename: '',
      phase: '',
      detail: '',
      percent: 0,
      indeterminate: false,
      backendDone: false,
      pageFilesTotal: 0,
      pageFilesProcessed: 0,
      startedAtMs: 0,
    }
  }

  function updateExtractionProgress({ phase, detail, percent, indeterminate, backendDone, pageFilesTotal, pageFilesProcessed } = {}) {
    if (!extractionProgress.active) return
    const next = { ...extractionProgress }
    if (phase) next.phase = phase
    if (typeof detail === 'string') next.detail = detail
    if (typeof indeterminate === 'boolean') next.indeterminate = indeterminate
    if (typeof backendDone === 'boolean') next.backendDone = backendDone
    if (Number.isFinite(Number(pageFilesTotal)) && Number(pageFilesTotal) >= 0) {
      next.pageFilesTotal = Number(pageFilesTotal)
    }
    if (Number.isFinite(Number(pageFilesProcessed)) && Number(pageFilesProcessed) >= 0) {
      next.pageFilesProcessed = Number(pageFilesProcessed)
    }
    if (Number.isFinite(Number(percent))) {
      // Keep progress monotonic and bounded.
      const bounded = Math.max(0, Math.min(100, Number(percent)))
      next.percent = Math.max(next.percent || 0, bounded)
    }
    extractionProgress = next
  }

  async function finalizeExtractionAfterBackendCompletion(detail = 'No bibliography entries found in this PDF.') {
    if (!extractionProgress.active) return
    const runId = extractionProgress.runId
    if (!runId || extractionCompletionCheckRunId === runId) return
    extractionCompletionCheckRunId = runId
    const baseName = String(latestEntriesBase || extractionProgress.baseName || '').trim()

    try {
      if (baseName) {
        const payload = await fetchBibliographyEntries(baseName)
        if (!extractionProgress.active || extractionProgress.runId !== runId) return
        const items = payload.items || []
        if (payload.seed_document) {
          latestSeedDocument = payload.seed_document
        }
        latestEntries = Array.isArray(items) ? items : []
        latestSelection = []
        if (latestEntries.length > 0) {
          latestEntriesStatus = `Loaded ${latestEntries.length} entries.`
        } else {
          latestEntriesStatus = detail
        }
        updateExtractionProgress({
          phase: 'completed',
          detail: latestEntries.length > 0 ? 'Extraction complete.' : detail,
          percent: 100,
          indeterminate: false,
        })
        await loadSeedSources({ quiet: true })
        await focusSeedSource('pdf', baseName)
        await loadIngestStats()
        await loadIngestRuns()
        const completedRunId = extractionProgress.runId
        setTimeout(() => {
          if (extractionProgress.active && extractionProgress.runId === completedRunId) {
            finishExtractionProgress()
          }
        }, 1200)
        return
      }

      if (!extractionProgress.active || extractionProgress.runId !== runId) return
      latestEntries = []
      latestSelection = []
      latestEntriesStatus = detail
      updateExtractionProgress({
        phase: 'completed',
        detail,
        percent: 100,
        indeterminate: false,
      })
      finishExtractionProgress()
    } catch (error) {
      if (error?.status === 401) {
        authStatus = 'unauthenticated'
        setAuthToken('')
        finishExtractionProgress()
        return
      }
      if (!extractionProgress.active || extractionProgress.runId !== runId) return
      latestEntriesStatus = detail
      updateExtractionProgress({
        phase: 'completed',
        detail,
        percent: 100,
        indeterminate: false,
      })
      finishExtractionProgress()
    }
  }

  function updateExtractionProgressFromLog(logLine) {
    if (!extractionProgress.active) return
    const text = String(logLine || '')
    if (!text) return

    const base = extractionProgress.baseName
    const backendName = extractionProgress.backendFilename
    const likelyExtractionLog =
      text.includes('extract-bibliography') ||
      text.includes('[get_bib_pages]') ||
      text.includes('[APIscraper')
    const mentionsCurrentRun =
      (base && text.includes(base)) || (backendName && text.includes(backendName))
    if (!likelyExtractionLog && !mentionsCurrentRun) return

    if (text.includes('Extracting seed document metadata')) {
      updateExtractionProgress({
        phase: 'seed_metadata',
        detail: 'Extracting seed metadata...',
        percent: 8,
        indeterminate: true,
      })
      return
    }

    const findRefsMatch = text.match(/Extracting bibliography pages for\s+(\d+)\s+PDF/i)
    if (findRefsMatch) {
      const docs = Number(findRefsMatch[1])
      updateExtractionProgress({
        phase: 'find_reference_pages',
        detail: `Finding reference sections (${docs} PDF${docs === 1 ? '' : 's'})...`,
        percent: 18,
        indeterminate: true,
      })
      return
    }

    const pagesExtractedMatch = text.match(/Done\.\s*Extracted\s+(\d+)\s+page\(s\)\s+in/i)
    if (pagesExtractedMatch) {
      const pages = Number(pagesExtractedMatch[1])
      updateExtractionProgress({
        phase: 'reference_pages_extracted',
        detail: `Extracted ${pages} reference page${pages === 1 ? '' : 's'}.`,
        percent: 38,
        indeterminate: false,
      })
      return
    }

    if (text.includes('Starting APIscraper')) {
      updateExtractionProgress({
        phase: 'parsing_pages',
        detail: 'Parsing extracted references...',
        percent: 45,
        indeterminate: true,
      })
      return
    }

    if (text.includes('Triggering inline citation fallback') || text.includes('No extracted page PDFs found')) {
      updateExtractionProgress({
        phase: 'inline_fallback',
        detail: 'No reference pages found. Trying inline extraction...',
        percent: 68,
        indeterminate: true,
      })
      return
    }

    const foundFilesMatch = text.match(/Found\s+(\d+)\s+PDF files/i)
    if (foundFilesMatch && text.includes('[APIscraper')) {
      const total = Number(foundFilesMatch[1])
      updateExtractionProgress({
        phase: 'parsing_pages',
        detail: `Parsing ${total} extracted page file${total === 1 ? '' : 's'}...`,
        percent: 50,
        indeterminate: false,
        pageFilesTotal: total,
        pageFilesProcessed: 0,
      })
      return
    }

    if (text.includes('[APIscraper') && text.includes('[INFO] Processing ')) {
      const total = Number(extractionProgress.pageFilesTotal || 0)
      const nextProcessed = Number(extractionProgress.pageFilesProcessed || 0) + 1
      if (total > 0) {
        const parsePct = 50 + Math.min(45, (nextProcessed / total) * 45)
        updateExtractionProgress({
          phase: 'parsing_pages',
          detail: `Parsing extracted pages (${Math.min(nextProcessed, total)}/${total})...`,
          percent: parsePct,
          indeterminate: false,
          pageFilesProcessed: Math.min(nextProcessed, total),
        })
      } else {
        updateExtractionProgress({
          phase: 'parsing_pages',
          detail: 'Parsing extracted pages...',
          percent: 55,
          indeterminate: true,
        })
      }
      return
    }

    if (text.includes('Processing Summary') && text.includes('[APIscraper')) {
      updateExtractionProgress({
        phase: 'finalizing',
        detail: 'Finalizing extracted entries...',
        percent: 95,
        indeterminate: false,
      })
      return
    }

    if (text.includes('No bibliography found by API')) {
      updateExtractionProgress({
        phase: 'finalizing',
        detail: 'No bibliography entries found. Finalizing...',
        percent: 98,
        indeterminate: false,
        backendDone: true,
      })
      return
    }

    const explicitDoneMatch = text.match(/\[extract-status\]\[corpus=.*?\]\[base=(.*?)\]\[file=(.*?)\]\s+done\s+status=(success|failed)(?:\s+mode=([^\s]+))?(?:\s+reason=(.+))?/i)
    if (explicitDoneMatch) {
      const [, signalBase = '', signalFile = '', signalStatus = 'success'] = explicitDoneMatch
      const baseMatches = base && signalBase === base
      const fileMatches = backendName && signalFile === backendName
      if (!baseMatches && !fileMatches) return
      updateExtractionProgress({
        phase: signalStatus === 'success' ? 'finalizing' : 'completed',
        detail: signalStatus === 'success' ? 'Finalizing extracted entries...' : 'Extraction failed.',
        percent: signalStatus === 'success' ? 98 : 100,
        indeterminate: false,
        backendDone: true,
      })
      if (signalStatus === 'success') {
        void finalizeExtractionAfterBackendCompletion('No bibliography entries found in this PDF.')
      } else {
        latestEntriesStatus = 'Extraction failed.'
        finishExtractionProgress()
      }
      return
    }

    if (text.includes('Bibliography extraction complete for')) {
      updateExtractionProgress({
        phase: 'finalizing',
        detail: 'Finalizing extracted entries...',
        percent: 98,
        indeterminate: false,
      })
      return
    }

    if (text.includes('Finished APIscraper inline fallback')) {
      updateExtractionProgress({
        phase: 'finalizing',
        detail: 'Finalizing extracted entries...',
        percent: 98,
        indeterminate: false,
      })
    }
  }

  function getLogsCorpusId() {
    if (!logsFilterCurrentCorpus) return null
    return Number.isFinite(Number(currentCorpusId)) ? Number(currentCorpusId) : null
  }

  function shouldTriggerPipelineRefresh(logLine) {
    const text = String(logLine || '')
    if (!text) return false
    return (
      text.includes('[pipeline-worker]') ||
      text.includes('[pipeline-worker mark]') ||
      text.includes('[pipeline-worker enrich]') ||
      text.includes('[download-worker] done') ||
      text.includes('[DAEMON] Picked up job') ||
      text.includes('[DAEMON] Successfully completed job') ||
      text.includes('[DAEMON] Job ') ||
      text.includes('[SUCCESS] Inserted') ||
      text.includes('Bibliography extraction complete') ||
      text.includes('Finished APIscraper') ||
      text.includes('Processing Summary')
    )
  }

  function isLogLineForCurrentCorpus(logLine) {
    if (!logsFilterCurrentCorpus) return true
    const corpusFilter = getLogsCorpusId()
    if (!corpusFilter) return true
    const text = String(logLine || '')
    const match = text.match(/corpus=([a-zA-Z0-9_-]+)/)
    if (!match) return true
    return match[1] === String(corpusFilter)
  }

  function schedulePipelineRefresh(delayMs = 1500) {
    if (pipelineRefreshTimer || pipelineRefreshInFlight) return
    pipelineRefreshTimer = setTimeout(async () => {
      pipelineRefreshTimer = null
      if (pipelineRefreshInFlight || authStatus !== 'authenticated') return
      pipelineRefreshInFlight = true
      try {
        const tasks = [
          loadIngestStats({ quiet: true }),
          loadIngestRuns(),
        ]
        // The corpus tab is selection-heavy; avoid replacing the list on every worker tick.
        if (activeTab !== 'corpus') {
          tasks.push(loadCorpus({ preserveSelection: true, quiet: true }))
        }
        if (diagnosticsEnabled) {
          tasks.push(
            loadPipelineWorkerStatus({ quiet: true }),
            loadDownloadWorkerStatus({ quiet: true }),
            loadDownloads(),
          )
        }
        await Promise.all(tasks)
      } finally {
        pipelineRefreshInFlight = false
      }
    }, delayMs)
  }

  function snapshotCorpusScroll() {
    return {
      raw: rawCorpusTableEl?.scrollTop ?? 0,
      meta: metaCorpusTableEl?.scrollTop ?? 0,
      downloaded: downloadedCorpusTableEl?.scrollTop ?? 0,
    }
  }

  function restoreCorpusScroll(snapshot) {
    if (!snapshot) return
    if (rawCorpusTableEl) rawCorpusTableEl.scrollTop = snapshot.raw
    if (metaCorpusTableEl) metaCorpusTableEl.scrollTop = snapshot.meta
    if (downloadedCorpusTableEl) downloadedCorpusTableEl.scrollTop = snapshot.downloaded
  }

  function shouldRunLiveRefresh() {
    if (authStatus !== 'authenticated') return false
    if (ingestTrackedRequest.active) return true
    if (diagnosticsEnabled && (pipelineWorker.running || pipelineWorker.in_flight)) return true
    if (diagnosticsEnabled && (downloadWorker.running || downloadWorker.in_flight)) return true
    return activeTab === 'workspace' || activeTab === 'dashboard' || activeTab === 'downloads'
  }

  async function runLiveRefreshCycle() {
    if (!shouldRunLiveRefresh()) return
    if (pipelineRefreshInFlight) return
    pipelineRefreshInFlight = true
    try {
      const tasks = [
        loadIngestStats({ quiet: true }),
        loadCorpus({ preserveSelection: true, quiet: true }),
      ]
      if (diagnosticsEnabled) {
        tasks.push(
          loadDownloads(),
          loadDownloadWorkerStatus({ quiet: true }),
          loadPipelineWorkerStatus({ quiet: true }),
        )
      }
      await Promise.all(tasks)
    } finally {
      pipelineRefreshInFlight = false
    }
  }

  async function refreshForActiveTab(tabId) {
    if (authStatus !== 'authenticated') return
    const now = Date.now()
    // Avoid duplicate refreshes from rapid tab/hash updates.
    if (now - lastTabRefreshAt < 800) return
    if (pipelineRefreshInFlight) return
    lastTabRefreshAt = now
    pipelineRefreshInFlight = true
    try {
      const tasks = [
        loadIngestStats({ quiet: true }),
      ]
      if (diagnosticsEnabled) {
        tasks.push(
          loadPipelineWorkerStatus({ quiet: true }),
          loadDownloadWorkerStatus({ quiet: true }),
        )
      }
      if (tabId === 'workspace' && seedSources.length === 0) {
        tasks.push(loadSeedSources({ quiet: true }))
      }
      if (tabId === 'dashboard' || tabId === 'workspace') {
        tasks.push(loadCorpus({ preserveSelection: true, quiet: true }))
      }
      if (diagnosticsEnabled && (tabId === 'dashboard' || tabId === 'downloads')) {
        tasks.push(loadDownloads())
      }
      if (diagnosticsEnabled && tabId === 'graph') {
        tasks.push(loadGraph())
      }
      await Promise.all(tasks)
    } finally {
      pipelineRefreshInFlight = false
    }
  }

  async function bootstrapAuth({ preserveAuthFlow = false } = {}) {
    authStatus = 'loading'
    authError = ''
    try {
      const payload = await fetchMe()
      authUser = payload.user
      corpora = payload.corpora || []
      currentCorpusId = authUser?.last_corpus_id || (corpora[0]?.id ?? null)
      authStatus = 'authenticated'
      if (!preserveAuthFlow) {
        authFlow = 'login'
        authFlowToken = ''
      }
      await loadRecursionConfig()
      await refreshAll()
      connectLogs()
      if (!preserveAuthFlow) {
        setActiveTab('workspace', { replace: true })
      }
    } catch (error) {
      authStatus = 'unauthenticated'
      authError = ''
      authUser = null
      corpora = []
      currentCorpusId = null
      disconnectLogs()
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
      authFlow = 'login'
      authFlowToken = ''
      await loadRecursionConfig()
      await refreshAll()
      connectLogs()
      setActiveTab('workspace', { replace: true })
    } catch (error) {
      authError = error.message || 'Login failed'
      authStatus = 'unauthenticated'
      disconnectLogs()
      setAuthToken('')
    }
  }

  async function handleForgotPassword(event) {
    event?.preventDefault?.()
    authError = ''
    authInfo = ''
    try {
      await requestPasswordReset(authEmail.trim())
      authInfo = 'If that email exists, a reset link has been sent.'
      authEmail = ''
    } catch (error) {
      authError = error.message || 'Failed to send password reset email'
    }
  }

  async function handleAcceptInvite(event) {
    event?.preventDefault?.()
    authError = ''
    authInfo = ''
    if (!authFlowToken) {
      authError = 'Invite token is missing.'
      return
    }
    if (authPassword.length < 8) {
      authError = 'Password must be at least 8 characters.'
      return
    }
    if (authPassword !== authPasswordConfirm) {
      authError = 'Passwords do not match.'
      return
    }
    try {
      const payload = await acceptInvite({ token: authFlowToken, password: authPassword })
      if (payload?.user) {
        authUser = payload.user
        currentCorpusId = payload.user.last_corpus_id || null
      }
      const me = await fetchMe()
      authUser = me.user
      corpora = me.corpora || []
      currentCorpusId = authUser?.last_corpus_id || currentCorpusId || corpora[0]?.id || null
      authStatus = 'authenticated'
      authFlow = 'login'
      authFlowToken = ''
      await loadRecursionConfig()
      await refreshAll()
      connectLogs()
      setActiveTab('workspace', { replace: true })
    } catch (error) {
      authError = error.message || 'Failed to accept invite'
    }
  }

  async function handleResetPassword(event) {
    event?.preventDefault?.()
    authError = ''
    authInfo = ''
    if (!authFlowToken) {
      authError = 'Reset token is missing.'
      return
    }
    if (authPassword.length < 8) {
      authError = 'Password must be at least 8 characters.'
      return
    }
    if (authPassword !== authPasswordConfirm) {
      authError = 'Passwords do not match.'
      return
    }
    try {
      const payload = await resetPassword({ token: authFlowToken, password: authPassword })
      if (payload?.user) {
        authUser = payload.user
        currentCorpusId = payload.user.last_corpus_id || null
      }
      const me = await fetchMe()
      authUser = me.user
      corpora = me.corpora || []
      currentCorpusId = authUser?.last_corpus_id || currentCorpusId || corpora[0]?.id || null
      authStatus = 'authenticated'
      authFlow = 'login'
      authFlowToken = ''
      await loadRecursionConfig()
      await refreshAll()
      connectLogs()
      setActiveTab('workspace', { replace: true })
    } catch (error) {
      authError = error.message || 'Failed to reset password'
    }
  }

  async function handleCreateInvitation(event) {
    event?.preventDefault?.()
    inviteStatus = ''
    inviteError = false
    try {
      const payload = await createUserInvitation({
        username: inviteUsername.trim(),
        email: inviteEmail.trim(),
      })
      inviteStatus = `Invitation sent to ${payload.email}.`
      inviteUsername = ''
      inviteEmail = ''
    } catch (error) {
      inviteStatus = error.message || 'Failed to create invitation'
      inviteError = true
    }
  }

  function handleLogout() {
    disconnectLogs()
    setAuthToken('')
    authStatus = 'unauthenticated'
    authError = ''
    authUser = null
    corpora = []
    currentCorpusId = null
    loginPassword = ''
    resetFrontendState()
  }

  async function refreshAll() {
    const tasks = [
      loadSeedSources(),
      loadIngestStats(),
      loadIngestRuns(),
      loadCorpus(),
      loadKantroposAssignment(),
    ]
    if (diagnosticsEnabled) {
      tasks.push(
        loadDownloads(),
        loadDownloadWorkerStatus(),
        loadPipelineWorkerStatus(),
        loadGraph(),
      )
    }
    await Promise.all(tasks)
  }

  function resetFrontendState() {
    resetIngestProgressTracking()
    ingestStats = { raw_pending: 0, matched: 0, queued_download: 0, downloaded: 0 }
    ingestStatsStatus = ''
    latestEntries = []
    latestEntriesStatus = ''
    latestEntriesBase = ''
    latestSeedDocument = null
    latestSelection = []
    enqueueStatus = ''
    processMarkedStatus = ''
    ingestRuns = []
    ingestRunsStatus = ''
    seedSources = []
    seedSourcesStatus = ''
    expandedSeedSourceId = ''
    seedCandidatesBySource = {}
    seedCandidatesLoading = {}
    seedSelections = {}
    seedActionStatus = ''
    seedActionBusy = false
    seedLastRunKey = ''
    searchQuery = ''
    searchSeedJson = ''
    searchResults = []
    searchStatus = ''
    searchSource = ''
    searchSelection = []
    searchQueueConfigs = {}
    searchQueueStatus = ''
    searchQueueBusy = false
    corpusItems = []
    corpusTotal = 0
    corpusLoadStatus = ''
    corpusSource = ''
    corpusStageTotals = { raw: 0, metadata: 0, downloaded: 0 }
    downloads = []
    downloadsSource = ''
    downloadsQuery = ''
    graphStatus = ''
    graphSource = ''
    graphNodes = []
    graphEdges = []
    graphStats = null
    graphLayout = []
    graphVisibleEdges = []
    graphNodeMap = new Map()
    graphDegreeMap = new Map()
    graphComponentMap = new Map()
    graphNodeColorMap = new Map()
    graphColorLegend = []
    logs = []
    logsCursor = 0
    logsHasMore = false
    logsTailStatus = ''
    pendingPromotionEvents = []
    promotionHighlights = new Set()
    corpusActionStatus = ''
    corpusActionError = false
    resetKantroposAssignmentState()
    finishExtractionProgress()
  }

  async function handleSelectCorpus(event) {
    corpusActionStatus = ''
    corpusActionError = false
    const nextId = Number(event.target.value)
    if (!nextId) return
    try {
      await selectCorpus(nextId)
      const me = await fetchMe()
      authUser = me.user
      corpora = me.corpora || []
      currentCorpusId = authUser?.last_corpus_id || nextId
      resetFrontendState()
      await refreshAll()
      if (logsType === 'pipeline') {
        handleLogsCorpusFilterChange()
      }
    } catch (error) {
      authError = error.message || 'Failed to select corpus'
    }
  }

  async function handleCreateCorpus() {
    if (creatingCorpus) return
    corpusActionStatus = ''
    corpusActionError = false
    const name = window.prompt('New corpus name')
    if (!name) return
    creatingCorpus = true
    try {
      await createCorpus(name.trim())
      const me = await fetchMe()
      authUser = me.user
      corpora = me.corpora || []
      currentCorpusId = authUser?.last_corpus_id || currentCorpusId
      resetFrontendState()
      await refreshAll()
      if (logsType === 'pipeline') {
        handleLogsCorpusFilterChange()
      }
      corpusActionStatus = `Created corpus "${name.trim()}".`
      corpusActionError = false
    } catch (error) {
      corpusActionStatus = error.message || 'Failed to create corpus.'
      corpusActionError = true
      authError = error.message || 'Failed to create corpus'
    } finally {
      creatingCorpus = false
    }
  }

  async function handleDeleteCorpus() {
    if (deletingCorpus) return
    corpusActionStatus = ''
    corpusActionError = false
    if (!currentCorpusId) {
      corpusActionStatus = 'Select a corpus first.'
      corpusActionError = true
      return
    }
    if (!canDeleteCurrentCorpus) {
      corpusActionStatus = 'Only corpus owners can delete corpora.'
      corpusActionError = true
      return
    }

    const corpusName = String(currentCorpus?.name || `Corpus ${currentCorpusId}`).trim()
    const confirmed = window.confirm(
      `Delete corpus "${corpusName}"?\n\nThis removes corpus shares, ingest rows, and queued jobs for this corpus.`
    )
    if (!confirmed) return

    deletingCorpus = true
    corpusActionStatus = `Deleting "${corpusName}"...`
    corpusActionError = false
    try {
      const payload = await deleteCorpusApi(currentCorpusId)
      const me = await fetchMe()
      authUser = me.user
      corpora = me.corpora || []
      currentCorpusId = authUser?.last_corpus_id || payload?.selected_corpus_id || corpora[0]?.id || null
      resetFrontendState()
      await refreshAll()
      if (logsType === 'pipeline') {
        handleLogsCorpusFilterChange()
      }
      const deletedName = payload?.deleted_name || corpusName
      corpusActionStatus = `Deleted corpus "${deletedName}".`
      corpusActionError = false
    } catch (error) {
      corpusActionStatus = error.message || 'Failed to delete corpus.'
      corpusActionError = true
    } finally {
      deletingCorpus = false
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

  async function loadKantroposTargets() {
    try {
      const payload = await fetchKantroposCorpora()
      kantroposTargets = payload?.corpora || []
    } catch (error) {
      kantroposTargets = []
      kantroposAssignmentStatus = error.message || 'Failed to load Kantropos corpora.'
      kantroposAssignmentError = true
    }
  }

  async function loadKantroposAssignment() {
    const corpusId = Number(currentCorpusId)
    const requestSeq = ++kantroposAssignmentRequestSeq
    resetKantroposAssignmentState()
    if (!Number.isFinite(corpusId) || corpusId <= 0) return
    kantroposAssignmentLoading = true
    try {
      if (kantroposTargets.length === 0) {
        await loadKantroposTargets()
      }
      const payload = await fetchCorpusKantroposAssignment(corpusId)
      if (requestSeq !== kantroposAssignmentRequestSeq || Number(currentCorpusId) !== corpusId) return
      kantroposTargets = payload?.corpora || kantroposTargets
      kantroposAssignment = payload?.assignment || null
      selectedKantroposTargetId = payload?.assignment?.target_id || ''
      if (!payload?.can_edit && currentCorpus?.role !== 'viewer') {
        // no-op; backend truth is enough, UI editability still derives from current corpus role
      }
    } catch (error) {
      if (requestSeq !== kantroposAssignmentRequestSeq || Number(currentCorpusId) !== corpusId) return
      kantroposAssignmentStatus = error.message || 'Failed to load Kantropos assignment.'
      kantroposAssignmentError = true
    } finally {
      if (requestSeq === kantroposAssignmentRequestSeq && Number(currentCorpusId) === corpusId) {
        kantroposAssignmentLoading = false
      }
    }
  }

  async function handleSaveKantroposAssignment() {
    kantroposAssignmentStatus = ''
    kantroposAssignmentError = false
    const corpusId = Number(currentCorpusId)
    if (!Number.isFinite(corpusId) || corpusId <= 0) {
      kantroposAssignmentStatus = 'Select a corpus first.'
      kantroposAssignmentError = true
      return
    }
    if (!canConfigureCurrentCorpusKantropos) {
      kantroposAssignmentStatus = 'Only corpus owners, editors, or the global admin can change the Kantropos assignment.'
      kantroposAssignmentError = true
      return
    }
    kantroposAssignmentSaving = true
    try {
      const payload = await saveCorpusKantroposAssignment(corpusId, selectedKantroposTargetId || null)
      kantroposAssignment = payload?.assignment || null
      selectedKantroposTargetId = payload?.assignment?.target_id || ''
      kantroposAssignmentStatus = kantroposAssignment
        ? `Assigned to Kantropos corpus "${kantroposAssignment.target_name}".`
        : 'Cleared Kantropos assignment.'
      kantroposAssignmentError = false
    } catch (error) {
      kantroposAssignmentStatus = error.message || 'Failed to save Kantropos assignment.'
      kantroposAssignmentError = true
    } finally {
      kantroposAssignmentSaving = false
    }
  }

  async function handleKantroposTargetChange(event) {
    selectedKantroposTargetId = String(event?.target?.value || '')
    await handleSaveKantroposAssignment()
  }

  async function loadLatestEntries(baseName, { waitForExtraction = false } = {}) {
    if (!waitForExtraction && extractionProgress.active) {
      finishExtractionProgress()
    }
    const waitRunId = waitForExtraction ? extractionProgress.runId : 0
    latestEntriesBase = baseName
    latestSeedDocument = null
    latestEntriesStatus = waitForExtraction ? 'Waiting for extracted entries...' : 'Loading extracted entries...'
    latestEntries = []
    latestSelection = []
    const startedAt = Date.now()
    const maxWaitMs = waitForExtraction ? 180_000 : 10_000
    const pollIntervalMs = waitForExtraction ? 2_000 : 1_500

    while (Date.now() - startedAt < maxWaitMs) {
      if (waitForExtraction && waitRunId && extractionProgress.runId !== waitRunId) {
        return
      }
      try {
        const payload = await fetchBibliographyEntries(baseName)
        const items = payload.items || []
        if (payload.seed_document) {
          latestSeedDocument = payload.seed_document
        }
        if (Array.isArray(items) && items.length > 0) {
          latestEntries = items
          if (waitForExtraction) {
            if (!extractionProgress.backendDone) {
              latestEntriesStatus = `Extracted ${items.length} entr${items.length === 1 ? 'y' : 'ies'} so far...`
            } else {
              latestEntriesStatus = `Loaded ${items.length} entries.`
            }
          } else {
            latestEntriesStatus = `Loaded ${items.length} entries.`
          }
          if (waitForExtraction && extractionProgress.backendDone) {
            updateExtractionProgress({
              phase: 'completed',
              detail: 'Extraction complete.',
              percent: 100,
              indeterminate: false,
            })
            // Briefly keep 100% visible, then clear.
            const completedRunId = extractionProgress.runId
            setTimeout(() => {
              if (extractionProgress.active && extractionProgress.runId === completedRunId) {
                finishExtractionProgress()
              }
            }, 1200)
            return
          }
          if (!waitForExtraction) {
            return
          }
        }
      } catch (error) {
        if (error?.status === 401) {
          authStatus = 'unauthenticated'
          setAuthToken('')
          finishExtractionProgress()
          return
        }
        if (!waitForExtraction) {
          latestEntriesStatus = 'Failed to load extracted entries.'
          return
        }
        // Keep waiting while extraction is in progress.
      }
      if (waitForExtraction) {
        const elapsedSec = Math.floor((Date.now() - startedAt) / 1000)
        if (!latestEntries.length) {
          latestEntriesStatus = extractionProgress.backendDone
            ? `Finalizing extracted entries... ${elapsedSec}s`
            : `Waiting for extracted entries... ${elapsedSec}s`
        }
      } else {
        latestEntriesStatus = 'Loading extracted entries...'
      }
      await sleep(pollIntervalMs)
    }

    if (waitForExtraction) {
      latestEntriesStatus = extractionProgress.backendDone
        ? 'Finalizing extracted entries...'
        : 'Extraction still running. Waiting for entries to appear...'
    } else {
      latestEntriesStatus = 'No extracted entries found.'
    }
  }

  function clearLatestEntriesView() {
    if (extractionProgress.active) {
      finishExtractionProgress()
    }
    latestEntries = []
    latestEntriesBase = ''
    latestSeedDocument = null
    latestEntriesStatus = ''
    latestSelection = []
  }

  async function handleToggleLatestRun(baseName) {
    const normalized = String(baseName || '').trim()
    if (!normalized) return
    if (String(latestEntriesBase || '').trim() === normalized) {
      clearLatestEntriesView()
      return
    }
    await loadLatestEntries(normalized)
  }

  function latestSelectionKey(entry, index = -1) {
    const primaryId = entry?.id ?? entry?.entry_id
    if (primaryId !== null && primaryId !== undefined && String(primaryId).trim() !== '') {
      return `id:${String(primaryId)}`
    }

    const doi = String(entry?.doi || '').trim().toLowerCase()
    if (doi) {
      return `doi:${doi}`
    }

    const title = String(formatTitle(entry) || '').trim().toLowerCase()
    const authors = String(formatAuthors(entry) || '').trim().toLowerCase()
    const year = entry?.year ?? ''
    if (title || authors || year) {
      return `tay:${title}|${authors}|${year}|${index}`
    }

    if (index >= 0) {
      return `idx:${index}`
    }
    return ''
  }

  function hasLatestEntryId(entry) {
    const primaryId = entry?.id ?? entry?.entry_id
    return primaryId !== null && primaryId !== undefined && String(primaryId).trim() !== ''
  }

  function latestIngestState(entry) {
    const state = String(entry?.ingest_state || '').trim().toLowerCase()
    if ([
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
    ].includes(state)) {
      return state
    }
    return 'pending'
  }

  function isLatestDownloaded(entry) {
    return latestIngestState(entry) === 'downloaded'
  }

  function isLatestProcessed(entry) {
    return latestIngestState(entry) === 'enriched'
  }

  function latestIngestTag(entry) {
    const state = latestIngestState(entry)
    switch (state) {
      case 'downloaded':
        return { label: 'Downloaded', className: 'downloaded' }
      case 'queued_download':
        return { label: 'Queued DL', className: 'queued' }
      case 'enriched':
        return { label: 'Enriched', className: 'completed' }
      case 'queued_enrichment':
        return { label: 'Queued', className: 'queued' }
      case 'staged_raw':
        return { label: 'Raw', className: 'pending' }
      case 'failed_enrichment':
        return { label: 'Enrich Fail', className: 'failed' }
      case 'failed_download':
        return { label: 'Download Fail', className: 'failed' }
      default:
        return { label: 'Pending', className: 'pending' }
    }
  }

  function isLatestSelectable(entry) {
    return hasLatestEntryId(entry) && ['pending', 'staged_raw'].includes(latestIngestState(entry))
  }

  function isLatestSelected(entry, index = -1) {
    if (!isLatestSelectable(entry)) return false
    const key = latestSelectionKey(entry, index)
    if (!key) return false
    return latestSelection.includes(key)
  }

  function clearLatestSelection() {
    latestSelection = []
  }

  function selectAllLatest() {
    latestSelection = selectableLatestKeys(latestEntries || [])
  }

  function toggleLatestSelection(entry, index) {
    if (!isLatestSelectable(entry)) return;
    const key = latestSelectionKey(entry, index);
    if (!key) return;
    if (latestSelection.includes(key)) {
      latestSelection = latestSelection.filter(k => k !== key);
    } else {
      latestSelection = [...latestSelection, key];
    }
  }

  async function enqueueSelectedLatest(withDownload = false) {
    enqueueStatus = ''
    const ids = (latestEntries || [])
      .filter((entry, index) => isLatestSelectable(entry) && isLatestSelected(entry, index))
      .map((entry) => entry.id ?? entry.entry_id)
      .filter((id) => id !== null && id !== undefined && String(id).trim() !== '')
    if (ids.length === 0) {
      enqueueStatus = 'Select at least one entry.'
      return
    }
    enqueueStatus = `Marking ${ids.length} entries for enrichment...`
    try {
      const payload = await enqueueIngestEntries(ids)
      enqueueStatus = `Marked ${payload.marked} (staged: ${payload.staged}, already present downstream: ${payload.already_processed}, duplicates: ${payload.duplicates}). Starting enrichment...`
      await loadDownloads()
      await loadIngestStats()
      
      // Automatically trigger enrichment after marking
      if (payload.marked > 0 || payload.staged > 0) {
        // Set limit to match what was just marked, or default to a reasonable batch size
        processMarkedLimit = Math.max(10, payload.marked + payload.staged)
        await processMarkedBatch({ withDownload })
      } else {
        enqueueStatus += ' No new entries to enrich.'
      }
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

  async function processMarkedBatch({ withDownload = false } = {}) {
    processMarkedStatus = ''
    if (processingMarked) return
    const limit = Number(processMarkedLimit)
    if (!Number.isFinite(limit) || limit <= 0) {
      processMarkedStatus = 'Limit must be a positive number.'
      return
    }
    processingMarked = true
    processMarkedStatus = `Processing up to ${limit} marked entries (enrich -> with metadata)...`
    try {
      const payload = await processMarkedIngestEntries({
        limit,
        includeDownstream,
        includeUpstream,
        relatedDepthDownstream,
        relatedDepthUpstream,
        maxRelated,
        enqueueDownload: withDownload,
        downloadBatchSize: Math.max(25, limit),
      })
      processMarkedStatus = payload.message || 'Jobs queued successfully.'
      if (payload?.jobs?.length > 0) {
        await trackIngestQueuedJobs(payload, { withDownload })
      } else {
        resetIngestProgressTracking()
      }
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

  async function handleFiles(event) {
    const files = Array.from(event.target.files || [])
    if (files.length === 0) return
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
    for (const item of newItems) {
      await uploadItem(item)
    }
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

  async function extractForItem(item) {
    if (!item.backendFilename) return
    const baseName = item.backendFilename.replace(/\.pdf$/i, '')
    beginExtractionProgress(baseName, item.backendFilename)
    updateUpload(item.id, { status: 'extracting', message: 'Extracting bibliography' })
    try {
      await extractBibliography(item.backendFilename)
      updateUpload(item.id, { status: 'extracted', message: 'Extraction started' })
      latestEntriesBase = baseName
      latestSeedDocument = null
      latestEntries = []
      latestSelection = []
      latestEntriesStatus = 'Waiting for extraction to finish...'
    } catch (error) {
      finishExtractionProgress()
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
      ingestRunsStatus = ''
    } catch (error) {
      if (error?.status === 401) {
        authStatus = 'unauthenticated'
        setAuthToken('')
      }
      ingestRunsStatus = 'Failed to load ingest runs.'
    }
  }

  async function loadIngestStats({ quiet = false, global = false } = {}) {
    if (!quiet) ingestStatsStatus = 'Loading ingest stats...'
    try {
      const payload = await fetchIngestStats({ global })
      if (global) {
        globalIngestStats = payload.stats || globalIngestStats
      } else {
        ingestStats = payload.stats || ingestStats
      }
      if (!quiet) ingestStatsStatus = ''
      apiStatus = 'online'
    } catch (error) {
      if (error?.status === 401) {
        authStatus = 'unauthenticated'
        setAuthToken('')
      }
      if (!quiet) ingestStatsStatus = 'Failed to load ingest stats.'
      apiStatus = 'offline'
    }
  }

  function seedSourceId(source) {
    if (!source) return ''
    return String(source.id || `${source.source_type}:${source.source_key}`)
  }

  function seedCandidateState(candidate) {
    const state = String(candidate?.state || '').trim().toLowerCase()
    if ([
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
    ].includes(state)) {
      return state
    }
    return 'pending'
  }

  function isSeedCandidateInCorpus(candidate) {
    return Boolean(candidate?.in_corpus)
  }

  function summarizeSeedCandidateStates(candidates = []) {
    const counts = {
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
    for (const candidate of candidates) {
      if (isSeedCandidateInCorpus(candidate)) {
        counts.in_corpus += 1
      }
      const state = seedCandidateState(candidate)
      counts[state] = (counts[state] || 0) + 1
    }
    return counts
  }

  function reconcileSeedSourceCandidates(sourceId, candidates) {
    const nextCandidates = Array.isArray(candidates) ? candidates : []
    if (nextCandidates.length === 0) {
      seedSources = seedSources.filter((source) => seedSourceId(source) !== sourceId)
      seedSelections = Object.fromEntries(
        Object.entries(seedSelections).filter(([key]) => key !== sourceId)
      )
      selectedSeedCandidateKeys = Object.fromEntries(
        Object.entries(selectedSeedCandidateKeys).filter(([key]) => key !== sourceId)
      )
      if (expandedSeedSourceId === sourceId) {
        expandedSeedSourceId = ''
      }
      return
    }
    const activeKey = selectedSeedCandidateKeys[sourceId]
    if (activeKey) {
      const stillExists = nextCandidates.some((candidate) => String(candidate?.candidate_key || '') === activeKey)
      if (!stillExists) {
        selectedSeedCandidateKeys = {
          ...selectedSeedCandidateKeys,
          [sourceId]: '',
        }
      }
    }
    const nextCounts = summarizeSeedCandidateStates(nextCandidates)
    seedSources = seedSources.map((source) => {
      if (seedSourceId(source) !== sourceId) return source
      return {
        ...source,
        candidate_count: nextCandidates.length,
        state_counts: nextCounts,
      }
    })
  }

  function seedCandidateTag(candidate) {
    switch (seedCandidateState(candidate)) {
      case 'downloaded':
        return { label: 'Downloaded', className: 'downloaded' }
      case 'downloaded_elsewhere':
        return { label: 'Elsewhere', className: 'elsewhere' }
      case 'queued_download':
        return { label: 'Queued DL', className: 'queued' }
      case 'enriched':
        return { label: 'Metadata', className: 'completed' }
      case 'queued_enrichment':
        return { label: 'Queued', className: 'queued' }
      case 'staged_raw':
        return { label: 'Raw', className: 'pending' }
      case 'added':
        return { label: 'Added', className: 'completed' }
      case 'failed_enrichment':
        return { label: 'Enrich Fail', className: 'failed' }
      case 'failed_download':
        return { label: 'Download Fail', className: 'failed' }
      default:
        return { label: 'Pending', className: 'pending' }
    }
  }

  function isSeedCandidateSelectable(candidate) {
    return seedCandidateState(candidate) !== 'downloaded' && !isSeedCandidateInCorpus(candidate)
  }

  function getSeedCandidatesForSource(source) {
    return seedCandidatesBySource[seedSourceId(source)] || []
  }

  function getSeedSelectionForSource(source) {
    return seedSelections[seedSourceId(source)] || []
  }

  function selectedSeedCount(source) {
    return getSeedSelectionForSource(source).length
  }

  function promotableSeedCandidateKeys(source) {
    return getSeedCandidatesForSource(source)
      .filter((candidate) => isSeedCandidateSelectable(candidate))
      .map((candidate) => String(candidate?.candidate_key || ''))
      .filter(Boolean)
  }

  function promotableSeedCount(source) {
    return promotableSeedCandidateKeys(source).length
  }

  function getSelectedSeedCandidate(source) {
    const sourceId = seedSourceId(source)
    const activeKey = selectedSeedCandidateKeys[sourceId]
    if (!activeKey) return null
    return getSeedCandidatesForSource(source).find((candidate) =>
      String(candidate?.candidate_key || '') === activeKey
    ) || null
  }

  function selectableSeedCount(source) {
    return getSeedCandidatesForSource(source).filter((candidate) => isSeedCandidateSelectable(candidate)).length
  }

  function estimatedSelectableSeedCount(source) {
    const loadedCount = selectableSeedCount(source)
    if (loadedCount > 0) return loadedCount
    const total = Number(source?.candidate_count || 0)
    const inCorpus = Number(source?.state_counts?.in_corpus || 0)
    const downloaded = Number(source?.state_counts?.downloaded || 0)
    return Math.max(0, total - Math.max(inCorpus, downloaded))
  }

  function isSeedCandidateSelected(source, candidate) {
    return getSeedSelectionForSource(source).includes(String(candidate?.candidate_key || ''))
  }

  function setSelectedSeedCandidate(source, candidate) {
    const sourceId = seedSourceId(source)
    const candidateKey = String(candidate?.candidate_key || '')
    if (!sourceId || !candidateKey) return
    selectedSeedCandidateKeys = {
      ...selectedSeedCandidateKeys,
      [sourceId]: candidateKey,
    }
  }

  function toggleSeedCandidateSelection(source, candidate) {
    const sourceId = seedSourceId(source)
    const candidateKey = String(candidate?.candidate_key || '')
    if (!sourceId || !candidateKey || !isSeedCandidateSelectable(candidate)) return
    const current = seedSelections[sourceId] || []
    seedSelections = current.includes(candidateKey)
      ? { ...seedSelections, [sourceId]: current.filter((value) => value !== candidateKey) }
      : { ...seedSelections, [sourceId]: [...current, candidateKey] }
    seedSelectionVersions = { ...seedSelectionVersions, [sourceId]: (seedSelectionVersions[sourceId] || 0) + 1 }
  }

  function clearSeedSelection(source) {
    const sourceId = seedSourceId(source)
    if (!sourceId) return
    seedSelections = { ...seedSelections, [sourceId]: [] }
    seedSelectionVersions = { ...seedSelectionVersions, [sourceId]: (seedSelectionVersions[sourceId] || 0) + 1 }
  }

  function selectAllSeedCandidates(source) {
    const sourceId = seedSourceId(source)
    if (!sourceId) return
    const keys = getSeedCandidatesForSource(source)
      .filter((candidate) => isSeedCandidateSelectable(candidate))
      .map((candidate) => String(candidate.candidate_key || ''))
      .filter(Boolean)
    seedSelections = { ...seedSelections, [sourceId]: keys }
    seedSelectionVersions = { ...seedSelectionVersions, [sourceId]: (seedSelectionVersions[sourceId] || 0) + 1 }
  }

  async function setAllSeedCandidatesSelected(source, checked) {
    if (checked) {
      const sourceId = seedSourceId(source)
      if (!sourceId) return
      if (!Array.isArray(seedCandidatesBySource[sourceId]) || seedCandidatesBySource[sourceId].length === 0) {
        await loadSeedCandidatesForSource(source, { quiet: true })
      }
      selectAllSeedCandidates(source)
      return
    }
    clearSeedSelection(source)
  }

  async function loadSeedSources({ quiet = false } = {}) {
    if (!quiet) seedSourcesStatus = 'Loading seed sources...'
    try {
      const payload = await fetchSeedSources(100)
      seedSources = payload.sources || []
      const validSourceIds = new Set(seedSources.map((source) => seedSourceId(source)))
      if (!quiet && expandedSeedSourceId && !validSourceIds.has(expandedSeedSourceId)) {
        expandedSeedSourceId = ''
      }
      seedSelections = Object.fromEntries(
        Object.entries(seedSelections).filter(([key]) => validSourceIds.has(key))
      )
      if (!quiet) {
        seedSourcesStatus = seedSources.length > 0
          ? `Loaded ${seedSources.length} seed source${seedSources.length === 1 ? '' : 's'}.`
          : 'No seed sources yet.'
      }
      if (expandedSeedSourceId) {
        const expandedSource = seedSources.find((source) => seedSourceId(source) === expandedSeedSourceId)
        if (expandedSource) {
          await loadSeedCandidatesForSource(expandedSource, { quiet: true, background: true })
        }
      }
    } catch (error) {
      if (error?.status === 401) {
        authStatus = 'unauthenticated'
        setAuthToken('')
        return
      }
      if (!quiet) seedSourcesStatus = error?.message || 'Failed to load seed sources.'
    }
  }

  async function loadSeedCandidatesForSource(source, { quiet = false, background = false } = {}) {
    const sourceId = seedSourceId(source)
    if (!sourceId) return
    const hasCachedCandidates = Array.isArray(seedCandidatesBySource[sourceId])
    const showLoadingState = !background || !hasCachedCandidates
    if (showLoadingState) {
      seedCandidatesLoading = { ...seedCandidatesLoading, [sourceId]: true }
    }
    try {
      const payload = await fetchSeedCandidates(source.source_type, source.source_key)
      const nextCandidates = payload.candidates || []
      seedCandidatesBySource = {
        ...seedCandidatesBySource,
        [sourceId]: nextCandidates,
      }
      reconcileSeedSourceCandidates(sourceId, nextCandidates)
      if (nextCandidates.length === 0) {
        return
      }
      const allowed = new Set(nextCandidates.map((candidate) => String(candidate.candidate_key || '')).filter(Boolean))
      const selectable = new Set(
        nextCandidates
          .filter((candidate) => isSeedCandidateSelectable(candidate))
          .map((candidate) => String(candidate.candidate_key || ''))
          .filter(Boolean)
      )
      seedSelections = {
        ...seedSelections,
        [sourceId]: (seedSelections[sourceId] || []).filter((key) => allowed.has(key) && selectable.has(key)),
      }
      if (!quiet) {
        seedActionStatus = ''
      }
    } catch (error) {
      if (error?.status === 401) {
        authStatus = 'unauthenticated'
        setAuthToken('')
      } else if (!quiet) {
        seedActionStatus = error?.message || 'Failed to load seed candidates.'
      }
    } finally {
      if (showLoadingState) {
        seedCandidatesLoading = { ...seedCandidatesLoading, [sourceId]: false }
      }
    }
  }

  async function toggleSeedSource(source) {
    const sourceId = seedSourceId(source)
    if (!sourceId) return
    if (expandedSeedSourceId === sourceId) {
      expandedSeedSourceId = ''
      return
    }
    const hasCachedCandidates = Array.isArray(seedCandidatesBySource[sourceId])
    expandedSeedSourceId = sourceId
    void loadSeedCandidatesForSource(source, {
      quiet: hasCachedCandidates,
      background: hasCachedCandidates,
    })
  }

  async function focusSeedSource(sourceType, sourceKey) {
    const nextSourceId = `${sourceType}:${sourceKey}`
    if (!seedSources.some((source) => seedSourceId(source) === nextSourceId)) {
      await loadSeedSources({ quiet: true })
    }
    const source = seedSources.find((entry) => seedSourceId(entry) === nextSourceId)
    if (!source) return
    const hasCachedCandidates = Array.isArray(seedCandidatesBySource[nextSourceId])
    expandedSeedSourceId = nextSourceId
    seedLastRunKey = nextSourceId
    void loadSeedCandidatesForSource(source, {
      quiet: true,
      background: hasCachedCandidates,
    })
  }

  async function handlePromoteSeedSource(source) {
    const sourceId = seedSourceId(source)
    const candidateKeys = (seedSelections[sourceId] || []).filter(Boolean)
    if (candidateKeys.length === 0) {
      seedActionStatus = 'Select at least one seed candidate to promote.'
      return
    }
    seedActionBusy = true
    seedActionStatus = `Queueing ${candidateKeys.length} candidate(s) for background processing...`
    try {
      const payload = await promoteSeedCandidates(source.source_type, source.source_key, {
        candidateKeys,
        includeDownstream,
        includeUpstream,
        relatedDepthDownstream,
        relatedDepthUpstream,
        maxRelated,
        enqueueDownload: true,
        downloadBatchSize: Math.max(25, candidateKeys.length),
        workers: 6,
      })
      seedActionStatus = payload.message || 'Promotion queued.'
      clearSeedSelection(source)
      if (payload?.jobs?.length > 0) {
        await trackIngestQueuedJobs(payload, { withDownload: true })
      } else {
        resetIngestProgressTracking()
      }
      const refreshTasks = [
        loadIngestStats({ quiet: true }),
        loadCorpus({ preserveSelection: true, quiet: true }),
      ]
      if (diagnosticsEnabled) {
        refreshTasks.push(
          loadDownloads(),
          loadDownloadWorkerStatus({ quiet: true }),
        )
      }
      await Promise.all(refreshTasks)
      const refreshedSource = seedSources.find((entry) => seedSourceId(entry) === sourceId)
      if (refreshedSource) {
        await loadSeedCandidatesForSource(refreshedSource, { quiet: true })
      }
    } catch (error) {
      if (error?.status === 401) {
        authStatus = 'unauthenticated'
        setAuthToken('')
        return
      }
      seedActionStatus = error?.message || 'Failed to promote seed candidates.'
    } finally {
      seedActionBusy = false
    }
  }

  async function handlePromoteWholeSeedSource(source) {
    const candidateCount = Math.max(
      0,
      Number(source?.candidate_count || 0)
        - Number(source?.state_counts?.in_corpus || 0)
        - Number(source?.state_counts?.downloaded || 0)
    )
    if (candidateCount === 0) {
      seedActionStatus = 'No promotable seed candidates remain in this source.'
      return
    }
    seedActionBusy = true
    seedActionStatus = `Queueing all promotable candidates from this source for background processing...`
    try {
      const payload = await promoteSeedCandidates(source.source_type, source.source_key, {
        candidateKeys: [],
        includeDownstream,
        includeUpstream,
        relatedDepthDownstream,
        relatedDepthUpstream,
        maxRelated,
        enqueueDownload: true,
        downloadBatchSize: Math.max(25, candidateCount),
        workers: 6,
      })
      seedActionStatus = payload.message || 'Promotion queued.'
      clearSeedSelection(source)
      if (payload?.jobs?.length > 0) {
        await trackIngestQueuedJobs(payload, { withDownload: true })
      } else {
        resetIngestProgressTracking()
      }
      const refreshTasks = [
        loadIngestStats({ quiet: true }),
        loadCorpus({ preserveSelection: true, quiet: true }),
      ]
      if (diagnosticsEnabled) {
        refreshTasks.push(
          loadDownloads(),
          loadDownloadWorkerStatus({ quiet: true }),
        )
      }
      await Promise.all(refreshTasks)
      const sourceId = seedSourceId(source)
      const refreshedSource = seedSources.find((entry) => seedSourceId(entry) === sourceId)
      if (refreshedSource) {
        await loadSeedCandidatesForSource(refreshedSource, { quiet: true })
      }
    } catch (error) {
      if (error?.status === 401) {
        authStatus = 'unauthenticated'
        setAuthToken('')
        return
      }
      seedActionStatus = error?.message || 'Failed to promote seed candidates.'
    } finally {
      seedActionBusy = false
    }
  }

  async function handleDismissSelectedSeed(source) {
    const sourceId = seedSourceId(source)
    const candidateKeys = (seedSelections[sourceId] || []).filter(Boolean)
    if (candidateKeys.length === 0) {
      seedActionStatus = 'Select at least one seed candidate to dismiss.'
      return
    }
    seedActionBusy = true
    seedActionStatus = `Removing ${candidateKeys.length} candidate(s) from Seed...`
    try {
      await dismissSeedCandidatesApi(source.source_type, source.source_key, candidateKeys)
      clearSeedSelection(source)
      await Promise.all([
        loadIngestStats({ quiet: true }),
      ])
      const refreshedSource = seedSources.find((entry) => seedSourceId(entry) === sourceId)
      if (refreshedSource) {
        await loadSeedCandidatesForSource(refreshedSource, { quiet: true })
      }
      seedActionStatus = `Removed ${candidateKeys.length} candidate(s) from Seed.`
    } catch (error) {
      if (error?.status === 401) {
        authStatus = 'unauthenticated'
        setAuthToken('')
        return
      }
      seedActionStatus = error?.message || 'Failed to dismiss seed candidates.'
    } finally {
      seedActionBusy = false
    }
  }

  async function handleRemoveSeedSource(source) {
    if (!window.confirm(`Remove "${source.label}" from Seed?`)) return
    seedActionBusy = true
    seedActionStatus = `Removing ${source.label} from Seed...`
    try {
      await removeSeedSource(source.source_type, source.source_key)
      const sourceId = seedSourceId(source)
      seedSources = seedSources.filter((entry) => seedSourceId(entry) !== sourceId)
      seedSelections = Object.fromEntries(
        Object.entries(seedSelections).filter(([key]) => key !== sourceId)
      )
      if (expandedSeedSourceId === sourceId) {
        expandedSeedSourceId = ''
      }
      seedActionStatus = `Removed ${source.label} from Seed.`
    } catch (error) {
      if (error?.status === 401) {
        authStatus = 'unauthenticated'
        setAuthToken('')
        return
      }
      seedActionStatus = error?.message || 'Failed to remove seed source.'
    } finally {
      seedActionBusy = false
    }
  }

  async function runSearch() {
    searchStatus = 'Searching...'
    try {
      const query = searchMode === 'query' ? searchQuery : ''
      const seedJson = searchMode === 'seed' ? searchSeedJson : ''
      const { data, source, expansion, runId } = await runKeywordSearch({
        query,
        seedJson,
        field: searchField,
        author: searchAuthor,
        yearFrom,
        yearTo,
        includeDownstream: false,
        includeUpstream: false,
        relatedDepthDownstream: 0,
        relatedDepthUpstream: 0,
        maxRelated: 30,
      })
      searchResults = data
      searchSource = source
      initializeSearchQueueConfig(data)
      searchQueueStatus = ''
      const suffix = expansion?.added ? ` (+${expansion.added} related works)` : ''
      searchStatus = source === 'api'
        ? `Added ${data.length} candidate(s) to Seed.${suffix}`
        : 'Sample results loaded.'
      if (source === 'api' && runId) {
        await loadSeedSources({ quiet: true })
        await focusSeedSource('search', runId)
      }
    } catch (error) {
      if (error?.status === 401) {
        authStatus = 'unauthenticated'
        setAuthToken('')
      }
      searchStatus = error?.message || 'Search failed.'
    }
  }

  function searchResultKey(result, index = -1) {
    const openalexId = String(result?.openalex_id || '').trim().toLowerCase()
    if (openalexId) return `oa:${openalexId}`
    const doi = String(result?.doi || '').trim().toLowerCase()
    if (doi) return `doi:${doi}`
    const id = String(result?.id || '').trim().toLowerCase()
    if (id) return `id:${id}`
    return `idx:${index}:${String(result?.title || '').trim().toLowerCase()}`
  }

  function defaultSearchQueueConfig() {
    return {
      includeDownstream: Boolean(includeDownstream),
      includeUpstream: Boolean(includeUpstream),
      relatedDepthDownstream: Math.max(0, Number(relatedDepthDownstream) || 0),
      relatedDepthUpstream: Math.max(0, Number(relatedDepthUpstream) || 0),
      maxRelated: Math.max(1, Number(maxRelated) || 1),
    }
  }

  function initializeSearchQueueConfig(results = searchResults) {
    const next = {}
    for (const [index, result] of (results || []).entries()) {
      const key = searchResultKey(result, index)
      if (!key) continue
      next[key] = {
        ...defaultSearchQueueConfig(),
        ...(searchQueueConfigs[key] || {}),
      }
    }
    searchQueueConfigs = next
    searchSelection = searchSelection.filter((key) => Boolean(next[key]))
  }

  function getSearchQueueConfig(key) {
    return searchQueueConfigs[key] || defaultSearchQueueConfig()
  }

  function updateSearchQueueConfig(key, patch) {
    const current = getSearchQueueConfig(key)
    searchQueueConfigs = {
      ...searchQueueConfigs,
      [key]: { ...current, ...patch },
    }
  }

  function clearSearchSelection() {
    searchSelection = []
  }

  function selectAllSearchResults() {
    searchSelection = (searchResults || []).map((result, index) => searchResultKey(result, index)).filter(Boolean)
  }

  function toggleSearchSelection(result, index) {
    const key = searchResultKey(result, index);
    if (!key) return;
    if (searchSelection.includes(key)) {
      searchSelection = searchSelection.filter(k => k !== key);
    } else {
      searchSelection = [...searchSelection, key];
    }
  }

  function buildSeedItemFromSearchResult(result) {
    const openalexId = String(result?.openalex_id || '').trim()
    if (openalexId) return { openalex_id: openalexId }

    const id = String(result?.id || '').trim()
    if (/^https?:\/\/openalex\.org\/w\d+$/i.test(id) || /^w\d+$/i.test(id)) {
      return { openalex_id: id }
    }

    const doi = String(result?.doi || '').trim()
    if (doi) return { doi }

    return { title: String(result?.title || id || '').trim() }
  }

  async function enqueueSelectedSearchResults(startPipeline = false) {
    searchQueueStatus = ''
    if (searchQueueBusy) return

    if (searchSource !== 'api') {
      searchQueueStatus = 'Only API search results can be enqueued. Run the search against the live API first.'
      return
    }

    const selectedRows = (searchResults || [])
      .map((result, index) => ({ result, index, key: searchResultKey(result, index) }))
      .filter(({ key }) => key && searchSelection.includes(key))

    if (selectedRows.length === 0) {
      searchQueueStatus = 'Select at least one search result.'
      return
    }

    searchQueueBusy = true
    let queuedSeeds = 0
    let queuedWorks = 0
    const failures = []

    try {
      for (let i = 0; i < selectedRows.length; i += 1) {
        const { result, key } = selectedRows[i]
        const cfg = getSearchQueueConfig(key)
        const seedPayload = JSON.stringify([buildSeedItemFromSearchResult(result)])
        const title = String(result?.title || result?.id || key).slice(0, 80)
        searchQueueStatus = `Queueing ${i + 1}/${selectedRows.length}: ${title}`
        try {
          const payload = await runKeywordSearch({
            query: '',
            seedJson: seedPayload,
            field: 'default',
            author: '',
            includeDownstream: Boolean(cfg.includeDownstream),
            includeUpstream: Boolean(cfg.includeUpstream),
            relatedDepthDownstream: Math.max(0, Number(cfg.relatedDepthDownstream) || 0),
            relatedDepthUpstream: Math.max(0, Number(cfg.relatedDepthUpstream) || 0),
            maxRelated: Math.max(1, Number(cfg.maxRelated) || 1),
            enqueue: true,
            fallbackToSample: false,
          })
          if (payload?.source !== 'api') {
            throw new Error('backend did not return live API results')
          }
          queuedSeeds += 1
          queuedWorks += Array.isArray(payload?.data) ? payload.data.length : 0
        } catch (error) {
          failures.push(`${title}: ${error?.message || 'enqueue failed'}`)
        }
      }

      await Promise.all([
        loadDownloads(),
        loadIngestStats({ quiet: true }),
        loadCorpus({ preserveSelection: true, quiet: true }),
      ])

      if (startPipeline && queuedSeeds > 0 && !pipelineWorker.running) {
        await handleStartPipelineWorker()
      }

      if (queuedSeeds === 0) {
        searchQueueStatus = failures.length > 0 ? `No items queued. ${failures[0]}` : 'No items queued.'
      } else {
        const failSuffix =
          failures.length > 0
            ? ` ${failures.length} item(s) failed${failures[0] ? ` (first: ${failures[0]})` : ''}.`
            : ''
        const pipelineSuffix = startPipeline && queuedSeeds > 0 ? ' Downloader started.' : ''
        searchQueueStatus = `Queued ${queuedSeeds} seed item(s); ${queuedWorks} work(s) sent to the download queue.${failSuffix}${pipelineSuffix}`
      }
    } finally {
      searchQueueBusy = false
    }
  }

  function resetSearchForm() {
    searchMode = 'query'
    searchQuery = ''
    searchSeedJson = ''
    searchField = 'default'
    searchAuthor = ''
    yearFrom = ''
    yearTo = ''
    searchStatus = ''
    searchSource = ''
    searchSelection = []
    searchQueueConfigs = {}
    searchQueueStatus = ''
  }

  async function loadCorpus({ append = false, preserveSelection = false, quiet = false } = {}) {
    if (!append && corpusLoading) return
    if (!append && quiet && corpusLoadingMore) return

    const requestSeq = ++corpusLoadRequestSeq
    const requestCorpusId = Number.isFinite(Number(currentCorpusId)) ? Number(currentCorpusId) : null
    const scrollSnapshot = !append && preserveSelection ? snapshotCorpusScroll() : null
    if (append) {
      if (corpusLoading || corpusLoadingMore || !corpusHasMore) return
      corpusLoadingMore = true
      if (!quiet) corpusLoadStatus = 'Loading more corpus entries...'
    } else {
      corpusLoading = true
      if (!quiet) {
        corpusLoadStatus = corpusItems.length > 0 ? 'Refreshing corpus entries...' : 'Loading corpus entries...'
      }
    }
    try {
      const offset = append ? corpusItems.length : 0
      const requestedLimit =
        append
          ? CORPUS_PAGE_SIZE
          : quiet && preserveSelection
            ? Math.max(CORPUS_PAGE_SIZE, corpusItems.length || 0)
            : CORPUS_PAGE_SIZE
      const { data, total, source, stageTotals } = await fetchCorpus({ limit: requestedLimit, offset })
      const incoming = (Array.isArray(data) ? data : []).map((item) => normalizeCorpusItem(item))
      const currentCorpusNumeric = Number.isFinite(Number(currentCorpusId)) ? Number(currentCorpusId) : null
      if (requestSeq !== corpusLoadRequestSeq || requestCorpusId !== currentCorpusNumeric) {
        return
      }
      corpusItems = append ? [...corpusItems, ...incoming] : incoming
      corpusTotal = Number.isFinite(Number(total)) ? Number(total) : corpusItems.length
      corpusHasMore = corpusItems.length < corpusTotal
      corpusSource = source
      if (stageTotals && typeof stageTotals === 'object') {
        corpusStageTotals = {
          raw: Number(stageTotals.raw || 0),
          metadata: Number(stageTotals.metadata || 0),
          downloaded: Number(stageTotals.downloaded || 0),
          failed_enrichment: Number(stageTotals.failed_enrichment || 0),
          failed_download: Number(stageTotals.failed_download || 0),
        }
      }
      if (corpusSource === 'sample') {
        if (!quiet) corpusLoadStatus = 'Showing sample corpus data.'
      } else {
        if (!quiet) {
          corpusLoadStatus = corpusHasMore
            ? `Loaded ${corpusItems.length} of ${corpusTotal} entries. Scroll to load more.`
            : `Loaded ${corpusItems.length} entries.`
        }
      }
      if (scrollSnapshot) {
        await tick()
        restoreCorpusScroll(scrollSnapshot)
      }
    } catch (error) {
      if (error?.status === 401) {
        authStatus = 'unauthenticated'
        setAuthToken('')
        return
      }
      if (!quiet) {
        corpusLoadStatus = error?.message || 'Failed to load corpus.'
      }
    } finally {
      if (requestSeq === corpusLoadRequestSeq) {
        corpusLoading = false
        corpusLoadingMore = false
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

  async function loadDownloadWorkerStatus({ quiet = false } = {}) {
    if (!quiet) downloadWorkerStatus = 'Loading worker status...'
    try {
      const payload = await fetchDownloadWorkerStatus()
      downloadWorker = payload
      if (!quiet) downloadWorkerStatus = ''
    } catch (error) {
      if (error?.status === 401) {
        authStatus = 'unauthenticated'
        setAuthToken('')
        return
      }
      if (!quiet) downloadWorkerStatus = error?.message || 'Failed to load worker status.'
    }
  }

  async function loadPipelineWorkerStatus({ quiet = false, global = false } = {}) {
    if (!quiet) pipelineWorkerStatus = 'Loading worker status...'
    try {
      const payload = await fetchPipelineDaemonStatus({ global })
      if (global) {
        globalWorkerBacklog = payload?.backlog || globalWorkerBacklog
        return
      }
      daemonStatus = payload?.daemon || daemonStatus
      workerBacklog = payload?.backlog || workerBacklog
      const enrich = daemonStatus?.workers?.enrich || {}
      const download = daemonStatus?.workers?.download || {}
      pipelineWorker = {
        ...pipelineWorker,
        running: Boolean(enrich.online),
        in_flight: String(enrich.status || '').toLowerCase() === 'running',
        started_at: enrich.last_seen_at || null,
        last_tick_at: enrich.last_seen_at || null,
        last_result: enrich.details || null,
        last_error: enrich.status === 'error' ? JSON.stringify(enrich.details || {}) : null,
      }
      downloadWorker = {
        ...downloadWorker,
        running: Boolean(download.online),
        in_flight: String(download.status || '').toLowerCase() === 'running',
        started_at: download.last_seen_at || null,
        last_tick_at: download.last_seen_at || null,
        last_result: download.details || null,
        last_error: download.status === 'error' ? JSON.stringify(download.details || {}) : null,
        queued_download_items: Number(workerBacklog?.queued_download || 0),
        running_jobs: Number(workerBacklog?.downloading || 0),
      }
      if (!quiet) pipelineWorkerStatus = ''
    } catch (error) {
      if (error?.status === 401) {
        authStatus = 'unauthenticated'
        setAuthToken('')
        return
      }
      if (!quiet) pipelineWorkerStatus = error?.message || 'Failed to load worker status.'
    }
  }

  async function handleStartPipelineWorker() {
    if (pipelineWorkerBusy) return
    pipelineWorkerBusy = true
    pipelineWorkerStatus = 'Starting global pipeline...'
    try {
      await startPipelineWorker({
        intervalSeconds: Number(pipelineWorker?.config?.intervalSeconds) || 15,
        promoteBatchSize: Number(pipelineWorker?.config?.promoteBatchSize) || 25,
        promoteWorkers: Number(pipelineWorker?.config?.promoteWorkers) || 6,
        downloadBatchSize: Number(pipelineWorker?.config?.downloadBatchSize) || 5,
        downloadWorkers: Number(pipelineWorker?.config?.downloadWorkers) || 0,
      })
      await Promise.all([
        loadPipelineWorkerStatus(),
        loadIngestStats(),
        loadCorpus(),
        loadDownloads(),
        loadDownloadWorkerStatus(),
      ])
      pipelineWorkerStatus = 'Global pipeline running.'
    } catch (error) {
      if (error?.status === 401) {
        authStatus = 'unauthenticated'
        setAuthToken('')
        return
      }
      pipelineWorkerStatus = error?.message || 'Failed to start global pipeline.'
    } finally {
      pipelineWorkerBusy = false
    }
  }

  async function handlePausePipelineWorker() {
    if (pipelineWorkerBusy) return
    pipelineWorkerBusy = true
    pipelineWorkerStatus = 'Pausing global pipeline...'
    try {
      await pausePipelineWorker({ force: false })
      await loadPipelineWorkerStatus()
      pipelineWorkerStatus = 'Global pipeline paused.'
    } catch (error) {
      if (error?.status === 401) {
        authStatus = 'unauthenticated'
        setAuthToken('')
        return
      }
      pipelineWorkerStatus = error?.message || 'Failed to pause global pipeline.'
    } finally {
      pipelineWorkerBusy = false
    }
  }

  async function handlePauseAllPipelineWorkers() {
    if (pipelineWorkerBusy) return
    pipelineWorkerBusy = true
    pipelineWorkerStatus = 'Pausing all pipeline workers...'
    try {
      await pauseAllPipelineWorkers({ force: true })
      await Promise.all([loadPipelineWorkerStatus(), loadDownloadWorkerStatus(), loadIngestStats(), loadCorpus(), loadDownloads(), ...(diagnosticsEnabled ? [loadIngestStats({ global: true }), loadPipelineDaemonStatus({ global: true })] : [])])
      pipelineWorkerStatus = 'Global pause complete.'
    } catch (error) {
      if (error?.status === 401) {
        authStatus = 'unauthenticated'
        setAuthToken('')
        return
      }
      pipelineWorkerStatus = error?.message || 'Failed to pause all workers.'
    } finally {
      pipelineWorkerBusy = false
    }
  }

  async function handleStartDownloadWorker() {
    if (downloadWorkerBusy) return
    downloadWorkerBusy = true
    downloadWorkerStatus = 'Starting worker...'
    try {
      const result = await startDownloadWorker()
      await loadDownloadWorkerStatus()
      if (Number(result?.queued_jobs || 0) === 0 && Number(result?.queued_download_items || 0) === 0) {
        downloadWorkerStatus = 'No queued download items for this corpus. Enrich/promote items first.'
      } else {
        downloadWorkerStatus = 'Worker started (auto throughput mode).'
      }
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
      await runDownloadWorkerOnce()
      await loadDownloadWorkerStatus()
      await loadDownloads()
      downloadWorkerStatus = 'Batch completed (auto throughput mode).'
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

  async function handleExportDownload(format, label) {
    if (exportBusy) return
    exportBusy = true
    exportStatus = `${label} export is being generated...`
    const exportFilters = {
      status: exportFilterStatus === 'all' ? '' : exportFilterStatus,
      yearFrom: exportFilterYearFrom ? Number(exportFilterYearFrom) : '',
      yearTo: exportFilterYearTo ? Number(exportFilterYearTo) : '',
      source: exportFilterSource.trim(),
    }
    try {
      const { blob, filename } = await downloadCorpusExport(format, exportFilters)
      const url = URL.createObjectURL(blob)
      const link = document.createElement('a')
      link.href = url
      link.download = filename
      document.body.appendChild(link)
      link.click()
      link.remove()
      URL.revokeObjectURL(url)
      exportStatus = `${label} export downloaded.`
    } catch (error) {
      if (error?.status === 401) {
        authStatus = 'unauthenticated'
        setAuthToken('')
        exportStatus = 'Session expired. Please sign in again.'
        return
      }
      exportStatus = error?.message || 'Failed to export corpus data.'
    } finally {
      exportBusy = false
    }
  }

  async function handleDownloadedCorpusFile(item) {
    const itemId = Number(item?.id);
    if (!Number.isFinite(itemId) || itemId <= 0) return;
    try {
      const { blob, filename } = await downloadCorpusItemFile(itemId)
      const url = URL.createObjectURL(blob)
      const link = document.createElement('a')
      link.href = url
      link.download = filename
      document.body.appendChild(link)
      link.click()
      link.remove()
      URL.revokeObjectURL(url)
    } catch (error) {
      if (error?.status === 401) {
        authStatus = 'unauthenticated'
        setAuthToken('')
        return
      }
      corpusLoadStatus = error?.message || 'Failed to download file.'
    }
  }
  function applyGraphData(data, source, statusText = '') {
    graphNodes = data.nodes || []
    graphEdges = data.edges || []
    graphStats = data.stats || null
    graphSource = source
    graphStatus = statusText || (source === 'api' ? 'Loaded from API.' : 'Sample graph loaded.')
    const degreeMap = buildDegreeMap(graphNodes, graphEdges)
    graphDegreeMap = degreeMap
    const { layoutEdges } = buildLayoutEdges(graphNodes, graphEdges, degreeMap)
    graphLayout = buildGraphLayout(graphNodes, graphEdges, layoutEdges, degreeMap, {
      focusConnected: graphFocusConnected,
    })
    graphNodeMap = new Map(graphLayout.map((node) => [node.id, node]))
    graphVisibleEdges = graphEdges.filter(
      (edge) => graphNodeMap.has(edge.source) && graphNodeMap.has(edge.target)
    )
    hoveredNodeId = null
    graphCanvasSize = graphLayout[0]?.canvasSize || 440
    graphViewBox = { x: 0, y: 0, size: graphCanvasSize }
  }

  async function loadGraph() {
    graphStatus = 'Loading graph...'
    try {
      const baseParams = {
        maxNodes: graphMaxNodes,
        relationship: graphRelationship,
        status: graphStatusFilter,
        yearFrom: graphYearFrom ? Number(graphYearFrom) : null,
        yearTo: graphYearTo ? Number(graphYearTo) : null,
      }
      const primary = await fetchGraph({
        ...baseParams,
        hideIsolates: graphHideIsolates,
      })
      const primaryData = primary.data || {}
      if (graphHideIsolates && (primaryData.nodes || []).length === 0) {
        const retry = await fetchGraph({ ...baseParams, hideIsolates: false })
        const retryData = retry.data || {}
        if ((retryData.nodes || []).length > 0) {
          graphHideIsolates = false
          applyGraphData(
            retryData,
            retry.source,
            'No connected citation edges for this corpus; showing isolated works.'
          )
          return
        }
      }
      applyGraphData(primaryData, primary.source)
    } catch (error) {
      if (error?.status === 401) {
        authStatus = 'unauthenticated'
        setAuthToken('')
      }
      graphStatus = 'Failed to load graph.'
    }
  }

  async function hydrateLogsFromTail({ force = false } = {}) {
    if (!force && logs.length > 0) return
    logsTailStatus = 'Loading recent logs...'
    try {
      const payload = await fetchLogsTail({
        type: logsType,
        lines: maxLogs,
        includeRotated: true,
        cursor: 0,
        corpusId: getLogsCorpusId(),
      })
      const entries = Array.isArray(payload?.entries) ? payload.entries : []
      logs = entries.slice(-maxLogHistory)
      logsCursor = Number.isFinite(Number(payload?.next_cursor))
        ? Number(payload.next_cursor)
        : logs.length
      logsHasMore = Boolean(payload?.has_more)
      logsTailStatus = entries.length > 0 ? `Loaded ${entries.length} recent log lines.` : 'No persisted logs yet.'
    } catch (error) {
      if (error?.status === 401) {
        authStatus = 'unauthenticated'
        setAuthToken('')
        return
      }
      logsTailStatus = error?.message || 'Failed to load persisted logs.'
    }
  }

  async function loadOlderLogs() {
    if (logsLoadingOlder || !logsHasMore) return
    logsLoadingOlder = true
    logsTailStatus = 'Loading older logs...'
    try {
      const payload = await fetchLogsTail({
        type: logsType,
        lines: maxLogs,
        includeRotated: true,
        cursor: logsCursor,
        corpusId: getLogsCorpusId(),
      })
      const entries = Array.isArray(payload?.entries) ? payload.entries : []
      if (entries.length > 0) {
        logs = [...entries, ...logs].slice(-maxLogHistory)
      }
      logsCursor = Number.isFinite(Number(payload?.next_cursor))
        ? Number(payload.next_cursor)
        : logsCursor + entries.length
      logsHasMore = Boolean(payload?.has_more)
      logsTailStatus = entries.length > 0 ? `Loaded ${entries.length} older log lines.` : 'No older logs found.'
    } catch (error) {
      if (error?.status === 401) {
        authStatus = 'unauthenticated'
        setAuthToken('')
        return
      }
      logsTailStatus = error?.message || 'Failed to load older logs.'
    } finally {
      logsLoadingOlder = false
    }
  }

  function connectLogs() {
    disconnectLogs()
    logsStatus = 'connecting'
    hydrateLogsFromTail()
    try {
      const envBase = String(import.meta.env.VITE_API_BASE || '').trim()
      const devBackendBase =
        window.location.port === '5175'
          ? `${window.location.protocol}//${window.location.hostname}:4000`
          : window.location.origin
      const rawBase = envBase || devBackendBase
      let wsUrl = 'ws://localhost:4000/api/ws'
      try {
        const u = new URL(rawBase, window.location.origin)
        wsUrl = `${u.protocol === 'https:' ? 'wss' : 'ws'}://${u.host}/api/ws`
      } catch (error) {
        // fall back to localhost default
      }
      const ws = new WebSocket(wsUrl)
      logsSocket = ws
      const scheduleReconnect = () => {
        if (authStatus !== 'authenticated') return
        if (logsReconnectTimer) return
        logsStatus = 'reconnecting'
        logsReconnectTimer = setTimeout(() => {
          logsReconnectTimer = null
          connectLogs()
        }, 3000)
      }
      ws.onopen = () => {
        if (logsSocket !== ws) return
        logsStatus = 'connected'
      }
      ws.onmessage = (event) => {
        if (logsSocket !== ws) return
        const promotionEvent = parsePromotionEventMessage(event.data)
        if (promotionEvent) {
          queuePromotionAnimation(promotionEvent)
          return
        }
        const extractionEvent = parseExtractionEventMessage(event.data)
        if (extractionEvent) {
          const currentCorpus = Number.isFinite(Number(currentCorpusId)) ? Number(currentCorpusId) : null
          const corpusMatches = extractionEvent.corpusId === null || currentCorpus === null || extractionEvent.corpusId === currentCorpus
          const baseMatches = extractionProgress.baseName && extractionEvent.baseName === extractionProgress.baseName
          const fileMatches = extractionProgress.backendFilename && extractionEvent.filename === extractionProgress.backendFilename
          if (extractionProgress.active && corpusMatches && (baseMatches || fileMatches)) {
            updateExtractionProgress({
              phase: extractionEvent.status === 'success' ? 'finalizing' : 'completed',
              detail: extractionEvent.status === 'success' ? 'Finalizing extracted entries...' : 'Extraction failed.',
              percent: extractionEvent.status === 'success' ? 98 : 100,
              indeterminate: false,
              backendDone: true,
            })
            if (extractionEvent.status === 'success') {
              void finalizeExtractionAfterBackendCompletion('No bibliography entries found in this PDF.')
            } else {
              latestEntriesStatus = 'Extraction failed.'
              finishExtractionProgress()
            }
          }
          return
        }
        const line = event.data
        if (typeof line === 'string' && line.startsWith('{')) return
        if (typeof line === 'string' && line.startsWith('WebSocket connection established')) return
        if (!isLogLineForCurrentCorpus(line)) return
        updateExtractionProgressFromLog(line)
        if (logsType !== 'pipeline') return
        const hadLines = logs.length > 0
        logs = [...logs, line].slice(-maxLogHistory)
        if (logsCursor > 0) {
          logsCursor = Math.min(maxLogHistory, logsCursor + 1)
        } else if (!hadLines) {
          logsCursor = logs.length
        }
        if (shouldTriggerPipelineRefresh(line)) {
          const isWorkerTick =
            String(line).includes('[pipeline-worker') || String(line).includes('[download-worker')
          schedulePipelineRefresh(isWorkerTick ? 300 : 1500)
        }
      }
      ws.onclose = () => {
        if (logsSocket === ws) {
          logsSocket = null
          scheduleReconnect()
        }
      }
      ws.onerror = () => {
        if (logsSocket === ws) {
          scheduleReconnect()
        }
      }
    } catch (error) {
      logsStatus = 'unavailable'
    }
  }

  function handleLogsCorpusFilterChange() {
    resetLogState()
    hydrateLogsFromTail({ force: true })
  }

  function disconnectLogs() {
    if (logsReconnectTimer) {
      clearTimeout(logsReconnectTimer)
      logsReconnectTimer = null
    }
    if (logsSocket) {
      try {
        logsSocket.close()
      } catch (error) {
        // ignore close errors
      }
      logsSocket = null
    }
    logsStatus = 'disconnected'
  }

  async function handleLogTypeChange(event) {
    const next = String(event?.target?.value || 'pipeline').toLowerCase()
    logsType = next === 'app' ? 'app' : 'pipeline'
    resetLogState()
    await hydrateLogsFromTail({ force: true })
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

  const graphColorPalette = [
    '#0f8b8d',
    '#bc6c25',
    '#5c7cfa',
    '#7f5539',
    '#3a86ff',
    '#2a9d8f',
    '#8f2d56',
    '#5465ff',
    '#607744',
    '#b56576',
    '#1d3557',
    '#ca6702',
  ]

  function labelFromStatus(status) {
    if (Array.isArray(status)) {
      if (status.length === 0) return 'Unknown'
      return status.map((part) => pipelineStatusLabel(part)).join(' + ')
    }
    return pipelineStatusLabel(status)
  }

  function yearBucket(year) {
    const numeric = Number(year)
    if (!Number.isFinite(numeric)) return 'Unknown year'
    const bucket = Math.floor(numeric / 10) * 10
    return `${bucket}s`
  }

  function buildComponentMap(nodes, edges) {
    const nodeIds = new Set(nodes.map((node) => node.id))
    const adjacency = new Map()
    for (const node of nodes) adjacency.set(node.id, [])
    for (const edge of edges) {
      if (!nodeIds.has(edge.source) || !nodeIds.has(edge.target)) continue
      adjacency.get(edge.source).push(edge.target)
      adjacency.get(edge.target).push(edge.source)
    }

    const componentByNode = new Map()
    const visited = new Set()
    let componentIndex = 0
    for (const node of nodes) {
      if (visited.has(node.id)) continue
      componentIndex += 1
      const stack = [node.id]
      visited.add(node.id)
      while (stack.length) {
        const current = stack.pop()
        componentByNode.set(current, `Cluster ${componentIndex}`)
        for (const next of adjacency.get(current) || []) {
          if (visited.has(next)) continue
          visited.add(next)
          stack.push(next)
        }
      }
    }
    return componentByNode
  }

  function graphClusterLabel(node, mode, componentMap) {
    if (mode === 'component') return componentMap.get(node.id) || 'Cluster ?'
    if (mode === 'status') return labelFromStatus(node.status)
    if (mode === 'year') return yearBucket(node.year)
    if (mode === 'type') return node.type || 'Unknown type'
    if (mode === 'source_path') return node.source_path || node.ingest_source || 'Unknown path'
    return 'Default'
  }

  function buildNodeColorState(nodes, mode, componentMap) {
    const counts = new Map()
    for (const node of nodes) {
      const key = graphClusterLabel(node, mode, componentMap)
      counts.set(key, (counts.get(key) || 0) + 1)
    }
    const keys = [...counts.keys()].sort((a, b) => {
      const aUnknown = String(a).toLowerCase() === 'unknown'
      const bUnknown = String(b).toLowerCase() === 'unknown'
      if (aUnknown !== bUnknown) return aUnknown ? 1 : -1
      const byCount = (counts.get(b) || 0) - (counts.get(a) || 0)
      if (byCount !== 0) return byCount
      return String(a).localeCompare(String(b))
    })
    const colorByKey = new Map()
    keys.forEach((key, index) => {
      colorByKey.set(key, graphColorPalette[index % graphColorPalette.length])
    })
    const colorByNode = new Map()
    for (const node of nodes) {
      const key = graphClusterLabel(node, mode, componentMap)
      colorByNode.set(node.id, colorByKey.get(key) || '#0f8b8d')
    }
    const legend = keys.map((key) => ({
      key,
      color: colorByKey.get(key),
      count: counts.get(key) || 0,
    }))
    return { colorByNode, legend }
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
  $: graphVisibleEdges = graphEdges.filter(
    (edge) => graphNodeMap.has(edge.source) && graphNodeMap.has(edge.target)
  )
  $: graphTotalNodeCount = graphStats?.node_count ?? graphNodes.length
  $: graphTotalEdgeCount = graphStats?.edge_count ?? graphEdges.length
  $: graphNodeCount = graphLayout.length
  $: graphEdgeCount = graphVisibleEdges.length
  $: graphRelationshipCounts = {
    references: graphVisibleEdges.filter((edge) => edge.relationship_type === 'references').length,
    cited_by: graphVisibleEdges.filter((edge) => edge.relationship_type === 'cited_by').length,
  }
  $: graphList = [...graphLayout].sort(
    (a, b) => (graphDegreeMap.get(b.id) || 0) - (graphDegreeMap.get(a.id) || 0)
  )
  $: graphComponentMap = buildComponentMap(graphLayout, graphVisibleEdges)
  $: {
    const { colorByNode, legend } = buildNodeColorState(graphLayout, graphColorMode, graphComponentMap)
    graphNodeColorMap = colorByNode
    graphColorLegend = legend.slice(0, 8)
  }
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

  $: tabIds = new Set(tabGroups.flatMap(group => group.tabs).map(tab => tab.id))

  function normalizeTab(tabId) {
    if (tabId === 'seed' || tabId === 'corpus') return 'workspace'
    return tabId
  }

  function readTabFromHash() {
    if (typeof window === 'undefined') return null
    const raw = window.location.hash || ''
    const cleaned = raw.replace(/^#\/?/, '').trim()
    const normalized = normalizeTab(cleaned)
    return tabIds.has(normalized) ? normalized : null
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
    const normalized = normalizeTab(tabId)
    const safeTab = tabIds.has(normalized) ? normalized : 'workspace'
    activeTab = safeTab
    updateHash(safeTab, replace)
    if (safeTab === 'workspace' && pendingPromotionEvents.length > 0 && !promotionFlushTimer) {
      promotionFlushTimer = window.setTimeout(() => {
        promotionFlushTimer = null
        flushPromotionAnimations()
      }, 80)
    }
    refreshForActiveTab(safeTab)
  }

  onMount(() => {
    const initialAuthFlow = parseAuthFlowFromHash()
    applyAuthFlowFromHash()
    if (initialAuthFlow.flow === 'login') {
      const initialTab = readTabFromHash() || activeTab
      setActiveTab(initialTab, { replace: true })
    }
    reducedMotionMediaQuery = window.matchMedia('(prefers-reduced-motion: reduce)')
    prefersReducedMotion = Boolean(reducedMotionMediaQuery?.matches)
    const onReducedMotionChange = (event) => {
      prefersReducedMotion = Boolean(event?.matches)
    }
    reducedMotionMediaQuery?.addEventListener?.('change', onReducedMotionChange)

    const onHashChange = () => {
      const authHash = parseAuthFlowFromHash()
      if (authHash.flow !== 'login') {
        applyAuthFlowFromHash()
        return
      }
      if (authStatus !== 'authenticated') {
        applyAuthFlowFromHash()
        return
      }
      const tab = readTabFromHash()
      if (tab && (tab !== activeTab || window.location.hash !== `#/${tab}`)) {
        setActiveTab(tab, { replace: true })
      }
    }
    window.addEventListener('hashchange', onHashChange)

    const onFocusOrVisible = () => {
      if (document.visibilityState === 'hidden') return
      refreshForActiveTab(activeTab)
    }
    window.addEventListener('focus', onFocusOrVisible)
    document.addEventListener('visibilitychange', onFocusOrVisible)

    const boot = async () => {
      const flow = parseAuthFlowFromHash()
      await bootstrapAuth({ preserveAuthFlow: flow.flow !== 'login' })
    }
    boot()

    liveRefreshIntervalId = setInterval(() => {
      runLiveRefreshCycle()
    }, 3000)

    return () => {
      window.removeEventListener('hashchange', onHashChange)
      window.removeEventListener('focus', onFocusOrVisible)
      document.removeEventListener('visibilitychange', onFocusOrVisible)
      reducedMotionMediaQuery?.removeEventListener?.('change', onReducedMotionChange)
      reducedMotionMediaQuery = null
      disconnectLogs()
      if (pipelineRefreshTimer) {
        clearTimeout(pipelineRefreshTimer)
        pipelineRefreshTimer = null
      }
      if (liveRefreshIntervalId) {
        clearInterval(liveRefreshIntervalId)
        liveRefreshIntervalId = null
      }
      stopIngestProgressPolling()
      if (promotionFlushTimer) {
        clearTimeout(promotionFlushTimer)
        promotionFlushTimer = null
      }
    }
  })
</script>

{#if authStatus !== 'authenticated' || authFlow !== 'login'}
  <div class="login-shell">
    <div class="login-card">
      <p class="eyebrow">Korpus Builder</p>
      {#if authFlow === 'accept-invite'}
        <h1>Set your password</h1>
        <p class="subtitle">Your account has been created. Set a password to activate it.</p>
        <form class="login-form" on:submit={handleAcceptInvite}>
          <label>
            New password
            <input type="password" bind:value={authPassword} autocomplete="new-password" required minlength="8" />
          </label>
          <label>
            Confirm password
            <input type="password" bind:value={authPasswordConfirm} autocomplete="new-password" required minlength="8" />
          </label>
          {#if authInfo}
            <p class="muted">{authInfo}</p>
          {/if}
          {#if authError}
            <p class="error">{authError}</p>
          {/if}
          <button type="submit">Activate account</button>
        </form>
      {:else if authFlow === 'reset-password'}
        <h1>Reset password</h1>
        <p class="subtitle">Choose a new password for your account.</p>
        <form class="login-form" on:submit={handleResetPassword}>
          <label>
            New password
            <input type="password" bind:value={authPassword} autocomplete="new-password" required minlength="8" />
          </label>
          <label>
            Confirm password
            <input type="password" bind:value={authPasswordConfirm} autocomplete="new-password" required minlength="8" />
          </label>
          {#if authInfo}
            <p class="muted">{authInfo}</p>
          {/if}
          {#if authError}
            <p class="error">{authError}</p>
          {/if}
          <button type="submit">Save password</button>
        </form>
      {:else if authFlow === 'forgot-password'}
        <h1>Forgot password</h1>
        <p class="subtitle">Enter your email address and we will send you a reset link.</p>
        <form class="login-form" on:submit={handleForgotPassword}>
          <label>
            Email
            <input type="email" bind:value={authEmail} autocomplete="email" required />
          </label>
          {#if authInfo}
            <p class="muted">{authInfo}</p>
          {/if}
          {#if authError}
            <p class="error">{authError}</p>
          {/if}
          <div class="auth-actions">
            <button type="submit">Send reset link</button>
            <a href="#/login" class="text-link">Back to sign in</a>
          </div>
        </form>
      {:else}
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
          {#if authInfo}
            <p class="muted">{authInfo}</p>
          {/if}
          {#if authError}
            <p class="error">{authError}</p>
          {/if}
          <div class="auth-actions">
            <button type="submit">Sign in</button>
            <a href="#/forgot-password" class="text-link">Forgot password?</a>
          </div>
        </form>
      {/if}
    </div>
  </div>
{:else}
  <div class="app-shell" class:workspace-mode={activeTab === 'workspace'}>
    <header class="app-header">
      <div class="header-main">
        <p class="eyebrow">Korpus Builder</p>
        <h1>Corpus orchestration workspace</h1>
        <p class="subtitle">
          Build, enrich, and monitor literature pipelines end-to-end. API status: {apiStatus}.
        </p>
      </div>
      <div class="header-panel">
        <div class="header-user">
          <span class="eyebrow">Signed in</span>
          <strong>{authUser?.username}</strong>
          <button class="secondary logout-button" type="button" on:click={handleLogout}>
            Log out
          </button>
        </div>
        <div class="header-corpus">
          <label>
            <span class="header-corpus-label">Corpus</span>
            <select on:change={handleSelectCorpus} bind:value={currentCorpusId}>
              {#each corpora as corpus}
                <option value={corpus.id}>{corpusOptionLabel(corpus)}</option>
              {/each}
            </select>
          </label>
          <label>
            <span class="header-corpus-label">Kantropos target</span>
            <select
              bind:value={selectedKantroposTargetId}
              on:change={handleKantroposTargetChange}
              disabled={kantroposAssignmentLoading || kantroposAssignmentSaving || !currentCorpusId || !canConfigureCurrentCorpusKantropos}
              title={canConfigureCurrentCorpusKantropos ? 'Assign this local corpus to a predefined Kantropos corpus' : 'Only owners or editors can change the Kantropos assignment'}
            >
              <option value="">No assignment</option>
              {#each kantroposTargets as target}
                <option value={target.id}>{target.name}</option>
              {/each}
            </select>
          </label>
          <div class="header-corpus-actions">
            <button class="secondary" type="button" on:click={handleCreateCorpus} disabled={creatingCorpus || deletingCorpus}>
              {creatingCorpus ? 'Creating...' : 'New corpus'}
            </button>
            <button
              class="danger"
              type="button"
              on:click={handleDeleteCorpus}
              disabled={deletingCorpus || creatingCorpus || !currentCorpusId || !canDeleteCurrentCorpus}
              title={canDeleteCurrentCorpus ? 'Delete selected corpus' : 'Only owners can delete corpora'}
            >
              {deletingCorpus ? 'Deleting...' : 'Delete corpus'}
            </button>
          </div>
          {#if corpusActionStatus}
            <p class={`${corpusActionError ? 'error' : 'muted'} header-corpus-status`}>{corpusActionStatus}</p>
          {/if}
          {#if kantroposAssignmentStatus}
            <p class={`${kantroposAssignmentError ? 'error' : 'muted'} header-corpus-status`}>{kantroposAssignmentStatus}</p>
          {:else if currentCorpusId && kantroposAssignment?.target_name}
            <p class="muted header-corpus-status">Kantropos: {kantroposAssignment.target_name}</p>
          {/if}
        </div>
        {#if isAdmin}
          <form class="header-invite" on:submit|preventDefault={handleCreateInvitation}>
            <label>
              <span class="header-corpus-label">Invite user</span>
              <input type="text" placeholder="username" bind:value={inviteUsername} required />
            </label>
            <label>
              <span class="header-corpus-label">Email</span>
              <input type="email" placeholder="user@example.com" bind:value={inviteEmail} required />
            </label>
            <button class="secondary" type="submit">Send invite</button>
            {#if inviteStatus}
              <p class={`${inviteError ? 'error' : 'muted'} header-corpus-status`}>{inviteStatus}</p>
            {/if}
          </form>
        {/if}
      </div>
    </header>

  <div class="layout" class:no-side-nav={!showSideNav}>
    {#if showSideNav}
      <aside class="side-nav" data-testid="side-nav">
        {#each tabGroups as group}
          <div class="nav-group">
            {#if group.title}
              <span class="nav-group-title">{group.title}</span>
            {/if}
            {#each group.tabs as tab}
              <button
                class:active={activeTab === tab.id}
                on:click={() => setActiveTab(tab.id)}
                type="button"
                data-testid={`tab-${tab.id}`}
              >
                {tab.label}
              </button>
            {/each}
          </div>
        {/each}
      </aside>
    {/if}

    <section class="content">
      {#if activeTab === 'workspace'}
        <div class="seed-corpus-workspace">
        <div class="card seed-corpus-toolbar">
          <div class="seed-corpus-toolbar__header">
            <div class="seed-corpus-toolbar__intro">
              <h2 class="workspace-section-title">1. Search</h2>
              <p>Collect seed sources, review candidates, and promote selected works into the corpus pipeline.</p>
            </div>

            <div class="seed-corpus-toolbar__meta">
              {#if seedActionStatus}
                <p class="muted">{seedActionStatus}</p>
              {/if}
              {#if ingestTrackedRequest.requestId || ingestJobProgress.total > 0 || ingestJobProgress.error}
                <div class="ingest-job-progress seed-toolbar-progress">
                  <div class="ingest-job-progress__top">
                    <span class="muted small">
                      {#if ingestTrackedRequest.requestId}
                        Request: {ingestTrackedRequest.requestId}
                      {:else}
                        Latest queued request
                      {/if}
                    </span>
                    <span class={`tag ${ingestProgressDone ? 'completed' : ingestJobProgress.running > 0 ? 'queued' : 'pending'}`}>
                      {#if ingestProgressDone}
                        Finished
                      {:else if ingestJobProgress.running > 0}
                        Running
                      {:else}
                        Queued
                      {/if}
                    </span>
                  </div>
                  <div
                    class="extract-progress-track"
                    role="progressbar"
                    aria-label="Promotion queued jobs progress"
                    aria-valuemin="0"
                    aria-valuemax="100"
                    aria-valuenow={Math.round(ingestProgressRatio * 100)}
                  >
                    <div class="extract-progress-bar" style={`width: ${Math.max(2, Math.min(100, ingestProgressRatio * 100))}%`}></div>
                  </div>
                  <div class="ingest-job-progress__stats">
                    <span>Pending: {ingestJobProgress.pending}</span>
                    <span>Running: {ingestJobProgress.running}</span>
                    <span>Completed: {ingestJobProgress.completed}</span>
                    <span>Failed: {ingestJobProgress.failed}</span>
                    <span>Cancelled: {ingestJobProgress.cancelled}</span>
                  </div>
                  <div class="ingest-job-progress__meta">
                    <span class="muted small">
                      Daemon:
                      {#if ingestJobProgress.daemonOnline === true}
                        Online
                      {:else if ingestJobProgress.daemonOnline === false}
                        Offline
                      {:else}
                        Unknown
                      {/if}
                    </span>
                    {#if ingestJobProgress.updatedAt}
                      <span class="muted small">Updated: {ingestJobProgress.updatedAt}</span>
                    {/if}
                  </div>
                  {#if ingestJobProgress.error}
                    <p class="error small">{ingestJobProgress.error}</p>
                  {/if}
                </div>
              {/if}
            </div>
          </div>

          <div class="seed-intake-grid">
            <div class="seed-intake-card seed-intake-card--upload">
              <div class="split">
                <h3>Document Search</h3>
                {#if extractionProgress.active}
                  <span class="tag queued">extracting</span>
                {/if}
              </div>
              <div class="uploader">
                <label class="upload-drop">
                  <input type="file" multiple accept=".pdf" on:change={handleFiles} />
                  <span>Drop PDFs or click to upload.</span>
                </label>
              </div>
              {#if uploads.length > 0}
                <div class="upload-list">
                  {#each uploads as item}
                    <div class="upload-item">
                      <div>
                        <strong>{item.name}</strong>
                        <span>{formatBytes(item.size)}</span>
                      </div>
                      <div class="upload-actions">
                        <span class={`status ${item.status}`}>{item.status}</span>
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
                </div>
              {/if}
              {#if extractionProgress.active}
                <div class="extract-progress">
                  <div
                    class={`extract-progress-track ${extractionProgress.indeterminate ? 'indeterminate' : ''}`}
                    role="progressbar"
                    aria-label="Extraction progress"
                    aria-valuemin="0"
                    aria-valuemax="100"
                    aria-valuenow={Math.round(extractionProgress.percent || 0)}
                  >
                    <div
                      class="extract-progress-bar"
                      style={extractionProgress.indeterminate ? '' : `width: ${Math.max(2, Math.min(100, extractionProgress.percent || 0))}%`}
                    ></div>
                  </div>
                  {#if extractionProgress.detail}
                    <p class="muted small">{extractionProgress.detail}</p>
                  {/if}
                </div>
              {/if}
            </div>

            <div class="seed-intake-card seed-intake-card--search">
              <h3>Keyword Search</h3>
              <form class="seed-search-form" on:submit|preventDefault={runSearch}>
                <div class="seed-search-row">
                  <label>
                    <span>Mode</span>
                    <select bind:value={searchMode}>
                      <option value="query">Query</option>
                      <option value="seed">Seed JSON</option>
                    </select>
                  </label>
                  <label class="seed-search-main">
                    <span>{searchMode === 'query' ? 'Query' : 'Seed list'}</span>
                    {#if searchMode === 'query'}
                      <input
                        type="text"
                        placeholder="institutional economics AND governance"
                        bind:value={searchQuery}
                      />
                    {:else}
                      <input
                        type="text"
                        placeholder={JSON.stringify(["W2741809807"])}
                        bind:value={searchSeedJson}
                      />
                    {/if}
                  </label>
                  <label>
                    <span>Field</span>
                    <select bind:value={searchField}>
                      <option value="default">All fields</option>
                      <option value="title">Title</option>
                      <option value="abstract">Abstract</option>
                      <option value="title_and_abstract">Title + abstract</option>
                      <option value="fulltext">Full text</option>
                    </select>
                  </label>
                </div>
                <div class="seed-search-row">
                  <label class="seed-search-author">
                    <span>Author</span>
                    <input
                      type="text"
                      placeholder="Elinor Ostrom"
                      bind:value={searchAuthor}
                    />
                  </label>
                  <label>
                    <span>Year from</span>
                    <input type="number" placeholder="Year" bind:value={yearFrom} />
                  </label>
                  <label>
                    <span>Year to</span>
                    <input type="number" placeholder="Year" bind:value={yearTo} />
                  </label>
                  <div class="seed-search-actions">
                    <button class="secondary" type="button" on:click={resetSearchForm}>Reset</button>
                    <button class="primary" type="submit">Search</button>
                  </div>
                </div>
              </form>
              <p class="muted">{searchStatus}</p>
            </div>
          </div>

        </div>

        <div class="seed-corpus-columns">
          <div class="seed-corpus-column seed-corpus-column--seed">
        <div class="card seed-sources-panel" data-testid="seed-panel">
          <div class="workspace-panel-header">
            <div class="workspace-panel-title">
              <h3 class="workspace-section-title">2. Seed</h3>
              <p class="muted">PDF extractions and search runs appear here as the same kind of expandable source.</p>
            </div>
            <div class="workspace-panel-actions">
              <button class="secondary" type="button" on:click={() => loadSeedSources()} disabled={seedActionBusy}>Refresh</button>
              {#if seedSourcesStatus}
                <span class="muted">{seedSourcesStatus}</span>
              {/if}
            </div>
          </div>

          <div class="seed-expansion-row seed-expansion-row--panel">
            <div class="seed-expansion-meta">
              <span class="muted small seed-expansion-label">Promotion settings</span>
              <span class="muted small">Applies to the selected Seed candidates when you promote them.</span>
            </div>
            <div class="seed-expansion-pill" class:opacity-50={!includeDownstream}>
              <label class="expansion-toggle">
                <input type="checkbox" bind:checked={includeDownstream} />
                <span>Downstream</span>
              </label>
              <span class="muted small">Depth</span>
              <input type="number" min="0" max="4" bind:value={relatedDepthDownstream} disabled={!includeDownstream} class="depth-input" />
            </div>
            <div class="seed-expansion-pill" class:opacity-50={!includeUpstream}>
              <label class="expansion-toggle">
                <input type="checkbox" bind:checked={includeUpstream} />
                <span>Upstream</span>
              </label>
              <span class="muted small">Depth</span>
              <input type="number" min="0" max="4" bind:value={relatedDepthUpstream} disabled={!includeUpstream} class="depth-input" />
            </div>
            <div class="seed-expansion-divider"></div>
            <div class="seed-expansion-pill">
              <span class="muted small">Max Related / Paper</span>
              <input type="number" min="1" max="100" bind:value={maxRelated} class="short-input" />
            </div>
          </div>

          {#if seedSources.length === 0}
            <p class="muted">No seed sources yet. Upload a PDF or run a search to start staging candidates.</p>
          {:else}
            <div class="seed-source-list">
              {#each seedSources as source (seedSourceId(source))}
                {@const sourceId = seedSourceId(source)}
                {@const isExpanded = expandedSeedSourceId === sourceId}
                {@const sourceCandidates = getSeedCandidatesForSource(source)}
                {@const selectionVersion = seedSelectionVersions[sourceId] || 0}
                <div class={`seed-source ${isExpanded ? 'expanded' : ''}`}>
                  <div
                    class="seed-source__summary"
                    role="button"
                    tabindex="0"
                    on:click={(e) => {
                      if (e.target?.closest?.('.seed-source__select, .seed-source__summary-actions')) return
                      toggleSeedSource(source)
                    }}
                    on:keydown={(e) => {
                      if (e.target?.closest?.('.seed-source__select, .seed-source__summary-actions')) return
                      if (e.key === 'Enter' || e.key === ' ') {
                        e.preventDefault();
                        toggleSeedSource(source);
                      }
                    }}
                  >
                    <div class="seed-source__summary-main">
                      <div class="seed-source__heading">
                        {#key `${sourceId}:summary-selection:${selectionVersion}`}
                          <label
                            class="seed-source__select"
                            title={estimatedSelectableSeedCount(source) > 0 && selectedSeedCount(source) === estimatedSelectableSeedCount(source)
                              ? 'Clear all candidates in this source'
                              : 'Select all candidates in this source'}
                          >
                            <input
                              type="checkbox"
                              checked={estimatedSelectableSeedCount(source) > 0 && selectedSeedCount(source) === estimatedSelectableSeedCount(source)}
                              disabled={seedActionBusy || estimatedSelectableSeedCount(source) === 0}
                              aria-label="Select all candidates in this source"
                              on:click|stopPropagation
                              on:keydown|stopPropagation
                              on:change|stopPropagation={(e) => setAllSeedCandidatesSelected(source, e.currentTarget.checked)}
                            />
                          </label>
                        {/key}
                        <span class={`tag ${source.source_type === 'pdf' ? 'pending' : 'queued'}`}>{source.source_type === 'pdf' ? 'PDF' : 'Search'}</span>
                        <strong>{source.label}</strong>
                      </div>
                      {#if source.subtitle}
                        <span class="muted small">{source.subtitle}</span>
                      {/if}
                    </div>
                    <div class="seed-source__summary-side">
                      {#key `${sourceId}:summary-actions:${selectionVersion}`}
                        <div class="seed-source__summary-actions">
                          <button
                            class="seed-source__action seed-source__action--promote"
                            type="button"
                            disabled={seedActionBusy || promotableSeedCount(source) === 0}
                            title={promotableSeedCount(source) === 0 ? 'No promotable candidates remain in this source' : 'Promote all promotable candidates from this source'}
                            aria-label="Promote all promotable candidates"
                            on:click|stopPropagation={() => handlePromoteWholeSeedSource(source)}
                          >
                            →
                          </button>
                          <button
                            class="seed-source__action seed-source__action--remove"
                            type="button"
                            disabled={seedActionBusy}
                            title="Remove this source from Seed"
                            aria-label="Remove source"
                            on:click|stopPropagation={() => handleRemoveSeedSource(source)}
                          >
                            ×
                          </button>
                        </div>
                      {/key}
                      <div class="pill-row">
                        <span class="pill">{source.candidate_count} candidates</span>
                        {#if source.state_counts.pending}
                          <span class="pill">Pending: {source.state_counts.pending}</span>
                        {/if}
                        {#if source.state_counts.added}
                          <span class="pill">Added: {source.state_counts.added}</span>
                        {/if}
                        {#if source.state_counts.in_corpus}
                          <span class="pill">In Corpus: {source.state_counts.in_corpus}</span>
                        {/if}
                        {#if source.state_counts.staged_raw}
                          <span class="pill">Raw: {source.state_counts.staged_raw}</span>
                        {/if}
                        {#if source.state_counts.enriched}
                          <span class="pill">Metadata: {source.state_counts.enriched}</span>
                        {/if}
                        {#if source.state_counts.downloaded}
                          <span class="pill">Downloaded: {source.state_counts.downloaded}</span>
                        {/if}
                        {#if source.state_counts.downloaded_elsewhere}
                          <span class="pill">Elsewhere: {source.state_counts.downloaded_elsewhere}</span>
                        {/if}
                      </div>
                      <span class="muted small">{isExpanded ? 'Hide' : 'Show'}</span>
                    </div>
                  </div>

                  {#if isExpanded}
                      <div class="seed-source__body">
                        {#key `${sourceId}:toolbar:${selectionVersion}`}
                          <div class="table-toolbar">
                            <div class="table-toolbar-left">
                              <span class="muted">Selected: {selectedSeedCount(source)} / {selectableSeedCount(source)} selectable</span>
                              <button class="secondary" type="button" on:click={() => selectAllSeedCandidates(source)} disabled={seedActionBusy}>Select all</button>
                              <button class="secondary" type="button" on:click={() => clearSeedSelection(source)} disabled={seedActionBusy}>Clear</button>
                            </div>
                            <div class="table-toolbar-right">
                              <div class="toolbar-actions">
                                <button class="secondary" type="button" on:click={() => handleDismissSelectedSeed(source)} disabled={seedActionBusy || selectedSeedCount(source) === 0}>
                                  Dismiss selected
                                </button>
                                <button class="primary" type="button" on:click={() => handlePromoteSeedSource(source)} disabled={seedActionBusy || selectedSeedCount(source) === 0}>
                                  Promote to Corpus
                                </button>
                                <button class="danger" type="button" on:click={() => handleRemoveSeedSource(source)} disabled={seedActionBusy}>
                                  Remove source
                                </button>
                              </div>
                            </div>
                          </div>
                        {/key}

                        {#if seedCandidatesLoading[sourceId]}
                          <p class="muted">Loading candidates...</p>
                        {:else if sourceCandidates.length === 0}
                          <p class="muted">No active candidates remain in this seed source.</p>
                        {:else}
                          {#key `${sourceId}:selection:${selectionVersion}`}
                            <div class="table table-scroll seed-candidate-table">
                              <div class="table-row header cols-7">
                                <span class="ingest-select-cell">State</span>
                                <span>Title</span>
                                <span>Authors</span>
                                <span>Year</span>
                                <span>Source</span>
                                <span>DOI</span>
                                <span aria-hidden="true"></span>
                              </div>
                              {#each sourceCandidates as candidate (candidate.candidate_key)}
                                {@const activeCandidateKey = String(selectedSeedCandidateKeys[sourceId] || '')}
                                {@const candidateKey = String(candidate?.candidate_key || '')}
                                {@const active = activeCandidateKey !== '' && activeCandidateKey === candidateKey}
                                <div
                                  class={`table-row cols-7 clickable ${isSeedCandidateSelected(source, candidate) ? 'selected' : ''} ${active ? 'active-row' : ''}`}
                                  on:click={(e) => {
                                    if (e.target.tagName === 'INPUT' || e.target.tagName === 'BUTTON' || e.target.tagName === 'A') return;
                                    setSelectedSeedCandidate(source, candidate);
                                  }}
                                  on:keydown={(e) => {
                                    if (e.key === 'Enter' || e.key === ' ') {
                                      if (e.target.tagName === 'INPUT' || e.target.tagName === 'BUTTON' || e.target.tagName === 'A') return;
                                      e.preventDefault();
                                      setSelectedSeedCandidate(source, candidate);
                                    }
                                  }}
                                  role="button"
                                  aria-pressed={active}
                                  tabindex="0"
                                >
                                  <span class="ingest-select-cell">
                                    {#if isSeedCandidateSelectable(candidate)}
                                      <input
                                        type="checkbox"
                                        checked={isSeedCandidateSelected(source, candidate)}
                                        on:click|stopPropagation
                                        on:change={() => {
                                          setSelectedSeedCandidate(source, candidate);
                                          toggleSeedCandidateSelection(source, candidate);
                                        }}
                                      />
                                    {:else}
                                      <input type="checkbox" disabled />
                                    {/if}
                                    <span class={`tag ${seedCandidateTag(candidate).className} ingest-state-tag`}>{seedCandidateTag(candidate).label}</span>
                                  </span>
                                  <span>{formatTitle(candidate)}</span>
                                  <span>{formatAuthors(candidate)}</span>
                                  <span>{candidate.year || ''}</span>
                                  <span>{candidate.source || candidate.publisher || ''}</span>
                                  <span>{candidate.doi || ''}</span>
                                  <span class="seed-candidate__corpus-cell">
                                    {#if isSeedCandidateInCorpus(candidate)}
                                      <span class="seed-candidate__promoted-check" title="Added to corpus" aria-label="Added to corpus">✓</span>
                                    {/if}
                                  </span>
                                </div>
                                {#if active}
                                  <div class="seed-inline-detail-row">
                                    <div class="inline-detail-card">
                                      <div class="inline-detail-main">
                                        <div class="inline-detail-meta-row">
                                          <span class={`inline-detail-chip ${isSeedCandidateInCorpus(candidate) ? 'inline-detail-chip--active' : ''}`}>
                                            Stage: {seedCandidateTag(candidate).label}
                                          </span>
                                          <span class="inline-detail-chip">Year: {candidate.year || '-'}</span>
                                          <span class="inline-detail-chip">Author: {formatAuthors(candidate) || '-'}</span>
                                          <span class="inline-detail-chip">Source: {candidate.source || candidate.publisher || '-'}</span>
                                          <span class={`inline-detail-chip ${isSeedCandidateInCorpus(candidate) ? 'inline-detail-chip--active' : ''}`}>
                                            In Corpus: {isSeedCandidateInCorpus(candidate) ? 'Yes' : 'No'}
                                          </span>
                                          {#if candidate?.doi}
                                            <a class="inline-detail-link" href={doiHref(candidate.doi)} target="_blank" rel="noreferrer">DOI</a>
                                          {/if}
                                          {#if candidate?.openalex_id}
                                            <a class="inline-detail-link" href={openAlexHref(candidate.openalex_id)} target="_blank" rel="noreferrer">OpenAlex</a>
                                          {/if}
                                        </div>
                                      </div>
                                    </div>
                                  </div>
                                {/if}
                              {/each}
                            </div>
                          {/key}
                        {/if}
                      </div>
                  {/if}
                </div>
              {/each}
            </div>
          {/if}
        </div>
          </div>

          <div class="seed-corpus-column seed-corpus-column--corpus">
            <div class="corpus-workspace-shell">
              <Corpus
                {corpusSource}
                {corpusTotal}
                {corpusItems}
                {rawTotal}
                {metadataTotal}
                {downloadedTotal}
                {failedEnrichmentTotal}
                {failedDownloadTotal}
                bind:shareUsername={shareUsername}
                bind:shareRole={shareRole}
                {shareStatus}
                {handleShareCorpus}
                {bucketLabel}
                {formatAuthors}
                {doiHref}
                {openAlexHref}
                {handleDownloadedCorpusFile}
                {corpusLoadStatus}
                {corpusHasMore}
                {corpusLoadingMore}
                {corpusLoading}
                {loadCorpus}
                {handleCorpusColumnScroll}
              />
            </div>
          </div>
        </div>

        </div>
      {/if}

      {#if activeTab === 'dashboard'}
        <Dashboard
          currentCorpusName={currentCorpus?.name || 'Current corpus'}
          {pipelineRawCount}
          {pipelineMetadataCount}
          {pipelineDownloadedCount}
          {daemonStatus}
          {workerBacklog}
          {pipelineWorkerStatus}
          {recentDownloads}
          showGlobalOverview={isAdmin}
          globalStats={globalIngestStats}
          globalBacklog={globalWorkerBacklog}
        />
      {/if}

      {#if activeTab === 'ingest'}
        <div class="card" data-testid="ingest-panel">
          <h2>Seed document</h2>
          <p>Upload PDFs, extract bibliographies, and select entries for further processing.</p>
          <div class="uploader">
            <label class="upload-drop">
              <input type="file" multiple accept=".pdf" on:change={handleFiles} />
              <span>Drop PDFs or click to add files (uploads start automatically)</span>
            </label>
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
          {#if latestEntriesStatus}
            <p class="muted">{latestEntriesStatus}</p>
          {/if}
          {#if extractionProgress.active && isWaitingLatestEntries}
            <div class="extract-progress">
              <div
                class={`extract-progress-track ${extractionProgress.indeterminate ? 'indeterminate' : ''}`}
                role="progressbar"
                aria-label="Extraction progress"
                aria-valuemin="0"
                aria-valuemax="100"
                aria-valuenow={Math.round(extractionProgress.percent || 0)}
              >
                <div
                  class="extract-progress-bar"
                  style={extractionProgress.indeterminate ? '' : `width: ${Math.max(2, Math.min(100, extractionProgress.percent || 0))}%`}
                ></div>
              </div>
              {#if extractionProgress.detail}
                <p class="muted small">{extractionProgress.detail}</p>
              {/if}
            </div>
          {/if}
          {#if latestEntriesBase}
            <p class="muted">Source: {formatSeedDocumentLabel(latestSeedDocument, latestEntriesBase)}</p>
            {#if formatSeedDocumentDetails(latestSeedDocument)}
              <p class="muted">{formatSeedDocumentDetails(latestSeedDocument)}</p>
            {/if}
          {/if}
          <div class="split">
            <button class="secondary" type="button" on:click={loadIngestStats}>Refresh stats</button>
            <button class="secondary" type="button" on:click={loadIngestRuns}>Refresh runs</button>
            {#if ingestStatsStatus}
              <span class="muted">{ingestStatsStatus}</span>
            {/if}
          </div>
          <div class="pill-row">
            <span class="pill">Raw: {pipelineRawCount}</span>
            <span class="pill">With metadata: {pipelineMetadataCount}</span>
            <span class="pill">Downloaded: {pipelineDownloadedCount}</span>
          </div>
          <div class="split">
            {#if ingestRunsStatus}
              <span class="muted">{ingestRunsStatus}</span>
            {/if}
          </div>
          {#if ingestRuns.length > 0}
            <div class="table">
              <div class="table-row header cols-runs">
                <span>Run</span>
                <span>Entries</span>
                <span>Last seen</span>
                <span>Source details</span>
              </div>
              {#each ingestRuns as run}
                <div 
                  class="table-row cols-runs clickable"
                  on:click={(e) => {
                    if (e.target.tagName === 'BUTTON' || e.target.tagName === 'A') return;
                    handleToggleLatestRun(run.ingest_source);
                  }}
                  on:keydown={(e) => {
                    if (e.key === 'Enter' || e.key === ' ') {
                      e.preventDefault();
                      handleToggleLatestRun(run.ingest_source);
                    }
                  }}
                  role="button"
                  tabindex="0"
                >
                  <span>
                    <button
                      class="link truncate-line"
                      type="button"
                      title={formatSeedDocumentLabel(
                        {
                          title: run.seed_title,
                          year: run.seed_year,
                          doi: run.seed_doi,
                        },
                        run.ingest_source
                      )}
                      on:click={() => handleToggleLatestRun(run.ingest_source)}
                    >
                      {formatSeedDocumentLabel(
                        {
                          title: run.seed_title,
                          year: run.seed_year,
                          doi: run.seed_doi,
                        },
                        run.ingest_source
                      )}
                    </button>
                  </span>
                  <span>{run.entry_count}</span>
                  <span class="nowrap">{run.last_created_at || ''}</span>
                  <span
                    class="truncate-line"
                    title={
                      formatSeedDocumentDetails({
                        authors: run.seed_authors,
                        source: run.seed_source,
                        publisher: run.seed_publisher,
                      }) || (run.source_pdf || '')
                    }
                  >
                    {formatSeedDocumentDetails({
                      authors: run.seed_authors,
                      source: run.seed_source,
                      publisher: run.seed_publisher,
                    }) || (run.source_pdf || '')}
                  </span>
                </div>
              {/each}
            </div>
          {/if}
          {#if latestEntries.length === 0 && !isWaitingLatestEntries}
            <p class="muted">{latestEntriesBase ? 'No extracted entries found for this source yet.' : 'Upload a PDF and run Extract to view entries.'}</p>
          {:else}
            <div class="table-toolbar">
              <div class="table-toolbar-left">
                <span class="muted">Selected: {selectedLatestCount} / {selectableLatestCount} selectable</span>
                <button class="secondary" type="button" on:click={selectAllLatest}>Select all</button>
                <button class="secondary" type="button" on:click={clearLatestSelection}>Clear</button>
              </div>
              <div class="table-toolbar-right">
                <div class="toolbar-actions">
                  <button
                    class="secondary"
                    type="button"
                    on:click={() => enqueueSelectedLatest(false)}
                    disabled={selectedLatestCount === 0 || processingMarked}
                  >
                    {processingMarked ? 'Processing...' : 'Enrich'}
                  </button>
                  <button
                    class="primary"
                    type="button"
                    on:click={() => enqueueSelectedLatest(true)}
                    disabled={selectedLatestCount === 0 || processingMarked}
                  >
                    {processingMarked ? 'Processing...' : 'Enrich + Download'}
                  </button>
                </div>
              </div>
            </div>

            <div class="expansion-settings-row" style="display: flex; align-items: center; gap: 20px; padding: 12px 16px; background: var(--surface); border: 1px solid var(--stroke); border-radius: 8px; margin-bottom: 16px; flex-wrap: nowrap; overflow-x: auto;">
              <span class="muted small" style="text-transform: uppercase; letter-spacing: 0.05em; font-weight: 600; white-space: nowrap;">Enrichment Expansion</span>
              
              <div style="display: flex; gap: 8px; align-items: center; background: rgba(0,0,0,0.03); padding: 4px 10px; border-radius: 6px; white-space: nowrap;" class:opacity-50={!includeDownstream}>
                <label class="expansion-toggle">
                  <input type="checkbox" bind:checked={includeDownstream} />
                  <span>Downstream</span>
                </label>
                <span class="muted small" style="margin-left: 4px;">Depth</span>
                <input type="number" min="0" max="4" bind:value={relatedDepthDownstream} disabled={!includeDownstream} class="depth-input" style="padding: 4px; width: 44px; border-radius: 4px; border: 1px solid var(--stroke); background: white;" />
              </div>

              <div style="display: flex; gap: 8px; align-items: center; background: rgba(0,0,0,0.03); padding: 4px 10px; border-radius: 6px;" class:opacity-50={!includeUpstream}>
                <label class="expansion-toggle">
                  <input type="checkbox" bind:checked={includeUpstream} />
                  <span>Upstream</span>
                </label>
                <span class="muted small" style="margin-left: 4px;">Depth</span>
                <input type="number" min="0" max="4" bind:value={relatedDepthUpstream} disabled={!includeUpstream} class="depth-input" style="padding: 4px; width: 44px; border-radius: 4px; border: 1px solid var(--stroke); background: white;" />
              </div>

              <div style="width: 1px; height: 20px; background: var(--stroke);"></div>
              
              <div style="display: flex; gap: 8px; align-items: center;">
                <span class="muted small">Max Related / Paper</span>
                <input type="number" min="1" max="100" bind:value={maxRelated} class="short-input" style="padding: 4px; width: 60px; border-radius: 4px; border: 1px solid var(--stroke);" />
              </div>
            </div>
            {#if enqueueStatus}
              <p class="muted">{enqueueStatus}</p>
            {/if}
            {#if processMarkedStatus}
              <p class="muted">{processMarkedStatus}</p>
            {/if}
            {#if ingestTrackedRequest.requestId || ingestJobProgress.total > 0 || ingestJobProgress.error}
              <div class="ingest-job-progress">
                <div class="ingest-job-progress__top">
                  <span class="muted small">
                    {#if ingestTrackedRequest.requestId}
                      Request: {ingestTrackedRequest.requestId}
                    {:else}
                      Latest queued request
                    {/if}
                  </span>
                  <span class={`tag ${ingestProgressDone ? 'completed' : ingestJobProgress.running > 0 ? 'queued' : 'pending'}`}>
                    {#if ingestProgressDone}
                      Finished
                    {:else if ingestJobProgress.running > 0}
                      Running
                    {:else}
                      Queued
                    {/if}
                  </span>
                </div>
                <div
                  class="extract-progress-track"
                  role="progressbar"
                  aria-label="Ingest queued jobs progress"
                  aria-valuemin="0"
                  aria-valuemax="100"
                  aria-valuenow={Math.round(ingestProgressRatio * 100)}
                >
                  <div
                    class="extract-progress-bar"
                    style={`width: ${Math.max(2, Math.min(100, ingestProgressRatio * 100))}%`}
                  ></div>
                </div>
                <div class="ingest-job-progress__stats">
                  <span>Pending: {ingestJobProgress.pending}</span>
                  <span>Running: {ingestJobProgress.running}</span>
                  <span>Completed: {ingestJobProgress.completed}</span>
                  <span>Failed: {ingestJobProgress.failed}</span>
                  <span>Cancelled: {ingestJobProgress.cancelled}</span>
                </div>
                <div class="ingest-job-progress__meta">
                  <span class="muted small">
                    Daemon:
                    {#if ingestJobProgress.daemonOnline === true}
                      Online
                    {:else if ingestJobProgress.daemonOnline === false}
                      Offline
                    {:else}
                      Unknown
                    {/if}
                  </span>
                  {#if ingestJobProgress.updatedAt}
                    <span class="muted small">Updated: {ingestJobProgress.updatedAt}</span>
                  {/if}
                </div>
                {#if ingestJobProgress.error}
                  <p class="error small">{ingestJobProgress.error}</p>
                {/if}
              </div>
            {/if}
            <div class="table">
              <div class="table-row header cols-6">
                <span class="ingest-select-cell">
                  <input
                    type="checkbox"
                    aria-label="Select all"
                    bind:checked={allLatestSelected}
                    disabled={selectableLatestCount === 0}
                    on:change={(e) => (e.target.checked ? selectAllLatest() : clearLatestSelection())}
                  />
                  <span>Status</span>
                </span>
                <span>Title</span>
                <span>Authors</span>
                <span>Year</span>
                <span>Source</span>
                <span>DOI</span>
              </div>
              {#each latestEntries as entry, index}
                <div 
                  class={`table-row cols-6 clickable ${isLatestSelected(entry, index) ? 'selected' : ''} ${isLatestDownloaded(entry) ? 'downloaded' : isLatestProcessed(entry) ? 'enriched' : ''}`}
                  on:click={(e) => {
                    if (e.target.tagName === 'INPUT' || e.target.tagName === 'BUTTON' || e.target.tagName === 'A') return;
                    toggleLatestSelection(entry, index);
                  }}
                  on:keydown={(e) => {
                    if (e.key === 'Enter' || e.key === ' ') {
                      if (e.target.tagName === 'INPUT' || e.target.tagName === 'BUTTON' || e.target.tagName === 'A') return;
                      e.preventDefault();
                      toggleLatestSelection(entry, index);
                    }
                  }}
                  role="checkbox"
                  aria-checked={isLatestSelected(entry, index)}
                  tabindex="0"
                >
                  <span class="ingest-select-cell">
                    {#if isLatestSelectable(entry)}
                      <input
                        type="checkbox"
                        aria-label={`Select ${formatTitle(entry)}`}
                        bind:group={latestSelection}
                        value={latestSelectionKey(entry, index)}
                      />
                    {:else}
                      <input
                        type="checkbox"
                        aria-label={`${formatTitle(entry)} is not selectable`}
                        disabled
                      />
                    {/if}
                    <span class={`tag ${latestIngestTag(entry).className} ingest-state-tag`}>{latestIngestTag(entry).label}</span>
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
          <p>Search OpenAlex for works and optionally expand related references.</p>
          <div class="search-explanation">
            <details>
              <summary>How to use this search (quick guide)</summary>
              <p>
                Search runs in two modes:
                <strong>Text query</strong> (boolean) or <strong>JSON seed</strong> mode.
                The backend sends the request to the OpenAlex script, normalizes input,
                then deduplicates results before returning.
              </p>
              <ul>
                <li>Text mode is for normal boolean searches like <code>institutional AND economics</code>.</li>
                <li>Seed mode is for exact starting points like OpenAlex IDs/DOI/title.</li>
                <li>Field selector controls which metadata is searched.</li>
                <li>Depth/Max Related controls how far citation graph expansion should go.</li>
              </ul>
            </details>
          </div>
          <form class="search-form" on:submit|preventDefault={runSearch}>
            <div class="search-primary-row">
              <div class="search-field search-mode">
                <label for="search-mode">Mode</label>
                <select
                  id="search-mode"
                  bind:value={searchMode}
                  data-testid="search-mode"
                  title="Text mode runs a boolean OpenAlex query; Seed mode starts from a JSON list."
                >
                  <option value="query">Query</option>
                  <option value="seed">Seed</option>
                </select>
              </div>
              <div class="search-field search-main-input">
                {#if searchMode === 'query'}
                  <div class="label-row">
                    <label for="search-query">Text query</label>
                    <span class="field-hint label-hint">Use <code>AND</code>, <code>OR</code>, <code>NOT</code>. Example: <code>tax AND welfare NOT Keynes</code>.</span>
                  </div>
                  <input
                    id="search-query"
                    type="text"
                    placeholder="institutional economics AND governance"
                    bind:value={searchQuery}
                    data-testid="search-query"
                    title='Use AND/OR/NOT. Missing operators are treated as implicit AND.'
                  />
                {:else}
                  <div class="label-row">
                    <label for="search-seed-json">Seed list (JSON)</label>
                    <span class="field-hint label-hint">Supported: <code>\"W123...\"</code>, <code>&#123;&quot;doi&quot;:&quot;...&quot;&#125;</code></span>
                  </div>
                  <input
                    id="search-seed-json"
                    type="text"
                    placeholder={JSON.stringify(["W2741809807"])}
                    bind:value={searchSeedJson}
                    data-testid="search-seed-json"
                    title="JSON array of OpenAlex IDs, DOIs, or objects."
                  />
                {/if}
              </div>
              <div class="search-field search-scope">
                <label for="search-field">Search field</label>
                <select id="search-field" bind:value={searchField} title="Limit the OpenAlex query to title/abstract/fulltext scope.">
                  <option value="default">All fields</option>
                  <option value="title">Title</option>
                  <option value="abstract">Abstract</option>
                  <option value="title_and_abstract">Title + abstract</option>
                  <option value="fulltext">Full text</option>
                </select>
              </div>
              <button
                class="primary search-submit-btn"
                type="submit"
                data-testid="search-submit"
                title="Run Search"
              >
                Search
              </button>
            </div>

            <div class="search-secondary-row">
              <div class="filter-group search-author-group">
                <div class="filter-label">Author</div>
                <input
                  id="search-author"
                  type="text"
                  placeholder="Elinor Ostrom"
                  bind:value={searchAuthor}
                  title="Resolve an OpenAlex author first, then filter works by that author ID."
                />
              </div>
              <div class="filter-group">
                <div class="filter-label">Published between</div>
                <div class="filter-inputs">
                  <input id="search-year-from" type="number" placeholder="Year" bind:value={yearFrom} class="year-input" />
                  <span class="range-sep">–</span>
                  <input id="search-year-to" type="number" placeholder="Year" bind:value={yearTo} class="year-input" />
                </div>
              </div>

              <div class="spacer"></div>

              <button
                class="text-btn reset-btn"
                type="button"
                on:click={resetSearchForm}
              >
                Reset filters
              </button>
            </div>
            
            {#if searchMode === 'query'}
                 <p class="field-hint">Use <code>AND</code>, <code>OR</code>, <code>NOT</code>. Example: <code>tax AND welfare NOT Keynes</code>.</p>
            {:else}
                 <p class="field-hint">Supported: <code>\"W123...\"</code>, <code>&#123;&quot;doi&quot;:&quot;...&quot;&#125;</code></p>
            {/if}
          </form>
          <p class="muted">{searchStatus}{#if searchSource} ({searchSource}){/if}</p>
          <div class="table">
            <div class="table-row header cols-search">
              <span>Title</span>
              <span>Authors</span>
              <span>Year</span>
              <span>Type</span>
              <span>DOI</span>
            </div>
            {#each searchResults as result, index}
              <div class="table-row cols-search">
                <span>{result.title}</span>
                <span>{result.authors}</span>
                <span>{result.year}</span>
                <span>{result.type}</span>
                <span>{result.doi || ''}</span>
              </div>
            {/each}
          </div>
        </div>
      {/if}

      {#if activeTab === 'downloads'}
        <div class="card" data-testid="downloads-panel">
          <h2>Download activity</h2>
          <p class="muted">
            {#if !downloadsSource}
              Loading queue...
            {:else if downloadsSource === 'api'}
              Live download activity from API.
            {:else if downloadsSource === 'stub'}
              Stub download activity from API.
            {:else if downloadsSource === 'sample'}
              Sample download activity.
            {:else}
              Download activity loaded ({downloadsSource}).
            {/if}
          </p>

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
                  <button
                    class="primary"
                    type="button"
                    on:click={handleStartDownloadWorker}
                    disabled={downloadWorkerBusy || pipelineWorker.running || Number(downloadWorker?.queued_download_items || 0) === 0}
                  >
                    Start
                  </button>
                {/if}
              </div>
            </div>
            <div class="worker-card__meta">
              <span class="muted">{downloadWorkerStatus}</span>
              <span class="muted small">Throughput is auto-managed by the backend for maximum speed.</span>
              {#if !downloadWorker.running && Number(downloadWorker?.queued_download_items || 0) === 0}
                <span class="muted small">No queued download items for this corpus. Enrich/promote items first.</span>
              {/if}
              {#if pipelineWorker.running}
                <span class="muted small">Download-only worker is disabled while global pipeline is running.</span>
              {/if}
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

          <div class="export-card" data-testid="exports-panel">
            <div class="export-card__meta">
              <span class="eyebrow">Corpus exports</span>
              <strong>Final reference data</strong>
              <p class="muted">Download current corpus as JSON, BibTeX, PDFs only, or a full bundle ZIP.</p>
            </div>
            <div class="export-card__filters">
              <label>
                Status
                <select bind:value={exportFilterStatus} data-testid="export-filter-status">
                  <option value="all">All</option>
                  <option value="downloaded">Downloaded</option>
                  <option value="matched">Matched</option>
                  <option value="raw">Raw</option>
                </select>
              </label>
              <label>
                Year from
                <input type="number" min="0" step="1" placeholder="1900" bind:value={exportFilterYearFrom} data-testid="export-filter-year-from" />
              </label>
              <label>
                Year to
                <input type="number" min="0" step="1" placeholder="2026" bind:value={exportFilterYearTo} data-testid="export-filter-year-to" />
              </label>
              <label class="source-filter">
                Source contains
                <input type="text" placeholder="journal or source" bind:value={exportFilterSource} data-testid="export-filter-source" />
              </label>
            </div>
            <div class="export-card__actions">
              <button class="secondary" type="button" on:click={() => handleExportDownload('json', 'JSON')} disabled={exportBusy}>
                JSON
              </button>
              <button class="secondary" type="button" on:click={() => handleExportDownload('bibtex', 'BibTeX')} disabled={exportBusy}>
                BibTeX
              </button>
              <button class="secondary" type="button" on:click={() => handleExportDownload('pdfs', 'PDF ZIP')} disabled={exportBusy}>
                PDFs ZIP
              </button>
              <button class="primary" type="button" on:click={() => handleExportDownload('bundle', 'Bundle ZIP')} disabled={exportBusy} data-testid="export-bundle-button">
                Bundle ZIP
              </button>
            </div>
          </div>
          {#if exportStatus}
            <p class="muted">{exportStatus}</p>
          {/if}

          <div class="table-toolbar downloads-toolbar">
            <div class="table-toolbar-left">
              <input
                class="toolbar-input"
                type="text"
                placeholder="Filter titles..."
                bind:value={downloadsQuery}
              />
              <select class="toolbar-select" bind:value={downloadsStatusFilter}>
                <option value="all">All statuses</option>
                <option value="in_progress">In progress</option>
                <option value="queued">Queued</option>
                <option value="failed">Failed</option>
                <option value="downloaded">Downloaded</option>
              </select>
            </div>
            <div class="table-toolbar-right">
              <span class="muted">
                Showing {filteredDownloads.length} of {downloads.length}
              </span>
            </div>
          </div>

          <div class="table table-scroll downloads-table">
            <div class="table-row header cols-3">
              <span>Work</span>
              <span>Status</span>
              <span>Attempts</span>
            </div>
            {#each filteredDownloads as item (item.id)}
              <div class="table-row cols-3">
                <span class="work-title" title={item.title}>{item.title}</span>
                <span class={`tag status-chip ${item.status}`}>{item.status}</span>
                <span
                  class={`attempts-badge ${item.attempts >= 4 ? 'danger' : item.attempts >= 2 ? 'warn' : ''}`}
                  title={item.attempts > 0 ? `Attempts: ${item.attempts}` : 'No attempts yet'}
                >
                  {item.attempts}
                </span>
              </div>
            {:else}
              <div class="empty-state">
                <strong>No download items to show.</strong>
                <span class="muted">
                  {downloadsQuery.trim() || downloadsStatusFilter !== 'all'
                    ? 'No items match the current filters.'
                    : 'No queued or recently downloaded items are available for this corpus.'}
                </span>
              </div>
            {/each}
          </div>
        </div>
      {/if}

      {#if activeTab === 'logs'}
        <Logs
          bind:logsType={logsType}
          bind:logsFilterCurrentCorpus={logsFilterCurrentCorpus}
          {logsStatus}
          {logsLoadingOlder}
          {logsHasMore}
          {logsTailStatus}
          {logs}
          {handleLogTypeChange}
          {handleLogsCorpusFilterChange}
          {loadOlderLogs}
          {hydrateLogsFromTail}
        />
      {/if}

      {#if activeTab === 'graph'}
        <Graph
          bind:graphRelationship={graphRelationship}
          bind:graphStatusFilter={graphStatusFilter}
          bind:graphYearFrom={graphYearFrom}
          bind:graphYearTo={graphYearTo}
          bind:graphMaxNodes={graphMaxNodes}
          bind:graphColorMode={graphColorMode}
          bind:graphHideIsolates={graphHideIsolates}
          bind:graphFocusConnected={graphFocusConnected}
          {graphStatus}
          {graphSource}
          {loadGraph}
          {graphViewBox}
          {handleGraphWheel}
          {handleGraphPointerDown}
          {handleGraphPointerMove}
          {handleGraphPointerUp}
          {graphVisibleEdges}
          {graphNodeMap}
          {graphLayout}
          bind:hoveredNodeId={hoveredNodeId}
          {graphNodeColorMap}
          {graphNodeCount}
          {graphEdgeCount}
          {graphRelationshipCounts}
          {graphTotalNodeCount}
          {graphTotalEdgeCount}
          {hoveredNode}
          {labelFromStatus}
          {graphColorLegend}
          {resetGraphView}
          {graphList}
          {truncateLabel}
          {graphDegreeMap}
        />
      {/if}
    </section>
  </div>
</div>
{/if}
