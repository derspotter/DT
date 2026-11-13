<script>
  import { onMount } from 'svelte'
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
    fetchKantroposCorpusBrowse,
    fetchCorpusKantroposAssignment,
    saveCorpusKantroposAssignment,
    setAuthToken,
    getAuthToken,
    onUnauthorized,
    uploadPdf,
    importSeed,
    extractBibliography,
    fetchIngestStats,
    fetchIngestRuns,
    fetchSeedSources,
    fetchSeedCandidates,
    downloadSeedCandidateFile,
    promoteSeedCandidates,
    dismissSeedCandidates as dismissSeedCandidatesApi,
    removeSeedSource,
    runKeywordSearch,
    cancelKeywordSearch,
    fetchRecursionConfig,
    fetchCorpus,
    removeCorpusWork,
    removeCorpusWorks,
    fetchSeedSourceDocument,
    fetchAppSettings,
    saveAppSettings,
    fetchDownloadQueue,
    fetchDownloadWorkerStatus,
    startDownloadWorker,
    stopDownloadWorker,
    runDownloadWorkerOnce,
    fetchPipelineWorkerStatus,
    fetchPipelineDaemonStatus,
    fetchLogsTail,
    pausePipelineWorker,
    downloadCorpusExport,
    createCorpusItemDownloadUrl,
    fetchOpenAlexQuota,
    previewKeywordSearch,
    fetchTopicSuggestions,
  } from './lib/api'
  import Dashboard from './components/Dashboard.svelte'
  import Logs from './components/Logs.svelte'
  import ThreeGraph from './components/ThreeGraph.svelte'
  import Corpus from './components/Corpus.svelte'
  import ColumnPicker from './components/ColumnPicker.svelte'
  import {
    gridTemplate,
    loadVisibility as loadColumnVisibility,
    saveVisibility as saveColumnVisibility,
    visibleColumns,
  } from './lib/tableColumns'
  import UpstreamBrowser from './components/UpstreamBrowser.svelte'
  import AdminPanel from './components/AdminPanel.svelte'
  import ScraperLab from './components/ScraperLab.svelte'
  import { seedActivity, seedPromotionStatus } from './lib/seedStatus'
  import {
    workState,
    metadataLabel,
    downloadLabel,
    parseAuthors,
    formatAuthors,
    formatTitle,
    normalizeWorkItem,
    cellText,
    nextSort,
    parseSort,
    sortIndicator,
  } from './lib/works'

  const userTabGroups = [
    {
      title: '',
      tabs: [
        { id: 'workspace', label: 'Workspace' },
        { id: 'upstream', label: 'Korpus Management' },
        { id: 'scraper', label: 'Scraper Lab' },
        { id: 'graph', label: 'Graph' },
      ]
    }
  ]
  const adminTabGroups = [
    {
      title: 'Administration',
      tabs: [
        { id: 'admin', label: 'Admin' },
      ]
    },
    {
      title: 'Diagnostics',
      tabs: [
        { id: 'downloads', label: 'Downloads' },
        { id: 'dashboard', label: 'Pipeline Status' },
        { id: 'logs', label: 'Logs' },
      ]
    }
  ]
  const knownTabIds = new Set([...userTabGroups, ...adminTabGroups].flatMap(group => group.tabs).map(tab => tab.id))

  let activeTab = 'workspace'
  let apiStatus = 'unknown'
  $: backendStatusLabel = apiStatus === 'online' ? 'healthy' : apiStatus === 'offline' ? 'not responding' : 'checking'
  let authStatus = 'loading'
  let authError = ''
  let authUser = null
  let navigationRevision = 0
  const userIsAdmin = (user) => Boolean(user?.is_admin || user?.username === 'admin')
  $: isAdmin = userIsAdmin(authUser)
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
  $: itemsFoundCount = seedSources.reduce((total, source) => total + (Number(source?.candidate_count) || 0), 0)
  // Runs this session started but the seed list has not caught up with yet.
  // Deriving `anySearchRunning` from seedSources alone lost the very first
  // search of a session: the run only shows up in the seed list after a poll,
  // and the poll only starts once a run shows up.
  let startedSearchRunIds = new Set()
  const startedSearchRunMisses = new Map()
  const TERMINAL_RUN_STATUSES = new Set(['done', 'failed', 'cancelled'])
  // Two polls, so one seed-list response that raced the run's registration
  // does not drop it.
  const STARTED_RUN_MISS_LIMIT = 2

  function reconcileStartedSearchRuns(sources) {
    if (startedSearchRunIds.size === 0) return
    const reported = new Map()
    for (const source of sources || []) {
      if (source?.source_type !== 'search') continue
      reported.set(String(source.source_key), source?.run?.status || null)
    }
    const next = new Set()
    for (const runId of startedSearchRunIds) {
      const key = String(runId)
      if (reported.has(key)) {
        startedSearchRunMisses.delete(key)
        const status = reported.get(key)
        // Once the list reports the run, its own status is authoritative; a
        // missing status means the backend cannot track it, which is terminal
        // as far as polling goes.
        if (!status || TERMINAL_RUN_STATUSES.has(status)) continue
        next.add(runId)
        continue
      }
      const misses = (startedSearchRunMisses.get(key) || 0) + 1
      if (misses >= STARTED_RUN_MISS_LIMIT) {
        startedSearchRunMisses.delete(key)
        continue
      }
      startedSearchRunMisses.set(key, misses)
      next.add(runId)
    }
    if (next.size !== startedSearchRunIds.size) startedSearchRunIds = next
  }

  $: anySearchRunning = startedSearchRunIds.size > 0 || seedSources.some((s) => s?.run?.status === 'running')
  // Re-arm the refresh timer when the cadence may have changed (login, search
  // start/stop, enrichment/downloads or a worker discovered running/stopped).
  $: rearmRefreshOnChange(authStatus, anySearchRunning, ingestStats, pipelineWorker, downloadWorker)
  $: itemsPromotedCount = Number(corpusTotal) || corpusItems.length
  $: seedPipelineActivity = seedActivity(ingestStats)
  let seedWarningMessage = ''
  $: seedActionWarning = Boolean(seedWarningMessage && seedWarningMessage === seedActionStatus)
  // Reactive (not a function call) so the markup re-renders when the promotion
  // settings change; a zero-arg helper would have no tracked dependencies.
  $: expansionSummary = [
    includeDownstream && Number(relatedDepthDownstream) > 0 ? `downstream depth ${relatedDepthDownstream}` : '',
    includeUpstream && Number(relatedDepthUpstream) > 0 ? `upstream depth ${relatedDepthUpstream}` : '',
  ].filter(Boolean).join(' + ')
  let ingestStatsStatus = ''
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
  let ingestRuns = []
  let ingestRunsStatus = ''
  $: recentDocumentRuns = (ingestRuns || []).slice(0, 6)
  let seedSources = []
  let seedSourcesStatus = ''
  let expandedSeedSourceId = ''
  let seedCandidatesBySource = {}
  let seedCandidatesLoading = {}
  // Shared stand-in for "nothing selected"; never mutated (selections are
  // always replaced with a new Set).
  const EMPTY_SET = Object.freeze(new Set())
  const EMPTY_LIST = Object.freeze([])
  let seedSelections = {}    // sourceId -> Set of candidate keys, replaced (never mutated) on change
  // sortSeedCandidates memo, per source id on (candidates array reference,
  // sort string): the seed list re-renders on every poll, and re-sorting
  // every seed each time was wasted work. Candidate arrays are replaced,
  // never mutated, on load.
  const seedSortCache = new Map()
  let seedSorts = {}
  let seedPages = {}          // sourceId -> { total, offset, limit }
  let seedAllSelected = {}    // sourceId -> true when "All N items selected"
  let seedAppending = {}      // sourceId -> true while a "Show more" append is in flight
  const SEED_PAGE_SIZE = 200

  // Template callers pass `pages` (and the other trailing arguments of the
  // seed helpers below) so Svelte tracks that state: in legacy mode a call in
  // the markup only re-runs when one of its arguments changes.
  function seedPage(source, pages = seedPages) {
    return pages[seedSourceId(source)] || { total: 0, offset: 0, limit: SEED_PAGE_SIZE }
  }
  function seedSortParams(source) {
    const current = parseSort(seedSorts[seedSourceId(source)])
    return current ? { sort: current.column, dir: current.direction } : { sort: '', dir: 'asc' }
  }
  let seedColumnVisibility = loadColumnVisibility('seed')
  $: seedActiveColumns = visibleColumns('seed', seedColumnVisibility)
  // The seed table keeps a leading selection cell, so the grid gets an extra
  // fixed track in front of the shared columns.
  $: seedGridStyle = `grid-template-columns: 44px ${gridTemplate(seedActiveColumns)}`

  function updateSeedColumns(next) {
    seedColumnVisibility = next
    saveColumnVisibility('seed', next)
  }
  let selectedSeedCandidateKeys = {}
  let seedActionStatus = ''
  let seedActionBusy = false
  let seedLastRunKey = ''

  let searchQuery = ''
  let searchField = 'default'
  let searchAuthor = ''
  let yearFrom = ''
  let yearTo = ''
  const DEFAULT_SEARCH_MAX_RESULTS = 50
  let searchMaxResults = DEFAULT_SEARCH_MAX_RESULTS
  let searchSort = 'relevance'
  let includeDownstream = false
  let includeUpstream = false
  let relatedDepthDownstream = 0
  let relatedDepthUpstream = 0
  let maxRelated = 30
  // Spec line 12: how related papers are ranked before the max-related cap.
  let relatedSort = 'most_cited'
  // Spec lines 10 & 11: new_seed reviews the expansion in section 2 first;
  // download_all is the historical behaviour. Default is review-first.
  let promotionMode = 'new_seed'
  let keywordRecursionConfig = {
    includeDownstream: false,
    includeUpstream: false,
    relatedDepthDownstream: 0,
    relatedDepthUpstream: 0,
    maxRelated: 30,
  }
  let searchStatus = ''
  let searchPreview = null
  let searchWarning = false
  let searchPreviewBusy = false
  // Topic field (OpenAlex topics, ~4,500 of them). A topic-only run is a pure
  // filter listing and costs 1 credit a page instead of 10 for text search.
  let searchTopics = []
  let topicQuery = ''
  let topicSuggestions = []
  let topicActiveIndex = -1
  let topicLookupToken = 0
  let topicDebounce = null
  $: topicOnlySearch = searchTopics.length > 0 && !String(searchQuery || '').trim()
  // relevance needs search text; OpenAlex rejects it for a filter-only listing.
  $: if (topicOnlySearch && searchSort === 'relevance') searchSort = 'cited_by_count'

  let openalexQuota = null
  let openalexQuotaLoading = false

  async function loadOpenAlexQuota() {
    if (openalexQuotaLoading || authStatus !== 'authenticated') return
    openalexQuotaLoading = true
    try {
      openalexQuota = await fetchOpenAlexQuota()
    } catch {
      // Leave the last known value; the pill is informational only.
    } finally {
      openalexQuotaLoading = false
    }
  }

  function formatResetIn(seconds) {
    if (!Number.isFinite(seconds) || seconds <= 0) return 'now'
    const totalMin = Math.round(seconds / 60)
    if (totalMin === 0) return 'now'
    const h = Math.floor(totalMin / 60)
    const m = totalMin % 60
    if (h === 0) return `${m} min`
    return m === 0 ? `${h} h` : `${h} h ${m} min`
  }

  // Copy is fixed by the spec; tone drives the pill colour.
  function formatOpenAlexQuota(quota) {
    if (!quota || !quota.available) {
      return { text: 'OpenAlex budget: unknown until the first request', tone: 'muted' }
    }
    if (!quota.api_key_present) {
      return { text: 'OpenAlex: no API key — daily budget not reported', tone: 'muted' }
    }
    const remaining = Number(quota.remaining)
    const limit = Number(quota.limit)
    if (!Number.isFinite(remaining) || !Number.isFinite(limit)) {
      return { text: 'OpenAlex budget: unknown until the first request', tone: 'muted' }
    }
    // reset_in_seconds is null when OpenAlex sent no usable reset header; that
    // is "unknown", not "reset already happened".
    const hasReset = quota.reset_in_seconds !== null && quota.reset_in_seconds !== undefined
    const resetIn = hasReset ? Number(quota.reset_in_seconds) : NaN
    if (hasReset && Number.isFinite(resetIn) && resetIn <= 0) {
      // The daily reset happened after the last OpenAlex request, so the
      // counts in the snapshot are pre-reset. The next request refreshes them.
      return { text: `OpenAlex budget: reset since the last request · ${limit.toLocaleString('en-US')} credits per day`, tone: 'muted' }
    }
    const counts = `OpenAlex budget: ${remaining.toLocaleString('en-US')} of ${limit.toLocaleString('en-US')} credits left`
    const base = Number.isFinite(resetIn) ? `${counts} · resets in ${formatResetIn(resetIn)}` : counts
    if (quota.stale) {
      const seen = quota.observed_at ? new Date(quota.observed_at).toLocaleString('en-US') : 'unknown'
      return { text: `${base} · last seen ${seen}`, tone: 'muted' }
    }
    if (remaining <= 0) return { text: base, tone: 'danger' }
    if (limit > 0 && remaining / limit < 0.1) return { text: base, tone: 'warn' }
    return { text: base, tone: 'ok' }
  }

  $: openalexQuotaView = formatOpenAlexQuota(openalexQuota)

  const CORPUS_PAGE_SIZE = 120
  // The poll's follow-up reload of every loaded row asks for pages this big.
  const CORPUS_POLL_PAGE_ROWS = 1000
  let corpusItems = []
  let corpusTotal = 0
  let corpusHasMore = false
  // Text filter (spec line 6). Applied server-side for both sections: the
  // corpus is paged, and seeds are collapsed, so client-side filtering would
  // only ever see what happens to be loaded.
  let appSettings = null
  let appSettingsDraft = {}
  let appSettingsStatus = ''
  let appSettingsError = false
  let seedFilterQuery = ''
  let corpusFilterQuery = ''
  let seedFilterDebounce = null
  let corpusFilterDebounce = null
  let corpusSort = ''
  let corpusLoading = false
  // The first page as the poll last applied it (rows, totals, filter, sort):
  // an identical answer leaves corpusItems alone, so the table does not re-render.
  let lastCorpusPageSignature = ''
  let corpusLoadingMore = false
  // "Show more" asked for while a first-page load runs: that load's finally
  // starts the append when it still owns the sequence (D1).
  let corpusAppendQueued = false
  let corpusLoadRequestSeq = 0
  let corpusLoadStatus = ''
  let corpusSource = ''
  // The last reload of the rows on screen failed or was abandoned.
  let corpusRefreshFailed = false
  let corpusStageTotals = { raw: 0, metadata: 0, downloaded: 0, failed_enrichment: 0, failed_download: 0 }
  $: rawTotal = Number(corpusStageTotals?.raw || 0)
  $: metadataTotal = Number(corpusStageTotals?.metadata || 0)
  $: downloadedTotal = Number(corpusStageTotals?.downloaded || 0)
  $: failedEnrichmentTotal = Number(corpusStageTotals?.failed_enrichment || 0)
  $: failedDownloadTotal = Number(corpusStageTotals?.failed_download || 0)
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
  let globalWorkerBacklog = {
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
  // The first close of the log socket since it was last open (null while
  // connected); a reconnect marks the gap from then to its reopen.
  let logsDisconnectedAt = null
  // Bumped by resetLogState (type/filter change, corpus switch, sign-out): a
  // tail or older-lines answer requested under an earlier scope is dropped.
  let logsScopeEpoch = 0
  const REFRESH_ACTIVE_MS = 2000
  const REFRESH_IDLE_MS = 20000
  const REFRESH_TRAILING_MS = 1500
  const REFRESH_WORKER_TICK_MS = 300
  let refreshTimer = null
  let refreshTimerDueAt = 0
  let refreshDirty = false
  let refreshDisposed = false
  let creatingCorpus = false
  let deletingCorpus = false
  let corpusActionStatus = ''
  // A failed switch from a picker. corpusActionStatus renders on Korpus
  // Management only; this one shows in the shell on every other tab.
  let corpusSwitchError = ''
  let corpusActionError = false
  $: currentCorpus = (corpora || []).find((c) => Number(c.id) === Number(currentCorpusId)) || null
  // The graph opens scoped to the workspace corpus; for the admin, "All corpora"
  // in its own dropdown restores the previous global view (spec line 22).
  $: graphCorpusId = Number.isFinite(Number(currentCorpusId)) && currentCorpusId ? String(currentCorpusId) : 'all'
  // Load settings the first time an admin opens the Admin tab.
  $: if (activeTab === 'admin' && isAdmin && appSettings === null) loadAppSettings()
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
  // The corpus the loaded assignment belongs to (null: not loaded).
  let kantroposAssignmentCorpusId = null
  let selectedUpstreamTargetId = ''
  let selectedUpstreamAssignedCorpusId = null
  let upstreamBrowse = {
    target: null,
    summary: {},
    assigned_corpora: [],
    current_items: [],
    pending_items: [],
    graph: { nodes: [], edges: [], stats: {} },
  }
  let upstreamBrowseStatus = ''
  let upstreamBrowseLoading = false
  let upstreamBrowseRequestSeq = 0
  let upstreamCurrentLimit = CORPUS_PAGE_SIZE
  let upstreamPendingLimit = CORPUS_PAGE_SIZE

  const maxLogs = 200
  const maxLogHistory = 5000

  // Live socket lines are buffered here (not reactive) and flushed into `logs`
  // at most once per 250 ms while the logs tab is open; on other tabs the
  // buffer is a ring capped at maxLogHistory, flushed when the tab opens.
  // Every entry is { seq, text, marker? }; seq comes from one increasing
  // counter and only keys the rows (older lines loaded later get higher
  // numbers too). A marker entry (a reconnect gap) is not a file line: it
  // never moves logsCursor.
  const LOG_FLUSH_MS = 250
  let logBuffer = []
  let logFlushTimer = null
  let logSeq = 0

  function toLogEntries(lines) {
    return lines.map((text) => ({ seq: ++logSeq, text }))
  }

  function clearLogBuffer() {
    if (logFlushTimer) {
      clearTimeout(logFlushTimer)
      logFlushTimer = null
    }
    logBuffer = []
  }

  function pushLogLine(text) {
    pushLogEntry({ seq: ++logSeq, text })
  }

  function pushLogEntry(entry) {
    logBuffer.push(entry)
    if (logBuffer.length > maxLogHistory) logBuffer.splice(0, logBuffer.length - maxLogHistory)
    if (activeTab === 'logs' && !logFlushTimer) logFlushTimer = setTimeout(flushLogs, LOG_FLUSH_MS)
  }

  function flushLogs() {
    if (logFlushTimer) {
      clearTimeout(logFlushTimer)
      logFlushTimer = null
    }
    if (logBuffer.length === 0) return
    const count = logBuffer.filter((entry) => !entry.marker).length
    const hadLines = logs.some((entry) => !entry.marker)
    logs = [...logs, ...logBuffer].slice(-maxLogHistory)
    logBuffer = []
    // Same per-line rule as before batching: an established cursor grows by
    // one per line (capped); a first line into an empty view starts it.
    if (logsCursor > 0) {
      logsCursor = Math.min(maxLogHistory, logsCursor + count)
    } else if (!hadLines) {
      logsCursor = Math.min(maxLogHistory, count)
    }
  }

  function tryParseJson(data) {
    if (typeof data !== 'string') return null
    try {
      const parsed = JSON.parse(data)
      return parsed && typeof parsed === 'object' ? parsed : null
    } catch (error) {
      return null
    }
  }

  function resetLogState() {
    logsScopeEpoch += 1
    clearLogBuffer()
    logs = []
    logsCursor = 0
    logsHasMore = false
    logsLoadingOlder = false
    logsTailStatus = ''
    logsDisconnectedAt = null
  }

  function resetKantroposAssignmentState() {
    kantroposAssignment = null
    kantroposAssignmentCorpusId = null
    selectedKantroposTargetId = ''
    kantroposAssignmentStatus = ''
    kantroposAssignmentError = false
    kantroposAssignmentLoading = false
    kantroposAssignmentSaving = false
  }

  function resetUpstreamBrowseState({ preserveTargetSelection = true } = {}) {
    if (!preserveTargetSelection) {
      selectedUpstreamTargetId = ''
    }
    upstreamCurrentLimit = CORPUS_PAGE_SIZE
    upstreamPendingLimit = CORPUS_PAGE_SIZE
    selectedUpstreamAssignedCorpusId = null
    upstreamBrowse = {
      target: null,
      summary: {},
      assigned_corpora: [],
      current_items: [],
      pending_items: [],
      graph: { nodes: [], edges: [], stats: {} },
    }
    upstreamBrowseStatus = ''
    upstreamBrowseLoading = false
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
    persistStoredUploads()
  }

  function updateUploadByBackendFilename(backendFilename, patch) {
    const normalized = String(backendFilename || '').trim()
    if (!normalized) return
    uploads = uploads.map((item) =>
      String(item.backendFilename || '').trim() === normalized ? { ...item, ...patch } : item
    )
    persistStoredUploads()
  }

  function uploadStorageKey() {
    const corpusId = Number(currentCorpusId)
    if (!Number.isFinite(corpusId) || corpusId <= 0) return ''
    return `rag-feeder:uploads:${corpusId}`
  }

  function normalizeStoredUploadStatus(status) {
    const value = String(status || '').trim().toLowerCase()
    if (value === 'extracted' || value === 'failed') return value
    if (value === 'queued' || value === 'extracting' || value === 'uploading') return 'uploaded'
    return 'uploaded'
  }

  function persistStoredUploads(nextUploads = uploads) {
    if (typeof window === 'undefined') return
    const key = uploadStorageKey()
    if (!key) return
    const extractedBaseNames = new Set(
      (ingestRuns || [])
        .map((run) => String(run?.ingest_source || '').trim())
        .filter(Boolean)
    )
    const persisted = (nextUploads || [])
      .filter((item) => String(item?.backendFilename || '').trim())
      .filter((item) => !extractedBaseNames.has(String(item.backendFilename || '').replace(/\.pdf$/i, '')))
      .filter((item) => normalizeStoredUploadStatus(item.status) !== 'extracted')
      .map((item) => ({
        id: String(item.id || item.backendFilename),
        name: String(item.name || item.backendFilename),
        size: Number(item.size || 0),
        status: normalizeStoredUploadStatus(item.status),
        backendFilename: String(item.backendFilename || ''),
        message: String(item.message || ''),
      }))
    window.localStorage.setItem(key, JSON.stringify(persisted))
  }

  function restoreStoredUploads() {
    if (typeof window === 'undefined') return
    const key = uploadStorageKey()
    if (!key) return
    try {
      const parsed = JSON.parse(window.localStorage.getItem(key) || '[]')
      uploads = Array.isArray(parsed)
        ? parsed
          .filter((item) => String(item?.backendFilename || '').trim())
          .map((item) => ({
            id: String(item.id || item.backendFilename),
            file: null,
            name: String(item.name || item.backendFilename),
            size: Number(item.size || 0),
            status: normalizeStoredUploadStatus(item.status),
            backendFilename: String(item.backendFilename || ''),
            message: String(item.message || ''),
          }))
        : []
    } catch {
      uploads = []
    }
  }

  function pruneExtractedUploads() {
    const extractedBaseNames = new Set(
      (ingestRuns || [])
        .map((run) => String(run?.ingest_source || '').trim())
        .filter(Boolean)
    )
    if (extractedBaseNames.size === 0) return
    const nextUploads = uploads.filter((item) => {
      const backendFilename = String(item?.backendFilename || '').trim()
      // An upload still in flight has no backend filename yet. It must survive:
      // this prune runs on the ingest-runs poll, so dropping it here deletes the
      // row mid-upload and the later updateUpload() patch finds nothing to
      // update, leaving the file silently gone from the list.
      if (!backendFilename) return true
      const baseName = backendFilename.replace(/\.pdf$/i, '')
      return !extractedBaseNames.has(baseName) && normalizeStoredUploadStatus(item.status) !== 'extracted'
    })
    if (nextUploads.length !== uploads.length) {
      uploads = nextUploads
      persistStoredUploads(nextUploads)
    }
  }

  function normalizeCorpusItem(entry) {
    return normalizeWorkItem(entry)
  }

  function normalizeUpstreamBrowseItem(entry) {
    if (!entry || typeof entry !== 'object') return entry
    return {
      ...normalizeWorkItem(entry),
      source_corpora: Array.isArray(entry.source_corpora) ? entry.source_corpora : [],
      source_corpus_ids: Array.isArray(entry.source_corpus_ids) ? entry.source_corpus_ids : [],
    }
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
    const authors = parseAuthors(meta.authors)
    if (authors.length > 0) return authors.join(', ')
    const source = String(meta.source || '').trim()
    const publisher = String(meta.publisher || '').trim()
    if (source && publisher) return `${source} · ${publisher}`
    return source || publisher || ''
  }

  function extractionEventFromObject(parsed) {
    const event = String(parsed?.event || '').trim().toLowerCase()
    if (!parsed || parsed.kind !== 'extraction' || !['started', 'done'].includes(event)) return null
    return {
      event,
      corpusId: Number.isFinite(Number(parsed.corpus_id)) ? Number(parsed.corpus_id) : null,
      baseName: String(parsed.base_name || '').trim(),
      filename: String(parsed.filename || '').trim(),
      status: String(parsed.status || 'success').trim().toLowerCase(),
      mode: String(parsed.mode || '').trim().toLowerCase(),
      reason: String(parsed.reason || '').trim(),
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
    // A load in flight queues the append (loadCorpus), so only a running
    // append stops the trigger.
    if (!corpusHasMore || corpusLoadingMore) return
    const target = event?.currentTarget
    if (!target) return
    const remaining = target.scrollHeight - target.scrollTop - target.clientHeight
    if (remaining <= 120) {
      loadCorpus({ append: true })
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

  function ensureExtractionRunActive(baseName, backendFilename = '') {
    const normalizedBase = String(baseName || '').trim()
    const normalizedFile = String(backendFilename || '').trim()
    const sameRun =
      extractionProgress.active &&
      extractionProgress.baseName === normalizedBase &&
      extractionProgress.backendFilename === normalizedFile

    if (!sameRun) {
      beginExtractionProgress(normalizedBase, normalizedFile)
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
    const baseName = String(extractionProgress.baseName || '').trim()
    // A corpus reset also ends the run (finishExtractionProgress); the epoch
    // check keeps the post-await steps from acting on the new corpus as well.
    const epoch = corpusScopeEpoch
    const stale = () => epoch !== corpusScopeEpoch || !extractionProgress.active || extractionProgress.runId !== runId

    try {
      if (baseName) {
        // A corpus change or a newer run supersedes this completion: it must
        // neither report on nor finish the run that replaced it.
        await loadSeedSources({ quiet: true })
        if (stale()) return
        // The pdf seed's candidate count says whether the extraction found
        // entries; same match as focusSeedSource('pdf', baseName). The list
        // may not show it: a seed filter hides it, or a newer reload dropped
        // this one's answer. Only the seed itself can say it has no entries.
        const seedId = `pdf:${baseName}`
        const seed = seedSources.find((entry) => seedSourceId(entry) === seedId)
        let outcome = 'Extraction complete.'
        if (!(Number(seed?.candidate_count) > 0)) {
          try {
            const page = await fetchSeedCandidates('pdf', baseName, { limit: 1, lightSummary: true })
            if (!(Number(page?.total) > 0)) outcome = detail
          } catch (error) {
            if (error?.status === 401) throw error
            outcome = 'Extraction finished.'
          }
          if (stale()) return
        }
        updateExtractionProgress({
          phase: 'completed',
          detail: outcome,
          percent: 100,
          indeterminate: false,
        })
        await focusSeedSource('pdf', baseName)
        if (stale()) return
        await loadIngestStats()
        if (stale()) return
        await loadIngestRuns()
        if (stale()) return
        setTimeout(() => {
          if (extractionProgress.active && extractionProgress.runId === runId) {
            finishExtractionProgress()
          }
        }, 1200)
        return
      }

      if (stale()) return
      updateExtractionProgress({
        phase: 'completed',
        detail,
        percent: 100,
        indeterminate: false,
      })
      finishExtractionProgress()
    } catch (error) {
      if (error?.status === 401) {
        finishExtractionProgress()
        return
      }
      if (stale()) return
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
      updateUploadByBackendFilename(
        signalFile,
        signalStatus === 'success'
          ? { status: 'extracted', message: 'Extraction complete' }
          : { status: 'failed', message: 'Extraction failed' }
      )
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

  function refreshReason() {
    if (typeof document !== 'undefined' && document.visibilityState === 'hidden') return 'hidden'
    if (authStatus !== 'authenticated') return 'none'
    if (anySearchRunning) return 'active'
    if (diagnosticsEnabled && (workerBusy(pipelineWorker) || workerBusy(downloadWorker))) return 'active'
    // Non-admins cannot see worker status; works under an unexpired worker lease
    // are being enriched/downloaded right now, so the counts move. The plain
    // enriching/downloading counts also include claims a killed worker left behind.
    // Only where a cycle refreshes the stats, otherwise the signal would never clear.
    const stats = ingestStats || {}
    if ((Number(stats.enriching_live) > 0 || Number(stats.downloading_live) > 0) && shouldRunLiveRefresh()) return 'active'
    // The Admin tab only refreshes the OpenAlex quota, at the idle cadence.
    if (shouldRunLiveRefresh() || activeTab === 'admin') return 'idle'
    return 'none'
  }

  // Arms exactly one timer: 2 s while active, 20 s idle, nothing when hidden or logged out.
  // An already armed timer that is due sooner is kept, so a pending trailing run
  // is never pushed back to the regular cadence. An explicit delayMs skips the
  // reason check (log-triggered runs happen on any tab) but still never when hidden.
  function scheduleRefresh({ delayMs = null } = {}) {
    if (refreshDisposed) return
    let wait = delayMs
    const reason = refreshReason()
    if (reason === 'hidden') wait = null
    else if (wait === null) {
      if (reason === 'active') wait = REFRESH_ACTIVE_MS
      else if (reason === 'idle') wait = REFRESH_IDLE_MS
      // No cadence on this tab, but a log line left the data dirty: keep (or
      // arm) its trailing refresh instead of clearing it on a re-arm.
      else if (reason === 'none' && refreshDirty && authStatus === 'authenticated') wait = REFRESH_TRAILING_MS
    }
    if (wait === null) {
      if (refreshTimer) {
        clearTimeout(refreshTimer)
        refreshTimer = null
      }
      return
    }
    const dueAt = Date.now() + wait
    if (refreshTimer) {
      if (refreshTimerDueAt <= dueAt) return
      clearTimeout(refreshTimer)
    }
    refreshTimerDueAt = dueAt
    refreshTimer = setTimeout(() => {
      refreshTimer = null
      void runLiveRefreshCycle()
    }, wait)
  }

  // Worker log lines mark the data dirty and coalesce into one trailing run.
  function requestRefreshSoon(delayMs = REFRESH_TRAILING_MS) {
    if (refreshDisposed) return
    // Hidden tabs do not run, but they remember: the trailing refresh happens
    // once the tab is visible again (see onFocusOrVisible).
    refreshDirty = true
    if (refreshReason() === 'hidden') return
    scheduleRefresh({ delayMs })
  }

  // The arguments exist only to register them as `$:` dependencies.
  function rearmRefreshOnChange() {
    if (refreshDisposed) return
    scheduleRefresh()
  }

  // `running` only means the daemon heartbeat is fresh; it idles most of the
  // time. `in_flight` is a heartbeat in state "running", i.e. a batch in
  // progress, but a daemon killed mid-batch leaves that state behind with a
  // stale heartbeat, so both are required.
  function workerBusy(worker) {
    return Boolean(worker?.running && worker?.in_flight)
  }

  function shouldRunLiveRefresh() {
    if (authStatus !== 'authenticated') return false
    // A daemon that is merely online (idle heartbeat) does not warrant refreshing
    // the workspace from other tabs; a batch in flight does.
    if (diagnosticsEnabled && (workerBusy(pipelineWorker) || workerBusy(downloadWorker))) return true
    return activeTab === 'workspace' || activeTab === 'dashboard' || activeTab === 'downloads'
  }

  // The backend re-validates the selected corpus on every request and falls
  // back to another membership (or none) after a revocation. Polls fetch
  // /api/auth/me and follow that choice, so the UI never shows corpus A while
  // reads and writes already go to B. No select POST: the backend persisted it.
  // The pending reconciliation, shared by every caller: a refresh cycle that
  // starts while one (e.g. from handleCorpusPollForbidden) is pending waits
  // for it instead of starting scoped loads under the old corpus.
  let corpusReconcilePromise = null
  // The tab the last switching reconciliation reloaded (null: it cleared the
  // corpus and loaded nothing). A tab opened meanwhile loads its own resources.
  let corpusReconcileLoadedTab = null
  // Resolves true when it switched (or cleared) the workspace corpus.
  function reconcileCorpusWithBackend() {
    if (corpusReconcilePromise) return corpusReconcilePromise
    if (authStatus !== 'authenticated') return Promise.resolve(false)
    const pending = runCorpusReconcile()
    corpusReconcilePromise = pending
    pending.finally(() => {
      if (corpusReconcilePromise === pending) corpusReconcilePromise = null
    })
    return pending
  }

  // True when a pending reconciliation switched the corpus (it reloads the
  // workspace itself, so the caller skips its own scoped loads).
  async function awaitPendingCorpusReconcile() {
    return corpusReconcilePromise ? corpusReconcilePromise : false
  }

  async function runCorpusReconcile() {
    const corpusIdAtRequest = currentCorpusId
    const epoch = sessionEpoch
    try {
      const me = await fetchMe()
      // Requested under an earlier session (sign-out, then a new sign-in).
      if (epoch !== sessionEpoch) return false
      if (authStatus !== 'authenticated' || !me?.user) return false
      // The user switched corpus meanwhile; this answer may predate the switch.
      if (currentCorpusId !== corpusIdAtRequest) return false
      const nextCorpusId = me.user.last_corpus_id || null
      const nextCorpora = me.corpora || []
      if (JSON.stringify(nextCorpora) !== JSON.stringify(corpora)) corpora = nextCorpora
      if (JSON.stringify(me.user) !== JSON.stringify(authUser)) authUser = me.user
      if (Number(nextCorpusId || 0) === Number(currentCorpusId || 0)) return false
      currentCorpusId = nextCorpusId
      resetFrontendState()
      corpusReconcileLoadedTab = nextCorpusId ? activeTab : null
      if (nextCorpusId) {
        await refreshAll(corpusReconcileLoadedTab)
        if (logsType === 'pipeline') handleLogsCorpusFilterChange()
      }
      return true
    } catch (error) {
      // A failed /me poll changes nothing; the next cycle retries.
      return false
    }
  }

  // A corpus-scoped poll answered 403 (e.g. "Select a corpus first"): the
  // backend no longer serves the corpus the UI shows.
  function handleCorpusPollForbidden(error) {
    if (error?.status === 403) void reconcileCorpusWithBackend()
  }

  // What each tab renders, and therefore loads: boot (refreshAll), a tab
  // switch (refreshForActiveTab) and the cadence (runLiveRefreshCycleOnce)
  // all read this one table. The admin-only resources are added on top. The
  // cadence skips TAB_OPEN_RESOURCES.
  const TAB_RESOURCES = {
    workspace: ['stats', 'corpus', 'seeds', 'runs'],
    dashboard: ['stats', 'corpus', 'runs'],
    downloads: [],
    upstream: ['upstream'],
    graph: [],
    scraper: [],
    logs: [],
    admin: [],
  }
  // Loaded when the tab opens or on request, never by a tab's poll: ingest runs
  // follow worker activity (a dirty cycle loads them), and the Korpus Management
  // browse is a heavy read that its Refresh button repeats.
  const TAB_OPEN_RESOURCES = new Set(['runs', 'upstream'])
  const ADMIN_RESOURCES = {
    dashboard: ['downloads', 'daemon', 'globalStats', 'globalDaemon'],
    downloads: ['downloads', 'daemon'],
    workspace: ['daemon'],
  }

  // The shared header (.header-stats) shows items found / promoted /
  // downloaded on every tab, so every tab loads them on entry. Not the graph:
  // it loads its own snapshot and must not hit the pipeline reads. Polling
  // them is still up to the cadence (shouldRunLiveRefresh).
  const HEADER_RESOURCES = ['stats', 'corpus', 'seeds']

  function resourcesFor(tab, { admin = false } = {}) {
    const tabId = normalizeTab(tab)
    return [
      ...new Set([
        ...(tabId === 'graph' ? [] : HEADER_RESOURCES),
        ...(TAB_RESOURCES[tabId] || []),
        ...(admin ? ADMIN_RESOURCES[tabId] || [] : []),
      ]),
    ]
  }

  // `quiet` is false only for refreshAll (boot, sign-in, corpus switch),
  // which shows the loading/result status lines; tab switches and polls do not.
  const loaders = {
    stats: ({ quiet }) => loadIngestStats({ quiet }),
    corpus: ({ quiet }) => (quiet ? loadCorpus({ preserveSelection: true, quiet: true }) : loadCorpus()),
    seeds: ({ quiet }) => loadSeedSources({ quiet }),
    runs: ({ quiet }) => loadIngestRuns({ quiet }),
    upstream: ({ quiet }) => loadUpstreamBrowse({ quiet }),
    downloads: () => loadDownloads(),
    daemon: ({ quiet }) => loadPipelineWorkerStatus({ quiet }),
    globalStats: () => loadIngestStats({ quiet: true, global: true }),
    globalDaemon: () => loadPipelineWorkerStatus({ quiet: true, global: true }),
    quota: () => loadOpenAlexQuota(),
  }

  // One promise per resource in flight: a second request for the same
  // resource joins it instead of fetching again. Cleared on a corpus switch or
  // sign-out (invalidateCorpusScopedRequests), so the next corpus never joins
  // a load that was requested for the previous one.
  const resourcesInFlight = new Map()

  function startResource(name, quiet) {
    let result
    try {
      result = loaders[name]({ quiet })
    } catch (error) {
      result = Promise.reject(error)
    }
    // Loaders report their own failures; one that throws anyway must neither
    // reject the other loads nor leave its name stuck in the map.
    const pending = Promise.resolve(result)
      .catch((error) => console.error(`Refreshing "${name}" failed`, error))
      .finally(() => {
        if (resourcesInFlight.get(name) === pending) resourcesInFlight.delete(name)
      })
    resourcesInFlight.set(name, pending)
    return pending
  }

  async function refreshResources(names, { quiet = true } = {}) {
    const unique = [...new Set(names)].filter((name) => loaders[name])
    await Promise.all(unique.map((name) => resourcesInFlight.get(name) || startResource(name, quiet)))
  }

  function refreshQuota() {
    return refreshResources(['quota'])
  }

  async function runLiveRefreshCycle() {
    try {
      await runLiveRefreshCycleOnce()
    } finally {
      // Dirty again means a log burst landed while this cycle was in flight.
      scheduleRefresh({ delayMs: refreshDirty ? REFRESH_TRAILING_MS : null })
    }
  }

  async function runLiveRefreshCycleOnce() {
    const dirty = refreshDirty
    refreshDirty = false
    if (authStatus !== 'authenticated') return
    const admin = diagnosticsEnabled
    // Off the polling tabs only a search run (its terminal status has to be
    // discoverable from any tab, or the cadence never ends) or dirty data
    // (log lines) load anything; the tab's own resources are not refreshed.
    const tabCycle = shouldRunLiveRefresh()
    const names = tabCycle ? resourcesFor(activeTab, { admin }).filter((name) => !TAB_OPEN_RESOURCES.has(name)) : []
    if (anySearchRunning) names.push('seeds')
    if (dirty) {
      names.push('runs')
      if (!tabCycle) names.push('stats')
    }
    // Discover a worker started elsewhere, so refreshReason() can go active,
    // and keep polling a busy one, so it can go idle again.
    if (admin && (dirty || workerBusy(pipelineWorker) || workerBusy(downloadWorker))) names.push('daemon')
    // The OpenAlex budget (workspace search panel, Admin settings row) moves
    // only with searches and promotions, which refresh it themselves.
    if (refreshReason() === 'idle' && (activeTab === 'admin' || activeTab === 'workspace')) names.push('quota')
    if (names.length === 0) return
    // A corpus switch reloads the workspace itself; skip this cycle's loads.
    if (tabCycle ? await reconcileCorpusWithBackend() : await awaitPendingCorpusReconcile()) return
    // A load already in flight was requested before these log lines: join it,
    // but keep the data dirty so the trailing run fetches it fresh.
    if (dirty && names.some((name) => resourcesInFlight.has(name))) refreshDirty = true
    await refreshResources(names)
  }

  async function refreshForActiveTab(tabId) {
    if (authStatus !== 'authenticated') return
    if (await awaitPendingCorpusReconcile()) {
      // The switch reloaded the tab that was active when it started; another
      // tab opened since still needs its own loads (shared ones are joined).
      if (corpusReconcileLoadedTab === null || normalizeTab(corpusReconcileLoadedTab) === normalizeTab(tabId)) return
      if (authStatus !== 'authenticated') return
    }
    await refreshResources(resourcesFor(tabId, { admin: diagnosticsEnabled }))
  }

  async function bootstrapAuth({ preserveAuthFlow = false } = {}) {
    authStatus = 'loading'
    authError = ''
    const startupTab = !preserveAuthFlow ? readTabFromHash({ includeUnavailable: true }) : null
    const startupNavigationRevision = navigationRevision
    const landingTab = startupTab || activeTab
    // The backend scopes every workspace request to the token's corpus, so
    // the seed list, corpus table and counts do not have to wait for the
    // auth round trip: start them now and let refreshAll collect them. A bad
    // token fails them with a 401 exactly as it fails fetchMe below.
    const inFlight = tabNeedsWorkspaceData(landingTab) ? startWorkspaceLoads(landingTab) : null
    const epoch = sessionEpoch
    try {
      const payload = await fetchMe()
      if (epoch !== sessionEpoch) return
      authUser = payload.user
      corpora = payload.corpora || []
      currentCorpusId = authUser?.last_corpus_id || (corpora[0]?.id ?? null)
      authStatus = 'authenticated'
      if (!preserveAuthFlow) {
        authFlow = 'login'
        authFlowToken = ''
      }
      await Promise.all([loadRecursionConfig(), refreshAll(landingTab, { inFlight })])
      // A 401 during the loads signed the session out already.
      if (epoch !== sessionEpoch) return
      connectLogs()
      if (!preserveAuthFlow && navigationRevision === startupNavigationRevision) {
        const latestTab = readTabFromHash({ includeUnavailable: true }) || activeTab || startupTab || 'workspace'
        setActiveTab(latestTab, { replace: true, loadedTab: landingTab })
      }
    } catch (error) {
      // A 401 already ran expireSession (state reset, token cleared).
      if (error?.status === 401 || epoch !== sessionEpoch) return
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
    // A newer sign-in or a sign-out takes this attempt over; an expired token
    // does not (signInEpoch). Its success starts the session `epoch` watches.
    let attempt = signInEpoch
    let epoch = sessionEpoch
    try {
      const payload = await login({ username: loginUsername.trim(), password: loginPassword })
      if (attempt !== signInEpoch) return
      enterSession(payload)
      attempt = signInEpoch
      epoch = sessionEpoch
      const me = await fetchMe()
      if (epoch !== sessionEpoch) return
      authUser = me.user
      corpora = me.corpora || []
      currentCorpusId = authUser?.last_corpus_id || currentCorpusId || corpora[0]?.id || null
      authStatus = 'authenticated'
      authFlow = 'login'
      authFlowToken = ''
      const authNavigationRevision = navigationRevision
      await loadRecursionConfig()
      const refreshedTab = activeTab
      await refreshAll(refreshedTab)
      // A 401 during the loads signed the session out already.
      if (epoch !== sessionEpoch) return
      connectLogs()
      if (navigationRevision === authNavigationRevision) {
        setActiveTab('workspace', { replace: true, loadedTab: refreshedTab })
      }
    } catch (error) {
      // Only the attempt still in charge may report or clear. That includes a
      // 401 from its own /me, which already ran expireSession.
      if (attempt !== signInEpoch) return
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
    // A newer sign-in or a sign-out takes this attempt over; an expired token
    // does not (signInEpoch). Its success starts the session `epoch` watches.
    let attempt = signInEpoch
    let epoch = sessionEpoch
    try {
      const payload = await acceptInvite({ token: authFlowToken, password: authPassword })
      if (attempt !== signInEpoch) return
      enterSession(payload)
      attempt = signInEpoch
      epoch = sessionEpoch
      const me = await fetchMe()
      if (epoch !== sessionEpoch) return
      authUser = me.user
      corpora = me.corpora || []
      currentCorpusId = authUser?.last_corpus_id || currentCorpusId || corpora[0]?.id || null
      authStatus = 'authenticated'
      authFlow = 'login'
      authFlowToken = ''
      const authNavigationRevision = navigationRevision
      await loadRecursionConfig()
      const refreshedTab = activeTab
      await refreshAll(refreshedTab)
      // A 401 during the loads signed the session out already.
      if (epoch !== sessionEpoch) return
      connectLogs()
      if (navigationRevision === authNavigationRevision) {
        setActiveTab('workspace', { replace: true, loadedTab: refreshedTab })
      }
    } catch (error) {
      if (attempt !== signInEpoch) return
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
    // A newer sign-in or a sign-out takes this attempt over; an expired token
    // does not (signInEpoch). Its success starts the session `epoch` watches.
    let attempt = signInEpoch
    let epoch = sessionEpoch
    try {
      const payload = await resetPassword({ token: authFlowToken, password: authPassword })
      if (attempt !== signInEpoch) return
      enterSession(payload)
      attempt = signInEpoch
      epoch = sessionEpoch
      const me = await fetchMe()
      if (epoch !== sessionEpoch) return
      authUser = me.user
      corpora = me.corpora || []
      currentCorpusId = authUser?.last_corpus_id || currentCorpusId || corpora[0]?.id || null
      authStatus = 'authenticated'
      authFlow = 'login'
      authFlowToken = ''
      const authNavigationRevision = navigationRevision
      await loadRecursionConfig()
      const refreshedTab = activeTab
      await refreshAll(refreshedTab)
      // A 401 during the loads signed the session out already.
      if (epoch !== sessionEpoch) return
      connectLogs()
      if (navigationRevision === authNavigationRevision) {
        setActiveTab('workspace', { replace: true, loadedTab: refreshedTab })
      }
    } catch (error) {
      if (attempt !== signInEpoch) return
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
      if (error?.status === 401) return
      inviteStatus = error.message || 'Failed to create invitation'
      inviteError = true
    }
  }

  // Bumped on every sign-out and on every successful sign-in: an answer
  // requested under an earlier session, such as a pending /me poll, is dropped.
  let sessionEpoch = 0
  // What a sign-in attempt in flight answers to: a newer sign-in that succeeded
  // or a deliberate sign-out. Not an expired token: a boot 401 landing during
  // the submit must not cost the attempt its outcome.
  let signInEpoch = 0

  // The one path for an expired session: api.js runs it on every 401. Several
  // requests can fail together; once signed out, the rest change nothing.
  function expireSession() {
    if (authStatus === 'unauthenticated' && !getAuthToken()) return
    endSession()
  }
  onUnauthorized(expireSession)

  function handleLogout() {
    signInEpoch += 1
    endSession()
  }

  function endSession() {
    // During boot the sign-in forms are already up, so an error on screen is
    // the outcome of a submit, not of the session that is ending.
    const keepError = authStatus === 'loading'
    sessionEpoch += 1
    disconnectLogs()
    setAuthToken('')
    authStatus = 'unauthenticated'
    if (!keepError) authError = ''
    authUser = null
    corpora = []
    currentCorpusId = null
    loginPassword = ''
    resetFrontendState({ signOut: true })
  }

  // A sign-in answered: whatever session is still open ends here, such as a
  // live one under an invite or reset link, so nothing of it reaches the new
  // user.
  function enterSession(payload) {
    if (getAuthToken()) endSession()
    signInEpoch += 1
    sessionEpoch += 1
    if (payload?.token) setAuthToken(payload.token)
    if (payload?.user) {
      authUser = payload.user
      currentCorpusId = payload.user.last_corpus_id || null
    }
  }

  // The tabs whose loads bootstrapAuth may start before the auth round trip:
  // all of them are scoped by the token alone. The Korpus Management loads
  // need the corpus id from the auth payload, so they wait for refreshAll.
  function tabNeedsWorkspaceData(tabId) {
    return tabId === 'workspace' || tabId === 'dashboard' || tabId === 'downloads'
  }

  // The tab's loads that need nothing from the auth payload (the admin ones
  // wait for it): the backend scopes them to the token's corpus itself.
  // bootstrapAuth starts these before fetchMe returns and hands them to the
  // refreshAll that follows, which does not request them again (they may
  // have finished, and left the in-flight map, before the auth answer).
  function startWorkspaceLoads(tabId) {
    const names = resourcesFor(tabId)
    return { names, done: refreshResources(names, { quiet: false }) }
  }

  async function refreshAll(targetTab = activeTab, { inFlight = null } = {}) {
    restoreStoredUploads()
    // The OpenAlex quota pill is shown on both the workspace search panel
    // and the admin settings row, so it needs to load once after every
    // authentication regardless of which tab the user lands on.
    void refreshQuota()
    // Not `diagnosticsEnabled`: bootstrapAuth calls this right after assigning
    // authUser, before Svelte has flushed the `$:` derivations, so the reactive
    // flag is still false for an admin on the first refresh.
    const names = resourcesFor(targetTab, { admin: userIsAdmin(authUser) })
    await Promise.all([
      inFlight?.done,
      refreshResources(inFlight ? names.filter((name) => !inFlight.names.includes(name)) : names, { quiet: false }),
    ])
  }

  // Bumped whenever the workspace corpus changes (or clears): a corpus-scoped
  // loader drops a response that was requested under an older epoch, so a
  // slow answer for corpus A cannot repopulate the workspace after a switch.
  let corpusScopeEpoch = 0
  function invalidateCorpusScopedRequests() {
    corpusScopeEpoch += 1
    seedSourcesRequestToken += 1
    // The stale loadCorpus no longer owns the loading flags (its finally
    // checks the sequence), so release them for the next corpus's load.
    corpusLoadRequestSeq += 1
    corpusLoading = false
    corpusLoadingMore = false
    corpusAppendQueued = false
    lastCorpusPageSignature = ''
    resourcesInFlight.clear()
  }

  // Every corpus- and session-scoped value the workspace starts from. A
  // corpus switch or a sign-out goes back to exactly this.
  function initialWorkspaceState() {
    return {
      ingestStats: { raw_pending: 0, matched: 0, queued_download: 0, downloaded: 0 },
      ingestStatsStatus: '',
      ingestRuns: [],
      ingestRunsStatus: '',
      seedSources: [],
      seedSourcesStatus: '',
      expandedSeedSourceId: '',
      seedCandidatesBySource: {},
      seedCandidatesLoading: {},
      seedSelections: {},
      seedSorts: {},
      selectedSeedCandidateKeys: {},
      seedPages: {},
      seedAllSelected: {},
      seedAppending: {},
      seedActionStatus: '',
      seedActionBusy: false,
      seedLastRunKey: '',
      seedFilterQuery: '',
      seedWarningMessage: '',
      searchQuery: '',
      searchMaxResults: DEFAULT_SEARCH_MAX_RESULTS,
      searchStatus: '',
      searchPreview: null,
      searchWarning: false,
      startedSearchRunIds: new Set(),
      uploads: [],
      corpusItems: [],
      corpusTotal: 0,
      corpusHasMore: false,
      corpusFilterQuery: '',
      corpusLoadStatus: '',
      corpusSource: '',
      corpusRefreshFailed: false,
      corpusStageTotals: { raw: 0, metadata: 0, downloaded: 0, failed_enrichment: 0, failed_download: 0 },
      downloads: [],
      downloadsSource: '',
      downloadsQuery: '',
      workerBacklog: { raw_pending: 0, enriching: 0, matched: 0, queued_download: 0, downloading: 0, downloaded: 0, failed_enrichment: 0, failed_download: 0 },
      exportStatus: '',
      shareUsername: '',
      shareRole: 'viewer',
      shareStatus: '',
      logs: [],
      logsCursor: 0,
      logsHasMore: false,
      logsTailStatus: '',
      corpusActionStatus: '',
      corpusActionError: false,
      corpusSwitchError: '',
    }
  }

  // `signOut` also drops what belongs to the user rather than the corpus: the
  // admin settings (and their unsaved draft, which may hold API keys), the
  // invitation form, the instance-wide overview and the view choices (sorts,
  // filters, promotion mode). A corpus switch keeps them.
  function resetFrontendState({ signOut = false } = {}) {
    invalidateCorpusScopedRequests()
    clearTimeout(seedFilterDebounce)
    clearTimeout(corpusFilterDebounce)
    startedSearchRunMisses.clear()
    seedSortCache.clear()
    resetLogState()
    ;({
      ingestStats,
      ingestStatsStatus,
      ingestRuns,
      ingestRunsStatus,
      seedSources,
      seedSourcesStatus,
      expandedSeedSourceId,
      seedCandidatesBySource,
      seedCandidatesLoading,
      seedSelections,
      seedSorts,
      selectedSeedCandidateKeys,
      seedPages,
      seedAllSelected,
      seedAppending,
      seedActionStatus,
      seedActionBusy,
      seedLastRunKey,
      seedFilterQuery,
      seedWarningMessage,
      searchQuery,
      searchMaxResults,
      searchStatus,
      searchPreview,
      searchWarning,
      startedSearchRunIds,
      uploads,
      corpusItems,
      corpusTotal,
      corpusHasMore,
      corpusFilterQuery,
      corpusLoadStatus,
      corpusSource,
      corpusRefreshFailed,
      corpusStageTotals,
      downloads,
      downloadsSource,
      downloadsQuery,
      workerBacklog,
      exportStatus,
      shareUsername,
      shareRole,
      shareStatus,
      logs,
      logsCursor,
      logsHasMore,
      logsTailStatus,
      corpusActionStatus,
      corpusActionError,
      corpusSwitchError,
    } = initialWorkspaceState())
    resetTopicSearch()
    resetKantroposAssignmentState()
    resetUpstreamBrowseState()
    finishExtractionProgress()
    if (signOut) {
      // null makes the Admin tab fetch the settings again.
      appSettings = null
      appSettingsDraft = {}
      appSettingsStatus = ''
      appSettingsError = false
      inviteUsername = ''
      inviteEmail = ''
      inviteStatus = ''
      inviteError = false
      globalIngestStats = { raw_pending: 0, matched: 0, queued_download: 0, downloaded: 0 }
      globalWorkerBacklog = { raw_pending: 0, enriching: 0, matched: 0, queued_download: 0, downloading: 0, downloaded: 0, failed_enrichment: 0, failed_download: 0 }
      kantroposTargets = []
      selectedUpstreamTargetId = ''
      openalexQuota = null
      // The signed-out user's view choices.
      logsType = 'pipeline'
      logsFilterCurrentCorpus = true
      searchField = 'default'
      searchAuthor = ''
      yearFrom = ''
      yearTo = ''
      searchSort = 'relevance'
      relatedSort = 'most_cited'
      promotionMode = 'new_seed'
      corpusSort = ''
      downloadsStatusFilter = 'all'
      exportFilterStatus = 'all'
      exportFilterYearFrom = ''
      exportFilterYearTo = ''
      exportFilterSource = ''
    }
  }

  async function handleSelectCorpus(event) {
    corpusActionStatus = ''
    corpusActionError = false
    corpusSwitchError = ''
    const picker = event.target
    const nextId = Number(picker.value)
    if (!nextId) return
    // An answer that arrives after a sign-out (or a newer sign-in) is dropped.
    const epoch = sessionEpoch
    try {
      await selectCorpus(nextId)
      const me = await fetchMe()
      if (epoch !== sessionEpoch) return
      authUser = me.user
      corpora = me.corpora || []
      currentCorpusId = authUser?.last_corpus_id || nextId
      resetFrontendState()
      await refreshAll()
      if (epoch !== sessionEpoch) return
      if (logsType === 'pipeline') {
        handleLogsCorpusFilterChange()
      }
    } catch (error) {
      if (error?.status === 401 || epoch !== sessionEpoch) return
      corpusActionStatus = error.message || 'Failed to select corpus.'
      corpusActionError = true
      corpusSwitchError = corpusActionStatus
      // The select's value is one-way: put it back on the corpus still active.
      picker.value = String(currentCorpusId || '')
    }
  }

  async function handleCreateCorpus() {
    if (creatingCorpus) return
    corpusActionStatus = ''
    corpusActionError = false
    const name = window.prompt('New corpus name')
    if (!name) return
    creatingCorpus = true
    const epoch = sessionEpoch
    try {
      await createCorpus(name.trim())
      const me = await fetchMe()
      if (epoch !== sessionEpoch) return
      authUser = me.user
      corpora = me.corpora || []
      currentCorpusId = authUser?.last_corpus_id || currentCorpusId
      resetFrontendState()
      await refreshAll()
      if (epoch !== sessionEpoch) return
      if (logsType === 'pipeline') {
        handleLogsCorpusFilterChange()
      }
      corpusActionStatus = `Created corpus "${name.trim()}".`
      corpusActionError = false
    } catch (error) {
      if (error?.status === 401 || epoch !== sessionEpoch) return
      corpusActionStatus = error.message || 'Failed to create corpus.'
      corpusActionError = true
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
    const epoch = sessionEpoch
    try {
      const payload = await deleteCorpusApi(currentCorpusId)
      const me = await fetchMe()
      if (epoch !== sessionEpoch) return
      authUser = me.user
      corpora = me.corpora || []
      currentCorpusId = authUser?.last_corpus_id || payload?.selected_corpus_id || corpora[0]?.id || null
      resetFrontendState()
      await refreshAll()
      if (epoch !== sessionEpoch) return
      if (logsType === 'pipeline') {
        handleLogsCorpusFilterChange()
      }
      const deletedName = payload?.deleted_name || corpusName
      corpusActionStatus = `Deleted corpus "${deletedName}".`
      corpusActionError = false
    } catch (error) {
      if (error?.status === 401 || epoch !== sessionEpoch) return
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
      if (error?.status === 401) return
      shareStatus = error.message || 'Failed to share corpus.'
    }
  }

  // One request for the target list at a time: the browse and the assignment
  // load both need it and may ask together.
  let kantroposTargetsPromise = null
  function loadKantroposTargets() {
    if (!kantroposTargetsPromise) {
      kantroposTargetsPromise = fetchKantroposTargets().finally(() => {
        kantroposTargetsPromise = null
      })
    }
    return kantroposTargetsPromise
  }

  async function fetchKantroposTargets() {
    try {
      const payload = await fetchKantroposCorpora()
      kantroposTargets = payload?.corpora || []
      const hasCurrentSelection = kantroposTargets.some((target) => String(target.id) === String(selectedUpstreamTargetId))
      if (!hasCurrentSelection) {
        const preferredTargetId = kantroposAssignment?.target_id || kantroposTargets[0]?.id || ''
        selectedUpstreamTargetId = String(preferredTargetId || '')
      }
    } catch (error) {
      if (error?.status === 401) return
      kantroposTargets = []
      kantroposAssignmentStatus = error.message || 'Failed to load Kantropos corpora.'
      kantroposAssignmentError = true
      if (!selectedUpstreamTargetId) {
        selectedUpstreamTargetId = ''
      }
    }
  }

  async function loadUpstreamBrowse({ quiet = false, appendCurrent = false, appendPending = false } = {}) {
    const requestSeq = ++upstreamBrowseRequestSeq
    const epoch = corpusScopeEpoch
    if (!quiet) {
      upstreamBrowseStatus = upstreamBrowse.target ? 'Refreshing upstream corpus browser...' : 'Loading upstream corpus browser...'
    }
    upstreamBrowseLoading = true
    try {
      // The assignment card lives on this tab only: load it with the browse
      // (once per corpus for the quiet refreshes, so the card does not blink).
      // It asks for the target list itself; an empty or failed list is not
      // asked for twice in one load.
      let targetsChecked = false
      if (!appendCurrent && !appendPending && (!quiet || kantroposAssignmentCorpusId !== Number(currentCorpusId))) {
        targetsChecked = await loadKantroposAssignment()
        if (requestSeq !== upstreamBrowseRequestSeq) return
      }
      if (!targetsChecked && kantroposTargets.length === 0) {
        await loadKantroposTargets()
      }
      const targetId = String(selectedUpstreamTargetId || kantroposAssignment?.target_id || kantroposTargets[0]?.id || '').trim()
      if (!targetId) {
        if (!quiet) upstreamBrowseStatus = 'No upstream corpora are configured.'
        resetUpstreamBrowseState()
        return
      }
      selectedUpstreamTargetId = targetId
      const nextCurrentLimit = appendCurrent
        ? Math.max(CORPUS_PAGE_SIZE, Number(upstreamCurrentLimit || CORPUS_PAGE_SIZE) + CORPUS_PAGE_SIZE)
        : Math.max(CORPUS_PAGE_SIZE, Number(upstreamCurrentLimit || CORPUS_PAGE_SIZE))
      const nextPendingLimit = appendPending
        ? Math.max(CORPUS_PAGE_SIZE, Number(upstreamPendingLimit || CORPUS_PAGE_SIZE) + CORPUS_PAGE_SIZE)
        : Math.max(CORPUS_PAGE_SIZE, Number(upstreamPendingLimit || CORPUS_PAGE_SIZE))
      const payload = await fetchKantroposCorpusBrowse(targetId, {
        currentLimit: nextCurrentLimit,
        pendingLimit: nextPendingLimit,
      })
      if (requestSeq !== upstreamBrowseRequestSeq || String(selectedUpstreamTargetId) !== targetId) return
      upstreamCurrentLimit = nextCurrentLimit
      upstreamPendingLimit = nextPendingLimit

      const assignedCorpora = Array.isArray(payload?.assigned_corpora) ? payload.assigned_corpora : []
      const stillValidAssignedFilter = assignedCorpora.some((entry) => Number(entry.id) === Number(selectedUpstreamAssignedCorpusId))
      if (!stillValidAssignedFilter) {
        selectedUpstreamAssignedCorpusId = null
      }

      upstreamBrowse = {
        target: payload?.target || null,
        summary: payload?.summary || {},
        assigned_corpora: assignedCorpora,
        current_items: (Array.isArray(payload?.current_items) ? payload.current_items : []).map((item) => normalizeUpstreamBrowseItem(item)),
        pending_items: (Array.isArray(payload?.pending_items) ? payload.pending_items : []).map((item) => normalizeUpstreamBrowseItem(item)),
        graph: payload?.graph || { nodes: [], edges: [], stats: {} },
      }
      if (!quiet) {
        upstreamBrowseStatus = 'Loaded upstream corpus browser.'
      }
    } catch (error) {
      if (error?.status === 401 || requestSeq !== upstreamBrowseRequestSeq || epoch !== corpusScopeEpoch) return
      if (!quiet) {
        upstreamBrowseStatus = error?.message || 'Failed to load upstream corpus browser.'
      }
    } finally {
      if (requestSeq === upstreamBrowseRequestSeq) {
        upstreamBrowseLoading = false
      }
    }
  }

  async function handleSelectUpstreamTarget(targetId) {
    const normalized = String(targetId || '').trim()
    if (!normalized || normalized === String(selectedUpstreamTargetId || '').trim()) return
    selectedUpstreamTargetId = normalized
    selectedUpstreamAssignedCorpusId = null
    await loadUpstreamBrowse()
  }

  function handleToggleUpstreamAssignedCorpus(corpusId) {
    if (corpusId === null || corpusId === undefined || corpusId === '') {
      selectedUpstreamAssignedCorpusId = null
      return
    }
    const normalized = Number(corpusId)
    if (!Number.isFinite(normalized) || normalized <= 0) {
      selectedUpstreamAssignedCorpusId = null
      return
    }
    selectedUpstreamAssignedCorpusId = Number(selectedUpstreamAssignedCorpusId) === normalized ? null : normalized
  }

  // True once the target list was checked (with no corpus there is nothing to
  // load, so the caller still has to).
  async function loadKantroposAssignment() {
    const corpusId = Number(currentCorpusId)
    const requestSeq = ++kantroposAssignmentRequestSeq
    resetKantroposAssignmentState()
    if (!Number.isFinite(corpusId) || corpusId <= 0) return false
    kantroposAssignmentLoading = true
    try {
      if (kantroposTargets.length === 0) {
        await loadKantroposTargets()
      }
      const payload = await fetchCorpusKantroposAssignment(corpusId)
      if (requestSeq !== kantroposAssignmentRequestSeq || Number(currentCorpusId) !== corpusId) return true
      kantroposTargets = payload?.corpora || kantroposTargets
      kantroposAssignment = payload?.assignment || null
      kantroposAssignmentCorpusId = corpusId
      selectedKantroposTargetId = payload?.assignment?.target_id || ''
      if (!payload?.can_edit && currentCorpus?.role !== 'viewer') {
        // no-op; backend truth is enough, UI editability still derives from current corpus role
      }
    } catch (error) {
      if (requestSeq !== kantroposAssignmentRequestSeq || Number(currentCorpusId) !== corpusId) return true
      kantroposAssignmentStatus = error.message || 'Failed to load Kantropos assignment.'
      kantroposAssignmentError = true
    } finally {
      if (requestSeq === kantroposAssignmentRequestSeq && Number(currentCorpusId) === corpusId) {
        kantroposAssignmentLoading = false
      }
    }
    return true
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
      if (error?.status === 401) return
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

  function seedKindForName(name) {
    const ext = String(name || '').toLowerCase().split('.').pop()
    if (ext === 'bib') return 'bib'
    if (ext === 'json') return 'json'
    return null
  }

  async function uploadItem(item) {
    updateUpload(item.id, { status: 'uploading', message: '' })
    try {
      const payload = await uploadPdf(item.file)
      const seedKind = seedKindForName(item.name)
      if (seedKind) {
        // BibTeX/JSON seeds are imported straight into the corpus (no PDF
        // bibliography extraction step applies).
        updateUpload(item.id, { status: 'importing', backendFilename: payload.filename, message: 'Importing seed…' })
        const result = await importSeed(payload.filename, seedKind)
        const linked = Number(result.corpus_linked || 0)
        updateUpload(item.id, {
          status: 'imported',
          backendFilename: payload.filename,
          message: `Imported ${result.added || 0} works (${result.skipped || 0} skipped${linked ? `, ${linked} added to corpus` : ''}).`,
        })
      } else {
        updateUpload(item.id, {
          status: 'uploaded',
          backendFilename: payload.filename,
          message: 'Uploaded',
        })
      }
      apiStatus = 'online'
    } catch (error) {
      if (error?.status === 401) {
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
    updateUpload(item.id, { status: 'extracting', message: 'Submitting extraction request...' })
    try {
      const payload = await extractBibliography(item.backendFilename)
      const state = String(payload?.state || 'started').trim().toLowerCase()
      if (state === 'queued') {
        updateUpload(item.id, { status: 'queued', message: 'Queued for extraction' })
      } else {
        updateUpload(item.id, { status: 'extracting', message: 'Extracting bibliography' })
        ensureExtractionRunActive(baseName, item.backendFilename)
      }
      apiStatus = 'online'
    } catch (error) {
      finishExtractionProgress()
      if (error?.status === 401) {
        updateUpload(item.id, { status: 'failed', message: 'Session expired. Please sign in again.' })
        return
      }
      updateUpload(item.id, { status: 'failed', message: error.message || 'Extraction failed' })
    }
  }

  async function loadIngestRuns({ quiet = false } = {}) {
    if (!quiet) ingestRunsStatus = 'Loading ingest runs...'
    const epoch = corpusScopeEpoch
    try {
      const payload = await fetchIngestRuns(20)
      if (epoch !== corpusScopeEpoch) return
      ingestRuns = payload.runs || []
      pruneExtractedUploads()
      ingestRunsStatus = ''
    } catch (error) {
      if (error?.status === 401) return
      if (epoch !== corpusScopeEpoch) return
      ingestRunsStatus = 'Failed to load ingest runs.'
    }
  }

  async function loadIngestStats({ quiet = false, global = false } = {}) {
    if (!quiet) ingestStatsStatus = 'Loading ingest stats...'
    const epoch = corpusScopeEpoch
    try {
      const payload = await fetchIngestStats({ global })
      if (!global && epoch !== corpusScopeEpoch) return
      if (global) {
        globalIngestStats = payload.stats || globalIngestStats
      } else {
        ingestStats = payload.stats || ingestStats
      }
      if (!quiet) ingestStatsStatus = ''
      apiStatus = 'online'
    } catch (error) {
      if (error?.status === 401) return
      // Requested for a corpus the workspace has left: change nothing.
      if (!global && epoch !== corpusScopeEpoch) return
      if (!global) handleCorpusPollForbidden(error)
      if (!quiet) ingestStatsStatus = 'Failed to load ingest stats.'
      apiStatus = error?.status === 403 ? 'online' : 'offline'
    }
  }

  function seedSourceId(source) {
    if (!source) return ''
    return String(source.id || `${source.source_type}:${source.source_key}`)
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
      downloaded_elsewhere_available: 0,
      downloaded_elsewhere_missing: 0,
      failed_enrichment: 0,
      failed_download: 0,
    }
    for (const candidate of candidates) {
      if (isSeedCandidateInCorpus(candidate)) {
        counts.in_corpus += 1
      }
      const state = workState(candidate?.state)
      counts[state] = (counts[state] || 0) + 1
      if (state === 'downloaded_elsewhere') {
        if (candidate?.file_available === true) {
          counts.downloaded_elsewhere_available += 1
        } else {
          counts.downloaded_elsewhere_missing += 1
        }
      }
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
      seedSortCache.delete(sourceId)
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
    // A paged seed (e.g. a 70,000-item search) only has its first page(s)
    // loaded: counting those would overwrite the server's totals. Recount
    // client-side only once the seed is fully loaded.
    const page = seedPages[sourceId]
    if (page && nextCandidates.length < page.total) return
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
    switch (workState(candidate?.state)) {
      case 'downloaded':
        return {
          label: candidate?.file_available === false ? 'File missing' : 'PDF downloaded',
          className: candidate?.file_available === false ? 'failed' : 'downloaded',
        }
      case 'downloaded_elsewhere':
        return candidate?.file_available === true
          ? { label: 'PDF downloaded', className: 'downloaded', hint: 'Downloaded by another corpus and the file is present — promoting reuses it' }
          : { label: 'Stale record', className: 'pending', hint: 'Another corpus recorded a download but the file is gone — promoting downloads it fresh' }
      case 'queued_download':
        return { label: 'Download queued', className: 'queued' }
      case 'enriched':
        return { label: 'Metadata ready', className: 'completed' }
      case 'queued_enrichment':
        return { label: 'Fetching metadata', className: 'queued' }
      case 'staged_raw':
        return { label: 'Awaiting metadata', className: 'pending' }
      case 'added':
        return { label: 'In corpus', className: 'completed' }
      case 'failed_enrichment':
        return { label: 'Metadata not confirmed', className: 'failed', hint: 'No Crossref match found' }
      case 'failed_download':
        return { label: 'Not retrievable', className: 'failed', hint: 'Document not retrievable' }
      default:
        return { label: 'To review', className: 'pending' }
    }
  }

  // With downstream expansion at depth >= 1 an already-downloaded or already
  // in-corpus item is still worth promoting: the point is its references, not
  // the item itself. Depend on the reactive vars directly so toggling the
  // Downstream checkbox re-evaluates every row.
  function isSeedCandidateSelectable(candidate, downstreamOn = includeDownstream, downstreamDepth = relatedDepthDownstream) {
    if (downstreamOn && downstreamDepth >= 1) return true
    return workState(candidate?.state) !== 'downloaded' && !isSeedCandidateInCorpus(candidate)
  }

  $: expansionEnabled = (includeDownstream && relatedDepthDownstream >= 1) || (includeUpstream && relatedDepthUpstream >= 1)

  // Debounced so typing does not fire a request per keystroke. Both reset to
  // offset 0 implicitly: loadSeedSources reloads from scratch and loadCorpus
  // without `append` starts at offset 0.
  function handleSeedFilterInput(event) {
    seedFilterQuery = String(event?.target?.value || '')
    clearTimeout(seedFilterDebounce)
    seedFilterDebounce = setTimeout(() => {
      seedCandidatesBySource = {}
      seedPages = {}
      seedAllSelected = {}
      seedAppending = {}
      loadSeedSources({ quiet: true })
    }, 300)
  }

  function handleCorpusFilterInput(event) {
    corpusFilterQuery = String(event?.target?.value || '')
    clearTimeout(corpusFilterDebounce)
    corpusFilterDebounce = setTimeout(() => {
      loadCorpus({ quiet: true, supersede: true })
    }, 300)
  }

  // Unlinks the work from this corpus only — the work row and its PDF survive,
  // so it stays reusable and can be promoted again (spec line 23).
  async function handleRemoveCorpusWork(item) {
    const workId = Number(item?.work_id ?? item?.id)
    if (!Number.isFinite(workId) || workId <= 0) return
    try {
      await removeCorpusWork(workId)
      corpusItems = corpusItems.filter((entry) => Number(entry?.work_id ?? entry?.id) !== workId)
      corpusTotal = Math.max(0, corpusTotal - 1)
      await Promise.all([
        loadCorpus({ quiet: true, preserveSelection: true }),
        loadSeedSources({ quiet: true }),
        loadIngestStats({ quiet: true }),
      ])
    } catch (error) {
      if (error?.status === 401) return
      corpusLoadStatus = error?.message || 'Failed to remove the item from this corpus.'
    }
  }

  // Bulk counterpart of handleRemoveCorpusWork. Same semantics: unlink from
  // this corpus only, the works and their PDFs survive (spec line 23).
  async function handleRemoveSelectedCorpusWorks(workIds) {
    const ids = (Array.isArray(workIds) ? workIds : [])
      .map((value) => Number(value))
      .filter((value) => Number.isFinite(value) && value > 0)
    if (ids.length === 0) return
    try {
      const result = await removeCorpusWorks(ids)
      const removed = new Set((result?.removed_work_ids || ids).map((value) => Number(value)))
      corpusItems = corpusItems.filter((entry) => !removed.has(Number(entry?.work_id ?? entry?.id)))
      corpusTotal = Math.max(0, corpusTotal - removed.size)
      corpusLoadStatus = `Removed ${removed.size} item(s) from this corpus.`
      await Promise.all([
        loadCorpus({ quiet: true, preserveSelection: true }),
        loadSeedSources({ quiet: true }),
        loadIngestStats({ quiet: true }),
      ])
    } catch (error) {
      if (error?.status === 401) return
      corpusLoadStatus = error?.message || 'Failed to remove the selected items.'
    }
  }

  function toggleCorpusSort(column) {
    corpusSort = nextSort(corpusSort, column)
    loadCorpus({ quiet: true, supersede: true })
  }

  function getSeedCandidatesForSource(source) {
    return seedCandidatesBySource[seedSourceId(source)] || []
  }

  // Seed cells keep their '' blank, and a candidate without a state reads as
  // pending (see metadataLabel in lib/works).
  const SEED_CELL = { blank: '', seed: true }

  // Accessors used by the client-side sort (sortSeedCandidates), which only
  // applies to PDF seeds — those load in full. Search seeds page server-side
  // and sort server-side too (see toggleSeedSort / sortSeedCandidates).
  const SEED_SORT_ACCESSORS = {
    metadata: (candidate) => metadataLabel(candidate, SEED_CELL),
    download: (candidate) => downloadLabel(candidate, SEED_CELL),
    title: (candidate) => formatTitle(candidate),
    authors: (candidate) => formatAuthors(candidate),
    year: (candidate) => candidate?.year,
    source: (candidate) => candidate?.source || '',
    seed: (candidate) => candidate?.ingest_source || '',
    refs: (candidate) => candidate?.refs_count,
    cited_by: (candidate) => candidate?.cited_by_count,
  }

  function toggleSeedSort(source, column) {
    if (!SEED_SORT_ACCESSORS[column]) return
    const sourceId = seedSourceId(source)
    seedSorts = { ...seedSorts, [sourceId]: nextSort(seedSorts[sourceId], column) }
    // Search seeds sort server-side (the metadata/download axes require every
    // row resolved, which the client only has for the loaded page) — reload
    // page one under the new sort. PDF seeds keep the client-side sort below.
    if (source.source_type === 'search') {
      void loadSeedCandidatesForSource(source, { quiet: true })
    }
  }

  // `sorts` is passed in from the template rather than read off the module
  // binding so Svelte tracks seedSorts as a dependency of the markup.
  function seedSortIndicator(source, column, sorts = seedSorts) {
    return sortIndicator(sorts[seedSourceId(source)], column)
  }

  function sortSeedCandidates(source, candidates, sorts = seedSorts) {
    const sourceId = seedSourceId(source)
    const sortKey = String(sorts[sourceId] || '')
    const cached = seedSortCache.get(sourceId)
    if (cached && cached.candidates === candidates && cached.sortKey === sortKey) return cached.sorted
    const sorted = computeSortedSeedCandidates(source, candidates, sorts)
    seedSortCache.set(sourceId, { candidates, sortKey, sorted })
    return sorted
  }

  function computeSortedSeedCandidates(source, candidates, sorts) {
    // Search seeds are already sorted server-side across the full result set
    // (see toggleSeedSort); re-sorting the loaded page client-side would only
    // reorder what happens to be loaded, not the whole set. PDF seeds load in
    // full, so the client-side sort below still gives a correct order.
    if (source.source_type === 'search') return candidates
    const current = parseSort(sorts[seedSourceId(source)])
    const accessor = current && SEED_SORT_ACCESSORS[current.column]
    if (!accessor) return candidates
    const factor = current.direction === 'desc' ? -1 : 1
    return [...candidates].sort((left, right) => {
      const a = accessor(left)
      const b = accessor(right)
      const aMissing = a === null || a === undefined || a === ''
      const bMissing = b === null || b === undefined || b === ''
      // Blanks sort last in both directions so a descending sort does not fill
      // the top of the table with empty cells.
      if (aMissing && bMissing) return 0
      if (aMissing) return 1
      if (bMissing) return -1
      if (current.column === 'year' || current.column === 'refs' || current.column === 'cited_by') {
        return (Number(a) - Number(b)) * factor
      }
      return String(a).localeCompare(String(b), undefined, { sensitivity: 'base', numeric: true }) * factor
    })
  }

  function getSeedSelectionForSource(source) {
    return seedSelections[seedSourceId(source)] || EMPTY_SET
  }

  // Keys of the loaded candidates that can currently be selected.
  function loadedSelectableKeys(source) {
    return getSeedCandidatesForSource(source)
      .filter((candidate) => isSeedCandidateSelectable(candidate))
      .map((candidate) => String(candidate?.candidate_key || ''))
      .filter(Boolean)
  }

  // All-selected mode selects every selectable row (D4): its count is the
  // selectable estimate, not the seed total, and it always reads as full.
  function selectedSeedCount(source) {
    const sourceId = seedSourceId(source)
    return seedAllSelected[sourceId] ? estimatedSelectableSeedCount(source) : getSeedSelectionForSource(source).size
  }

  function isSeedFullySelected(source) {
    if (seedAllSelected[seedSourceId(source)]) return true
    const selectableCount = estimatedSelectableSeedCount(source)
    return selectableCount > 0 && selectedSeedCount(source) === selectableCount
  }

  function getSelectedSeedCandidate(source) {
    const sourceId = seedSourceId(source)
    const activeKey = selectedSeedCandidateKeys[sourceId]
    if (!activeKey) return null
    return getSeedCandidatesForSource(source).find((candidate) =>
      String(candidate?.candidate_key || '') === activeKey
    ) || null
  }

  function selectableSeedCount(source, loaded = getSeedCandidatesForSource(source), downstreamOn = includeDownstream, downstreamDepth = relatedDepthDownstream) {
    return loaded.filter((candidate) => isSeedCandidateSelectable(candidate, downstreamOn, downstreamDepth)).length
  }

  function estimatedSelectableSeedCount(
    source,
    page = seedPages[seedSourceId(source)],
    loaded = seedCandidatesBySource[seedSourceId(source)],
    loadedSelectableCount = null,
    downstreamOn = includeDownstream,
    downstreamDepth = relatedDepthDownstream,
  ) {
    const total = page ? page.total : Number(source?.candidate_count || 0)
    // Once every row is actually loaded, count selectability exactly instead
    // of estimating — isSeedCandidateSelectable excludes more than just
    // in_corpus/downloaded (e.g. the includeDownstream override), so this is
    // strictly more accurate than any formula below.
    if (page && Array.isArray(loaded) && loaded.length >= total) {
      return loadedSelectableCount ?? selectableSeedCount(source, loaded, downstreamOn, downstreamDepth)
    }
    // Downstream expansion makes every item selectable (see
    // isSeedCandidateSelectable), including the rows not loaded yet.
    if (downstreamOn && downstreamDepth >= 1) return total
    // state_counts is not resolved server-side above the metadata/download
    // sort limit (Task 5) — in that case the total is the best estimate.
    const stateCounts = source?.state_counts
    if (stateCounts == null) return total
    // isSeedCandidateSelectable excludes exactly two things: already in the
    // corpus, or state === 'downloaded' (not downloaded_elsewhere, which
    // stays selectable) — subtract both, matching summarizeStates' key names.
    const inCorpus = Number(stateCounts.in_corpus || 0)
    const downloaded = Number(stateCounts.downloaded || 0)
    return Math.max(0, total - inCorpus - downloaded)
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
    const current = getSeedSelectionForSource(source)
    const isSelected = current.has(candidateKey)
    if (isSelected && seedAllSelected[sourceId]) {
      // Deselecting one row out of "all N selected" drops into explicit mode:
      // everything else that's loaded stays selected, just not this row.
      seedAllSelected = { ...seedAllSelected, [sourceId]: false }
      const next = new Set(loadedSelectableKeys(source))
      next.delete(candidateKey)
      seedSelections = { ...seedSelections, [sourceId]: next }
      return
    }
    const next = new Set(current)
    if (isSelected) next.delete(candidateKey)
    else next.add(candidateKey)
    seedSelections = { ...seedSelections, [sourceId]: next }
  }

  function clearSeedSelection(source) {
    const sourceId = seedSourceId(source)
    if (!sourceId) return
    seedSelections = { ...seedSelections, [sourceId]: new Set() }
    seedAllSelected = { ...seedAllSelected, [sourceId]: false }
  }

  function selectAllSeedCandidates(source) {
    const sourceId = seedSourceId(source)
    if (!sourceId) return
    seedSelections = { ...seedSelections, [sourceId]: new Set(loadedSelectableKeys(source)) }
  }

  async function setAllSeedCandidatesSelected(source, checked) {
    if (!checked) {
      clearSeedSelection(source)
      return
    }
    const sourceId = seedSourceId(source)
    if (!sourceId) return
    // Search seeds can hold far more items than are loaded — "select all"
    // there means every promotable/dismissable item server-side, tracked by
    // seedAllSelected rather than by materializing every candidate key.
    if (source.source_type === 'search') {
      seedAllSelected = { ...seedAllSelected, [sourceId]: true }
    }
    if (!Array.isArray(seedCandidatesBySource[sourceId]) || seedCandidatesBySource[sourceId].length === 0) {
      await loadSeedCandidatesForSource(source, { quiet: true })
    }
    selectAllSeedCandidates(source)
  }

  // Monotonic request tokens: the 2s live poll can have several seed-list
  // requests in flight at once, and an older, slower response overwriting a
  // newer one used to resurrect stale run status and lose a just-started run.
  let seedSourcesRequestToken = 0

  async function loadSeedSources({ quiet = false } = {}) {
    if (!quiet) seedSourcesStatus = 'Loading seeds...'
    const token = ++seedSourcesRequestToken
    try {
      const payload = await fetchSeedSources(100, { q: seedFilterQuery })
      if (token !== seedSourcesRequestToken) return
      seedSources = payload.sources || []
      reconcileStartedSearchRuns(seedSources)
      const validSourceIds = new Set(seedSources.map((source) => seedSourceId(source)))
      if (expandedSeedSourceId && !validSourceIds.has(expandedSeedSourceId)) {
        expandedSeedSourceId = ''
      }
      seedSelections = Object.fromEntries(
        Object.entries(seedSelections).filter(([key]) => validSourceIds.has(key))
      )
      for (const key of seedSortCache.keys()) {
        if (!validSourceIds.has(key)) seedSortCache.delete(key)
      }
      if (!quiet) {
        seedSourcesStatus = seedSources.length > 0
          ? `Loaded ${seedSources.length} seed${seedSources.length === 1 ? '' : 's'}.`
          : 'No seeds yet.'
      }
      // Only re-pull the expanded seed's rows while something is actually
      // filling in. The workspace polls the (cheap) seed list continuously,
      // but an idle expanded seed has nothing new to show, and re-requesting
      // it every cycle made the backend re-resolve the whole seed forever.
      if (expandedSeedSourceId) {
        const expandedSource = seedSources.find((source) => seedSourceId(source) === expandedSeedSourceId)
        if (expandedSource && (anySearchRunning || expandedSource?.run?.status === 'running')) {
          await loadSeedCandidatesForSource(expandedSource, { quiet: true, background: true })
        }
      }
    } catch (error) {
      if (error?.status === 401) return
      handleCorpusPollForbidden(error)
      if (token !== seedSourcesRequestToken) return
      if (!quiet) seedSourcesStatus = error?.message || 'Failed to load seeds.'
    }
  }

  function runSubtitle(source) {
    const run = source?.run
    if (!run || !run.status || run.status === 'done') return ''
    const fetched = Number(run.fetched_count || 0).toLocaleString('en-US')
    const expected = run.expected_count === null || run.expected_count === undefined ? '…' : Number(run.expected_count).toLocaleString('en-US')
    if (run.status === 'running') return `fetching ${fetched} of ${expected}`
    if (run.status === 'failed') return `stopped after ${fetched} of ${expected}: ${run.error || 'unknown error'}`
    if (run.status === 'cancelled') return `cancelled at ${fetched} of ${expected}`
    return ''
  }

  async function handleCancelSearch(source) {
    try {
      await cancelKeywordSearch(source.source_key)
      seedSourcesStatus = 'Cancelling search…'
      await loadSeedSources({ quiet: true })
    } catch (error) {
      if (error?.status === 401) return
      seedSourcesStatus = error?.message || 'Could not cancel the search.'
    }
  }

  // Per-seed request tokens, same reason as seedSourcesRequestToken: a
  // background reload racing a user-driven "Show more" must not win.
  const seedCandidatesRequestTokens = new Map()

  async function loadSeedCandidatesForSource(source, { quiet = false, background = false, append = false } = {}) {
    const sourceId = seedSourceId(source)
    if (!sourceId) return
    const token = (seedCandidatesRequestTokens.get(sourceId) || 0) + 1
    seedCandidatesRequestTokens.set(sourceId, token)
    const epoch = corpusScopeEpoch
    const hasCachedCandidates = Array.isArray(seedCandidatesBySource[sourceId])
    const showLoadingState = !background || !hasCachedCandidates
    if (showLoadingState) {
      seedCandidatesLoading = { ...seedCandidatesLoading, [sourceId]: true }
    }
    try {
      const currentlyLoaded = seedCandidatesBySource[sourceId] || []
      const offset = append ? currentlyLoaded.length : 0
      // A background (non-append) reload — e.g. loadSeedSources polling the
      // expanded seed — must not silently drop pages the user already
      // fetched via "Show more": re-request at least as much as is currently
      // loaded (the backend clamps to 2000 regardless).
      const limit = background && !append ? Math.max(SEED_PAGE_SIZE, currentlyLoaded.length) : SEED_PAGE_SIZE
      const payload = await fetchSeedCandidates(source.source_type, source.source_key, {
        q: seedFilterQuery, limit, offset, ...seedSortParams(source),
        // A background poll never reads source_summary; skipping its state
        // counts saves a full-seed resolution per poll.
        lightSummary: background,
      })
      if (token !== seedCandidatesRequestTokens.get(sourceId) || epoch !== corpusScopeEpoch) return
      const incoming = payload.candidates || []
      // A run still filling in shifts rows down under `sr.id DESC`, so the
      // page at offset=loaded.length can overlap what is already on screen.
      // Appending it blind produced duplicate keys (and duplicate rows).
      let nextCandidates = incoming
      if (append) {
        const known = new Set(currentlyLoaded.map((c) => String(c?.candidate_key || '')).filter(Boolean))
        const fresh = incoming.filter((candidate) => {
          const key = String(candidate?.candidate_key || '')
          if (!key || known.has(key)) return false
          known.add(key)
          return true
        })
        nextCandidates = [...currentlyLoaded, ...fresh]
      }
      seedCandidatesBySource = {
        ...seedCandidatesBySource,
        [sourceId]: nextCandidates,
      }
      seedPages = {
        ...seedPages,
        [sourceId]: { total: Number(payload.total || nextCandidates.length), offset, limit: Number(payload.limit || limit) },
      }
      reconcileSeedSourceCandidates(sourceId, nextCandidates)
      if (nextCandidates.length === 0) {
        return
      }
      // Keep only selected keys that are still loaded and selectable.
      const selectable = new Set(loadedSelectableKeys(source))
      const current = getSeedSelectionForSource(source)
      if ([...current].some((key) => !selectable.has(key))) {
        seedSelections = {
          ...seedSelections,
          [sourceId]: new Set([...current].filter((key) => selectable.has(key))),
        }
      }
      if (!quiet) {
        seedActionStatus = ''
      }
    } catch (error) {
      if (error?.status !== 401 && !quiet && token === seedCandidatesRequestTokens.get(sourceId) && epoch === corpusScopeEpoch) {
        seedActionStatus = error?.message || 'Failed to load seed candidates.'
      }
    } finally {
      if (showLoadingState && epoch === corpusScopeEpoch) {
        seedCandidatesLoading = { ...seedCandidatesLoading, [sourceId]: false }
      }
    }
  }

  // background:true means loadSeedCandidatesForSource does not toggle
  // seedCandidatesLoading when candidates are already cached (it only shows
  // that spinner state on a cold load), so the footer button needs its own
  // in-flight flag — otherwise a double-click fires two appends at the same
  // offset, producing duplicate candidate_key rows in the keyed {#each} and
  // a Svelte "keys must be unique" crash.
  async function loadMoreSeedCandidates(source) {
    const sourceId = seedSourceId(source)
    if (!sourceId || seedAppending[sourceId]) return
    seedAppending = { ...seedAppending, [sourceId]: true }
    try {
      await loadSeedCandidatesForSource(source, { quiet: true, background: true, append: true })
      if (seedAllSelected[sourceId]) {
        // Keep "all N selected" visually consistent: newly appended rows
        // should render checked too, not just the rows that were loaded
        // when the checkbox was first ticked.
        selectAllSeedCandidates(source)
      }
    } finally {
      seedAppending = { ...seedAppending, [sourceId]: false }
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

  function jumpToSection(id) {
    const el = document.getElementById(id)
    if (!el) return
    const behavior = prefersReducedMotion ? 'auto' : 'smooth'
    el.scrollIntoView({ behavior, block: 'start' })
  }

  function collapseExpandedSeed() {
    expandedSeedSourceId = ''
    jumpToSection('section-seed')
  }

  async function focusSeedSource(sourceType, sourceKey) {
    const nextSourceId = `${sourceType}:${sourceKey}`
    // The seed belongs to the corpus current at entry; after a corpus reset
    // during the reload, a same-named seed of the new corpus is not it.
    const epoch = corpusScopeEpoch
    if (!seedSources.some((source) => seedSourceId(source) === nextSourceId)) {
      await loadSeedSources({ quiet: true })
      if (epoch !== corpusScopeEpoch) return
    }
    const source = seedSources.find((entry) => seedSourceId(entry) === nextSourceId)
    if (!source) return
    const hasCachedCandidates = Array.isArray(seedCandidatesBySource[nextSourceId])
    expandedSeedSourceId = nextSourceId
    seedLastRunKey = nextSourceId
    await loadSeedCandidatesForSource(source, {
      quiet: true,
      background: hasCachedCandidates,
    })
  }

  // Shared promote path for the selection-scoped bulk button and the per-row
  // inline promote. Both must honour the same Promotion Settings, so the
  // expansion block lives here rather than being duplicated per caller.
  async function promoteSeedCandidateKeys(source, candidateKeys, { clearSelection = false } = {}) {
    const sourceId = seedSourceId(source)
    if (candidateKeys.length === 0) {
      seedActionStatus = 'Select at least one seed candidate to promote.'
      return
    }
    seedActionBusy = true
    seedWarningMessage = ''
    seedActionStatus = `Promoting ${candidateKeys.length} candidate(s) into the corpus...`
    try {
      const result = await promoteSeedCandidates(source.source_type, source.source_key, {
        candidateKeys,
        includeDownstream,
        includeUpstream,
        relatedDepthDownstream,
        relatedDepthUpstream,
        maxRelated,
        relatedSort,
        promotionMode,
        enqueueDownload: true,
        downloadBatchSize: Math.max(25, candidateKeys.length),
        workers: 6,
      })
      if (clearSelection) clearSeedSelection(source)
      const refreshTasks = [
        loadSeedSources({ quiet: true }),
        loadIngestStats({ quiet: true }),
        loadCorpus({ preserveSelection: true, quiet: true }),
      ]
      // The daemon status also fills the download worker card.
      if (diagnosticsEnabled) refreshTasks.push(loadPipelineWorkerStatus({ quiet: true }))
      await Promise.all(refreshTasks)
      const refreshedSource = seedSources.find((entry) => seedSourceId(entry) === sourceId)
      if (refreshedSource) {
        await loadSeedCandidatesForSource(refreshedSource, { quiet: true })
      }
      const status = seedPromotionStatus(result, candidateKeys.length)
      seedActionStatus = status.text
      seedWarningMessage = status.warning ? status.text : ''
    } catch (error) {
      if (error?.status === 401) return
      seedActionStatus = error?.message || 'Failed to promote seed candidates.'
    } finally {
      seedActionBusy = false
      void refreshQuota()
    }
  }

  async function handlePromoteSeedSource(source) {
    const sourceId = seedSourceId(source)
    if (seedAllSelected[sourceId]) {
      await handlePromoteWholeSeedSource(source)
      return
    }
    const candidateKeys = [...getSeedSelectionForSource(source)]
    await promoteSeedCandidateKeys(source, candidateKeys, { clearSelection: true })
  }

  async function handlePromoteSingleSeedCandidate(source, candidate) {
    const candidateKey = String(candidate?.candidate_key || '')
    if (!candidateKey) return
    await promoteSeedCandidateKeys(source, [candidateKey])
  }

  // Sends no candidateKeys: the backend resolves "every promotable item in
  // this source" itself, so this is correct even when only one page of a
  // large search seed is loaded client-side — exactly the semantics
  // "all N items selected" needs (handlePromoteSeedSource delegates here).
  async function handlePromoteWholeSeedSource(source) {
    seedActionBusy = true
    try {
      const sourceId = seedSourceId(source)
      if (!Array.isArray(seedCandidatesBySource[sourceId])) {
        await loadSeedCandidatesForSource(source, { quiet: true })
      }
      const candidateCount = estimatedSelectableSeedCount(source)
      if (candidateCount === 0) {
        seedActionStatus = 'No promotable seed candidates remain in this source.'
        return
      }
      seedActionStatus = `Promoting ${candidateCount.toLocaleString('en-US')} candidate(s) into the corpus...`
      seedWarningMessage = ''
      const result = await promoteSeedCandidates(source.source_type, source.source_key, {
        q: seedFilterQuery,
        includeDownstream,
        includeUpstream,
        relatedDepthDownstream,
        relatedDepthUpstream,
        maxRelated,
        relatedSort,
        promotionMode,
        enqueueDownload: true,
        downloadBatchSize: Math.max(25, candidateCount),
        workers: 6,
      })
      clearSeedSelection(source)
      const refreshTasks = [
        loadSeedSources({ quiet: true }),
        loadIngestStats({ quiet: true }),
        loadCorpus({ preserveSelection: true, quiet: true }),
      ]
      // The daemon status also fills the download worker card.
      if (diagnosticsEnabled) refreshTasks.push(loadPipelineWorkerStatus({ quiet: true }))
      await Promise.all(refreshTasks)
      const refreshedSource = seedSources.find((entry) => seedSourceId(entry) === sourceId)
      if (refreshedSource) {
        await loadSeedCandidatesForSource(refreshedSource, { quiet: true })
      }
      const status = seedPromotionStatus(result, candidateCount)
      seedActionStatus = status.text
      seedWarningMessage = status.warning ? status.text : ''
    } catch (error) {
      if (error?.status === 401) return
      // estimatedSelectableSeedCount is just that — an estimate. When it
      // undercounts already-excluded items (e.g. state_counts unresolved
      // above the summarization limit) the backend can still come back with
      // "nothing left to promote"; surface the friendly message rather than
      // the raw 400 body.
      seedActionStatus = String(error?.message || '').includes('No selectable seed candidates found for promotion')
        ? 'No promotable seed candidates remain in this source.'
        : error?.message || 'Failed to promote seed candidates.'
    } finally {
      seedActionBusy = false
      void refreshQuota()
    }
  }

  async function handleDismissSelectedSeed(source) {
    const sourceId = seedSourceId(source)
    const allSelected = Boolean(seedAllSelected[sourceId])
    const candidateKeys = [...getSeedSelectionForSource(source)]
    if (!allSelected && candidateKeys.length === 0) {
      seedActionStatus = 'Select at least one seed candidate to dismiss.'
      return
    }
    const dismissLabel = allSelected ? `all ${seedPage(source).total.toLocaleString('en-US')}` : String(candidateKeys.length)
    seedActionBusy = true
    seedActionStatus = `Removing ${dismissLabel} candidate(s) from Seed...`
    try {
      if (allSelected) {
        await dismissSeedCandidatesApi(source.source_type, source.source_key, [], { all: true, q: seedFilterQuery })
      } else {
        await dismissSeedCandidatesApi(source.source_type, source.source_key, candidateKeys)
      }
      clearSeedSelection(source)
      await Promise.all([
        loadIngestStats({ quiet: true }),
      ])
      const refreshedSource = seedSources.find((entry) => seedSourceId(entry) === sourceId)
      if (refreshedSource) {
        await loadSeedCandidatesForSource(refreshedSource, { quiet: true })
      }
      seedActionStatus = `Removed ${dismissLabel} candidate(s) from Seed.`
    } catch (error) {
      if (error?.status === 401) return
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
      seedSortCache.delete(sourceId)
      if (expandedSeedSourceId === sourceId) {
        expandedSeedSourceId = ''
      }
      seedActionStatus = `Removed ${source.label} from Seed.`
    } catch (error) {
      if (error?.status === 401) return
      seedActionStatus = error?.message || 'Failed to remove seed source.'
    } finally {
      seedActionBusy = false
    }
  }

  function buildSearchBody(maxResults) {
    return {
      query: searchQuery, seedJson: '', field: searchField, author: searchAuthor, yearFrom, yearTo,
      topics: searchTopics.map(({ id, label }) => ({ id, label })),
      maxResults: searchResultCap(maxResults), sort: searchSort,
      includeDownstream: false, includeUpstream: false, relatedDepthDownstream: 0, relatedDepthUpstream: 0,
      maxRelated: 30,
    }
  }

  function searchResultCap(value) {
    if (value === '' || value === null || value === undefined) return DEFAULT_SEARCH_MAX_RESULTS
    const parsed = Number(value)
    return Number.isFinite(parsed) && parsed >= 0 ? Math.trunc(parsed) : DEFAULT_SEARCH_MAX_RESULTS
  }

  // Measured wall-clock cost of one OpenAlex page in this pipeline. The
  // configured RPS is a *ceiling* for parallel callers; keyword paging is
  // strictly sequential (each request needs the previous page's cursor), so
  // dividing by RPS understated a large fetch by more than an order of
  // magnitude. 1.1s/request is the observed round trip including rate-limit
  // sleep and the per-page DB write.
  const SEARCH_SECONDS_PER_REQUEST = 1.1
  // 90 minutes: past this an answer in minutes stops being readable.
  const SEARCH_HOURS_CUTOFF_SECONDS = 5400

  // `quota` is a parameter rather than a read of the module-level
  // `openalexQuota` so the {@const} in the warning markup re-runs when the
  // quota arrives (Svelte only tracks what the template expression names).
  // OpenAlex charges a list request with search text 10 credits and a pure
  // filter listing (topics, years, author) 1, whatever the page size.
  function creditsPerPageFor(preview) {
    const fromServer = Number(preview?.creditsPerPage)
    if (Number.isFinite(fromServer) && fromServer > 0) return fromServer
    return String(searchQuery || '').trim() ? 10 : 1
  }

  function searchEstimate(count, quota, creditsPerPage = 10) {
    const requests = Math.ceil(count / 200)
    const credits = requests * creditsPerPage
    const seconds = requests * SEARCH_SECONDS_PER_REQUEST
    const duration = seconds >= SEARCH_HOURS_CUTOFF_SECONDS
      ? `roughly ${(seconds / 3600).toFixed(1)} hours`
      : `roughly ${(seconds / 60).toFixed(1)} minutes`
    const remaining = Number(quota?.remaining)
    const limit = Number(quota?.limit)
    // Only a fresh, live quota reading may drive copy and a Cap button; a
    // stale snapshot would cap against yesterday's leftovers.
    const budgetKnown = Boolean(quota?.available) && !quota?.stale && Number.isFinite(remaining)
    const overBudget = budgetKnown && credits > remaining
    // Pages the remaining credits still pay for, times 200 works a page.
    const capAtBudget = budgetKnown ? Math.floor(remaining / creditsPerPage) * 200 : 0
    return {
      requests,
      credits,
      creditsPerPage,
      duration,
      remaining: budgetKnown ? remaining : null,
      capAtBudget,
      budgetSentence: overBudget
        ? `That is more than today's remaining OpenAlex budget: ${remaining.toLocaleString('en-US')} of ${Number.isFinite(limit) ? limit.toLocaleString('en-US') : 'unknown'} credits left.`
        : '',
      canCapAtBudget: overBudget && capAtBudget > 0,
    }
  }

  // --- Topic field -------------------------------------------------------
  function handleTopicInput(event) {
    topicQuery = String(event?.target?.value || '')
    // Invalidate immediately, not only after the next debounce fires: an
    // earlier response must not revive suggestions for cleared/changed input.
    closeTopicSuggestions()
    const q = topicQuery.trim()
    if (q.length < 2) return
    const token = topicLookupToken
    topicDebounce = setTimeout(async () => {
      topicDebounce = null
      try {
        const found = await fetchTopicSuggestions(q)
        if (token !== topicLookupToken) return
        const chosen = new Set(searchTopics.map((t) => t.id))
        topicSuggestions = found.filter((t) => !chosen.has(t.id)).slice(0, 8)
        topicActiveIndex = topicSuggestions.length > 0 ? 0 : -1
      } catch (error) {
        if (token !== topicLookupToken) return
        if (error?.status === 401) return
        topicSuggestions = []
      }
    }, 250)
  }

  function addTopic(topic) {
    if (!topic?.id || searchTopics.some((t) => t.id === topic.id)) return
    searchTopics = [...searchTopics, { id: topic.id, label: topic.label, hint: topic.hint || '', works_count: topic.works_count ?? null }]
    topicQuery = ''
    closeTopicSuggestions()
    searchPreview = null
    searchWarning = false
  }

  function removeTopic(id) {
    searchTopics = searchTopics.filter((t) => t.id !== id)
    searchPreview = null
    searchWarning = false
  }

  function closeTopicSuggestions() {
    clearTimeout(topicDebounce)
    topicDebounce = null
    topicLookupToken += 1
    topicSuggestions = []
    topicActiveIndex = -1
  }

  function resetTopicSearch() {
    closeTopicSuggestions()
    searchTopics = []
    topicQuery = ''
    searchPreview = null
    searchWarning = false
  }

  function handleTopicKeydown(event) {
    if (event.key === 'ArrowDown' && topicSuggestions.length > 0) {
      event.preventDefault()
      topicActiveIndex = (topicActiveIndex + 1) % topicSuggestions.length
    } else if (event.key === 'ArrowUp' && topicSuggestions.length > 0) {
      event.preventDefault()
      topicActiveIndex = (topicActiveIndex - 1 + topicSuggestions.length) % topicSuggestions.length
    } else if (event.key === 'Enter') {
      // Enter picks a suggestion instead of submitting the form while the
      // picker is open; with nothing to pick it submits as usual.
      if (topicSuggestions.length > 0) {
        event.preventDefault()
        addTopic(topicSuggestions[Math.max(0, topicActiveIndex)])
      }
    } else if (event.key === 'Escape') {
      event.preventDefault()
      closeTopicSuggestions()
    } else if (event.key === 'Backspace' && !topicQuery && searchTopics.length > 0) {
      removeTopic(searchTopics[searchTopics.length - 1].id)
    }
  }

  function formatWorksCount(n) {
    const value = Number(n)
    if (!Number.isFinite(value)) return ''
    return `${value.toLocaleString('en-US')} works`
  }

  // Submit: ask for the count first; warn above the threshold, else start.
  async function runSearch() {
    // The preview takes up to a minute: a sign-out or a corpus switch meanwhile
    // ends this search, whose form has been reset under it.
    const session = sessionEpoch
    const scope = corpusScopeEpoch
    searchWarning = false
    searchStatus = 'Checking how many works match...'
    searchPreviewBusy = true
    let preview = null
    try {
      preview = await previewKeywordSearch(buildSearchBody(searchMaxResults))
    } catch (error) {
      if (error?.status === 401) return
      // Any other failed preview must not block the search.
    } finally {
      searchPreviewBusy = false
    }
    if (session !== sessionEpoch || scope !== corpusScopeEpoch) return
    searchPreview = preview
    // The preview costs OpenAlex credits too.
    void refreshQuota()
    const cap = searchResultCap(searchMaxResults)
    if (searchPreview && searchPreview.count >= searchPreview.threshold && (cap === 0 || cap >= searchPreview.threshold)) {
      searchWarning = true
      searchStatus = ''
      return
    }
    await startSearch()
  }

  async function startSearch({ maxResultsOverride = null } = {}) {
    searchWarning = false
    if (maxResultsOverride !== null) searchMaxResults = maxResultsOverride
    searchStatus = 'Starting search...'
    try {
      const { data, expansion, runId, running } = await runKeywordSearch(buildSearchBody(searchMaxResults))
      void refreshQuota()
      if (runId && running) {
        // Start polling on the 202 itself; the seed list may not report this
        // run for another poll or two (or at all, if registration lagged).
        startedSearchRunIds = new Set([...startedSearchRunIds, Number(runId)])
      }
      if (runId) {
        await loadSeedSources({ quiet: true })
        await focusSeedSource('search', runId)
      }
      if (running) {
        searchStatus = 'Fetching in the background. The seed below fills in as pages arrive.'
        return
      }
      const suffix = expansion?.added ? ` (+${expansion.added} related works)` : ''
      searchStatus = `Search complete. Added ${data.length} item(s) to Seed.${suffix}`
    } catch (error) {
      if (error?.status === 401) return
      searchStatus = error?.message || 'Search failed.'
    }
  }

  function resetSearchForm() {
    searchQuery = ''
    searchField = 'default'
    searchAuthor = ''
    yearFrom = ''
    yearTo = ''
    searchMaxResults = DEFAULT_SEARCH_MAX_RESULTS
    searchSort = 'relevance'
    resetTopicSearch()
    searchStatus = ''
    searchPreview = null
    searchWarning = false
  }

  function corpusPageSignature(data, total, stageTotals) {
    return JSON.stringify([corpusFilterQuery, corpusSort, total, stageTotals, data])
  }

  function applyCorpusCounts(total, stageTotals) {
    corpusTotal = Number.isFinite(Number(total)) ? Number(total) : corpusItems.length
    corpusHasMore = corpusItems.length < corpusTotal
    if (stageTotals && typeof stageTotals === 'object') {
      corpusStageTotals = {
        raw: Number(stageTotals.raw || 0),
        metadata: Number(stageTotals.metadata || 0),
        downloaded: Number(stageTotals.downloaded || 0),
        failed_enrichment: Number(stageTotals.failed_enrichment || 0),
        failed_download: Number(stageTotals.failed_download || 0),
      }
    }
  }

  // A user reload (filter, sort) passes supersede: it starts even while
  // another load runs, and the newer sequence discards that load's answer and
  // leaves the flags to this one. Every other first-page load (the poll among
  // them) still yields to a load in flight. An append asked for while a
  // first-page load runs is queued and starts from that load's finally (D1).
  async function loadCorpus({ append = false, preserveSelection = false, quiet = false, supersede = false } = {}) {
    if (append) {
      if (corpusLoadingMore || !corpusHasMore) return
      if (corpusLoading) {
        corpusAppendQueued = true
        return
      }
    } else if (supersede) {
      // The new query starts from offset 0: a queued or running append
      // belongs to the old one.
      corpusAppendQueued = false
      corpusLoadingMore = false
    } else {
      if (corpusLoading) return
      if (quiet && corpusLoadingMore) return
    }

    const requestSeq = ++corpusLoadRequestSeq
    // null until the auth payload names the corpus (the initial value is 0):
    // the backend then answers for the token's corpus, which is that one.
    const requestCorpusId = Number(currentCorpusId) > 0 ? Number(currentCorpusId) : null
    if (append) {
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
      // The poll asks for the first page only, also when more rows are loaded.
      const poll = !append && quiet && preserveSelection
      const query = { q: corpusFilterQuery, sort: corpusSort }
      let { data, total, source, stageTotals } = await fetchCorpus({ limit: CORPUS_PAGE_SIZE, offset, ...query })
      // Drop the response if the user switched corpus meanwhile. A load that
      // started before the corpus was known (requestCorpusId null) was
      // answered for the token's corpus, which is the one fetchMe reports.
      const corpusScopeChanged = () => {
        const nowCorpus = Number(currentCorpusId) > 0 ? Number(currentCorpusId) : null
        return requestCorpusId !== null && requestCorpusId !== nowCorpus
      }
      if (requestSeq !== corpusLoadRequestSeq || corpusScopeChanged()) {
        return
      }
      // Committed only once the rows are replaced: a failed prefix reload
      // keeps the old signature (and rows), so the next poll retries it.
      let pageSignature = null
      if (!append) {
        pageSignature = corpusPageSignature(Array.isArray(data) ? data : [], total, stageTotals)
        if (poll && pageSignature === lastCorpusPageSignature) {
          corpusRefreshFailed = false
          return
        }
        // The first page's counts hold whatever the reload below does.
        applyCorpusCounts(total, stageTotals)
        // The first page changed while more pages are loaded: a fresh page in
        // front of the old tail would shift the offsets (rows lost or doubled
        // on the next "Show more"), so reload every loaded row, in sequential
        // pages of at most CORPUS_POLL_PAGE_ROWS. The rows are replaced only
        // once every page answered; a failed page throws and changes nothing.
        // A write between two pages shifts the offsets (a row doubled or
        // skipped at the boundary). Each page after the first starts on the
        // last row already read, and the sort ends in a unique id, so any such
        // shift shows there: the row in that slot is a different one. Such an
        // attempt is retried once, and a second inconsistent one leaves rows
        // and signature for the next poll. Two writes that cancel out exactly
        // at a boundary stay invisible until the first page next changes.
        if (poll && corpusItems.length > CORPUS_PAGE_SIZE) {
          const loadedRows = corpusItems.length
          let prefix = null
          for (let attempt = 0; attempt < 2 && !prefix; attempt += 1) {
            const rows = []
            const totals = new Set()
            let last = null
            let aligned = true
            while (aligned && rows.length < loadedRows) {
              const overlap = rows.length > 0 ? 1 : 0
              const pageOffset = rows.length - overlap
              const pageLimit = Math.min(CORPUS_POLL_PAGE_ROWS, loadedRows - pageOffset)
              const answer = await fetchCorpus({ limit: pageLimit, offset: pageOffset, ...query })
              if (requestSeq !== corpusLoadRequestSeq || corpusScopeChanged()) return
              const pageRows = Array.isArray(answer.data) ? answer.data : []
              aligned = !overlap || pageRows[0]?.id === rows[rows.length - 1]?.id
              rows.push(...pageRows.slice(overlap))
              totals.add(Number(answer.total))
              last = answer
              // The corpus ends before the loaded count (rows were removed).
              if (pageRows.length < pageLimit) break
            }
            // Corpus keys its rows by id.
            const ids = rows.map((row) => row?.id)
            if (aligned && totals.size === 1 && new Set(ids).size === ids.length) {
              prefix = rows
              ;({ total, source, stageTotals } = last)
            }
          }
          if (!prefix) {
            console.warn('Corpus refresh abandoned: the pages disagreed twice.')
            corpusRefreshFailed = true
            return
          }
          data = prefix
        }
      }
      let incoming = (Array.isArray(data) ? data : []).map((item) => normalizeCorpusItem(item))
      if (append) {
        // A row inserted since the last page shifts the offsets, so the next
        // page can repeat a loaded row; Corpus keys its rows by id (D2). The
        // next offset stays corpusItems.length and corpusHasMore still
        // compares with the total, so a dropped row never ends paging early.
        const loadedIds = new Set(corpusItems.map((item) => item?.id))
        incoming = incoming.filter((item) => !loadedIds.has(item?.id))
      }
      corpusItems = append ? [...corpusItems, ...incoming] : incoming
      if (pageSignature !== null) lastCorpusPageSignature = pageSignature
      applyCorpusCounts(total, stageTotals)
      corpusSource = source
      if (!append) corpusRefreshFailed = false
      if (!quiet) {
        corpusLoadStatus = corpusHasMore
          ? `Showing ${corpusItems.length} of ${corpusTotal} entries.`
          : `Showing ${corpusItems.length} entries.`
      }
    } catch (error) {
      if (error?.status === 401) return
      handleCorpusPollForbidden(error)
      // A newer load or a corpus switch owns the status now.
      if (requestSeq !== corpusLoadRequestSeq) return
      if (!append) {
        console.warn('Corpus refresh failed', error)
        corpusRefreshFailed = true
      }
      if (!quiet) {
        corpusLoadStatus = error?.message || 'Failed to load corpus.'
      }
    } finally {
      if (requestSeq === corpusLoadRequestSeq) {
        corpusLoading = false
        corpusLoadingMore = false
        if (!append && corpusAppendQueued) {
          corpusAppendQueued = false
          if (corpusHasMore) loadCorpus({ append: true })
        }
      }
    }
  }

  async function loadDownloads() {
    const epoch = corpusScopeEpoch
    try {
      const { data, source } = await fetchDownloadQueue()
      if (epoch !== corpusScopeEpoch) return
      downloads = data
      downloadsSource = source
    } catch (error) {
      if (error?.status === 401 || epoch !== corpusScopeEpoch) return
      downloadsSource = 'error'
    }
  }

  async function loadDownloadWorkerStatus({ quiet = false } = {}) {
    if (!quiet) downloadWorkerStatus = 'Loading worker status...'
    try {
      const payload = await fetchDownloadWorkerStatus()
      downloadWorker = payload
      if (!quiet) downloadWorkerStatus = ''
    } catch (error) {
      if (error?.status === 401) return
      if (!quiet) downloadWorkerStatus = error?.message || 'Failed to load worker status.'
    }
  }

  async function loadPipelineWorkerStatus({ quiet = false, global = false } = {}) {
    if (!quiet) pipelineWorkerStatus = 'Loading worker status...'
    const epoch = corpusScopeEpoch
    try {
      const payload = await fetchPipelineDaemonStatus({ global })
      if (global) {
        globalWorkerBacklog = payload?.backlog || globalWorkerBacklog
        return
      }
      // The backlog is corpus-scoped. A stale response leaves the status
      // alone: it may already show the newer corpus's request.
      if (epoch !== corpusScopeEpoch) return
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
      if (error?.status === 401 || epoch !== corpusScopeEpoch) return
      if (!quiet) pipelineWorkerStatus = error?.message || 'Failed to load worker status.'
    }
  }

  async function handlePausePipelineWorker() {
    if (pipelineWorkerBusy) return
    pipelineWorkerBusy = true
    pipelineWorkerStatus = 'Pausing pipeline...'
    try {
      await pausePipelineWorker({ force: false })
      await loadPipelineWorkerStatus()
      pipelineWorkerStatus = 'Pipeline paused.'
    } catch (error) {
      if (error?.status === 401) return
      pipelineWorkerStatus = error?.message || 'Failed to pause pipeline.'
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
      if (error?.status === 401) return
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
      if (error?.status === 401) return
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
      if (error?.status === 401) return
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
      corpusLoadStatus = 'Preparing download...'
      const url = await createCorpusItemDownloadUrl(itemId)
      const link = document.createElement('a')
      link.href = url
      document.body.appendChild(link)
      link.click()
      link.remove()
      corpusLoadStatus = 'Download started.'
    } catch (error) {
      if (error?.status === 401) return
      corpusLoadStatus = error?.message || 'Failed to download file.'
    }
  }

  async function loadAppSettings() {
    if (!isAdmin) return
    const epoch = sessionEpoch
    try {
      const payload = await fetchAppSettings()
      if (epoch !== sessionEpoch) return
      appSettings = payload?.settings || null
      // Secrets are never returned, so their draft fields start blank and a
      // blank field means "leave unchanged" on save.
      appSettingsDraft = {
        openalex_api_key: '',
        openai_api_key: '',
        gemini_api_key: '',
        openalex_rps: appSettings?.openalex_rps?.value || '',
        search_warn_threshold: appSettings?.search_warn_threshold?.value || '',
        llm_provider: appSettings?.llm_provider?.value || '',
        openai_base_url: appSettings?.openai_base_url?.value || '',
        extract_model: appSettings?.extract_model?.value || '',
        openai_model: appSettings?.openai_model?.value || '',
        gemini_model: appSettings?.gemini_model?.value || '',
        inline_cheap_model: appSettings?.inline_cheap_model?.value || '',
        inline_model: appSettings?.inline_model?.value || '',
      }
    } catch (error) {
      if (error?.status === 401 || epoch !== sessionEpoch) return
      appSettingsStatus = error?.message || 'Failed to load settings.'
      appSettingsError = true
    }
  }

  async function handleSaveAppSettings() {
    const payload = { ...appSettingsDraft }
    // A blank secret means "keep what is stored", so omit it entirely rather
    // than sending '' — which the API treats as "clear this override".
    for (const key of ['openalex_api_key', 'openai_api_key', 'gemini_api_key']) {
      if (!String(payload[key] || '').trim()) delete payload[key]
    }
    try {
      appSettingsError = false
      appSettingsStatus = 'Saving...'
      await saveAppSettings(payload)
      appSettingsStatus = 'Settings saved. New values apply to the next pipeline run.'
      await loadAppSettings()
    } catch (error) {
      if (error?.status === 401) return
      appSettingsStatus = error?.message || 'Failed to save settings.'
      appSettingsError = true
    }
  }

  async function handleDownloadSeedDocument(ingestSource) {
    const sourceKey = String(ingestSource || '').trim()
    if (!sourceKey) return
    let objectUrl = ''
    try {
      const { blob, filename } = await fetchSeedSourceDocument(sourceKey)
      objectUrl = URL.createObjectURL(blob)
      const link = document.createElement('a')
      link.href = objectUrl
      link.download = filename
      document.body.appendChild(link)
      link.click()
      link.remove()
    } catch (error) {
      if (error?.status === 401) return
      ingestRunsStatus = error?.message || 'Failed to download the seed document.'
    } finally {
      if (objectUrl) URL.revokeObjectURL(objectUrl)
    }
  }

  async function handleSeedCandidateFile(candidate) {
    const workId = Number(candidate?.downloaded_work_id)
    const sourceType = String(candidate?.source_type || '').trim()
    const sourceKey = String(candidate?.source_key || '').trim()
    const candidateKey = String(candidate?.candidate_key || '').trim()
    if (!Number.isFinite(workId) || workId <= 0 || !sourceType || !sourceKey || !candidateKey) return
    try {
      const { blob, filename } = await downloadSeedCandidateFile(sourceType, sourceKey, candidateKey)
      const url = URL.createObjectURL(blob)
      const link = document.createElement('a')
      link.href = url
      link.download = filename
      document.body.appendChild(link)
      link.click()
      link.remove()
      URL.revokeObjectURL(url)
      seedActionStatus = `Downloaded ${filename}.`
    } catch (error) {
      if (error?.status === 401) return
      seedActionStatus = error?.message || 'Failed to download seed candidate file.'
    }
  }

  function canDownloadSeedCandidate(candidate) {
    const state = workState(candidate?.state)
    const workId = Number(candidate?.downloaded_work_id)
    return (state === 'downloaded' || state === 'downloaded_elsewhere') &&
      Number.isFinite(workId) &&
      workId > 0 &&
      candidate?.file_available === true
  }
  // A reconnect (force = false) never hydrates over content, rendered or
  // buffered; only the first connect with nothing and the explicit refreshes
  // (Refresh from file, type/filter change) replace the view with the tail.
  async function hydrateLogsFromTail({ force = false } = {}) {
    if (!force && (logs.length > 0 || logBuffer.length > 0)) return
    const epoch = logsScopeEpoch
    logsTailStatus = 'Loading recent logs...'
    try {
      const payload = await fetchLogsTail({
        type: logsType,
        lines: maxLogs,
        includeRotated: true,
        cursor: 0,
        corpusId: getLogsCorpusId(),
      })
      if (epoch !== logsScopeEpoch) return
      const entries = Array.isArray(payload?.entries) ? payload.entries : []
      // The tail replaces the view: pending lines are dropped, not appended
      // after it (the backend persists a line before broadcasting it, so the
      // tail may hold them). A line that arrived between the request and this
      // answer is dropped from the view as before batching; it is in the file
      // and the next refresh shows it.
      clearLogBuffer()
      logs = toLogEntries(entries.slice(-maxLogHistory))
      logsCursor = Number.isFinite(Number(payload?.next_cursor))
        ? Number(payload.next_cursor)
        : logs.length
      logsHasMore = Boolean(payload?.has_more)
      logsTailStatus = entries.length > 0 ? `Loaded ${entries.length} recent log lines.` : 'No persisted logs yet.'
    } catch (error) {
      if (error?.status === 401 || epoch !== logsScopeEpoch) return
      logsTailStatus = error?.message || 'Failed to load persisted logs.'
    }
  }

  const LOG_VIEW_FULL_STATUS = 'The log view holds 5,000 lines. Older lines stay in the log file.'

  // Older lines are admitted only while the view has room (D6): a full view
  // sends no request, and capacity is decided again when the answer arrives,
  // because live lines may have filled the view meanwhile. Nothing fetched is
  // trimmed; the cursor advances only by the lines admitted, so the rest are
  // fetched again by the next request.
  async function loadOlderLogs() {
    if (logsLoadingOlder || !logsHasMore) return
    // The cursor counts the newest lines already shown; buffered lines are
    // in the file too, so they go into the view (and the cursor) first, or
    // the page would repeat lines and skip as many older ones.
    flushLogs()
    if (logs.length >= maxLogHistory) {
      logsTailStatus = LOG_VIEW_FULL_STATUS
      return
    }
    const epoch = logsScopeEpoch
    const cursorAtRequest = logsCursor
    logsLoadingOlder = true
    logsTailStatus = 'Loading older logs...'
    try {
      const payload = await fetchLogsTail({
        type: logsType,
        lines: Math.min(maxLogs, maxLogHistory - logs.length),
        includeRotated: true,
        cursor: cursorAtRequest,
        corpusId: getLogsCorpusId(),
      })
      if (epoch !== logsScopeEpoch) return
      const entries = Array.isArray(payload?.entries) ? payload.entries : []
      flushLogs()
      const room = Math.max(0, maxLogHistory - logs.length)
      // The newest lines sit next to the view. slice(-0) would be the whole page.
      const accepted = room > 0 ? entries.slice(-room) : []
      if (accepted.length > 0) {
        logs = [...toLogEntries(accepted), ...logs]
      }
      // Lines flushed while the request was pending advanced the cursor;
      // keep that advance on top of the answer's.
      const advanced = logsCursor - cursorAtRequest
      const allAccepted = accepted.length === entries.length
      const nextCursor = allAccepted && Number.isFinite(Number(payload?.next_cursor))
        ? Number(payload.next_cursor)
        : cursorAtRequest + accepted.length
      logsCursor = nextCursor + advanced
      // Lines left out are still to be loaded.
      logsHasMore = allAccepted ? Boolean(payload?.has_more) : true
      if (entries.length === 0) {
        logsTailStatus = 'No older logs found.'
      } else if (accepted.length === 0) {
        logsTailStatus = LOG_VIEW_FULL_STATUS
      } else if (!allAccepted) {
        logsTailStatus = `Loaded ${accepted.length} of ${entries.length} older log lines; the view holds 5,000 lines.`
      } else {
        logsTailStatus = `Loaded ${entries.length} older log lines.`
      }
    } catch (error) {
      if (error?.status === 401 || epoch !== logsScopeEpoch) return
      logsTailStatus = error?.message || 'Failed to load older logs.'
    } finally {
      if (epoch === logsScopeEpoch) logsLoadingOlder = false
    }
  }

  function handleExtractionFrame(frame) {
    const extractionEvent = extractionEventFromObject(frame)
    if (!extractionEvent) return
    const currentCorpus = Number.isFinite(Number(currentCorpusId)) ? Number(currentCorpusId) : null
    const corpusMatches = extractionEvent.corpusId === null || currentCorpus === null || extractionEvent.corpusId === currentCorpus
    if (extractionEvent.event === 'started') {
      updateUploadByBackendFilename(extractionEvent.filename, {
        status: 'extracting',
        message: 'Extracting bibliography',
      })
      if (corpusMatches) {
        ensureExtractionRunActive(extractionEvent.baseName, extractionEvent.filename)
      }
      return
    }

    updateUploadByBackendFilename(
      extractionEvent.filename,
      extractionEvent.status === 'success'
        ? { status: 'extracted', message: 'Extraction complete' }
        : { status: 'failed', message: 'Extraction failed' }
    )
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
        finishExtractionProgress()
      }
    }
  }

  function logGapTime(date) {
    return date.toLocaleTimeString('en-GB', { hour12: false })
  }

  // The first connect hydrates from the tail. A reconnect never hydrates and
  // never replaces the view: when it opens it marks the gap since the first
  // close instead (D7); "Reload logs" on the marker is the explicit refresh.
  function connectLogs({ reconnect = false } = {}) {
    disconnectLogs()
    logsStatus = 'connecting'
    if (!reconnect) hydrateLogsFromTail()
    try {
      const envBase = String(import.meta.env.VITE_API_BASE || '').trim()
      const rawBase = envBase || window.location.origin
      let wsUrl = 'ws://localhost:4000/api/ws'
      try {
        const u = new URL(rawBase, window.location.origin)
        wsUrl = `${u.protocol === 'https:' ? 'wss' : 'ws'}://${u.host}/api/ws`
      } catch (error) {
        // fall back to localhost default
      }
      // The log socket authenticates via Sec-WebSocket-Protocol: ['bearer', <jwt>].
      const token = getAuthToken()
      const ws = token ? new WebSocket(wsUrl, ['bearer', token]) : new WebSocket(wsUrl)
      logsSocket = ws
      const scheduleReconnect = () => {
        if (authStatus !== 'authenticated') return
        if (logsReconnectTimer) return
        logsStatus = 'reconnecting'
        logsReconnectTimer = setTimeout(() => {
          logsReconnectTimer = null
          connectLogs({ reconnect: true })
        }, 3000)
      }
      ws.onopen = () => {
        if (logsSocket !== ws) return
        logsStatus = 'connected'
        if (reconnect && logsDisconnectedAt) {
          // Live lines exist only for the pipeline view; another type has no gap.
          if (logsType === 'pipeline') {
            const from = logGapTime(logsDisconnectedAt)
            const to = logGapTime(new Date())
            pushLogEntry({
              seq: ++logSeq,
              text: `Connection lost ${from}–${to}. Lines from that period may be missing.`,
              marker: true,
            })
          }
        }
        logsDisconnectedAt = null
      }
      ws.onmessage = (event) => {
        if (logsSocket !== ws) return
        const line = event.data
        // JSON frames are parsed once: extraction events drive the extraction
        // card, every other JSON frame is ignored and never becomes a log row.
        const msg = typeof line === 'string' && line.startsWith('{') ? tryParseJson(line) : null
        if (msg) {
          if (msg.kind === 'extraction') handleExtractionFrame(msg)
          return
        }
        if (typeof line === 'string' && line.startsWith('{')) return
        if (typeof line === 'string' && line.startsWith('WebSocket connection established')) return
        if (!isLogLineForCurrentCorpus(line)) return
        updateExtractionProgressFromLog(line)
        if (logsType !== 'pipeline') return
        pushLogLine(line)
        if (shouldTriggerPipelineRefresh(line)) {
          const isWorkerTick =
            String(line).includes('[pipeline-worker') || String(line).includes('[download-worker')
          requestRefreshSoon(isWorkerTick ? REFRESH_WORKER_TICK_MS : REFRESH_TRAILING_MS)
        }
      }
      ws.onclose = (event) => {
        if (logsSocket !== ws) return
        logsSocket = null
        if (event?.code === 4401) {
          // Token rejected by the server: full sign-out, no reconnect. The
          // status is set after handleLogout() so disconnectLogs() keeps it.
          handleLogout()
          logsStatus = 'unauthorized'
          return
        }
        // A series of failed reconnects keeps the first close time.
        if (!logsDisconnectedAt) logsDisconnectedAt = new Date()
        scheduleReconnect()
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

  // Transport teardown only: buffered lines survive a reconnect. They are
  // dropped with the log state (resetLogState, resetFrontendState on sign-out
  // and corpus switch).
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

  $: tabIds = new Set(tabGroups.flatMap(group => group.tabs).map(tab => tab.id))

  function normalizeTab(tabId) {
    if (tabId === 'seed' || tabId === 'corpus') return 'workspace'
    return tabId
  }

  function readTabFromHash({ includeUnavailable = false } = {}) {
    if (typeof window === 'undefined') return null
    const raw = window.location.hash || ''
    const cleaned = raw.replace(/^#\/?/, '').trim()
    const normalized = normalizeTab(cleaned)
    const allowedTabs = includeUnavailable ? knownTabIds : tabIds
    return allowedTabs.has(normalized) ? normalized : null
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

  // `loadedTab`: the tab a refreshAll just loaded (sign-in, boot); landing on
  // it again does not fetch everything a second time.
  function setActiveTab(tabId, { replace = false, loadedTab = null } = {}) {
    const normalized = normalizeTab(tabId)
    const safeTab = tabIds.has(normalized) ? normalized : 'workspace'
    navigationRevision += 1
    activeTab = safeTab
    if (safeTab === 'logs') flushLogs()
    updateHash(safeTab, replace)
    if (loadedTab === null || normalizeTab(loadedTab) !== safeTab) void refreshForActiveTab(safeTab)
    // The cadence depends on the tab (e.g. corpus has none, workspace is idle-polled).
    scheduleRefresh()
  }

  onMount(() => {
    const initialAuthFlow = parseAuthFlowFromHash()
    applyAuthFlowFromHash()
    if (initialAuthFlow.flow === 'login') {
      const initialTab = readTabFromHash({ includeUnavailable: true })
      if (initialTab && !tabIds.has(initialTab)) {
        activeTab = initialTab
      } else {
        setActiveTab(initialTab || activeTab, { replace: true })
      }
    }
    reducedMotionMediaQuery = window.matchMedia('(prefers-reduced-motion: reduce)')
    prefersReducedMotion = Boolean(reducedMotionMediaQuery?.matches)
    const onReducedMotionChange = (event) => {
      prefersReducedMotion = Boolean(event?.matches)
    }
    reducedMotionMediaQuery?.addEventListener?.('change', onReducedMotionChange)

    const onHashChange = () => {
      navigationRevision += 1
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

    // `focus` and `visibilitychange` usually fire together: one refresh per
    // 250 ms window.
    let focusRefreshTimer = null
    const onFocusOrVisible = () => {
      if (document.visibilityState === 'hidden') {
        if (focusRefreshTimer) clearTimeout(focusRefreshTimer)
        focusRefreshTimer = null
        scheduleRefresh()
        return
      }
      if (focusRefreshTimer) return
      focusRefreshTimer = setTimeout(() => {
        focusRefreshTimer = null
        // Becoming visible refreshes once; the regular cadence resumes afterwards.
        // Log lines that arrived while hidden left the data dirty: run their
        // trailing refresh now instead of at the next regular cycle.
        void refreshForActiveTab(activeTab).finally(() => scheduleRefresh({ delayMs: refreshDirty ? REFRESH_TRAILING_MS : null }))
      }, 250)
    }
    window.addEventListener('focus', onFocusOrVisible)
    document.addEventListener('visibilitychange', onFocusOrVisible)

    const boot = async () => {
      const flow = parseAuthFlowFromHash()
      await bootstrapAuth({ preserveAuthFlow: flow.flow !== 'login' })
    }
    boot()

    refreshDisposed = false
    scheduleRefresh()
    if (import.meta.env.DEV) {
      window.__dtRefresh = {
        requestRefreshSoon,
        reason: () => refreshReason(),
        workers: () => ({ diagnosticsEnabled, pipelineWorker, downloadWorker, ingestStats }),
      }
    }

    return () => {
      closeTopicSuggestions()
      window.removeEventListener('hashchange', onHashChange)
      window.removeEventListener('focus', onFocusOrVisible)
      document.removeEventListener('visibilitychange', onFocusOrVisible)
      if (focusRefreshTimer) clearTimeout(focusRefreshTimer)
      reducedMotionMediaQuery?.removeEventListener?.('change', onReducedMotionChange)
      reducedMotionMediaQuery = null
      disconnectLogs()
      clearLogBuffer()
      refreshDisposed = true
      if (refreshTimer) {
        clearTimeout(refreshTimer)
        refreshTimer = null
      }
      if (import.meta.env.DEV) delete window.__dtRefresh
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
        <form class="login-form" data-testid="login-form" on:submit={handleLogin}>
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
  <div class="app-shell" class:workspace-mode={activeTab === 'workspace'} class:upstream-mode={activeTab === 'upstream'}>
    <header class="app-header">
      <div class="header-main header-main--workspace">
        <div class="header-main__copy">
          <h1 class="eyebrow">Korpus Builder</h1>
          <p class="subtitle">
            Feed it seed documents and OpenAlex searches, decide which found items belong, and the pipeline fetches bibliographic metadata and PDFs for everything you promote.
          </p>
        </div>
        <div class="header-stats">
          <!-- The graph tab does not load these counts (resourcesFor), so it does not show them. -->
          {#if activeTab !== 'graph'}
            <span title="Items collected across all seeds"><strong>{itemsFoundCount}</strong> items found</span>
            <span title="Items promoted into the corpus"><strong>{itemsPromotedCount}</strong> items promoted</span>
            <span title="Items with a downloaded PDF"><strong>{pipelineDownloadedCount}</strong> items downloaded</span>
          {/if}
          <span title="Whether the app's backend server answered the most recent request">backend <strong class:status-bad={apiStatus === 'offline'}>{backendStatusLabel}</strong></span>
        </div>
        <div class="header-main__top">
          <label class="header-corpus-picker">
            <span class="eyebrow">Corpus Workspace</span>
            <select value={currentCorpusId || ''} on:change={handleSelectCorpus}>
              {#if !currentCorpusId}
                <option value="" disabled>Select a corpus</option>
              {/if}
              {#each corpora as corpus}
                <option value={corpus.id}>{corpusOptionLabel(corpus)}</option>
              {/each}
            </select>
          </label>
          <div class="header-user header-user--compact">
            <span class="eyebrow">Signed in</span>
            <strong data-testid="auth-user">{authUser?.username}</strong>
            <button class="secondary logout-button" type="button" on:click={handleLogout}>
              Log out
            </button>
          </div>
        </div>
        {#if corpusSwitchError && activeTab !== 'upstream'}
          <p class="error small" role="alert" data-testid="corpus-switch-error">{corpusSwitchError}</p>
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
        {#if activeTab === 'workspace'}
          <nav class="nav-group workspace-sticky-bar" aria-label="Workspace sections" data-testid="workspace-sticky-bar">
            <span class="nav-group-title">Sections</span>
            <div class="workspace-sticky-bar__links">
              <button type="button" class="workspace-sticky-bar__link" on:click={() => jumpToSection('section-find')}>1 Find items</button>
              <button type="button" class="workspace-sticky-bar__link" on:click={() => jumpToSection('section-seed')}>2 Seed <span class="muted small">({seedSources.length})</span></button>
              <button type="button" class="workspace-sticky-bar__link" on:click={() => jumpToSection('section-corpus')}>3 Corpus <span class="muted small">({corpusTotal})</span></button>
            </div>
            <div class="workspace-sticky-bar__actions">
              {#if expandedSeedSourceId}
                <button type="button" class="secondary" on:click={collapseExpandedSeed}>Collapse seed</button>
              {/if}
              <button type="button" class="secondary" on:click={() => jumpToSection('section-find')}>Top</button>
            </div>
          </nav>
        {/if}
      </aside>
    {/if}

    <section class="content">
      {#if activeTab === 'workspace'}
        <div class="seed-corpus-workspace">
        <div class="card seed-corpus-toolbar" id="section-find">
          <div class="seed-corpus-toolbar__header">
            <div class="seed-corpus-toolbar__intro">
              <h2 class="workspace-section-title">1. Find items</h2>
              <p>Two ways in: extract the bibliography of an uploaded seed document (Document items), or run a keyword search on OpenAlex (Search items). Review everything in Seed below.</p>
            </div>

          </div>

          <div class="seed-intake-grid">
            <div class="seed-intake-card seed-intake-card--upload">
              <div class="split">
                <h3>Seed document</h3>
                {#if extractionProgress.active}
                  <span class="tag queued">extracting</span>
                {/if}
              </div>
              <div class="uploader">
                <label class="upload-drop">
                  <input type="file" multiple accept=".pdf,.bib,.json" on:change={handleFiles} />
                  <span>Drop PDF, BibTeX, or JSON files, or click to upload.</span>
                </label>
              </div>
              {#if uploads.length > 0}
	                <div class="upload-list">
	                  {#each uploads as item}
	                    <div class="upload-item">
	                      <div>
	                        <strong>{item.name}</strong>
	                        <span>{item.message || (item.size ? formatBytes(item.size) : item.backendFilename)}</span>
	                      </div>
	                      <div class="upload-actions">
	                        {#if item.status && item.status !== 'uploaded'}
	                          <span class={`status ${item.status}`}>{item.status}</span>
	                        {/if}
                        {#if !seedKindForName(item.name)}
                          <button
                            type="button"
                            on:click={() => extractForItem(item)}
                            disabled={!item.backendFilename || item.status === 'queued' || item.status === 'extracting'}
                          >
                            Extract
                          </button>
                        {/if}
	                      </div>
	                    </div>
	                  {/each}
	                </div>
	              {/if}
	              {#if recentDocumentRuns.length > 0}
	                <div class="document-run-list">
	                  <div class="document-run-list__header">
	                    <span class="muted small">Extracted seed documents</span>
	                    {#if ingestRunsStatus}
	                      <span class="muted small" role="status">{ingestRunsStatus}</span>
	                    {/if}
	                  </div>
	                  {#each recentDocumentRuns as run}
	                    <button
	                      class="document-run"
	                      type="button"
	                      title={`${run.source_pdf ? 'Click to download the original seed document. ' : ''}Bibliographic metadata extracted from this PDF. ${formatSeedDocumentDetails({
	                        authors: run.seed_authors,
	                        source: run.seed_source,
	                        publisher: run.seed_publisher,
	                      }) || (run.source_pdf || '')}`.trim()}
	                      on:click={() => (run.source_pdf
	                        ? handleDownloadSeedDocument(run.ingest_source)
	                        : focusSeedSource('pdf', run.ingest_source))}
	                    >
	                      <span class="truncate-line">
	                        {formatSeedDocumentLabel(
	                          {
	                            title: run.seed_title,
	                            year: run.seed_year,
	                            doi: run.seed_doi,
	                          },
	                          run.ingest_source
	                        )}
	                      </span>
	                      <span class="muted small">{run.entry_count} entries</span>
	                    </button>
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
              <div class="seed-intake-card__header">
                <h3>Keyword search <span class="muted small keyword-search-note">Queries run against the OpenAlex scholarly index.</span></h3>
                <span
                  class={`openalex-quota-pill openalex-quota-pill--${openalexQuotaView.tone}`}
                  data-testid="openalex-quota"
                  title="Daily OpenAlex API budget as reported by the last request"
                >{openalexQuotaView.text}</span>
              </div>
              <form class="seed-search-form" on:submit|preventDefault={runSearch}>
                <div class="seed-search-row">
                  <label class="seed-search-main">
                    <span>Query</span>
                    <input
                      type="text"
                      placeholder="institutional economics AND governance"
                      bind:value={searchQuery}
                    />
                  </label>
                  <div class="seed-search-topic" data-testid="topic-field">
                    <span id="topic-field-label">Topic <span class="muted small">— OpenAlex topics; alone they cost 1 credit per 200 works, text search costs 10</span></span>
                    <div class="topic-picker">
                      {#each searchTopics as topic (topic.id)}
                        <span class="topic-chip" data-testid="topic-chip" title={topic.hint}>
                          <span class="topic-chip__label">{topic.label}</span>
                          {#if topic.works_count !== null}<span class="topic-chip__count">{formatWorksCount(topic.works_count)}</span>{/if}
                          <button type="button" class="topic-chip__remove" aria-label={`Remove topic ${topic.label}`} on:click={() => removeTopic(topic.id)}>×</button>
                        </span>
                      {/each}
                      <input
                        type="text"
                        role="combobox"
                        aria-labelledby="topic-field-label"
                        aria-autocomplete="list"
                        aria-expanded={topicSuggestions.length > 0}
                        aria-controls="topic-suggestions"
                        placeholder={searchTopics.length > 0 ? 'Add another topic' : 'Labor market dynamics and wage inequality'}
                        value={topicQuery}
                        on:input={handleTopicInput}
                        on:keydown={handleTopicKeydown}
                        on:blur={() => setTimeout(closeTopicSuggestions, 150)}
                      />
                      {#if topicSuggestions.length > 0}
                        <ul class="topic-suggestions" id="topic-suggestions" role="listbox" aria-label="Matching topics">
                          {#each topicSuggestions as suggestion, index (suggestion.id)}
                            <li role="option" aria-selected={index === topicActiveIndex}>
                              <button type="button" class:active={index === topicActiveIndex} on:mousedown|preventDefault on:click={() => addTopic(suggestion)}>
                                <span class="topic-suggestion__title"><strong>{suggestion.label}</strong> <span class="muted small">{formatWorksCount(suggestion.works_count)}</span></span>
                                {#if suggestion.hint}<span class="muted small topic-suggestion__hint">{suggestion.hint}</span>{/if}
                              </button>
                            </li>
                          {/each}
                        </ul>
                      {/if}
                    </div>
                  </div>
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
                  <label class="seed-search-author">
                    <span>Author</span>
                    <input
                      type="text"
                      placeholder="Elinor Ostrom"
                      bind:value={searchAuthor}
                    />
                  </label>
                  <label>
                    <span>From</span>
                    <input type="number" placeholder="Year" bind:value={yearFrom} />
                  </label>
                  <label>
                    <span>To</span>
                    <input type="number" placeholder="Year" bind:value={yearTo} />
                  </label>
                  <label>
                    <span>Max results</span>
                    <input type="number" min="0" step="1" placeholder="50 (0 = no cap)" bind:value={searchMaxResults} />
                  </label>
                  <label>
                    <span>Sort</span>
                    <select bind:value={searchSort}>
                      {#if !topicOnlySearch}<option value="relevance">Relevance (default)</option>{/if}
                      <option value="cited_by_count">Most cited</option>
                      <option value="newest">Newest</option>
                      <option value="oldest">Oldest</option>
                    </select>
                  </label>
                  <div class="seed-search-footer">
                    <p class="muted">{searchStatus}</p>
                    {#if searchPreview && !searchWarning}
                      <p class="muted small search-preview" data-testid="search-preview">About {searchPreview.count.toLocaleString('en-US')} works match · {creditsPerPageFor(searchPreview)} {creditsPerPageFor(searchPreview) === 1 ? 'credit' : 'credits'} per page of 200</p>
                    {/if}
                    {#if searchWarning && searchPreview}
                      {@const est = searchEstimate(searchPreview.count, openalexQuota, creditsPerPageFor(searchPreview))}
                      <div class="search-warning" role="alert" data-testid="search-warning">
                        <p>This search matches {searchPreview.count.toLocaleString('en-US')} works. Fetching all of them takes about {est.requests.toLocaleString('en-US')} OpenAlex requests ({est.credits.toLocaleString('en-US')} credits) and {est.duration}.{#if est.budgetSentence} {est.budgetSentence}{/if} Narrow the query, or:</p>
                        <div class="search-warning__actions">
                          <button class="secondary" type="button" on:click={() => startSearch({ maxResultsOverride: Math.floor(searchPreview.threshold / 10) })}>Cap at {Math.floor(searchPreview.threshold / 10).toLocaleString('en-US')}</button>
                          {#if est.canCapAtBudget}
                            <button class="secondary" type="button" data-testid="search-cap-at-budget" on:click={() => startSearch({ maxResultsOverride: est.capAtBudget })}>Cap at budget</button>
                          {/if}
                          <button class="primary" type="button" on:click={() => startSearch()}>Fetch all {searchPreview.count.toLocaleString('en-US')}</button>
                        </div>
                      </div>
                    {/if}
                    <div class="seed-search-actions">
                      <button class="secondary" type="button" on:click={resetSearchForm}>Reset</button>
                      <button class="primary" type="submit" disabled={searchPreviewBusy}>Search</button>
                    </div>
                  </div>
                </div>
              </form>
            </div>
          </div>

        </div>

        <div class="seed-corpus-columns">
          <div class="seed-corpus-column seed-corpus-column--seed">
        <div class="card seed-sources-panel" id="section-seed" data-testid="seed-panel">
          <div class="workspace-panel-header">
            <div class="workspace-panel-title">
              <h3 class="workspace-section-title">2. Seed <span class="muted small">({seedSources.length})</span></h3>
              <p class="muted">Every seed document and search run lands here as a seed. Expand one to review its items and promote the keepers to the corpus.</p>
            </div>
            <div class="workspace-panel-actions">
              {#if seedPipelineActivity.text}
                <span class="seed-activity" title="Work the background pipeline still has to do for this corpus">
                  <span class="seed-activity__dot" class:seed-activity__dot--active={seedPipelineActivity.active} aria-hidden="true"></span>
                  {seedPipelineActivity.text}
                </span>
              {/if}
              {#if seedActionStatus}
                <span class:seed-action-warning={seedActionWarning} role={seedActionWarning ? 'alert' : 'status'}>{seedActionStatus}</span>
              {/if}
              {#if seedSourcesStatus}
                <span class="muted">{seedSourcesStatus}</span>
              {/if}
            </div>
          </div>

          <div class="table-filter">
            <input
              type="search"
              class="table-filter__input"
              placeholder="Filter by title, author or publication"
              aria-label="Filter seed items by title, author or publication"
              value={seedFilterQuery}
              on:input={handleSeedFilterInput}
            />
            {#if seedFilterQuery}
              <span class="muted small">Seeds without a match are hidden.</span>
            {/if}
          </div>

          <div class="seed-expansion-row seed-expansion-row--panel">
            <div class="seed-expansion-meta">
              <span class="muted small seed-expansion-label">Promotion settings</span>
              <span class="muted small">Applies to the selected Seed items when you promote them.</span>
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
            <div class="seed-expansion-pill" class:opacity-50={!expansionEnabled}>
              <span class="muted small" title="Which related papers survive the Max Related cap">Related papers</span>
              <select bind:value={relatedSort} disabled={!expansionEnabled}>
                <option value="most_cited">Most cited</option>
                <option value="newest">Newest</option>
              </select>
            </div>
            <div class="seed-expansion-divider"></div>
            <div class="seed-expansion-pill" class:opacity-50={!expansionEnabled}>
              <span
                class="muted small"
                title="New seed: related works land in Seed for review. Download everything: they go straight into the corpus."
              >Expansion</span>
              <select bind:value={promotionMode} disabled={!expansionEnabled}>
                <option value="new_seed">Make new seed (review first)</option>
                <option value="download_all">Download everything</option>
              </select>
            </div>
          </div>

          {#if seedSources.length === 0}
            <p class="muted">No seeds yet. Upload a seed document or run a keyword search to start collecting items.</p>
          {:else}
            <div class="seed-source-list">
              {#each seedSources as source (seedSourceId(source))}
                {@const sourceId = seedSourceId(source)}
                {@const isExpanded = expandedSeedSourceId === sourceId}
                {@const loaded = seedCandidatesBySource[sourceId]}
                {@const sourceCandidates = sortSeedCandidates(source, loaded || EMPTY_LIST, seedSorts)}
                {@const page = seedPage(source, seedPages)}
                {@const selection = seedSelections[sourceId] || EMPTY_SET}
                {@const loadedSelectableCount = selectableSeedCount(source, loaded || EMPTY_LIST, includeDownstream, relatedDepthDownstream)}
                {@const selectableCount = estimatedSelectableSeedCount(source, seedPages[sourceId], loaded, loadedSelectableCount, includeDownstream, relatedDepthDownstream)}
                {@const selectedCount = seedAllSelected[sourceId] ? selectableCount : selection.size}
                {@const fullySelected = Boolean(seedAllSelected[sourceId]) || (selectableCount > 0 && selectedCount === selectableCount)}
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
                        <label
                          class="seed-source__select"
                          title={fullySelected ? 'Clear all items in this seed' : 'Select all items in this seed'}
                        >
                          <input
                            type="checkbox"
                            checked={fullySelected}
                            disabled={seedActionBusy || selectableCount === 0}
                            aria-label="Select all items in this seed"
                            on:click|stopPropagation
                            on:keydown|stopPropagation
                            on:change|stopPropagation={async (e) => {
                              const input = e.currentTarget
                              await setAllSeedCandidatesSelected(source, input.checked)
                              // The click flipped the DOM state; if the model did not follow
                              // (nothing selectable loaded), put it back.
                              input.checked = isSeedFullySelected(source)
                            }}
                          />
                        </label>
                        {#if source.seed_kind === 'snowball'}
                          <span
                            class="tag snowball"
                            title={`${source.snowball?.direction === 'upstream' ? 'Upstream' : 'Downstream'} of «${source.snowball?.of_title || 'untitled work'}»`}
                          >Snowball items</span>
                        {:else if source.source_type === 'pdf'}
                          <span class="tag pending">Document items</span>
                        {:else}
                          <span class="tag queued">Search items</span>
                        {/if}
                        <strong>{source.label}</strong>
                      </div>
                      {#if runSubtitle(source)}
                        <span class={`muted small seed-run-status seed-run-status--${source.run.status}`} data-testid="seed-run-status">
                          {runSubtitle(source)}
                          {#if source.run.status === 'running'}
                            · <button type="button" class="link" on:click|stopPropagation={() => handleCancelSearch(source)}>cancel</button>
                          {/if}
                        </span>
                      {:else if source.subtitle}
                        <span class="muted small">{source.subtitle}</span>
                      {/if}
                    </div>
                    <div class="seed-source__summary-side">
                      <div class="seed-source__summary-actions">
                        <button
                          class="seed-source__action seed-source__action--promote"
                          type="button"
                          disabled={seedActionBusy || selectableCount === 0}
                          title={selectableCount === 0 ? 'No promotable items remain in this seed' : 'Promote all promotable items from this seed'}
                          aria-label="Promote all promotable items"
                          on:click|stopPropagation={() => handlePromoteWholeSeedSource(source)}
                        >
                          →
                        </button>
                        <button
                          class="seed-source__action seed-source__action--remove"
                          type="button"
                          disabled={seedActionBusy}
                          title="Remove this seed and its items"
                          aria-label="Remove seed"
                          on:click|stopPropagation={() => handleRemoveSeedSource(source)}
                        >
                          ×
                        </button>
                      </div>
                      <div class="pill-row">
                        <span class="pill" title="Total items found in this seed">{source.candidate_count} items</span>
                        {#if source.candidate_count - (source.state_counts?.in_corpus || 0) > 0}
                          <span class="pill" title="Not yet promoted or dismissed — your decision pending">To review: {source.candidate_count - (source.state_counts?.in_corpus || 0)}</span>
                        {/if}
                        {#if source.state_counts?.in_corpus}
                          <span class="pill" title="Already a member of this corpus — expand for each item's stage">In corpus: {source.state_counts.in_corpus}</span>
                        {/if}
                        {#if source.state_counts?.downloaded_elsewhere_available}
                          <span class="pill" title="Same work downloaded by another corpus and the file is present — promoting reuses it">PDF downloaded: {source.state_counts.downloaded_elsewhere_available}</span>
                        {/if}
                      </div>
                      <span class={`disclosure-chevron ${isExpanded ? 'open' : ''}`} aria-hidden="true">▸</span>
                    </div>
                  </div>

                  {#if isExpanded}
                      <div class="seed-source__body">
                        <div class="table-toolbar">
                          <div class="table-toolbar-left">
                            <span class="muted">
                              {#if seedAllSelected[sourceId]}
                                All {selectedCount.toLocaleString('en-US')} items selected
                              {:else}
                                Selected: {selectedCount} / {loadedSelectableCount} selectable
                              {/if}
                            </span>
                            <ColumnPicker table="seed" visibility={seedColumnVisibility} onChange={updateSeedColumns} />
                            <button class="secondary" type="button" on:click={() => setAllSeedCandidatesSelected(source, true)} disabled={seedActionBusy}>Select all</button>
                            <button class="secondary" type="button" on:click={() => clearSeedSelection(source)} disabled={seedActionBusy}>Clear</button>
                          </div>
                          <div class="table-toolbar-right">
                            <div class="toolbar-actions">
                              <button class="secondary" type="button" on:click={() => handleDismissSelectedSeed(source)} disabled={seedActionBusy || selectedCount === 0}>
                                Dismiss selected
                              </button>
                              <button class="primary" type="button" on:click={() => handlePromoteSeedSource(source)} disabled={seedActionBusy || selectedCount === 0}>
                                Promote to Corpus
                              </button>
                            </div>
                          </div>
                        </div>

                        {#if seedCandidatesLoading[sourceId]}
                          <p class="muted">Loading items...</p>
                        {:else if sourceCandidates.length === 0}
                          <p class="muted">No active items remain in this seed.</p>
                        {:else}
                          <div class="table table-scroll seed-candidate-table">
                            <div class="table-row header" style={seedGridStyle}>
                              <span class="ingest-select-cell" aria-hidden="true"></span>
                              {#each seedActiveColumns as column (column.key)}
                                {@const sortDisabled = (column.key === 'metadata' || column.key === 'download') && source.source_type === 'search' && page.total > 2000}
                                <span title={column.hint || ''}>
                                  {#if column.sortable}
                                    <button
                                      class="table-sort"
                                      type="button"
                                      disabled={sortDisabled}
                                      title={sortDisabled ? 'Sorting by state needs every item resolved; not available above 2,000 items' : ''}
                                      on:click={() => toggleSeedSort(source, column.key)}
                                    >
                                      {column.label}{seedSortIndicator(source, column.key, seedSorts)}
                                    </button>
                                  {:else}
                                    {column.label}
                                  {/if}
                                </span>
                              {/each}
                            </div>
                            {#each sourceCandidates as candidate (candidate.candidate_key)}
                              {@const activeCandidateKey = String(selectedSeedCandidateKeys[sourceId] || '')}
                              {@const candidateKey = String(candidate?.candidate_key || '')}
                              {@const active = activeCandidateKey !== '' && activeCandidateKey === candidateKey}
                              {@const checked = selection.has(candidateKey)}
                              {@const selectable = isSeedCandidateSelectable(candidate, includeDownstream, relatedDepthDownstream)}
                              <div
                                data-testid="seed-candidate-row"
                                class={`table-row clickable ${checked ? 'selected' : ''} ${active ? 'active-row' : ''}`}
                                style={seedGridStyle}
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
                                  {#if selectable}
                                    <input
                                      type="checkbox"
                                      {checked}
                                      on:click|stopPropagation
                                      on:change={(e) => {
                                        setSelectedSeedCandidate(source, candidate);
                                        toggleSeedCandidateSelection(source, candidate);
                                        e.currentTarget.checked = getSeedSelectionForSource(source).has(candidateKey);
                                      }}
                                    />
                                  {:else}
                                    <input type="checkbox" disabled />
                                  {/if}
                                </span>
                                {#each seedActiveColumns as column (column.key)}
                                  {@const text = cellText(candidate, column.key, SEED_CELL)}
                                  {#if column.key === 'corpus'}
                                    <span class="seed-candidate__corpus-cell">
                                      {#if isSeedCandidateInCorpus(candidate)}
                                        <span class="seed-candidate__promoted-check" title="Added to corpus" aria-label="Added to corpus">✓</span>
                                      {:else if selectable}
                                        <button
                                          class="seed-candidate__promote"
                                          type="button"
                                          title="Promote this item to the corpus using the current Promotion Settings"
                                          aria-label={`Promote ${formatTitle(candidate)} to corpus`}
                                          disabled={seedActionBusy}
                                          on:click|stopPropagation={() => handlePromoteSingleSeedCandidate(source, candidate)}
                                        >→ Promote</button>
                                      {/if}
                                    </span>
                                  {:else if column.key === 'download' && canDownloadSeedCandidate(candidate)}
                                    <span>
                                      <button
                                        class="link seed-download-link"
                                        type="button"
                                        title={`Download the file from work ${candidate.downloaded_work_id}`}
                                        on:click|stopPropagation={() => handleSeedCandidateFile(candidate)}
                                      >{text} ⤓</button>
                                    </span>
                                  {:else if column.key === 'authors'}
                                    <span title={formatAuthors(candidate)}>{text}</span>
                                  {:else}
                                    <span title={text}>{text}</span>
                                  {/if}
                                {/each}
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
                                      <div class="seed-refs-cell" title="Downstream: works this item cites. Upstream: works citing it.">
                                        <span class="seed-refs-cell__down">↓ Refs: {cellText(candidate, 'refs', SEED_CELL)}</span>
                                        <span class="seed-refs-cell__up">↑ Cited: {cellText(candidate, 'cited_by', SEED_CELL)}</span>
                                      </div>
                                      <div class="inline-detail-actions">
                                        {#if selectable}
                                          <button
                                            class="primary"
                                            type="button"
                                            disabled={seedActionBusy}
                                            title={expansionSummary
                                              ? `Promote just this item (reference expansion is on: ${expansionSummary})`
                                              : 'Promote just this item into the corpus'}
                                            on:click|stopPropagation={() => handlePromoteSingleSeedCandidate(source, candidate)}
                                          >
                                            Promote this item
                                          </button>
                                          {#if expansionSummary}
                                            <span class="muted small">Pulls in related works: {expansionSummary}, max {maxRelated}/paper.</span>
                                          {/if}
                                        {:else}
                                          <span class="muted small">
                                            {isSeedCandidateInCorpus(candidate) ? 'Already in this corpus.' : 'Already downloaded.'}
                                          </span>
                                        {/if}
                                      </div>
                                    </div>
                                  </div>
                                </div>
                              {/if}
                            {/each}
                          </div>
                          {#if page.total > sourceCandidates.length}
                            <div class="seed-table-footer">
                              <span class="muted small">Showing {sourceCandidates.length.toLocaleString('en-US')} of {page.total.toLocaleString('en-US')}</span>
                              <button class="secondary" type="button" disabled={seedCandidatesLoading[sourceId] || seedAppending[sourceId]} on:click|stopPropagation={() => loadMoreSeedCandidates(source)}>Show more</button>
                            </div>
                          {/if}
                        {/if}
                      </div>
                  {/if}
                </div>
              {/each}
            </div>
          {/if}
        </div>
          </div>

          <div class="seed-corpus-column seed-corpus-column--corpus" id="section-corpus">
            <div class="corpus-workspace-shell">
              <!-- Keyed by corpus (D3): a switch resets the selection, the stage
                   filter and the expanded row. -->
              {#key currentCorpusId}
                <Corpus
                  {corpusSource}
                  {corpusRefreshFailed}
                  {corpusTotal}
                  {corpusItems}
                  {rawTotal}
                  {metadataTotal}
                  {downloadedTotal}
                  {failedEnrichmentTotal}
                  {failedDownloadTotal}
                  {bucketLabel}
                  {corpusFilterQuery}
                  {handleCorpusFilterInput}
                  {handleRemoveCorpusWork}
                  {handleRemoveSelectedCorpusWorks}
                  {toggleCorpusSort}
                  {corpusSort}
                  {doiHref}
                  {openAlexHref}
                  {handleDownloadedCorpusFile}
                  {corpusLoadStatus}
                  {corpusHasMore}
                  {corpusLoadingMore}
                  {loadCorpus}
                  {handleCorpusColumnScroll}
                />
              {/key}
            </div>
          </div>
        </div>

        </div>
      {/if}

      {#if activeTab === 'upstream'}
        <UpstreamBrowser
          corpora={corpora}
          currentCorpus={currentCorpus}
          currentCorpusId={currentCorpusId}
          corpusOptionLabel={corpusOptionLabel}
          onSelectCorpusChange={handleSelectCorpus}
          onCreateCorpus={handleCreateCorpus}
          onDeleteCorpus={handleDeleteCorpus}
          creatingCorpus={creatingCorpus}
          deletingCorpus={deletingCorpus}
          canDeleteCurrentCorpus={canDeleteCurrentCorpus}
          corpusActionStatus={corpusActionStatus}
          corpusActionError={corpusActionError}
          targets={kantroposTargets}
          selectedTargetId={selectedUpstreamTargetId}
          selectTarget={handleSelectUpstreamTarget}
          selectedKantroposTargetId={selectedKantroposTargetId}
          onKantroposTargetChange={handleKantroposTargetChange}
          canConfigureCurrentCorpusKantropos={canConfigureCurrentCorpusKantropos}
          kantroposAssignmentLoading={kantroposAssignmentLoading}
          kantroposAssignmentSaving={kantroposAssignmentSaving}
          kantroposAssignment={kantroposAssignment}
          kantroposAssignmentStatus={kantroposAssignmentStatus}
          kantroposAssignmentError={kantroposAssignmentError}
          bind:shareUsername={shareUsername}
          bind:shareRole={shareRole}
          {shareStatus}
          {handleShareCorpus}
          refresh={loadUpstreamBrowse}
          loadMoreCurrent={() => loadUpstreamBrowse({ appendCurrent: true })}
          loadMorePending={() => loadUpstreamBrowse({ appendPending: true })}
          loading={upstreamBrowseLoading}
          status={upstreamBrowseStatus}
          browse={upstreamBrowse}
          selectedAssignedCorpusId={selectedUpstreamAssignedCorpusId}
          toggleAssignedCorpus={handleToggleUpstreamAssignedCorpus}
          {doiHref}
          {openAlexHref}
        />
      {/if}

      {#if activeTab === 'scraper'}
        <ScraperLab />
      {/if}

      {#if activeTab === 'admin'}
        <AdminPanel
          bind:inviteUsername={inviteUsername}
          bind:inviteEmail={inviteEmail}
          {inviteStatus}
          {inviteError}
          {handleCreateInvitation}
          {appSettings}
          {appSettingsStatus}
          {appSettingsError}
          bind:appSettingsDraft={appSettingsDraft}
          {handleSaveAppSettings}
          openalexQuotaText={openalexQuotaView.text}
        />
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
            {:else if downloadsSource === 'error'}
              Failed to load download activity.
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
                <span class="muted small">Download-only worker is disabled while the corpus pipeline is running.</span>
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
          {logsCursor}
          {handleLogTypeChange}
          {handleLogsCorpusFilterChange}
          {loadOlderLogs}
          {hydrateLogsFromTail}
        />
      {/if}

      {#if activeTab === 'graph'}
        <ThreeGraph {corpora} selectedGraphCorpusId={graphCorpusId} allowAllCorpora={isAdmin} />
      {/if}
    </section>
  </div>
</div>
{/if}
