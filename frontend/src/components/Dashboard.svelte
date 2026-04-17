<script>
  export let currentCorpusName = 'Current corpus'
  export let pipelineRawCount
  export let pipelineMetadataCount
  export let pipelineDownloadedCount
  export let daemonStatus = { online: false, workers: {} }
  export let workerBacklog = {}
  export let pipelineWorkerStatus = ''
  export let recentDownloads = []
  export let showGlobalOverview = false
  export let globalStats = { raw_pending: 0, matched: 0, queued_download: 0, downloaded: 0 }
  export let globalBacklog = {}

  $: enrichWorker = daemonStatus?.workers?.enrich || { online: false, status: 'offline', last_seen_at: null, details: null }
  $: downloadWorker = daemonStatus?.workers?.download || { online: false, status: 'offline', last_seen_at: null, details: null }

  function workerLabel(worker) {
    if (!worker) return 'Offline'
    if (worker.online && worker.status) return String(worker.status)
    return worker.online ? 'Online' : 'Offline'
  }

  function formatWorkerDetail(worker) {
    const details = worker?.details || {}
    if (details?.state === 'processed_batch' && details?.result) {
      const result = details.result
      const parts = []
      if (Number(result.processed || 0) > 0) parts.push(`processed ${result.processed}`)
      if (Number(result.promoted || 0) > 0) parts.push(`promoted ${result.promoted}`)
      if (Number(result.queued || 0) > 0) parts.push(`queued ${result.queued}`)
      if (Number(result.downloaded || 0) > 0) parts.push(`downloaded ${result.downloaded}`)
      if (Number(result.failed || 0) > 0) parts.push(`failed ${result.failed}`)
      return parts.join(' · ')
    }
    if (details?.state === 'running_job' && details?.job_type) {
      return `${details.job_type}${details.corpus_id ? ` · corpus ${details.corpus_id}` : ''}`
    }
    return ''
  }
</script>

<div class="grid">
  <div class="card highlight" data-testid="dashboard-overview">
    <h2>Current corpus</h2>
    <p>Live pipeline state for {currentCorpusName}.</p>
    <div class="pipeline-strip" data-testid="pipeline-strip">
      <div class="pipeline-stage">
        <span class="stat-label" title="Raw references">Raw</span>
        <strong>{pipelineRawCount}</strong>
      </div>
      <span class="pipeline-arrow" aria-hidden="true">→</span>
      <div class="pipeline-stage">
        <span class="stat-label" title="With metadata">Meta</span>
        <strong>{pipelineMetadataCount}</strong>
      </div>
      <span class="pipeline-arrow" aria-hidden="true">→</span>
      <div class="pipeline-stage">
        <span class="stat-label" title="Downloaded">Done</span>
        <strong>{pipelineDownloadedCount}</strong>
      </div>
    </div>
  </div>

  <div class="card">
    <h2>Current corpus backlog</h2>
    <p>Current in-flight and queued work for {currentCorpusName}. Finished downloads are shown separately below.</p>
    <div class="pipeline-strip">
      <div class="pipeline-stage">
        <span class="stat-label">Enrich</span>
        <strong class:running={enrichWorker.online}>{workerLabel(enrichWorker)}</strong>
        {#if formatWorkerDetail(enrichWorker)}
          <span class="muted small">{formatWorkerDetail(enrichWorker)}</span>
        {/if}
      </div>
      <div class="pipeline-stage">
        <span class="stat-label">Download</span>
        <strong class:running={downloadWorker.online}>{workerLabel(downloadWorker)}</strong>
        {#if formatWorkerDetail(downloadWorker)}
          <span class="muted small">{formatWorkerDetail(downloadWorker)}</span>
        {/if}
      </div>
    </div>
    <div class="pipeline-strip" style="margin-top: 12px;">
      <div class="pipeline-stage">
        <span class="stat-label">Raw pending</span>
        <strong>{Number(workerBacklog?.raw_pending || 0)}</strong>
      </div>
      <div class="pipeline-stage">
        <span class="stat-label">Enriching</span>
        <strong>{Number(workerBacklog?.enriching || 0)}</strong>
      </div>
      <div class="pipeline-stage">
        <span class="stat-label">Queued DL</span>
        <strong>{Number(workerBacklog?.queued_download || 0)}</strong>
      </div>
      <div class="pipeline-stage">
        <span class="stat-label">Downloading</span>
        <strong>{Number(workerBacklog?.downloading || 0)}</strong>
      </div>
    </div>
    {#if pipelineWorkerStatus}
      <span class="muted small" style="display: block; margin-top: 10px;">{pipelineWorkerStatus}</span>
    {/if}
  </div>

  {#if showGlobalOverview}
    <div class="card">
      <h2>Global overview</h2>
      <p>Pipeline totals across all corpora in the app.</p>
      <div class="pipeline-strip">
        <div class="pipeline-stage">
          <span class="stat-label">Raw</span>
          <strong>{Number(globalStats?.raw_pending || 0)}</strong>
        </div>
        <span class="pipeline-arrow" aria-hidden="true">→</span>
        <div class="pipeline-stage">
          <span class="stat-label">Meta</span>
          <strong>{Number(globalStats?.matched || 0)}</strong>
        </div>
        <span class="pipeline-arrow" aria-hidden="true">→</span>
        <div class="pipeline-stage">
          <span class="stat-label">Queued DL</span>
          <strong>{Number(globalStats?.queued_download || 0)}</strong>
        </div>
        <span class="pipeline-arrow" aria-hidden="true">→</span>
        <div class="pipeline-stage">
          <span class="stat-label">Done</span>
          <strong>{Number(globalStats?.downloaded || 0)}</strong>
        </div>
      </div>
      <div class="pipeline-strip" style="margin-top: 12px;">
        <div class="pipeline-stage">
          <span class="stat-label">Enriching</span>
          <strong>{Number(globalBacklog?.enriching || 0)}</strong>
        </div>
        <div class="pipeline-stage">
          <span class="stat-label">Downloading</span>
          <strong>{Number(globalBacklog?.downloading || 0)}</strong>
        </div>
        <div class="pipeline-stage">
          <span class="stat-label">Failed enrich</span>
          <strong>{Number(globalBacklog?.failed_enrichment || 0)}</strong>
        </div>
        <div class="pipeline-stage">
          <span class="stat-label">Failed DL</span>
          <strong>{Number(globalBacklog?.failed_download || 0)}</strong>
        </div>
      </div>
    </div>
  {/if}

  <div class="card">
    <h2>Recent downloads</h2>
    <ul class="activity">
      {#each recentDownloads as item}
        <li>
          <div>
            <strong>{item.title}</strong>
            <span>Downloaded</span>
          </div>
          <time>{item.downloaded_at || ''}</time>
        </li>
      {:else}
        <li>
          <div>
            <strong>No recent downloads</strong>
            <span>The backend has not reported any downloaded items yet.</span>
          </div>
          <time></time>
        </li>
      {/each}
    </ul>
  </div>
</div>
