<script>
  export let pipelineRawCount;
  export let pipelineMetadataCount;
  export let pipelineDownloadedCount;
  export let pipelineWorker;
  export let pipelineWorkerBusy;
  export let pipelineWorkerStatus;
  export let sampleActivity;
  export let handleStartPipelineWorker;
  export let handlePausePipelineWorker;
</script>

<div class="grid">
  <div class="card highlight" data-testid="dashboard-overview">
    <h2>At a glance</h2>
    <p>Seed ingestion, keyword search, and corpus health in one view.</p>
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
    <h2>Corpus Pipeline</h2>
    <p>Control the background workers for extracting, enriching, and downloading references in the current corpus.</p>
    
    <div class="header-pipeline" style="border: none; padding-top: 0;">
      <div class="header-pipeline__title">
        <div class="header-pipeline__state">
          <strong class:running={pipelineWorker.running}>{pipelineWorker.running ? 'Running' : 'Paused'}</strong>
          {#if pipelineWorker.in_flight}
            <span class="tag queued">in progress</span>
          {/if}
        </div>
      </div>
      <div class="header-pipeline__stats">
        <div class="pipeline-kpi">
          <span class="muted small">Raw → Meta batch</span>
          <strong>{pipelineWorker?.config?.promoteBatchSize ?? 25}</strong>
        </div>
        <div class="pipeline-kpi">
          <span class="muted small">Enrich workers</span>
          <strong>{pipelineWorker?.config?.promoteWorkers ?? 6}</strong>
        </div>
        <div class="pipeline-kpi">
          <span class="muted small">Download batch</span>
          <strong>{pipelineWorker?.config?.downloadBatchSize ?? 5}</strong>
        </div>
        <div class="pipeline-kpi">
          <span class="muted small">Download workers</span>
          <strong>{Math.max(1, Number(pipelineWorker?.resolved_download_workers || 0))}</strong>
        </div>
      </div>
      <div class="header-pipeline__actions" style="margin-top: 10px;">
        <button
          class="primary"
          type="button"
          on:click={handleStartPipelineWorker}
          disabled={pipelineWorkerBusy || pipelineWorker.running}
        >
          Start / Continue
        </button>
        <button
          class="secondary"
          type="button"
          on:click={handlePausePipelineWorker}
          disabled={!pipelineWorker.running && !pipelineWorker.in_flight}
        >
          Pause
        </button>
      </div>
      {#if pipelineWorkerStatus}
        <span class="muted small" style="display: block; margin-top: 10px;">{pipelineWorkerStatus}</span>
      {/if}
      {#if pipelineWorker.last_error}
        <span class="error small" style="display: block; margin-top: 5px;">{pipelineWorker.last_error}</span>
      {/if}
    </div>
  </div>
</div>
