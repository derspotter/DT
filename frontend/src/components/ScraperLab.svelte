<script>
  import { onMount } from 'svelte'
  import {
    applyScraperArtifacts,
    createScraperRun,
    downloadScraperMetadataDraft,
    fetchKantroposCorpora,
    fetchScraperRun,
    fetchScraperRuns,
  } from '../lib/api'

  let sourceType = 'eurlex'
  let eurlexForm = {
    fm_coded: '',
    dd_year: '',
    celex_id: '',
    lang: 'EN',
    text_query: '',
  }
  let expertGroupId = ''
  let runs = []
  let selectedRunId = ''
  let selectedRun = null
  let artifacts = []
  let selectedArtifactIds = []
  let targets = []
  let selectedTargetId = ''
  let status = ''
  let error = ''
  let busy = false
  let loadingRuns = false
  let loadingRun = false
  let applyResult = null
  let pollTimer = null
  const eurlexDocumentTypeExamples = [
    { code: 'REG', label: 'Regulation' },
    { code: 'DIR', label: 'Directive' },
    { code: 'DEC', label: 'Decision' },
  ]

  $: selectedRunIsActive = ['pending', 'running'].includes(String(selectedRun?.status || '').toLowerCase())
  $: downloadedArtifacts = artifacts.filter((artifact) => artifact.status === 'downloaded')
  $: failedArtifacts = artifacts.filter((artifact) => artifact.status === 'failed')
  $: selectedCount = selectedArtifactIds.length

  function sourceLabel(value) {
    return value === 'expert_groups' ? 'Expert Groups' : 'EUR-Lex'
  }

  function runStatusLabel(run) {
    const status = String(run?.status || 'pending')
    const total = Number(run?.total_count || 0)
    const downloaded = Number(run?.downloaded_count || 0)
    const failed = Number(run?.failed_count || 0)
    if (status === 'completed') return `${downloaded}/${total} ready${failed ? `, ${failed} failed` : ''}`
    if (status === 'failed') return run?.error || 'Run failed'
    if (status === 'running') return 'Worker is collecting documents'
    return 'Queued for background worker'
  }

  function statusClass(value) {
    const status = String(value || '').toLowerCase()
    if (status === 'downloaded' || status === 'completed') return 'completed'
    if (status === 'failed') return 'failed'
    if (status === 'running') return 'in_progress'
    return 'pending'
  }

  function resetSelection() {
    selectedArtifactIds = []
    applyResult = null
  }

  function scraperQuery() {
    if (sourceType === 'expert_groups') {
      return { group_id: expertGroupId.trim() }
    }
    return Object.fromEntries(
      Object.entries(eurlexForm).map(([key, value]) => [key, String(value || '').trim()])
    )
  }

  async function loadTargets() {
    const payload = await fetchKantroposCorpora()
    targets = payload.corpora || []
    if (!selectedTargetId && targets.length > 0) {
      selectedTargetId = String(targets[0].id)
    }
  }

  async function loadRuns({ quiet = false } = {}) {
    if (!quiet) {
      loadingRuns = true
      status = 'Loading scraper runs...'
    }
    try {
      const payload = await fetchScraperRuns(50)
      runs = payload.runs || []
      if (!selectedRunId && runs.length > 0) {
        selectedRunId = String(runs[0].id)
        await loadRun(selectedRunId, { quiet: true })
      }
      if (!quiet) status = runs.length ? 'Loaded scraper runs.' : 'No scraper runs yet.'
    } catch (err) {
      error = err?.message || 'Failed to load scraper runs.'
    } finally {
      loadingRuns = false
    }
  }

  async function loadRun(runId, { quiet = false } = {}) {
    if (!runId) return
    if (!quiet) {
      loadingRun = true
      status = 'Loading scraper run...'
    }
    try {
      const payload = await fetchScraperRun(runId)
      selectedRun = payload.run || null
      artifacts = payload.artifacts || []
      selectedRunId = String(runId)
      selectedArtifactIds = selectedArtifactIds.filter((id) => artifacts.some((artifact) => Number(artifact.id) === Number(id)))
      if (!quiet) status = 'Loaded scraper run.'
    } catch (err) {
      error = err?.message || 'Failed to load scraper run.'
    } finally {
      loadingRun = false
    }
  }

  async function startRun(event) {
    event.preventDefault()
    busy = true
    error = ''
    applyResult = null
    status = `Starting ${sourceLabel(sourceType)} scrape...`
    try {
      const payload = await createScraperRun({ sourceType, query: scraperQuery() })
      await loadRuns({ quiet: true })
      selectedRunId = String(payload?.run?.id || '')
      if (selectedRunId) await loadRun(selectedRunId, { quiet: true })
      status = 'Scraper run queued. The background daemon will process it.'
    } catch (err) {
      error = err?.message || 'Failed to start scraper run.'
    } finally {
      busy = false
    }
  }

  function toggleArtifact(artifact) {
    if (!artifact || artifact.status !== 'downloaded') return
    const id = Number(artifact.id)
    selectedArtifactIds = selectedArtifactIds.includes(id)
      ? selectedArtifactIds.filter((value) => value !== id)
      : [...selectedArtifactIds, id]
  }

  function selectAllDownloaded() {
    selectedArtifactIds = downloadedArtifacts.map((artifact) => Number(artifact.id))
  }

  async function applySelected() {
    if (!selectedTargetId || selectedArtifactIds.length === 0) return
    busy = true
    error = ''
    applyResult = null
    status = 'Creating metadata.bib draft...'
    try {
      applyResult = await applyScraperArtifacts({
        targetId: selectedTargetId,
        artifactIds: selectedArtifactIds,
      })
      status = `Drafted ${applyResult.applied_count || 0}; skipped ${applyResult.skipped_count || 0}; errors ${applyResult.error_count || 0}.`
      selectedArtifactIds = []
      if (selectedRunId) await loadRun(selectedRunId, { quiet: true })
    } catch (err) {
      error = err?.message || 'Failed to create metadata.bib draft.'
    } finally {
      busy = false
    }
  }

  async function downloadMetadataDraft() {
    if (!applyResult?.event_id) return
    busy = true
    error = ''
    status = 'Downloading generated metadata.bib...'
    try {
      const { blob, filename } = await downloadScraperMetadataDraft(applyResult.event_id)
      const url = URL.createObjectURL(blob)
      const link = document.createElement('a')
      link.href = url
      link.download = filename || 'metadata.bib'
      document.body.appendChild(link)
      link.click()
      link.remove()
      URL.revokeObjectURL(url)
      status = 'Generated metadata.bib downloaded.'
    } catch (err) {
      error = err?.message || 'Failed to download generated metadata.bib.'
    } finally {
      busy = false
    }
  }

  onMount(() => {
    void loadTargets()
    void loadRuns()
    pollTimer = setInterval(() => {
      if (selectedRunId && selectedRunIsActive) {
        void loadRun(selectedRunId, { quiet: true })
        void loadRuns({ quiet: true })
      }
    }, 3000)
    return () => {
      if (pollTimer) clearInterval(pollTimer)
      pollTimer = null
    }
  })
</script>

<div class="scraper-lab">
  <section class="scraper-hero">
    <div class="scraper-hero__copy">
      <p class="eyebrow">Scraper Lab</p>
      <h2>Collect EU source material without touching the literature corpus.</h2>
      <p>
        EUR-Lex and Expert Groups runs stay quarantined as raw artifacts. Selected downloaded files create a reviewable metadata.bib draft.
      </p>
    </div>
    <div class="scraper-hero__stats">
      <span><strong>{runs.length}</strong> runs</span>
      <span><strong>{artifacts.length}</strong> artifacts shown</span>
      <span><strong>{downloadedArtifacts.length}</strong> ready</span>
    </div>
  </section>

  <section class="card scraper-card scraper-card--launcher">
    <div class="scraper-card__header">
      <div>
        <h3>Start a run</h3>
        <p class="muted">Choose a source and submit the smallest useful query. Broad scrapes can take a while.</p>
      </div>
      <button class="secondary" type="button" on:click={() => loadRuns()} disabled={loadingRuns}>Refresh</button>
    </div>

    <form class="scraper-form" on:submit={startRun}>
      <div class="source-switch" role="group" aria-label="Scraper source">
        <button
          type="button"
          class:active={sourceType === 'eurlex'}
          on:click={() => { sourceType = 'eurlex' }}
        >
          <span>EUR-Lex</span>
          <small>CELEX, titles, legal documents</small>
        </button>
        <button
          type="button"
          class:active={sourceType === 'expert_groups'}
          on:click={() => { sourceType = 'expert_groups' }}
        >
          <span>Expert Groups</span>
          <small>Meetings and attached documents</small>
        </button>
      </div>

      {#if sourceType === 'eurlex'}
        <div class="field-help-panel">
          <strong>EUR-Lex search fields</strong>
          <span>Use one field or combine several. If unsure, start with a year and a few title words.</span>
        </div>
        <div class="scraper-grid">
          <label>
            <span>Document type</span>
            <input bind:value={eurlexForm.fm_coded} placeholder="REG" list="eurlex-document-types" />
            <small>Technical field: <code>FM_CODED</code>. Examples: REG = regulation, DIR = directive, DEC = decision.</small>
          </label>
          <label>
            <span>Document year</span>
            <input bind:value={eurlexForm.dd_year} placeholder="2024" inputmode="numeric" />
            <small>Technical field: <code>DD_YEAR</code>. The year the document was published/adopted.</small>
          </label>
          <label>
            <span>CELEX number</span>
            <input bind:value={eurlexForm.celex_id} placeholder="32024R1689" />
            <small>The unique EUR-Lex document id. Best option if you already know the exact document.</small>
          </label>
          <label>
            <span>Language</span>
            <input bind:value={eurlexForm.lang} placeholder="EN" />
            <small>Usually <code>EN</code>. Use <code>DE</code> for German documents.</small>
          </label>
        </div>
        <datalist id="eurlex-document-types">
          {#each eurlexDocumentTypeExamples as item}
            <option value={item.code}>{item.label}</option>
          {/each}
        </datalist>
        <label>
          <span>Title text</span>
          <input bind:value={eurlexForm.text_query} placeholder="title contains..." />
          <small>Search words in the document title. Example: <code>artificial intelligence</code>.</small>
        </label>
      {:else}
        <div class="field-help-panel">
          <strong>Expert Groups search fields</strong>
          <span>Use the numeric groupID from the EU Expert Groups Register URL.</span>
        </div>
        <div class="scraper-grid">
          <label>
            <span>Expert group ID</span>
            <input bind:value={expertGroupId} placeholder="3745" inputmode="numeric" />
            <small>Enter the groupID from the EU Expert Groups Register URL. E.g. For the Group "E03745" look at the url "https://ec.europa.eu/transparency/expert-groups-register/screen/expert-groups/consult?lang=fr&amp;groupID=3745" and use the groupID at the end -> 3745.</small>
          </label>
        </div>
      {/if}

      <div class="scraper-actions">
        <span class="muted small">Runs are processed by the background worker. Results appear below automatically.</span>
        <button class="primary" type="submit" disabled={busy}>
          {busy ? 'Working...' : `Start ${sourceLabel(sourceType)} scrape`}
        </button>
      </div>
    </form>

    {#if status}<p class="muted">{status}</p>{/if}
    {#if error}<p class="error">{error}</p>{/if}
  </section>

  <section class="card scraper-card">
    <div class="scraper-card__header">
      <div>
        <h3>Runs</h3>
        <p class="muted">Recent scraper jobs. Select one to review its downloaded artifacts.</p>
      </div>
    </div>
    {#if runs.length === 0}
      <div class="empty-state">
        <strong>No scraper runs yet.</strong>
        <span>Start with a narrow EUR-Lex CELEX or year query.</span>
      </div>
    {:else}
      <div class="scraper-run-list">
        {#each runs as run}
          <button
            type="button"
            class:active={String(run.id) === String(selectedRunId)}
            on:click={() => { resetSelection(); loadRun(run.id) }}
          >
            <span class="run-card__top">
              <strong>{sourceLabel(run.source_type)}</strong>
              <span class={`tag ${statusClass(run.status)}`}>{run.status}</span>
            </span>
            <span>{runStatusLabel(run)}</span>
            <small>Run #{run.id} · {run.created_at || 'unknown time'}</small>
          </button>
        {/each}
      </div>
    {/if}
  </section>

  {#if selectedRun}
    <section class="card scraper-card">
      <div class="scraper-card__header">
        <div>
          <h3>Artifacts for run #{selectedRun.id}</h3>
          <p class="muted">{sourceLabel(selectedRun.source_type)} · {selectedRun.status} · {downloadedArtifacts.length} ready · {failedArtifacts.length} failed</p>
        </div>
        <button class="secondary" type="button" on:click={() => loadRun(selectedRunId)} disabled={loadingRun}>Reload run</button>
      </div>

      <div class="scraper-apply-bar">
        <div class="apply-target">
          <label>
            <span>Apply target</span>
            <select bind:value={selectedTargetId}>
              {#each targets as target}
                <option value={target.id}>{target.name}</option>
              {/each}
            </select>
          </label>
        </div>
        <div class="apply-actions">
          <span class="selection-pill">{selectedCount} selected</span>
          <button class="secondary" type="button" on:click={selectAllDownloaded} disabled={downloadedArtifacts.length === 0 || busy}>Select ready</button>
          <button class="secondary" type="button" on:click={resetSelection} disabled={selectedCount === 0 || busy}>Clear</button>
          <button class="primary" type="button" on:click={applySelected} disabled={!selectedTargetId || selectedCount === 0 || busy}>
            Create metadata.bib draft
          </button>
        </div>
        <span class="apply-target-help muted small">The upstream metadata.bib is read only; this creates a downloadable draft.</span>
      </div>

      {#if applyResult}
        <div class="scraper-draft-result">
          <p class="muted">Last draft: {applyResult.applied_count} entries, {applyResult.skipped_count} skipped, {applyResult.error_count} errors.</p>
          {#if applyResult.generated_bib_url}
            <button class="secondary" type="button" on:click={downloadMetadataDraft} disabled={busy}>Download metadata.bib</button>
          {/if}
        </div>
      {/if}

      {#if artifacts.length === 0}
        <div class="empty-state">
          <strong>No artifacts for this run yet.</strong>
          <span>If the run is still pending, wait for the worker to pick it up.</span>
        </div>
      {:else}
      <div class="scraper-table-shell">
        <div class="table table-scroll scraper-table">
        <div class="table-row header scraper-cols">
          <span></span>
          <span>Title</span>
          <span>Date</span>
          <span>Status</span>
          <span>Key</span>
        </div>
        {#each artifacts as artifact}
          <div class={`table-row scraper-cols ${selectedArtifactIds.includes(Number(artifact.id)) ? 'selected' : ''}`}>
            <input
              type="checkbox"
              checked={selectedArtifactIds.includes(Number(artifact.id))}
              disabled={artifact.status !== 'downloaded'}
              on:change={() => toggleArtifact(artifact)}
              aria-label={`Select ${artifact.title}`}
            />
            <span class="line-clamp-2" title={artifact.title}>{artifact.title || 'Untitled'}</span>
            <span>{artifact.date || artifact.year || '-'}</span>
            <span class={`tag ${statusClass(artifact.status)}`}>{artifact.status}</span>
            <span class="small">{artifact.bibtex_key}</span>
            {#if artifact.error}
              <div class="table-row scraper-detail">
                <span class="error small">{artifact.error}</span>
              </div>
            {/if}
          </div>
        {/each}
        </div>
      </div>
      {/if}
    </section>
  {/if}
</div>

<style>
  .scraper-lab {
    display: grid;
    gap: 18px;
  }

  .scraper-hero {
    position: relative;
    overflow: hidden;
    display: grid;
    grid-template-columns: minmax(0, 1fr) auto;
    gap: 18px;
    padding: 24px;
    border: 1px solid rgba(15, 139, 141, 0.22);
    border-radius: 24px;
    background:
      radial-gradient(circle at 92% 10%, rgba(244, 184, 96, 0.32), transparent 32%),
      linear-gradient(135deg, rgba(255, 255, 255, 0.94), rgba(231, 244, 242, 0.92));
    box-shadow: var(--shadow);
  }

  .scraper-hero::before {
    content: '';
    position: absolute;
    inset: auto -40px -78px auto;
    width: 220px;
    height: 220px;
    border-radius: 999px;
    border: 36px solid rgba(15, 139, 141, 0.12);
  }

  .scraper-hero__copy {
    position: relative;
    max-width: 860px;
  }

  .scraper-hero h2 {
    max-width: 760px;
    margin: 0;
    font-family: 'Instrument Serif', serif;
    font-size: clamp(2rem, 4vw, 3.4rem);
    line-height: 0.96;
    color: #18313a;
  }

  .scraper-hero p:not(.eyebrow) {
    max-width: 720px;
    margin: 12px 0 0;
    color: var(--muted);
    font-size: 1rem;
  }

  .scraper-hero__stats {
    position: relative;
    display: grid;
    gap: 10px;
    align-content: center;
    min-width: 180px;
  }

  .scraper-hero__stats span,
  .selection-pill {
    display: inline-flex;
    align-items: baseline;
    justify-content: space-between;
    gap: 10px;
    padding: 10px 12px;
    border: 1px solid rgba(15, 139, 141, 0.2);
    border-radius: 999px;
    background: rgba(255, 255, 255, 0.78);
    color: var(--muted);
    font-size: 0.84rem;
  }

  .scraper-hero__stats strong {
    color: var(--ink);
    font-size: 1.1rem;
  }

  .scraper-card__header {
    align-items: flex-end;
    display: flex;
    flex-wrap: wrap;
    gap: 1rem;
    justify-content: space-between;
  }

  .scraper-card__header > div {
    min-width: min(320px, 100%);
  }

  .scraper-card__header > button {
    flex: 0 0 auto;
  }

  .scraper-apply-bar {
    display: grid;
    grid-template-columns: minmax(280px, 420px) minmax(0, 1fr);
    gap: 8px 16px;
    align-items: end;
  }

  .scraper-card--launcher {
    background:
      linear-gradient(180deg, rgba(255, 255, 255, 0.98), rgba(251, 248, 244, 0.98));
  }

  .scraper-form {
    display: grid;
    gap: 1rem;
    margin-top: 1rem;
  }

  .source-switch {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 10px;
  }

  .source-switch button {
    display: grid;
    gap: 4px;
    min-height: 74px;
    padding: 14px 16px;
    border: 1px solid var(--stroke);
    border-radius: 16px;
    background: #fbfaf8;
    color: var(--ink);
    text-align: left;
    cursor: pointer;
  }

  .source-switch button.active {
    border-color: rgba(15, 139, 141, 0.46);
    background: linear-gradient(135deg, rgba(15, 139, 141, 0.12), rgba(244, 184, 96, 0.18));
    box-shadow: inset 0 0 0 1px rgba(15, 139, 141, 0.14);
  }

  .source-switch span {
    font-weight: 700;
  }

  .source-switch small,
  .run-card__top + span {
    color: var(--muted);
  }

  .scraper-grid {
    display: grid;
    align-items: start;
    gap: 0.75rem;
    grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
  }

  .scraper-form label,
  .apply-target label {
    display: grid;
    gap: 7px;
    color: var(--muted);
    font-size: 0.82rem;
  }

  .scraper-form input,
  .apply-target select {
    box-sizing: border-box;
    width: 100%;
    min-height: 40px;
    padding: 8px 12px;
    border: 1px solid var(--stroke);
    border-radius: 10px;
    background: #fff;
    color: var(--ink);
    font-size: 0.9rem;
  }

  .scraper-form small {
    color: var(--muted);
    font-size: 0.75rem;
    line-height: 1.35;
  }

  .scraper-form code {
    padding: 1px 5px;
    border-radius: 6px;
    background: rgba(15, 139, 141, 0.09);
    color: #17454b;
    font-size: 0.72rem;
  }

  .field-help-panel {
    display: grid;
    gap: 4px;
    padding: 12px 14px;
    border: 1px solid rgba(15, 139, 141, 0.18);
    border-radius: 14px;
    background: linear-gradient(135deg, rgba(235, 248, 246, 0.9), rgba(255, 249, 238, 0.92));
    color: var(--muted);
    font-size: 0.84rem;
  }

  .field-help-panel strong {
    color: var(--ink);
  }

  .scraper-form label > span,
  .apply-target label > span {
    letter-spacing: 0.04em;
    text-transform: uppercase;
    font-size: 0.68rem;
  }

  .scraper-actions {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
  }

  .scraper-run-list {
    display: grid;
    gap: 0.75rem;
    grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
  }

  .scraper-run-list button {
    align-items: flex-start;
    display: grid;
    gap: 0.45rem;
    min-height: 112px;
    padding: 14px;
    border: 1px solid var(--stroke);
    border-radius: 16px;
    background: #fbfaf8;
    text-align: left;
    cursor: pointer;
  }

  .scraper-run-list button.active {
    border-color: rgba(15, 139, 141, 0.52);
    background: linear-gradient(135deg, rgba(235, 248, 246, 0.98), rgba(255, 248, 235, 0.96));
  }

  .run-card__top,
  .apply-actions {
    display: flex;
    flex-wrap: wrap;
    gap: 10px;
    align-items: center;
  }

  .run-card__top {
    justify-content: space-between;
  }

  .apply-target {
    min-width: 0;
  }

  .apply-actions {
    justify-content: flex-end;
  }

  .apply-actions button {
    flex: 0 0 auto;
  }

  .apply-actions .selection-pill,
  .apply-actions button {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    min-height: 40px;
    line-height: 1;
  }

  .apply-target-help {
    grid-column: 1 / -1;
    margin-top: 0;
  }

  .scraper-draft-result {
    display: flex;
    flex-wrap: wrap;
    gap: 10px;
    align-items: center;
    justify-content: space-between;
    margin-top: 10px;
  }

  .scraper-draft-result p {
    margin: 0;
  }

  .scraper-table-shell {
    margin-top: 12px;
    overflow: hidden;
    border: 1px solid var(--stroke);
    border-radius: 16px;
    background: #fff;
  }

  .scraper-cols {
    grid-template-columns: 40px minmax(220px, 1fr) 130px 110px 220px;
  }

  .scraper-detail {
    grid-column: 2 / -1;
  }

  .empty-state {
    display: grid;
    gap: 5px;
    padding: 18px;
    border: 1px dashed rgba(91, 102, 112, 0.34);
    border-radius: 16px;
    background: rgba(245, 242, 238, 0.56);
    color: var(--muted);
  }

  .empty-state strong {
    color: var(--ink);
  }

  @media (max-width: 760px) {
    .scraper-hero {
      grid-template-columns: 1fr;
      padding: 20px;
    }

    .scraper-hero__stats,
    .source-switch {
      grid-template-columns: 1fr;
    }

    .scraper-apply-bar,
    .apply-actions,
    .scraper-actions {
      align-items: stretch;
    }

    .scraper-apply-bar {
      grid-template-columns: 1fr;
    }

    .apply-actions,
    .scraper-actions {
      flex-direction: column;
    }

    .scraper-cols {
      grid-template-columns: 32px minmax(160px, 1fr) 90px;
    }

    .scraper-cols > span:nth-child(4),
    .scraper-cols > span:nth-child(5) {
      display: none;
    }
  }
</style>
