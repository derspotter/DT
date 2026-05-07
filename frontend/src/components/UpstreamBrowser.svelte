<script>
  import UpstreamCorpusMap from './UpstreamCorpusMap.svelte'
  import { createUpstreamCorpusItemDownloadUrl } from '../lib/api'

  export let corpora = []
  export let currentCorpus = null
  export let currentCorpusId = null
  export let corpusOptionLabel = (corpus) => corpus?.name || ''
  export let onSelectCorpusChange = () => {}
  export let onCreateCorpus = () => {}
  export let onDeleteCorpus = () => {}
  export let creatingCorpus = false
  export let deletingCorpus = false
  export let canDeleteCurrentCorpus = false
  export let corpusActionStatus = ''
  export let corpusActionError = false
  export let targets = []
  export let selectedTargetId = ''
  export let selectTarget = () => {}
  export let selectedKantroposTargetId = ''
  export let onKantroposTargetChange = () => {}
  export let canConfigureCurrentCorpusKantropos = false
  export let kantroposAssignmentLoading = false
  export let kantroposAssignmentSaving = false
  export let kantroposAssignment = null
  export let kantroposAssignmentStatus = ''
  export let kantroposAssignmentError = false
  export let shareUsername = ''
  export let shareRole = 'viewer'
  export let shareStatus = ''
  export let handleShareCorpus = () => {}
  export let refresh = () => {}
  export let loading = false
  export let status = ''
  export let browse = {
    target: null,
    summary: {},
    assigned_corpora: [],
    current_items: [],
    pending_items: [],
  }
  export let selectedAssignedCorpusId = null
  export let toggleAssignedCorpus = () => {}
  export let loadMoreCurrent = () => {}
  export let loadMorePending = () => {}
  export let formatAuthors = () => ''
  export let doiHref = () => ''
  export let openAlexHref = () => ''

  let pendingTextFilter = ''
  let currentTextFilter = ''
  let activePendingKey = ''
  let activeCurrentKey = ''
  let currentDownloadStatus = ''
  let currentDownloadError = false

  function itemKey(prefix, item) {
    if (!item || item.id === null || item.id === undefined) return ''
    return `${prefix}:${item.id}`
  }

  function itemAuthors(item) {
    if (!item) return ''
    if (typeof item.authors_display === 'string' && item.authors_display.trim()) return item.authors_display
    return formatAuthors(item)
  }

  function itemSource(item) {
    const value = String(item?.source || '').trim()
    if (!value) return ''
    if (value.endsWith('/metadata.bib')) return 'metadata.bib import'
    return value
  }

  function itemOpenAlexLabel(item) {
    const value = String(item?.openalex_id || '').trim()
    if (!value) return ''
    return value
  }

  function itemMatchesFilter(item, filterValue) {
    const filter = String(filterValue || '').trim().toLowerCase()
    if (!filter) return true
    const keywords = filter.split(/\s+/).filter(Boolean)
    const title = String(item?.title || '').toLowerCase()
    const authors = String(itemAuthors(item) || '').toLowerCase()
    const year = String(item?.year || '').toLowerCase()
    const sourceCorpora = Array.isArray(item?.source_corpora)
      ? item.source_corpora.map((entry) => `${entry?.name || ''} ${entry?.owner_username || ''}`.toLowerCase()).join(' ')
      : ''
    return keywords.every((keyword) => (
      title.includes(keyword) ||
      authors.includes(keyword) ||
      year.includes(keyword) ||
      sourceCorpora.includes(keyword)
    ))
  }

  function togglePendingItem(item) {
    const key = itemKey('pending', item)
    if (!key) return
    activePendingKey = activePendingKey === key ? '' : key
  }

  function toggleCurrentItem(item) {
    const key = itemKey('current', item)
    if (!key) return
    activeCurrentKey = activeCurrentKey === key ? '' : key
  }

  function isCurrentItemDownloadable(item) {
    return String(item?.download_status || '').toLowerCase() === 'downloaded' && Boolean(String(item?.file_path || '').trim())
  }

  async function downloadCurrentItem(item) {
    if (!selectedTargetId || !isCurrentItemDownloadable(item)) return
    currentDownloadStatus = 'Preparing download...'
    currentDownloadError = false
    try {
      const url = await createUpstreamCorpusItemDownloadUrl(selectedTargetId, item.id)
      const link = document.createElement('a')
      link.href = url
      link.rel = 'noopener'
      document.body.appendChild(link)
      link.click()
      link.remove()
      currentDownloadStatus = 'Download started.'
    } catch (err) {
      currentDownloadError = true
      currentDownloadStatus = err?.message || 'Failed to download upstream file.'
    }
  }

  function assignedButtonLabel(entry) {
    if (!entry) return ''
    const base = String(entry.name || '').trim() || `Corpus ${entry.id}`
    const pendingCount = Number(entry.pending_count || 0)
    return `${base} (${pendingCount})`
  }

  function formatMetadataDate(value) {
    const text = String(value || '').trim()
    if (!text) return 'Unknown'
    const date = new Date(text)
    if (Number.isNaN(date.getTime())) return text
    return date.toLocaleString()
  }

  function currentCorpusLabel() {
    const name = String(currentCorpus?.name || '').trim()
    if (name) return name
    const numericId = Number(currentCorpusId)
    return Number.isFinite(numericId) && numericId > 0 ? `Corpus ${numericId}` : 'No corpus selected'
  }

  $: allAssignedPendingCount = Number.isFinite(Number(browse?.summary?.pending_items_count))
    ? Number(browse.summary.pending_items_count)
    : (browse?.assigned_corpora || []).reduce((total, entry) => total + Number(entry?.pending_count || 0), 0)

  $: filteredPendingItems = (browse?.pending_items || []).filter((item) => {
    if (selectedAssignedCorpusId !== null && selectedAssignedCorpusId !== undefined && selectedAssignedCorpusId !== '') {
      const sourceIds = Array.isArray(item?.source_corpus_ids) ? item.source_corpus_ids.map((value) => Number(value)) : []
      if (!sourceIds.includes(Number(selectedAssignedCorpusId))) return false
    }
    return itemMatchesFilter(item, pendingTextFilter)
  })

  $: filteredCurrentItems = (browse?.current_items || []).filter((item) => itemMatchesFilter(item, currentTextFilter))

  $: if (activePendingKey) {
    const exists = filteredPendingItems.some((item) => itemKey('pending', item) === activePendingKey)
    if (!exists) activePendingKey = ''
  }

  $: if (activeCurrentKey) {
    const exists = filteredCurrentItems.some((item) => itemKey('current', item) === activeCurrentKey)
    if (!exists) activeCurrentKey = ''
  }
</script>

<div class="upstream-browser">
  <section class="tab-hero tab-hero--upstream">
    <div class="tab-hero__copy">
      <p class="eyebrow">Korpus Management</p>
      <h2>Keep local corpora aligned with the upstream baseline.</h2>
      <p>
        Map app corpora to their upstream targets, share access, and compare pending local additions against the imported metadata.bib baseline.
      </p>
    </div>
    <div class="tab-hero__stats">
      <span><strong>{targets.length}</strong> targets</span>
      <span><strong>{browse?.summary?.pending_items_count || 0}</strong> pending</span>
      <span><strong>{browse?.summary?.current_items_count || 0}</strong> upstream</span>
    </div>
  </section>

  <div class="card upstream-browser__toolbar">
    <div class="workspace-panel-header">
      <div class="workspace-panel-title upstream-browser__title">
        <h2 class="workspace-section-title">Management Console</h2>
        <p>Choose the active upstream target, configure corpus mapping, and manage sharing for the selected local corpus.</p>
      </div>
    </div>

    <div class="upstream-management-grid">
      <div class="upstream-management-card upstream-management-card--mapping">
        <div class="workspace-panel-title upstream-management-card__title">
          <h3 class="workspace-section-title">Corpus Mapping</h3>
          <p class="muted">Select an app corpus and map it to the upstream corpus it belongs to.</p>
        </div>
        <div class="upstream-management-form">
          <div class="upstream-management-form__app-corpus">
            <label class="upstream-management-form__field">
              <span class="muted small">App corpus</span>
              <select value={currentCorpusId || ''} on:change={onSelectCorpusChange}>
                {#each corpora as corpus}
                  <option value={corpus.id}>{corpusOptionLabel(corpus)}</option>
                {/each}
              </select>
            </label>

            <div class="upstream-management-form__actions">
              <button class="secondary" type="button" on:click={onCreateCorpus} disabled={creatingCorpus || deletingCorpus}>
                {creatingCorpus ? 'Creating...' : 'New corpus'}
              </button>
              <button
                class="danger"
                type="button"
                on:click={onDeleteCorpus}
                disabled={deletingCorpus || creatingCorpus || !currentCorpusId || !canDeleteCurrentCorpus}
                title={canDeleteCurrentCorpus ? 'Delete selected corpus' : 'Only owners can delete corpora'}
              >
                {deletingCorpus ? 'Deleting...' : 'Delete corpus'}
              </button>
            </div>
          </div>

          <label class="upstream-management-form__field">
            <span class="muted small">Upstream target</span>
            <select
              bind:value={selectedKantroposTargetId}
              on:change={onKantroposTargetChange}
              disabled={kantroposAssignmentLoading || kantroposAssignmentSaving || !currentCorpusId || !canConfigureCurrentCorpusKantropos}
              title={canConfigureCurrentCorpusKantropos ? 'Assign this local corpus to a predefined Kantropos corpus' : 'Only owners or editors can change the Kantropos assignment'}
            >
              <option value="">No assignment</option>
              {#each targets as target}
                <option value={target.id}>{target.name}</option>
              {/each}
            </select>
          </label>

          {#if corpusActionStatus}
            <p class={`${corpusActionError ? 'error' : 'muted'} upstream-management-form__status`}>{corpusActionStatus}</p>
          {/if}

          {#if kantroposAssignmentStatus}
            <p class={`${kantroposAssignmentError ? 'error' : 'muted'} upstream-management-form__status`}>{kantroposAssignmentStatus}</p>
          {:else if currentCorpusId && kantroposAssignment?.target_name}
            <p class="muted upstream-management-form__status">Assigned to: {kantroposAssignment.target_name}</p>
          {/if}
        </div>
      </div>

      <div class="upstream-management-card upstream-management-card--sharing">
        <div class="workspace-panel-title upstream-management-card__title">
          <h3 class="workspace-section-title">Sharing</h3>
          <p class="muted">Share the currently selected corpus after selecting and assigning it.</p>
        </div>
        <div class="upstream-management-card__summary muted">Selected corpus: {currentCorpusLabel()}</div>
        <form class="corpus-share upstream-management-card__share" on:submit|preventDefault={handleShareCorpus}>
          <div>
            <label>
              Share with
              <input type="text" placeholder="username" bind:value={shareUsername} disabled={!currentCorpusId} />
            </label>
          </div>
          <div>
            <label>
              Role
              <select bind:value={shareRole} disabled={!currentCorpusId}>
                <option value="viewer">Viewer</option>
                <option value="editor">Editor</option>
                <option value="owner">Owner</option>
              </select>
            </label>
          </div>
          <button class="secondary" type="submit" disabled={!currentCorpusId}>Share</button>
          {#if shareStatus}
            <span class="muted">{shareStatus}</span>
          {/if}
        </form>
      </div>
    </div>

    {#if status}
      <p class="muted upstream-browser__status">{status}</p>
    {/if}

    {#if browse?.target}
      <div class="upstream-summary-grid">
        <div class="upstream-summary-card">
          <span class="muted small">Pending additions</span>
          <strong>{browse?.summary?.pending_items_count || 0}</strong>
        </div>
        <div class="upstream-summary-card">
          <span class="muted small">Current upstream items</span>
          <strong>{browse?.summary?.current_items_count || 0}</strong>
        </div>
        <div class="upstream-summary-card">
          <span class="muted small">Assigned local corpora</span>
          <strong>{browse?.summary?.assigned_corpora_count || 0}</strong>
        </div>
        <div class="upstream-summary-card">
          <span class="muted small">metadata.bib modified</span>
          <strong>{formatMetadataDate(browse?.summary?.metadata_bib_modified_at)}</strong>
        </div>
      </div>

    {/if}
  </div>

  {#if browse?.target}
    <UpstreamCorpusMap {browse} {loading} />
  {/if}

  <div class="corpus-workspace-shell upstream-workspace-shell">
    <div class="card corpus-panel upstream-panel upstream-panel--combined">
      <div class="workspace-panel-header upstream-browser__current-header">
        <div class="workspace-panel-title">
          <h3 class="workspace-section-title">Upstream Corpus</h3>
          <p class="muted">Pending additions and imported baseline for the selected upstream corpus.</p>
        </div>
        <div class="workspace-panel-actions upstream-browser__toolbar-controls">
          <label class="upstream-browser__target-select">
            <span class="muted small">Upstream corpus</span>
            <select bind:value={selectedTargetId} on:change={(event) => selectTarget(event?.target?.value)} disabled={loading || targets.length === 0}>
              {#if targets.length === 0}
                <option value="">No upstream corpora</option>
              {:else}
                {#each targets as target}
                  <option value={target.id}>{target.name}</option>
                {/each}
              {/if}
            </select>
          </label>
          <button class="secondary" type="button" on:click={refresh} disabled={loading || !selectedTargetId}>
            {loading ? 'Refreshing...' : 'Refresh'}
          </button>
        </div>
      </div>

      {#if browse?.target}
        <div class="upstream-assignment-toolbar">
          <button
            type="button"
            class:selected={selectedAssignedCorpusId === null || selectedAssignedCorpusId === ''}
            on:click={() => toggleAssignedCorpus(null)}
          >
            All assigned corpora ({allAssignedPendingCount})
          </button>
          {#each browse?.assigned_corpora || [] as corpus}
            <button
              type="button"
              class:selected={Number(selectedAssignedCorpusId) === Number(corpus.id)}
              on:click={() => toggleAssignedCorpus(corpus.id)}
              title={corpus.owner_username ? `Owner: ${corpus.owner_username}` : ''}
            >
              {assignedButtonLabel(corpus)}
            </button>
          {/each}
        </div>
      {/if}

      <div class="upstream-comparison-grid">
        <section class="upstream-comparison-section">
          <div class="workspace-panel-title upstream-comparison-section__header">
            <h3 class="workspace-section-title">Pending Additions</h3>
            <p class="muted">
              {#if browse?.summary?.pending_is_provisional}
                No imported DT baseline yet. Showing downloaded items from assigned local corpora.
              {:else}
                Downloaded items from assigned local corpora that are not yet in the imported upstream baseline.
              {/if}
            </p>
          </div>

          <div class="table-toolbar corpus-toolbar">
            <div class="table-toolbar-left corpus-toolbar__filters">
              <label class="corpus-filter corpus-filter--wide">
                <span class="muted small">Search</span>
                <input type="text" bind:value={pendingTextFilter} placeholder="Filter pending items..." />
              </label>
            </div>
          </div>

          <div class="corpus-table-unified">
            <div class="table table-scroll corpus-table">
              <div class="table-row header cols-corpus-workspace">
                <span>Title</span>
                <span>Year</span>
                <span>Author</span>
              </div>
              {#each filteredPendingItems as item (item.id)}
                {@const itemId = itemKey('pending', item)}
                {@const selected = itemId !== '' && itemId === activePendingKey}
                <div
                  class={`table-row cols-corpus-workspace clickable corpus-select-row ${selected ? 'selected active-row' : ''}`}
                  on:click={() => togglePendingItem(item)}
                  on:keydown={(e) => {
                    if (e.key === 'Enter' || e.key === ' ') {
                      if (e.target.tagName === 'BUTTON' || e.target.tagName === 'A') return
                      e.preventDefault()
                      togglePendingItem(item)
                    }
                  }}
                  role="button"
                  tabindex="0"
                  aria-pressed={selected}
                >
                  <span class="corpus-row-main">
                    <span class="corpus-row-title line-clamp-2" title={item.title}>{item.title || 'Untitled'}</span>
                  </span>
                  <span class="nowrap">{item.year || '-'}</span>
                  <span class="muted small line-clamp-2" title={itemAuthors(item) || '-'}>{itemAuthors(item) || '-'}</span>
                </div>
                {#if selected}
                  <div class="table-row corpus-inline-detail-row">
                    <div class="inline-detail-card">
                      <div class="corpus-row-main">
                        <div class="inline-detail-meta-row">
                          {#if itemSource(item)}
                            <span class="inline-detail-chip">Source: {itemSource(item)}</span>
                          {/if}
                          {#if item.doi}
                            <a class="inline-detail-link" href={doiHref(item.doi)} target="_blank" rel="noreferrer">DOI: {item.doi}</a>
                          {/if}
                          {#if item.openalex_id}
                            <a class="inline-detail-link" href={openAlexHref(item.openalex_id)} target="_blank" rel="noreferrer">OpenAlex: {itemOpenAlexLabel(item)}</a>
                          {/if}
                        </div>
                        <div class="upstream-provenance-row">
                          <span class="muted small">Source corpora</span>
                          <div class="upstream-provenance-list">
                            {#each item.source_corpora || [] as sourceCorpus}
                              <span class="pill">{sourceCorpus.name}{sourceCorpus.owner_username ? ` · ${sourceCorpus.owner_username}` : ''}</span>
                            {/each}
                          </div>
                        </div>
                      </div>
                    </div>
                  </div>
                {/if}
              {/each}
              {#if filteredPendingItems.length === 0}
                <div class="table-row muted corpus-empty-state">
                  No pending additions match the current filters.
                </div>
              {/if}
            </div>
          </div>

          <div class="corpus-lazy-status">
            <span class="muted">Loaded {browse?.summary?.pending_items_loaded || filteredPendingItems.length} of {browse?.summary?.pending_items_count || 0} entries.</span>
            {#if browse?.summary?.pending_items_has_more}
              <button class="secondary" type="button" on:click={() => loadMorePending()} disabled={loading}>
                {loading ? 'Loading…' : 'Load more'}
              </button>
            {/if}
          </div>
        </section>

        <section class="upstream-comparison-section">
          <div class="workspace-panel-title upstream-comparison-section__header">
            <h3 class="workspace-section-title">Current Upstream Corpus</h3>
            <p class="muted">Imported upstream baseline currently available in DT.</p>
          </div>

          {#if !browse?.summary?.has_imported_current}
            <div class="upstream-empty-state">
              <p class="muted">This upstream corpus has not been imported into DT yet.</p>
            </div>
          {:else}
            <div class="table-toolbar corpus-toolbar">
              <div class="table-toolbar-left corpus-toolbar__filters">
                <label class="corpus-filter corpus-filter--wide">
                  <span class="muted small">Search</span>
                  <input type="text" bind:value={currentTextFilter} placeholder="Filter upstream items..." />
                </label>
              </div>
            </div>

            <div class="corpus-table-unified">
              <div class="table table-scroll corpus-table">
                <div class="table-row header cols-corpus-workspace cols-upstream-current">
                  <span>Title</span>
                  <span>Year</span>
                  <span>Author</span>
                  <span>File</span>
                </div>
                {#each filteredCurrentItems as item (item.id)}
                  {@const itemId = itemKey('current', item)}
                  {@const selected = itemId !== '' && itemId === activeCurrentKey}
                  <div
                    class={`table-row cols-corpus-workspace cols-upstream-current clickable corpus-select-row ${selected ? 'selected active-row' : ''}`}
                    on:click={() => toggleCurrentItem(item)}
                    on:keydown={(e) => {
                      if (e.key === 'Enter' || e.key === ' ') {
                        if (e.target.tagName === 'BUTTON' || e.target.tagName === 'A') return
                        e.preventDefault()
                        toggleCurrentItem(item)
                      }
                    }}
                    role="button"
                    tabindex="0"
                    aria-pressed={selected}
                  >
                    <span class="corpus-row-main">
                      <span class="corpus-row-title line-clamp-2" title={item.title}>{item.title || 'Untitled'}</span>
                    </span>
                    <span class="nowrap">{item.year || '-'}</span>
                    <span class="muted small line-clamp-2" title={itemAuthors(item) || '-'}>{itemAuthors(item) || '-'}</span>
                    <span class="upstream-current-download-cell">
                      {#if isCurrentItemDownloadable(item)}
                        <button class="secondary upstream-current-download-button" type="button" on:click|stopPropagation={() => downloadCurrentItem(item)}>
                          Download
                        </button>
                      {:else}
                        <span class="muted small">-</span>
                      {/if}
                    </span>
                  </div>
                  {#if selected}
                    <div class="table-row corpus-inline-detail-row">
                      <div class="inline-detail-card">
                        <div class="corpus-row-main">
                          <div class="inline-detail-meta-row">
                            {#if itemSource(item)}
                              <span class="inline-detail-chip">Source: {itemSource(item)}</span>
                            {/if}
                            {#if item.doi}
                              <a class="inline-detail-link" href={doiHref(item.doi)} target="_blank" rel="noreferrer">DOI: {item.doi}</a>
                            {/if}
                            {#if item.openalex_id}
                              <a class="inline-detail-link" href={openAlexHref(item.openalex_id)} target="_blank" rel="noreferrer">OpenAlex: {itemOpenAlexLabel(item)}</a>
                            {/if}
                          </div>
                        </div>
                      </div>
                    </div>
                  {/if}
                {/each}
                {#if filteredCurrentItems.length === 0}
                  <div class="table-row muted corpus-empty-state">
                    No imported upstream items match the current filters.
                  </div>
                {/if}
              </div>
            </div>

            {#if currentDownloadStatus}
              <p class={`${currentDownloadError ? 'error' : 'muted'} upstream-download-status`}>{currentDownloadStatus}</p>
            {/if}

            <div class="corpus-lazy-status">
              <span class="muted">Loaded {browse?.summary?.current_items_loaded || filteredCurrentItems.length} of {browse?.summary?.current_items_count || 0} entries.</span>
              {#if browse?.summary?.current_items_has_more}
                <button class="secondary" type="button" on:click={() => loadMoreCurrent()} disabled={loading}>
                  {loading ? 'Loading…' : 'Load more'}
                </button>
              {/if}
            </div>
          {/if}
        </section>
      </div>
    </div>
  </div>
</div>
