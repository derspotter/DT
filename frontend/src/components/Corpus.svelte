<script>
  import {
    RAW_STATUSES,
    METADATA_STATUSES,
    FAILED_ENRICHMENT_STATUSES,
    FAILED_DOWNLOAD_STATUSES,
    DOWNLOADED_STATUSES,
    bucketForStatus,
    formatAuthors,
    workSource,
    cellText,
    sortIndicator,
  } from '../lib/works';
  export let corpusSource;
  export let corpusTotal;
  export let corpusItems;
  export let rawTotal;
  export let metadataTotal;
  export let downloadedTotal;
  export let failedEnrichmentTotal;
  export let failedDownloadTotal;
  
  export let bucketLabel;
  export let doiHref;
  export let openAlexHref;
  export let handleDownloadedCorpusFile;

  export let corpusLoadStatus;
  export let corpusHasMore;
  export let corpusLoadingMore;
  export let corpusLoading;
  export let loadCorpus;
  export let handleCorpusColumnScroll;

  // The text filter is server-side (spec line 6): the corpus is paged, so a
  // client-side filter would only ever search the page already loaded.
  export let corpusFilterQuery = '';
  export let handleCorpusFilterInput = () => {};
  export let toggleCorpusSort = () => {};
  export let corpusSort = '';
  export let handleRemoveCorpusWork = () => {};

  import ColumnPicker from './ColumnPicker.svelte'
  import { gridTemplate, loadVisibility, saveVisibility, visibleColumns } from '../lib/tableColumns'

  let columnVisibility = loadVisibility('corpus')
  $: activeColumns = visibleColumns('corpus', columnVisibility)
  // A leading fixed track carries the selection checkbox, mirroring the seed table.
  // Trailing 36px column holds the per-row ✕ so it stays put whatever columns
  // the user hides (it used to ride on whichever column happened to be last).
  $: gridStyle = `grid-template-columns: 44px ${gridTemplate(activeColumns)} 36px`

  function updateColumns(next) {
    columnVisibility = next
    saveVisibility('corpus', next)
  }

  // Multi-select for bulk actions (spec line 23). Deliberately separate from
  // activeCorpusKey, which is the single-row detail expansion — a row can be
  // expanded without being selected and vice versa.
  export let handleRemoveSelectedCorpusWorks = () => {}

  let selectedWorkIds = []

  function workIdOf(item) {
    const raw = Number(item?.work_id ?? item?.id)
    return Number.isFinite(raw) && raw > 0 ? raw : null
  }

  $: selectableWorkIds = filteredItems.map(workIdOf).filter((id) => id !== null)
  // Derived rather than written back into selectedWorkIds: a reactive statement
  // that assigns to its own dependency loops. Ids hidden by the stage filter are
  // ignored here so the count never claims more than the table can act on, but
  // they survive in selectedWorkIds if the filter is cleared again.
  $: selectableWorkIdSet = new Set(selectableWorkIds)
  $: visibleSelection = selectedWorkIds.filter((id) => selectableWorkIdSet.has(id))
  $: allSelected = selectableWorkIds.length > 0 && visibleSelection.length === selectableWorkIds.length
  $: selectedWorkIdSet = new Set(selectedWorkIds)

  function isRowSelected(item) {
    const id = workIdOf(item)
    return id !== null && selectedWorkIdSet.has(id)
  }

  function toggleRowSelection(item) {
    const id = workIdOf(item)
    if (id === null) return
    selectedWorkIds = selectedWorkIdSet.has(id)
      ? selectedWorkIds.filter((value) => value !== id)
      : [...selectedWorkIds, id]
  }

  function toggleSelectAll() {
    selectedWorkIds = allSelected ? [] : [...selectableWorkIds]
  }

  function clearSelection() {
    selectedWorkIds = []
  }

  async function removeSelected() {
    const targets = visibleSelection
    if (targets.length === 0) return
    // Light confirm on bulk (spec line 177); the per-row cross stays immediate.
    const ok = confirm(
      `Remove ${targets.length} item${targets.length === 1 ? '' : 's'} from this corpus?\n\n` +
      'The works and their PDFs are kept — they are only unlinked from this corpus.'
    )
    if (!ok) return
    await handleRemoveSelectedCorpusWorks([...targets])
    selectedWorkIds = []
  }
  let stageFilter = 'all';
  let activeCorpusKey = '';

  $: filteredItems = corpusItems.filter(item => {
    if (stageFilter === 'raw') {
      if (item.status && !RAW_STATUSES.has(item.status)) return false;
    } else if (stageFilter === 'metadata') {
      if (!METADATA_STATUSES.has(item.status)) return false;
    } else if (stageFilter === 'failed_enrichment') {
      if (!FAILED_ENRICHMENT_STATUSES.has(item.status)) return false;
    } else if (stageFilter === 'failed_download') {
      if (!FAILED_DOWNLOAD_STATUSES.has(item.status)) return false;
    } else if (stageFilter === 'downloaded') {
      if (!DOWNLOADED_STATUSES.has(item.status)) return false;
    }

    return true;
  });

  function getBucketForItem(item) {
    return bucketForStatus(item?.status);
  }

  function itemStageLabel(item, bucket = getBucketForItem(item)) {
    if (!item) return '-';
    if (FAILED_ENRICHMENT_STATUSES.has(item.status)) return 'Metadata not confirmed';
    if (FAILED_DOWNLOAD_STATUSES.has(item.status)) return 'Not retrievable';
    if (item.status === 'enriching') return 'Enriching';
    if (item.status === 'queued_download') return 'Queued download';
    if (item.status === 'matched') return 'Matched';
    if (DOWNLOADED_STATUSES.has(item.status)) return 'Downloaded';
    if (!item.status || RAW_STATUSES.has(item.status)) {
      return 'Seed';
    }
    return bucketLabel(bucket);
  }

  function canDownloadSelectedItem(row) {
    return Boolean(
      row &&
      row.bucket === 'downloaded' &&
      row.item &&
      DOWNLOADED_STATUSES.has(row.item.status) &&
      row.item.file_path
    );
  }

  function corpusItemKey(item, bucket) {
    if (!item || item.id === undefined || item.id === null) return ''
    return `${bucket}:${item.id}`
  }

  // Spec line 8: cells show at most three authors (authors_short, via
  // cellText); the full list stays on hover and in the expanded detail card.
  // Both come from normalizeWorkItem — never re-split authors_display.
  function corpusItemAuthorsFull(item) {
    return formatAuthors(item)
  }

  function toggleCorpusItemSelection(item, bucket) {
    const key = corpusItemKey(item, bucket)
    if (!key) return
    activeCorpusKey = activeCorpusKey === key ? '' : key
  }

  $: if (activeCorpusKey) {
    const activeKey = activeCorpusKey
    const stillExists = filteredItems.some((item) => corpusItemKey(item, getBucketForItem(item)) === activeKey)
    if (!stillExists) activeCorpusKey = ''
  }
</script>

<div class="card corpus-panel" data-testid="corpus-panel">
  <div class="workspace-panel-header">
    <div class="workspace-panel-title">
      <h3 class="workspace-section-title">3. Corpus</h3>
      <p class="muted">
        {corpusSource === 'api' ? 'Live corpus from API.' : 'Sample corpus data.'}
        {#if corpusTotal > 0} Loaded {corpusItems.length}/{corpusTotal}.{/if}
      </p>
    </div>
  </div>

  <div class="table-toolbar corpus-toolbar">
    <div class="table-toolbar-left corpus-toolbar__filters">
      <label class="corpus-filter corpus-filter--wide">
        <span class="muted small">Search title, author or publication</span>
        <input
          type="search"
          value={corpusFilterQuery}
          on:input={handleCorpusFilterInput}
          placeholder="Filter by title, author or publication"
          aria-label="Filter corpus items by title, author or publication"
        />
      </label>
      <label class="corpus-filter corpus-filter--stage">
        <span class="muted small">Pipeline Stage</span>
        <select bind:value={stageFilter}>
          <option value="all">All stages ({corpusTotal || corpusItems.length})</option>
          <optgroup label="Seed stage">
            <option value="raw">Seed / raw ({rawTotal || 0})</option>
          </optgroup>
          <optgroup label="Promoted">
            <option value="metadata">Fetching bibliographic metadata ({metadataTotal || 0})</option>
            <option value="failed_enrichment">Metadata not confirmed ({failedEnrichmentTotal || 0})</option>
          </optgroup>
          <optgroup label="Downloaded">
            <option value="downloaded">Downloaded ({downloadedTotal || 0})</option>
            <option value="failed_download">Not retrievable ({failedDownloadTotal || 0})</option>
          </optgroup>
        </select>
      </label>
      <ColumnPicker table="corpus" visibility={columnVisibility} onChange={updateColumns} />
    </div>
  </div>

  <div class="corpus-bulk-bar">
    <span class="muted small">Selected: {visibleSelection.length} / {selectableWorkIds.length}</span>
    <button class="secondary" type="button" on:click={toggleSelectAll} disabled={selectableWorkIds.length === 0}>
      {allSelected ? 'Clear' : 'Select all'}
    </button>
    <button class="secondary" type="button" on:click={clearSelection} disabled={visibleSelection.length === 0}>
      Clear selection
    </button>
    <button class="danger" type="button" on:click={removeSelected} disabled={visibleSelection.length === 0}>
      Remove selected
    </button>
  </div>

  <div class="corpus-table-unified">
    <div
      class="table table-scroll corpus-table"
      on:scroll={handleCorpusColumnScroll}
    >
      <div class="table-row header" style={gridStyle}>
        <span class="ingest-select-cell">
          <input
            type="checkbox"
            checked={allSelected}
            disabled={selectableWorkIds.length === 0}
            on:change={toggleSelectAll}
            aria-label={allSelected ? 'Clear selection' : 'Select all visible corpus items'}
            title={allSelected ? 'Clear selection' : 'Select all visible corpus items'}
          />
        </span>
        {#each activeColumns as column (column.key)}
          <span>
            {#if column.sortable}
              <button class="table-sort" type="button" on:click={() => toggleCorpusSort(column.key)}>
                {column.label}{sortIndicator(corpusSort, column.key)}
              </button>
            {:else}
              {column.label}
            {/if}
          </span>
        {/each}
        <span class="corpus-actions-cell" aria-hidden="true"></span>
      </div>
      {#each filteredItems as item (item.id)}
        {@const bucket = getBucketForItem(item)}
        {@const itemKey = corpusItemKey(item, bucket)}
        {@const selected = itemKey !== '' && itemKey === activeCorpusKey}
        <div
          class={`table-row clickable corpus-select-row ${selected ? 'selected active-row' : ''} ${isRowSelected(item) ? 'row-checked' : ''}`}
          style={gridStyle}
          on:click={() => toggleCorpusItemSelection(item, bucket)}
          on:keydown={(e) => {
            if (e.key === 'Enter' || e.key === ' ') {
              // INPUT included so Space/Enter on the row checkbox toggles the
              // checkbox instead of being cancelled and expanding the row.
              if (e.target.tagName === 'INPUT' || e.target.tagName === 'BUTTON' || e.target.tagName === 'A') return
              e.preventDefault()
              toggleCorpusItemSelection(item, bucket)
            }
          }}
          role="button"
          aria-pressed={selected}
          tabindex="0"
        >
          <span class="ingest-select-cell">
            <input
              type="checkbox"
              checked={isRowSelected(item)}
              on:click|stopPropagation
              on:change={() => toggleRowSelection(item)}
              aria-label={`Select ${item.title || 'item'}`}
            />
          </span>
          {#each activeColumns as column (column.key)}
            {#if column.key === 'title'}
              <span class="corpus-row-main">
                <span class="corpus-title-line">
                  <span class={`disclosure-chevron ${selected ? 'open' : ''}`} aria-hidden="true">▸</span>
                  <span class="corpus-row-title line-clamp-2" title={item.title}>
                    {item.title || 'Untitled'}
                  </span>
                </span>
              </span>
            {:else if column.key === 'authors'}
              <span class="muted small line-clamp-2" title={corpusItemAuthorsFull(item)}>{cellText(item, column.key)}</span>
            {:else}
              <span class="muted small line-clamp-2 corpus-cell" title={cellText(item, column.key)}>
                {cellText(item, column.key)}
              </span>
            {/if}
          {/each}
          <span class="corpus-actions-cell">
            <button
              class="corpus-remove"
              type="button"
              title="Remove from this corpus (keeps the work and its PDF)"
              aria-label={`Remove ${item.title || 'item'} from this corpus`}
              on:click|stopPropagation={() => handleRemoveCorpusWork(item)}
            >✕</button>
          </span>
        </div>
        {#if selected}
          <div class="table-row corpus-inline-detail-row">
            <div class="inline-detail-card">
              <div class="corpus-row-main">
                <div class="inline-detail-meta-row">
                  {#if canDownloadSelectedItem({ item, bucket })}
                    <button
                      class="inline-detail-chip inline-detail-chip--download"
                      type="button"
                      on:click|stopPropagation={() => handleDownloadedCorpusFile(item)}
                      title="Download the stored file"
                    >
                      Stage: {itemStageLabel(item, bucket)}
                    </button>
                  {:else}
                    <span class="inline-detail-chip">Stage: {itemStageLabel(item, bucket)}</span>
                  {/if}
                  <span class="inline-detail-chip">Year: {item.year || '-'}</span>
                  <span class="inline-detail-chip">Source: {workSource(item) || '-'}</span>
                  <span class="inline-detail-chip">Author: {corpusItemAuthorsFull(item) || '-'}</span>
                  {#if item.doi}
                    <a class="inline-detail-link" href={doiHref(item.doi)} target="_blank" rel="noreferrer">DOI</a>
                  {/if}
                  {#if item.openalex_id}
                    <a class="inline-detail-link" href={openAlexHref(item.openalex_id)} target="_blank" rel="noreferrer">OpenAlex</a>
                  {/if}
                </div>
              </div>
            </div>
          </div>
        {/if}
      {/each}
      {#if filteredItems.length === 0}
        <div class="table-row muted corpus-empty-state">
          No items match your filters.
        </div>
      {/if}
    </div>
  </div>

  <div class="corpus-lazy-status">
    <span class="muted">{corpusLoadStatus}</span>
    {#if corpusHasMore}
      <button class="secondary" type="button" on:click={() => loadCorpus({ append: true })} disabled={corpusLoadingMore || corpusLoading}>
        {corpusLoadingMore ? 'Loading…' : 'Show more'}
      </button>
    {/if}
  </div>
</div>
