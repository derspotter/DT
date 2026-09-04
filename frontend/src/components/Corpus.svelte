<script>
  export let corpusSource;
  export let corpusTotal;
  export let corpusItems;
  export let rawTotal;
  export let metadataTotal;
  export let downloadedTotal;
  export let failedEnrichmentTotal;
  export let failedDownloadTotal;
  
  export let bucketLabel;
  export let formatAuthors;
  export let formatAuthorsShort = (entry) => formatAuthors(entry);
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
  export let corpusSortIndicator = () => '';
  export let corpusSort = '';
  export let handleRemoveCorpusWork = () => {};

  import ColumnPicker from './ColumnPicker.svelte'
  import { gridTemplate, loadVisibility, saveVisibility, visibleColumns } from '../lib/tableColumns'

  let columnVisibility = loadVisibility('corpus')
  $: activeColumns = visibleColumns('corpus', columnVisibility)
  // A leading fixed track carries the selection checkbox, mirroring the seed table.
  $: gridStyle = `grid-template-columns: 44px ${gridTemplate(activeColumns)}`

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
  $: visibleSelection = selectedWorkIds.filter((id) => selectableWorkIds.includes(id))
  $: allSelected = selectableWorkIds.length > 0 && visibleSelection.length === selectableWorkIds.length

  function isRowSelected(item) {
    const id = workIdOf(item)
    return id !== null && selectedWorkIds.includes(id)
  }

  function toggleRowSelection(item) {
    const id = workIdOf(item)
    if (id === null) return
    selectedWorkIds = selectedWorkIds.includes(id)
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
  const RAW_STATUSES = new Set(['raw', 'extract_references_from_pdf', 'pending']);
  const METADATA_STATUSES = new Set(['enriching', 'matched', 'queued_download']);
  const FAILED_ENRICHMENT_STATUSES = new Set(['failed_enrichment']);
  const FAILED_DOWNLOAD_STATUSES = new Set(['failed_download']);
  const DOWNLOADED_STATUSES = new Set(['downloaded']);

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
    if (!item.status || RAW_STATUSES.has(item.status)) return 'raw';
    if (METADATA_STATUSES.has(item.status)) return 'metadata';
    if (FAILED_ENRICHMENT_STATUSES.has(item.status) || FAILED_DOWNLOAD_STATUSES.has(item.status)) return 'failed';
    if (DOWNLOADED_STATUSES.has(item.status)) return 'downloaded';
    return 'raw';
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

  // Spec line 8: cells show at most three authors; the full list stays on hover
  // and in the expanded detail card.
  function corpusItemAuthors(item) {
    if (!item) return ''
    if (typeof item.authors_display === 'string' && item.authors_display.trim()) {
      return formatAuthorsShort({ authors: item.authors_display.split(',').map((name) => name.trim()) })
    }
    return formatAuthorsShort(item)
  }

  function corpusItemAuthorsFull(item) {
    if (!item) return ''
    if (typeof item.authors_display === 'string' && item.authors_display.trim()) return item.authors_display
    return formatAuthors(item)
  }

  // Spec line 19: "Source" is the article's venue in both tables. Provenance
  // moved to its own "Seed" column below.
  function corpusItemSource(item) {
    return String(item?.source || '').trim()
  }

  // Spec C2 + line 19: where the item came from — the search query, or the
  // seed document's title and author.
  function corpusItemSeed(item) {
    const label = String(item?.source_label || '').trim()
    if (label) return label
    const raw = String(item?.origin || '').trim()
    if (!raw) return ''
    if (raw.endsWith('/metadata.bib')) return 'Upstream metadata.bib'
    if (raw.startsWith('search:')) return `Search #${raw.slice('search:'.length)}`
    return raw
  }

  const METADATA_LABELS = {
    pending: 'Pending',
    enriching: 'Enriching',
    matched: 'Confirmed',
    failed_enrichment: 'Metadata not confirmed',
  }
  const DOWNLOAD_LABELS = {
    not_requested: 'Not requested',
    queued: 'Queued',
    in_progress: 'Downloading',
    downloaded: 'Downloaded',
    failed: 'Not retrievable',
    failed_download: 'Not retrievable',
  }

  // Spec line 16: display the two axes separately. The collapsed `status` is
  // still what every behaviour (selection, stage filter, colours) keys off —
  // only the presentation splits.
  function metadataLabel(item) {
    if (FAILED_ENRICHMENT_STATUSES.has(item?.status)) return METADATA_LABELS.failed_enrichment
    const raw = String(item?.metadata_status || '').trim().toLowerCase()
    return METADATA_LABELS[raw] || (raw ? raw.replace(/_/g, ' ') : '-')
  }

  function downloadLabel(item) {
    if (FAILED_DOWNLOAD_STATUSES.has(item?.status)) return DOWNLOAD_LABELS.failed_download
    const raw = String(item?.download_status || '').trim().toLowerCase()
    return DOWNLOAD_LABELS[raw] || (raw ? raw.replace(/_/g, ' ') : '-')
  }

  function cellText(item, key, bucket) {
    switch (key) {
      case 'metadata': return metadataLabel(item)
      case 'download': return downloadLabel(item)
      case 'title': return item?.title || 'Untitled'
      case 'authors': return corpusItemAuthors(item)
      case 'year': return item?.year || '-'
      case 'source': return corpusItemSource(item) || '-'
      case 'seed': return corpusItemSeed(item) || '-'
      case 'doi': return item?.doi || '-'
      case 'publisher': return item?.publisher || '-'
      case 'type': return item?.type || '-'
      case 'openalex': return item?.openalex_id || '-'
      case 'pages': return item?.pages || '-'
      case 'open_access': return item?.open_access_url ? 'Yes' : 'No'
      case 'file': return item?.file_path ? 'Yes' : 'No'
      default: return itemStageLabel(item, bucket)
    }
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
                {column.label}{corpusSortIndicator(column.key, corpusSort)}
              </button>
            {:else}
              {column.label}
            {/if}
          </span>
        {/each}
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
          {#each activeColumns as column, columnIndex (column.key)}
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
              <span class="muted small line-clamp-2" title={corpusItemAuthorsFull(item)}>{cellText(item, column.key, bucket)}</span>
            {:else}
              <span class="muted small line-clamp-2 corpus-cell" title={cellText(item, column.key, bucket)}>
                {cellText(item, column.key, bucket)}
                {#if columnIndex === activeColumns.length - 1}
                  <button
                    class="corpus-remove"
                    type="button"
                    title="Remove from this corpus (keeps the work and its PDF)"
                    aria-label={`Remove ${item.title || 'item'} from this corpus`}
                    on:click|stopPropagation={() => handleRemoveCorpusWork(item)}
                  >✕</button>
                {/if}
              </span>
            {/if}
          {/each}
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
                  <span class="inline-detail-chip">Source: {corpusItemSource(item) || '-'}</span>
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
