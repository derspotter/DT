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
  export let doiHref;
  export let openAlexHref;
  export let handleDownloadedCorpusFile;

  export let corpusLoadStatus;
  export let corpusHasMore;
  export let corpusLoadingMore;
  export let corpusLoading;
  export let loadCorpus;
  export let handleCorpusColumnScroll;

  let textFilter = '';
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

    if (textFilter.trim() !== '') {
      const keywords = textFilter.toLowerCase().split(/\s+/).filter(Boolean);
      const titleStr = (item.title || '').toLowerCase();
      const yearStr = String(item.year || '');
      
      const allMatch = keywords.every(kw => titleStr.includes(kw) || yearStr.includes(kw));
      if (!allMatch) return false;
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
    if (FAILED_ENRICHMENT_STATUSES.has(item.status)) return 'Enrich failed';
    if (FAILED_DOWNLOAD_STATUSES.has(item.status)) return 'Download failed';
    if (item.status === 'enriching') return 'Enriching';
    if (item.status === 'queued_download') return 'Queued download';
    if (item.status === 'matched') return 'Matched';
    if (DOWNLOADED_STATUSES.has(item.status)) return 'Downloaded';
    if (!item.status || RAW_STATUSES.has(item.status)) {
      return 'Raw';
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

  function corpusItemAuthors(item) {
    if (!item) return ''
    if (typeof item.authors_display === 'string' && item.authors_display.trim()) return item.authors_display
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
        <span class="muted small">Search Title and/or Year</span>
        <input type="text" bind:value={textFilter} placeholder="Filter items..." />
      </label>
      <label class="corpus-filter corpus-filter--stage">
        <span class="muted small">Pipeline Stage</span>
        <select bind:value={stageFilter}>
          <option value="all">All stages ({corpusTotal || corpusItems.length})</option>
          <option value="raw">Raw / Seeds ({rawTotal || 0})</option>
          <option value="metadata">Metadata / Enriching ({metadataTotal || 0})</option>
          <option value="failed_enrichment">Failed enrichments ({failedEnrichmentTotal || 0})</option>
          <option value="failed_download">Failed downloads ({failedDownloadTotal || 0})</option>
          <option value="downloaded">Downloaded ({downloadedTotal || 0})</option>
        </select>
      </label>
    </div>
  </div>

  <div class="corpus-table-unified">
    <div
      class="table table-scroll corpus-table"
      on:scroll={handleCorpusColumnScroll}
    >
      <div class="table-row header cols-corpus-workspace">
        <span>Title</span>
        <span>Year</span>
        <span>Stage</span>
      </div>
      {#each filteredItems as item (item.id)}
        {@const bucket = getBucketForItem(item)}
        {@const itemKey = corpusItemKey(item, bucket)}
        {@const selected = itemKey !== '' && itemKey === activeCorpusKey}
        <div
          class={`table-row cols-corpus-workspace clickable corpus-select-row ${selected ? 'selected active-row' : ''}`}
          on:click={() => toggleCorpusItemSelection(item, bucket)}
          on:keydown={(e) => {
            if (e.key === 'Enter' || e.key === ' ') {
              if (e.target.tagName === 'BUTTON' || e.target.tagName === 'A') return
              e.preventDefault()
              toggleCorpusItemSelection(item, bucket)
            }
          }}
          role="button"
          aria-pressed={selected}
          tabindex="0"
        >
          <span class="corpus-row-main">
            <span class="corpus-row-title line-clamp-2" title={item.title}>
              {item.title || 'Untitled'}
            </span>
          </span>
          <span class="nowrap">{item.year || '-'}</span>
          <span class="nowrap muted small corpus-stage-pill">{itemStageLabel(item, bucket)}</span>
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
                      on:click={() => handleDownloadedCorpusFile(item)}
                      title="Download the stored file"
                    >
                      Stage: {itemStageLabel(item, bucket)}
                    </button>
                  {:else}
                    <span class="inline-detail-chip">Stage: {itemStageLabel(item, bucket)}</span>
                  {/if}
                  <span class="inline-detail-chip">Year: {item.year || '-'}</span>
                  <span class="inline-detail-chip">Author: {corpusItemAuthors(item) || '-'}</span>
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
        {corpusLoadingMore ? 'Loading…' : 'Load more'}
      </button>
    {/if}
  </div>
</div>
