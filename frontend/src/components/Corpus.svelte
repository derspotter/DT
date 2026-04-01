<script>
  export let corpusSource;
  export let corpusTotal;
  export let corpusItems;
  export let rawTotal;
  export let metadataTotal;
  export let downloadedTotal;
  export let failedEnrichmentTotal;
  export let failedDownloadTotal;
  export let shareUsername;
  export let shareRole;
  export let shareStatus;
  export let handleShareCorpus;
  
  export let bucketLabel;
  export let formatAuthors;
  export let doiHref;
  export let openAlexHref;
  export let handleDownloadedCorpusFile;
  
  export let isCorpusItemSelected;
  export let toggleCorpusItemSelection;
  export let isPromotionHighlighted;
  export let bindCorpusRow;
  export let corpusMotionKey;
  
  export let corpusLoadStatus;
  export let corpusHasMore;
  export let corpusLoadingMore;
  export let corpusLoading;
  export let loadCorpus;
  export let handleCorpusColumnScroll;

  let textFilter = '';
  let stageFilter = 'all';

  $: filteredItems = corpusItems.filter(item => {
    if (stageFilter === 'raw') {
      if (item.status && item.status !== 'no_metadata' && item.status !== 'extract_references_from_pdf' && item.status !== 'pending') return false;
    } else if (stageFilter === 'metadata') {
      if (item.status !== 'with_metadata' && item.status !== 'to_download_references') return false;
    } else if (stageFilter === 'failed_enrichment') {
      if (item.status !== 'failed_enrichments') return false;
    } else if (stageFilter === 'failed_download') {
      if (item.status !== 'failed_downloads') return false;
    } else if (stageFilter === 'downloaded') {
      if (item.status !== 'downloaded_references') return false;
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
    if (!item.status || item.status === 'no_metadata' || item.status === 'extract_references_from_pdf' || item.status === 'pending') return 'raw';
    if (item.status === 'with_metadata' || item.status === 'to_download_references') return 'metadata';
    if (item.status === 'failed_enrichments' || item.status === 'failed_downloads') return 'failed';
    if (item.status === 'downloaded_references') return 'downloaded';
    return 'raw';
  }

  function itemStageLabel(item, bucket = getBucketForItem(item)) {
    if (!item) return '-';
    if (item.status === 'failed_enrichments') return 'Enrich failed';
    if (item.status === 'failed_downloads') return 'Download failed';
    if (item.status === 'to_download_references') return 'Queued download';
    if (item.status === 'with_metadata') return 'With metadata';
    if (item.status === 'downloaded_references') return 'Downloaded';
    if (!item.status || item.status === 'no_metadata' || item.status === 'extract_references_from_pdf' || item.status === 'pending') {
      return 'Raw';
    }
    return bucketLabel(bucket);
  }

  function canDownloadSelectedItem(row) {
    return Boolean(
      row &&
      row.bucket === 'downloaded' &&
      row.item &&
      row.item.status === 'downloaded_references' &&
      row.item.file_path
    );
  }
</script>

<div class="card corpus-panel" data-testid="corpus-panel">
  <div class="workspace-panel-header">
    <div class="workspace-panel-title">
      <h3>Corpus</h3>
      <p class="muted">
        {corpusSource === 'api' ? 'Live corpus from API.' : 'Sample corpus data.'}
        {#if corpusTotal > 0} Loaded {corpusItems.length}/{corpusTotal}.{/if}
      </p>
    </div>
    <form class="workspace-panel-actions corpus-share" on:submit|preventDefault={handleShareCorpus}>
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
      <button class="secondary" type="submit">Share</button>
      {#if shareStatus}
        <span class="muted">{shareStatus}</span>
      {/if}
    </form>
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
          <option value="metadata">With Metadata ({metadataTotal || 0})</option>
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
      {#each filteredItems as item}
        {@const bucket = getBucketForItem(item)}
        {@const selected = isCorpusItemSelected(item, bucket)}
        <button
          class={`table-row cols-corpus-workspace clickable corpus-select-row ${selected ? 'selected' : ''}`}
          class:promotion-target-flash={isPromotionHighlighted(bucket, item.id)}
          type="button"
          use:bindCorpusRow={{ stage: bucket, key: corpusMotionKey(bucket, item.id) }}
          on:click={() => toggleCorpusItemSelection(item, bucket)}
        >
          <span class="line-clamp-2" title={item.title}>{item.title || 'Untitled'}</span>
          <span class="nowrap">{item.year || '-'}</span>
          <span class="nowrap muted small corpus-stage-pill">{itemStageLabel(item, bucket)}</span>
        </button>
        {#if selected}
          <div class="table-row corpus-inline-detail-row">
            <div class="inline-detail-card">
              <div class="inline-detail-main">
                <strong class="inline-detail-title">{item.title || 'Untitled'}</strong>
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
                  <span class="inline-detail-chip">Author: {formatAuthors(item) || '-'}</span>
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
