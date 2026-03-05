<script>
  export let corpusSource;
  export let corpusTotal;
  export let corpusItems;
  export let rawTotal;
  export let metadataTotal;
  export let downloadedTotal;
  export let failedEnrichmentTotal;
  export let shareUsername;
  export let shareRole;
  export let shareStatus;
  export let handleShareCorpus;
  
  export let selectedCorpusRow;
  export let bucketLabel;
  export let pipelineStatusLabel;
  export let doiHref;
  export let openAlexHref;
  
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

  let textFilter = '';
  let stageFilter = 'all';

  $: filteredItems = corpusItems.filter(item => {
    if (stageFilter === 'raw') {
      if (item.status && item.status !== 'no_metadata' && item.status !== 'extract_references_from_pdf' && item.status !== 'pending') return false;
    } else if (stageFilter === 'metadata') {
      if (item.status !== 'with_metadata' && item.status !== 'to_download_references') return false;
    } else if (stageFilter === 'failed_enrichment') {
      if (item.status !== 'failed_enrichments') return false;
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
</script>

<div class="card" data-testid="corpus-panel">
  <div class="corpus-header-row">
    <div>
      <h2>Corpus</h2>
      <p class="muted">
        {corpusSource === 'api' ? 'Live corpus from API.' : 'Sample corpus data.'}
        {#if corpusTotal > 0} Loaded {corpusItems.length}/{corpusTotal}.{/if}
      </p>
    </div>
    <form class="corpus-share" on:submit|preventDefault={handleShareCorpus}>
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

  <div class="corpus-details">
    <h3>Selected entry details</h3>
    <div class="corpus-details-grid" class:muted={!selectedCorpusRow}>
      <div>
        <span>Stage</span>
        <strong>{selectedCorpusRow ? bucketLabel(selectedCorpusRow.bucket) : '-'}</strong>
      </div>
      <div>
        <span>Status</span>
        <strong>{selectedCorpusRow ? pipelineStatusLabel(selectedCorpusRow.item.status) : '-'}</strong>
      </div>
      <div>
        <span>ID</span>
        <strong>{selectedCorpusRow?.item.id ?? '-'}</strong>
      </div>
      <div>
        <span>Year</span>
        <strong>{selectedCorpusRow?.item.year || '-'}</strong>
      </div>
      <div class="span-2">
        <span>Title</span>
        <strong>{selectedCorpusRow?.item.title || (selectedCorpusRow ? 'Untitled' : '-')}</strong>
      </div>
      <div class="span-2">
        <span>Source</span>
        <strong>{selectedCorpusRow?.item.source || '-'}</strong>
      </div>
      <div class="span-2">
        <span>DOI</span>
        {#if selectedCorpusRow?.item.doi}
          <a href={doiHref(selectedCorpusRow.item.doi)} target="_blank" rel="noreferrer">{selectedCorpusRow.item.doi}</a>
        {:else}
          <strong>-</strong>
        {/if}
      </div>
      <div class="span-2">
        <span>OpenAlex</span>
        {#if selectedCorpusRow?.item.openalex_id}
          <a href={openAlexHref(selectedCorpusRow.item.openalex_id)} target="_blank" rel="noreferrer">{selectedCorpusRow.item.openalex_id}</a>
        {:else}
          <strong>-</strong>
        {/if}
      </div>
    </div>
  </div>

  <div class="corpus-filters" style="display: flex; gap: 16px; margin: 20px 0;">
    <label style="flex: 1; display: flex; flex-direction: column; gap: 6px;">
      <span class="muted small">Search Title and/or Year</span>
      <input type="text" bind:value={textFilter} placeholder="Filter items..." style="padding: 10px 12px; border-radius: 10px; border: 1px solid var(--stroke); background: white;" />
    </label>
    <label style="width: 220px; display: flex; flex-direction: column; gap: 6px;">
      <span class="muted small">Pipeline Stage</span>
      <select bind:value={stageFilter} style="padding: 10px 12px; border-radius: 10px; border: 1px solid var(--stroke); background: white;">
        <option value="all">All stages ({corpusTotal || corpusItems.length})</option>
        <option value="raw">Raw / Seeds ({rawTotal || 0})</option>
        <option value="metadata">With Metadata ({metadataTotal || 0})</option>
        <option value="failed_enrichment">Failed enrichments ({failedEnrichmentTotal || 0})</option>
        <option value="downloaded">Downloaded ({downloadedTotal || 0})</option>
      </select>
    </label>
  </div>

  <div class="corpus-table-unified">
    <div class="table table-scroll corpus-table" style="max-height: 500px; overflow-y: auto;">
      <div class="table-row header" style="grid-template-columns: 2fr 80px 100px;">
        <span>Title</span>
        <span>Year</span>
        <span>Stage</span>
      </div>
      {#each filteredItems as item}
        <button
          class={`table-row corpus-select-row ${isCorpusItemSelected(item, getBucketForItem(item)) ? 'selected' : ''}`}
          class:promotion-target-flash={isPromotionHighlighted(getBucketForItem(item), item.id)}
          type="button"
          style="grid-template-columns: 2fr 80px 100px; text-align: left;"
          use:bindCorpusRow={{ stage: getBucketForItem(item), key: corpusMotionKey(getBucketForItem(item), item.id) }}
          on:click={() => toggleCorpusItemSelection(item, getBucketForItem(item))}
        >
          <span class="line-clamp-2" title={item.title}>{item.title || 'Untitled'}</span>
          <span class="nowrap">{item.year || '-'}</span>
          <span class="nowrap muted small" style="text-transform: capitalize;">{getBucketForItem(item)}</span>
        </button>
      {/each}
      {#if filteredItems.length === 0}
        <div class="table-row muted" style="justify-content: center; padding: 20px;">
          No items match your filters.
        </div>
      {/if}
    </div>
  </div>

  <div class="corpus-lazy-status" style="margin-top: 16px;">
    <span class="muted">{corpusLoadStatus}</span>
    {#if corpusHasMore}
      <button class="secondary" type="button" on:click={() => loadCorpus({ append: true })} disabled={corpusLoadingMore || corpusLoading}>
        {corpusLoadingMore ? 'Loading…' : 'Load more'}
      </button>
    {/if}
  </div>
</div>
