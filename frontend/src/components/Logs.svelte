<script>
  export let logsStatus;
  export let logsType;
  export let logsFilterCurrentCorpus;
  export let logsLoadingOlder;
  export let logsHasMore;
  export let logsTailStatus;
  export let logs;
  
  export let handleLogTypeChange;
  export let handleLogsCorpusFilterChange;
  export let loadOlderLogs;
  export let hydrateLogsFromTail;
</script>

<div class="card" data-testid="logs-panel">
  <h2>Logs</h2>
  <p class="muted">WebSocket status: {logsStatus}</p>
  <div class="log-toolbar">
    <div class="log-toolbar__actions">
      <label class="log-type-select">
        <span class="muted small">Type</span>
        <select bind:value={logsType} on:change={handleLogTypeChange} data-testid="logs-type-select">
          <option value="pipeline">Pipeline</option>
          <option value="app">App</option>
        </select>
      </label>
      <label class="log-filter-current">
        <input type="checkbox" bind:checked={logsFilterCurrentCorpus} on:change={handleLogsCorpusFilterChange} />
        <span class="muted small">Current corpus only</span>
      </label>
      <button
        class="secondary"
        type="button"
        on:click={loadOlderLogs}
        disabled={logsLoadingOlder || !logsHasMore}
      >
        {logsLoadingOlder ? 'Loading…' : 'Load older'}
      </button>
      <button
        class="secondary"
        type="button"
        on:click={() => hydrateLogsFromTail({ force: true })}
        disabled={logsLoadingOlder}
      >
        Refresh from file
      </button>
    </div>
    <span class="muted small">
      {logsTailStatus}
      {#if logs.length > 0}
        {' '}Showing {logs.length} lines.
      {/if}
    </span>
  </div>
  <div class="log-box">
    {#if logs.length === 0}
      <p class="muted">Waiting for pipeline output...</p>
    {:else}
      {#each logs as entry}
        <div class="log-line">{entry}</div>
      {/each}
    {/if}
  </div>
</div>
