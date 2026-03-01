<script>
  export let graphStatus;
  export let graphSource;
  export let graphRelationship;
  export let graphStatusFilter;
  export let graphYearFrom;
  export let graphYearTo;
  export let graphMaxNodes;
  export let graphColorMode;
  export let graphHideIsolates;
  export let graphFocusConnected;
  export let loadGraph;
  export let graphViewBox;
  export let handleGraphWheel;
  export let handleGraphPointerDown;
  export let handleGraphPointerMove;
  export let handleGraphPointerUp;
  export let graphVisibleEdges;
  export let graphNodeMap;
  export let graphLayout;
  export let hoveredNodeId;
  export let graphNodeColorMap;
  export let graphNodeCount;
  export let graphEdgeCount;
  export let graphRelationshipCounts;
  export let graphTotalNodeCount;
  export let graphTotalEdgeCount;
  export let hoveredNode;
  export let labelFromStatus;
  export let graphColorLegend;
  export let resetGraphView;
  export let graphList;
  export let truncateLabel;
  export let graphDegreeMap;
</script>

<div class="card" data-testid="graph-panel">
  <h2>Graph overview</h2>
  <p class="muted">{graphStatus} ({graphSource})</p>
  <div class="graph-controls">
    <label>
      <span>Relationship</span>
      <select bind:value={graphRelationship}>
        <option value="both">References + Cited by</option>
        <option value="references">References only</option>
        <option value="cited_by">Cited by only</option>
      </select>
    </label>
    <label>
      <span>Status</span>
      <select bind:value={graphStatusFilter}>
        <option value="all">All</option>
        <option value="with_metadata">With metadata</option>
        <option value="downloaded">Downloaded</option>
        <option value="queued">Queued</option>
      </select>
    </label>
    <label>
      <span>Year from</span>
      <input type="number" min="1600" max="2100" bind:value={graphYearFrom} placeholder="1900" />
    </label>
    <label>
      <span>Year to</span>
      <input type="number" min="1600" max="2100" bind:value={graphYearTo} placeholder="2026" />
    </label>
    <label>
      <span>Max nodes</span>
      <input type="number" min="20" max="2000" step="20" bind:value={graphMaxNodes} />
    </label>
    <label>
      <span>Color by</span>
      <select bind:value={graphColorMode}>
        <option value="source_path">Search path</option>
        <option value="component">Cluster</option>
        <option value="status">Pipeline status</option>
        <option value="year">Year decade</option>
        <option value="type">Work type</option>
      </select>
    </label>
    <label class="graph-toggle">
      <span>Hide isolates</span>
      <input type="checkbox" bind:checked={graphHideIsolates} />
    </label>
    <label class="graph-toggle">
      <span>Largest cluster only</span>
      <input type="checkbox" bind:checked={graphFocusConnected} />
    </label>
    <div class="graph-controls-actions">
      <button class="secondary" type="button" on:click={loadGraph}>Apply filters</button>
    </div>
  </div>
  <div class="graph-layout">
    <div class="graph-wrap graph-canvas">
      <svg
        viewBox={`${graphViewBox.x} ${graphViewBox.y} ${graphViewBox.size} ${graphViewBox.size}`}
        aria-label="Graph visualization"
        role="application"
        on:wheel={handleGraphWheel}
        on:pointerdown={handleGraphPointerDown}
        on:pointermove={handleGraphPointerMove}
        on:pointerup={handleGraphPointerUp}
        on:pointerleave={handleGraphPointerUp}
        style="touch-action: none;"
      >
        {#each graphVisibleEdges as edge}
          <line
            x1={graphNodeMap.get(edge.source).x}
            y1={graphNodeMap.get(edge.source).y}
            x2={graphNodeMap.get(edge.target).x}
            y2={graphNodeMap.get(edge.target).y}
            class={`edge ${edge.relationship_type === 'cited_by' ? 'edge--cited' : 'edge--ref'}`}
          />
        {/each}
        {#each graphLayout as node}
          <g
            class:active={hoveredNodeId === node.id}
            on:mouseenter={() => (hoveredNodeId = node.id)}
            on:mouseleave={() => (hoveredNodeId = null)}
            on:focus={() => (hoveredNodeId = node.id)}
            on:blur={() => (hoveredNodeId = null)}
            role="button"
            tabindex="0"
            aria-label={node.title}
          >
            <circle
              cx={node.x}
              cy={node.y}
              r={hoveredNodeId === node.id ? node.size + 4 : node.size}
              class="node"
              style={`fill: ${graphNodeColorMap.get(node.id) || '#0f8b8d'};`}
            />
            <title>{node.title}</title>
          </g>
        {/each}
      </svg>
    </div>
    <div class="graph-details">
      <div class="graph-summary">
        <div>
          <span class="eyebrow">Nodes</span>
          <strong>{graphNodeCount}</strong>
        </div>
        <div>
          <span class="eyebrow">Edges</span>
          <strong>{graphEdgeCount}</strong>
        </div>
        <div>
          <span class="eyebrow">References</span>
          <strong>{graphRelationshipCounts.references}</strong>
        </div>
        <div>
          <span class="eyebrow">Cited by</span>
          <strong>{graphRelationshipCounts.cited_by}</strong>
        </div>
      </div>
      <p class="muted small">
        Showing {graphNodeCount}/{graphTotalNodeCount} nodes and {graphEdgeCount}/{graphTotalEdgeCount} edges.
      </p>
      <div class="graph-hover">
        {#if hoveredNode}
          <span class="eyebrow">Selected node</span>
          <strong>{hoveredNode.title}</strong>
          <span class="muted small">
            {hoveredNode.year || 'Year ?'} · {hoveredNode.type || 'Type ?'} · {labelFromStatus(hoveredNode.status)}
          </span>
          <span class="muted small">Path: {hoveredNode.source_path || hoveredNode.ingest_source || 'unknown'}</span>
        {:else}
          <span class="eyebrow">Selected node</span>
          <strong>Hover a node to preview</strong>
          <span class="muted small">Connected node details will appear here.</span>
          <span class="muted small">Path: -</span>
        {/if}
      </div>
      <div class="graph-legend">
        <span class="eyebrow">Color legend</span>
        {#if graphColorLegend.length === 0}
          <p class="muted">No cluster groups available.</p>
        {:else}
          <div class="graph-legend-list">
            {#each graphColorLegend as item}
              <div class="graph-legend-item">
                <span class="graph-legend-swatch" style={`background:${item.color};`}></span>
                <span>{item.key}</span>
                <strong>{item.count}</strong>
              </div>
            {/each}
          </div>
        {/if}
      </div>
      <div class="graph-actions">
        <button class="secondary" type="button" on:click={resetGraphView}>Reset view</button>
      </div>
      <div class="graph-list">
        {#each graphList as node}
          <button
            class:active={hoveredNodeId === node.id}
            on:mouseenter={() => (hoveredNodeId = node.id)}
            on:mouseleave={() => (hoveredNodeId = null)}
            type="button"
          >
            <span class="graph-list-label">
              <span
                class="graph-legend-swatch"
                style={`background:${graphNodeColorMap.get(node.id) || '#0f8b8d'};`}
              ></span>
              <span>{truncateLabel(node.title)}</span>
            </span>
            <strong>{graphDegreeMap.get(node.id) ?? 0}</strong>
          </button>
        {/each}
      </div>
    </div>
  </div>
</div>
