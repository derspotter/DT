<script>
  export let browse = {
    graph: { nodes: [], edges: [], stats: {} },
    current_items: [],
    pending_items: [],
    summary: {},
  }
  export let loading = false

  const WIDTH = 760
  const HEIGHT = 430
  const NODE_LIMIT = 180
  const GOLDEN_ANGLE = Math.PI * (3 - Math.sqrt(5))

  let colorMode = 'state'
  let stateFilter = 'all'
  let activeNodeId = ''

  const statePalette = {
    current: '#0f8b8d',
    pending: '#f4a340',
    both: '#1f3b4d',
  }

  const statusPalette = {
    downloaded: '#0f8b8d',
    matched: '#3b82f6',
    queued_download: '#f4a340',
    failed_download: '#b91c1c',
    failed_enrichment: '#b91c1c',
    enriching: '#8b5cf6',
    raw: '#6f7b84',
  }

  const clusterPalette = ['#0f8b8d', '#f4a340', '#2f4858', '#e76f51', '#3b82f6', '#5f6f52', '#8a5a44', '#64748b']

  function cleanText(value, fallback = '') {
    const text = String(value || '').trim()
    return text || fallback
  }

  function truncate(value, max = 64) {
    const text = cleanText(value)
    return text.length > max ? `${text.slice(0, max - 3)}...` : text
  }

  function normalizeGraphId(item) {
    if (item?.graph_node_id) return String(item.graph_node_id)
    if (item?.openalex_id) {
      const match = String(item.openalex_id).match(/(W\d+)/i)
      if (match) return match[1].toUpperCase()
    }
    if (item?.doi) return String(item.doi).toUpperCase()
    if (item?.id !== undefined && item?.id !== null) return `work:${item.id}`
    return ''
  }

  function fallbackGraphFromBrowse(value) {
    const nodes = []
    for (const item of value?.current_items || []) {
      const id = normalizeGraphId(item)
      if (!id) continue
      nodes.push({
        id,
        work_id: item.work_id || item.id,
        title: item.title || 'Untitled',
        year: item.year ?? null,
        type: item.type || '',
        status: item.status || '',
        upstream_state: 'current',
        source: item.source || '',
        doi: item.doi || null,
        openalex_id: item.openalex_id || null,
        source_corpora: [],
        source_corpora_count: 0,
      })
    }
    for (const item of value?.pending_items || []) {
      const id = normalizeGraphId(item)
      if (!id) continue
      nodes.push({
        id,
        work_id: item.work_id || item.id,
        title: item.title || 'Untitled',
        year: item.year ?? null,
        type: item.type || '',
        status: item.status || '',
        upstream_state: 'pending',
        source: item.source || '',
        doi: item.doi || null,
        openalex_id: item.openalex_id || null,
        source_corpora: Array.isArray(item.source_corpora) ? item.source_corpora : [],
        source_corpora_count: Number(item.source_corpora_count || item.source_corpora?.length || 0),
      })
    }
    return { nodes, edges: [], stats: { node_count: nodes.length, edge_count: 0 } }
  }

  function graphFromBrowse(value) {
    const graph = value?.graph && Array.isArray(value.graph.nodes) ? value.graph : fallbackGraphFromBrowse(value)
    const nodes = Array.isArray(graph.nodes) ? graph.nodes : []
    const edges = Array.isArray(graph.edges) ? graph.edges : []
    return { nodes, edges, stats: graph.stats || {} }
  }

  function nodeState(node) {
    const state = cleanText(node?.upstream_state, 'current').toLowerCase()
    return state === 'pending' || state === 'both' ? state : 'current'
  }

  function sourceCluster(node) {
    const sourceCorpus = Array.isArray(node?.source_corpora) ? node.source_corpora[0] : null
    return cleanText(sourceCorpus?.name, cleanText(node?.source, nodeState(node) === 'current' ? 'metadata.bib' : 'local corpus'))
  }

  function yearCluster(node) {
    const year = Number(node?.year)
    if (!Number.isFinite(year) || year <= 0) return 'Unknown year'
    return `${Math.floor(year / 10) * 10}s`
  }

  function colorKey(node) {
    if (colorMode === 'status') return cleanText(node?.status, 'unknown')
    if (colorMode === 'source') return sourceCluster(node)
    if (colorMode === 'year') return yearCluster(node)
    return nodeState(node)
  }

  function colorForKey(key, index = 0) {
    if (colorMode === 'state') return statePalette[key] || '#64748b'
    if (colorMode === 'status') return statusPalette[key] || '#64748b'
    return clusterPalette[index % clusterPalette.length]
  }

  function buildDegreeMap(edges) {
    const degree = new Map()
    for (const edge of edges || []) {
      degree.set(edge.source, (degree.get(edge.source) || 0) + 1)
      degree.set(edge.target, (degree.get(edge.target) || 0) + 1)
    }
    return degree
  }

  function layoutGroup(nodes, centerX, centerY, maxRadius, degreeMap) {
    const count = nodes.length
    return nodes.map((node, index) => {
      const ringProgress = count <= 1 ? 0 : Math.sqrt((index + 0.35) / count)
      const radius = Math.min(maxRadius, 24 + ringProgress * maxRadius)
      const angle = index * GOLDEN_ANGLE
      const degree = degreeMap.get(node.id) || 0
      return {
        ...node,
        x: centerX + Math.cos(angle) * radius,
        y: centerY + Math.sin(angle) * radius * 0.78,
        size: Math.min(15, 6 + Math.sqrt(degree + Number(node.source_corpora_count || 0)) * 2.1),
        degree,
      }
    })
  }

  function buildVisualData(value, filter, mode) {
    const graph = graphFromBrowse(value)
    const baseEdges = graph.edges.filter((edge) => edge?.source && edge?.target)
    const degreeMap = buildDegreeMap(baseEdges)
    const seen = new Set()
    const nodes = graph.nodes
      .filter((node) => {
        if (!node?.id || seen.has(node.id)) return false
        seen.add(node.id)
        return filter === 'all' || nodeState(node) === filter
      })
      .sort((a, b) => (degreeMap.get(b.id) || 0) - (degreeMap.get(a.id) || 0) || cleanText(a.title).localeCompare(cleanText(b.title)))
      .slice(0, NODE_LIMIT)

    const visibleIds = new Set(nodes.map((node) => node.id))
    const edges = baseEdges.filter((edge) => visibleIds.has(edge.source) && visibleIds.has(edge.target))
    const visibleDegreeMap = buildDegreeMap(edges)
    const groups = {
      current: nodes.filter((node) => nodeState(node) === 'current'),
      pending: nodes.filter((node) => nodeState(node) === 'pending'),
      both: nodes.filter((node) => nodeState(node) === 'both'),
    }
    const activeGroups = Object.entries(groups).filter(([, groupNodes]) => groupNodes.length > 0)
    const positioned = []
    if (activeGroups.length <= 1) {
      positioned.push(...layoutGroup(nodes, WIDTH / 2, HEIGHT / 2, 160, visibleDegreeMap))
    } else {
      positioned.push(...layoutGroup(groups.current, WIDTH * 0.3, HEIGHT / 2, 126, visibleDegreeMap))
      positioned.push(...layoutGroup(groups.pending, WIDTH * 0.7, HEIGHT / 2, 126, visibleDegreeMap))
      positioned.push(...layoutGroup(groups.both, WIDTH * 0.5, HEIGHT * 0.5, 78, visibleDegreeMap))
    }

    const colorKeys = []
    const colorIndexByKey = new Map()
    for (const node of positioned) {
      const key = colorKey(node)
      if (!colorIndexByKey.has(key)) {
        colorIndexByKey.set(key, colorKeys.length)
        colorKeys.push(key)
      }
    }
    const colorByNode = new Map()
    for (const node of positioned) {
      const key = colorKey(node)
      colorByNode.set(node.id, colorForKey(key, colorIndexByKey.get(key) || 0))
    }
    const legend = colorKeys.slice(0, 8).map((key) => ({
      key,
      color: colorForKey(key, colorIndexByKey.get(key) || 0),
      count: positioned.filter((node) => colorKey(node) === key).length,
    }))

    return {
      nodes: positioned,
      edges,
      nodeById: new Map(positioned.map((node) => [node.id, node])),
      colorByNode,
      legend,
      totalNodes: graph.stats?.node_count ?? graph.nodes.length,
      totalEdges: graph.stats?.edge_count ?? graph.edges.length,
      currentCount: graph.nodes.filter((node) => nodeState(node) === 'current').length,
      pendingCount: graph.nodes.filter((node) => nodeState(node) === 'pending').length,
    }
  }

  $: visual = buildVisualData(browse, stateFilter, colorMode)
  $: activeNode = visual.nodes.find((node) => node.id === activeNodeId) || visual.nodes[0] || null
  $: activeNodeSourceCorpora = activeNode && Array.isArray(activeNode.source_corpora) ? activeNode.source_corpora : []
</script>

<section class="card upstream-map" data-testid="upstream-visualization">
  <div class="workspace-panel-header upstream-map__header">
    <div class="workspace-panel-title">
      <h3 class="workspace-section-title">Upstream Corpus Map</h3>
      <p class="muted">
        Visualizes the imported upstream baseline and downloaded local additions. Citation edges appear when OpenAlex/DOI relations are available.
      </p>
    </div>
    <div class="upstream-map__controls">
      <label>
        <span class="muted small">Show</span>
        <select bind:value={stateFilter}>
          <option value="all">Current + pending</option>
          <option value="current">Current upstream</option>
          <option value="pending">Pending additions</option>
        </select>
      </label>
      <label>
        <span class="muted small">Color by</span>
        <select bind:value={colorMode}>
          <option value="state">Corpus state</option>
          <option value="source">Source corpus/path</option>
          <option value="status">Pipeline status</option>
          <option value="year">Year decade</option>
        </select>
      </label>
    </div>
  </div>

  <div class="upstream-map__body">
    <div class="upstream-map__canvas">
      {#if visual.nodes.length === 0}
        <div class="upstream-map__empty">
          <strong>No upstream graph data loaded.</strong>
          <span class="muted small">Import metadata.bib or assign downloaded local corpora to populate the map.</span>
        </div>
      {:else}
        <svg viewBox={`0 0 ${WIDTH} ${HEIGHT}`} role="img" aria-label="Upstream corpus visualization">
          <defs>
            <radialGradient id="upstream-node-glow" cx="50%" cy="45%" r="62%">
              <stop offset="0%" stop-color="rgba(255,255,255,0.92)" />
              <stop offset="100%" stop-color="rgba(255,255,255,0)" />
            </radialGradient>
          </defs>
          <g class="upstream-map__halo">
            <circle cx={WIDTH * 0.3} cy={HEIGHT / 2} r="150" />
            <circle cx={WIDTH * 0.7} cy={HEIGHT / 2} r="150" />
          </g>
          <g class="upstream-map__edges">
            {#each visual.edges as edge}
              {@const source = visual.nodeById.get(edge.source)}
              {@const target = visual.nodeById.get(edge.target)}
              {#if source && target}
                <line
                  x1={source.x}
                  y1={source.y}
                  x2={target.x}
                  y2={target.y}
                  class:cited={edge.relationship_type === 'cited_by'}
                />
              {/if}
            {/each}
          </g>
          <g class="upstream-map__nodes">
            {#each visual.nodes as node}
              <g
                class:active={node.id === activeNodeId}
                on:mouseenter={() => (activeNodeId = node.id)}
                on:focus={() => (activeNodeId = node.id)}
                on:click={() => (activeNodeId = node.id)}
                on:keydown={(event) => {
                  if (event.key === 'Enter' || event.key === ' ') {
                    event.preventDefault()
                    activeNodeId = node.id
                  }
                }}
                role="button"
                tabindex="0"
                aria-label={node.title}
              >
                <circle class="node-glow" cx={node.x} cy={node.y} r={node.size + 7} />
                <circle
                  class={`upstream-map__node upstream-map__node--${nodeState(node)}`}
                  cx={node.x}
                  cy={node.y}
                  r={node.id === activeNodeId ? node.size + 3 : node.size}
                  style={`fill:${visual.colorByNode.get(node.id) || '#0f8b8d'};`}
                />
                <title>{node.title}</title>
              </g>
            {/each}
          </g>
        </svg>
      {/if}
    </div>

    <aside class="upstream-map__details">
      <div class="upstream-map__metrics">
        <div>
          <span class="eyebrow">Visible nodes</span>
          <strong>{visual.nodes.length}</strong>
        </div>
        <div>
          <span class="eyebrow">Citation edges</span>
          <strong>{visual.edges.length}</strong>
        </div>
        <div>
          <span class="eyebrow">Current</span>
          <strong>{visual.currentCount}</strong>
        </div>
        <div>
          <span class="eyebrow">Pending</span>
          <strong>{visual.pendingCount}</strong>
        </div>
      </div>

      <div class="upstream-map__selected">
        <span class="eyebrow">Selected reference</span>
        {#if activeNode}
          <strong>{truncate(activeNode.title, 92)}</strong>
          <span class="muted small">
            {activeNode.year || 'Year ?'} / {cleanText(activeNode.type, 'unknown type')} / {cleanText(activeNode.status, 'unknown status')}
          </span>
          <span class="muted small">State: {nodeState(activeNode) === 'pending' ? 'pending addition' : 'current upstream'}</span>
          {#if activeNodeSourceCorpora.length > 0}
            <span class="muted small">From: {activeNodeSourceCorpora.map((entry) => entry.name).filter(Boolean).join(', ')}</span>
          {:else}
            <span class="muted small">From: {cleanText(activeNode.source, 'metadata.bib')}</span>
          {/if}
        {:else}
          <strong>No reference selected</strong>
          <span class="muted small">Hover a node to inspect it.</span>
        {/if}
      </div>

      <div class="upstream-map__legend">
        <span class="eyebrow">Clusters</span>
        {#if visual.legend.length === 0}
          <p class="muted small">No cluster groups available.</p>
        {:else}
          {#each visual.legend as item}
            <div class="upstream-map__legend-row">
              <span class="upstream-map__swatch" style={`background:${item.color};`}></span>
              <span>{truncate(item.key, 28)}</span>
              <strong>{item.count}</strong>
            </div>
          {/each}
        {/if}
      </div>

      <p class="muted small upstream-map__footnote">
        Showing up to {NODE_LIMIT} loaded references{loading ? ' while refreshing.' : '.'}
        {#if visual.totalNodes > visual.nodes.length}
          Load more table rows to expand the map.
        {/if}
      </p>
    </aside>
  </div>
</section>
