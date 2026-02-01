<script>
  import { onMount } from 'svelte'
  import {
    fetchBibliographyList,
    uploadPdf,
    extractBibliography,
    consolidateBibliographies,
    runKeywordSearch,
    fetchCorpus,
    fetchDownloadQueue,
    fetchGraph,
  } from './lib/api'
  import { sampleActivity } from './lib/sample-data'

  const tabs = [
    { id: 'dashboard', label: 'Dashboard' },
    { id: 'ingest', label: 'Ingest' },
    { id: 'search', label: 'Keyword Search' },
    { id: 'corpus', label: 'Corpus' },
    { id: 'downloads', label: 'Downloads' },
    { id: 'logs', label: 'Logs' },
    { id: 'graph', label: 'Graph' },
  ]

  let activeTab = 'dashboard'
  let apiStatus = 'unknown'

  let uploads = []
  let bibliographyFiles = []
  let bibliographySource = 'sample'
  let bibliographyStatus = ''
  let consolidateStatus = ''

  let searchQuery = ''
  let searchField = 'default'
  let yearFrom = ''
  let yearTo = ''
  let searchResults = []
  let searchStatus = ''
  let searchSource = ''

  let corpusItems = []
  let corpusSource = ''
  let downloads = []
  let downloadsSource = ''

  let logs = []
  let logsStatus = 'connecting'
  let graphStatus = ''
  let graphSource = ''
  let graphNodes = []
  let graphEdges = []
  let graphStats = null
  let graphRelationship = 'both'
  let graphStatusFilter = 'with_metadata'
  let graphYearFrom = ''
  let graphYearTo = ''
  let graphMaxNodes = 200
  let graphHideIsolates = true
  let hoveredNodeId = null
  let graphLayout = []
  let graphNodeMap = new Map()
  let graphDegreeMap = new Map()
  let graphCanvasSize = 440
  let graphViewBox = { x: 0, y: 0, size: 440 }
  let isGraphPanning = false
  let graphPanStart = { x: 0, y: 0, viewX: 0, viewY: 0 }

  const maxLogs = 200

  function formatBytes(bytes) {
    if (bytes === 0) return '0 B'
    const k = 1024
    const sizes = ['B', 'KB', 'MB', 'GB']
    const i = Math.floor(Math.log(bytes) / Math.log(k))
    return `${(bytes / Math.pow(k, i)).toFixed(1)} ${sizes[i]}`
  }

  function updateUpload(id, patch) {
    uploads = uploads.map((item) => (item.id === id ? { ...item, ...patch } : item))
  }

  function handleFiles(event) {
    const files = Array.from(event.target.files || [])
    const newItems = files.map((file) => ({
      id: `${file.name}-${file.size}-${Date.now()}`,
      file,
      name: file.name,
      size: file.size,
      status: 'ready',
      backendFilename: null,
      message: '',
    }))
    uploads = [...uploads, ...newItems]
    event.target.value = ''
  }

  async function uploadItem(item) {
    updateUpload(item.id, { status: 'uploading', message: '' })
    try {
      const payload = await uploadPdf(item.file)
      updateUpload(item.id, {
        status: 'uploaded',
        backendFilename: payload.filename,
        message: 'Uploaded',
      })
      apiStatus = 'online'
    } catch (error) {
      updateUpload(item.id, { status: 'failed', message: error.message || 'Upload failed' })
      apiStatus = 'offline'
    }
  }

  async function uploadAll() {
    for (const item of uploads) {
      if (item.status === 'ready') {
        await uploadItem(item)
      }
    }
  }

  async function extractForItem(item) {
    if (!item.backendFilename) return
    updateUpload(item.id, { status: 'extracting', message: 'Extracting bibliography' })
    try {
      await extractBibliography(item.backendFilename)
      updateUpload(item.id, { status: 'extracted', message: 'Extraction started' })
      await loadBibliographyList()
    } catch (error) {
      updateUpload(item.id, { status: 'failed', message: error.message || 'Extraction failed' })
    }
  }

  async function consolidate() {
    consolidateStatus = 'Consolidating...'
    try {
      await consolidateBibliographies()
      consolidateStatus = 'Consolidation completed.'
      await loadBibliographyList()
    } catch (error) {
      consolidateStatus = `Consolidation failed: ${error.message || 'Unknown error'}`
    }
  }

  async function loadBibliographyList() {
    bibliographyStatus = 'Loading bibliography files...'
    const { data, source } = await fetchBibliographyList()
    bibliographyFiles = data
    bibliographySource = source
    bibliographyStatus = source === 'api' ? 'Loaded from API.' : 'Sample data loaded.'
    if (source === 'api') {
      apiStatus = 'online'
    }
  }

  async function runSearch() {
    searchStatus = 'Searching...'
    const { data, source } = await runKeywordSearch({
      query: searchQuery,
      field: searchField,
      yearFrom,
      yearTo,
    })
    searchResults = data
    searchSource = source
    searchStatus = source === 'api' ? 'Results loaded from API.' : 'Sample results loaded.'
  }

  async function loadCorpus() {
    const { data, source } = await fetchCorpus()
    corpusItems = data
    corpusSource = source
  }

  async function loadDownloads() {
    const { data, source } = await fetchDownloadQueue()
    downloads = data
    downloadsSource = source
  }

  async function loadGraph() {
    graphStatus = 'Loading graph...'
    const { data, source } = await fetchGraph({
      maxNodes: graphMaxNodes,
      relationship: graphRelationship,
      status: graphStatusFilter,
      yearFrom: graphYearFrom ? Number(graphYearFrom) : null,
      yearTo: graphYearTo ? Number(graphYearTo) : null,
      hideIsolates: graphHideIsolates,
    })
    graphNodes = data.nodes || []
    graphEdges = data.edges || []
    graphStats = data.stats || null
    graphSource = source
    graphStatus = source === 'api' ? 'Loaded from API.' : 'Sample graph loaded.'
    const degreeMap = buildDegreeMap(graphNodes, graphEdges)
    graphDegreeMap = degreeMap
    graphLayout = buildGraphLayout(graphNodes, graphEdges, degreeMap)
    graphNodeMap = new Map(graphLayout.map((node) => [node.id, node]))
    graphCanvasSize = graphLayout[0]?.canvasSize || 440
    graphViewBox = { x: 0, y: 0, size: graphCanvasSize }
  }

  function connectLogs() {
    try {
      const ws = new WebSocket('ws://localhost:4000')
      ws.onopen = () => {
        logsStatus = 'connected'
      }
      ws.onmessage = (event) => {
        logs = [...logs, event.data].slice(-maxLogs)
      }
      ws.onclose = () => {
        logsStatus = 'disconnected'
      }
      ws.onerror = () => {
        logsStatus = 'error'
      }
    } catch (error) {
      logsStatus = 'unavailable'
    }
  }

  function buildDegreeMap(nodes, edges) {
    const map = new Map()
    for (const node of nodes) {
      map.set(node.id, 0)
    }
    for (const edge of edges) {
      map.set(edge.source, (map.get(edge.source) || 0) + 1)
      map.set(edge.target, (map.get(edge.target) || 0) + 1)
    }
    return map
  }

  function computeNodeSize(degree) {
    const base = 12
    const scaled = Math.sqrt(Math.max(0, degree)) * 4
    return Math.min(28, base + scaled)
  }

  function runForceLayout(nodes, edges, degreeMap, canvasSize) {
    const center = canvasSize / 2
    const layout = nodes.map((node) => {
      const degree = degreeMap.get(node.id) || 0
      return {
        ...node,
        x: center + (Math.random() - 0.5) * canvasSize * 0.6,
        y: center + (Math.random() - 0.5) * canvasSize * 0.6,
        vx: 0,
        vy: 0,
        size: computeNodeSize(degree),
        canvasSize,
      }
    })

    const indexById = new Map(layout.map((node, idx) => [node.id, idx]))
    const springs = edges
      .map((edge) => ({
        source: indexById.get(edge.source),
        target: indexById.get(edge.target),
      }))
      .filter((edge) => edge.source !== undefined && edge.target !== undefined)

    const repulsion = 5200
    const springLength = Math.max(120, Math.min(200, Math.sqrt(nodes.length) * 18))
    const springStrength = 0.03
    const centerStrength = 0.06
    const damping = 0.85
    const iterations = Math.min(180, 90 + nodes.length * 0.4)

    for (let iter = 0; iter < iterations; iter += 1) {
      for (let i = 0; i < layout.length; i += 1) {
        let fx = 0
        let fy = 0
        const node = layout[i]
        for (let j = i + 1; j < layout.length; j += 1) {
          const other = layout[j]
          let dx = node.x - other.x
          let dy = node.y - other.y
          let distSq = dx * dx + dy * dy
          if (distSq < 1) distSq = 1
          const force = repulsion / distSq
          const dist = Math.sqrt(distSq)
          const nx = dx / dist
          const ny = dy / dist
          fx += nx * force
          fy += ny * force
          other.vx -= nx * force
          other.vy -= ny * force
        }
        node.vx += fx
        node.vy += fy
      }

      for (const edge of springs) {
        const source = layout[edge.source]
        const target = layout[edge.target]
        let dx = target.x - source.x
        let dy = target.y - source.y
        let dist = Math.sqrt(dx * dx + dy * dy) || 1
        const displacement = dist - springLength
        const force = displacement * springStrength
        const nx = dx / dist
        const ny = dy / dist
        source.vx += nx * force
        source.vy += ny * force
        target.vx -= nx * force
        target.vy -= ny * force
      }

      for (const node of layout) {
        const dx = center - node.x
        const dy = center - node.y
        node.vx += dx * centerStrength
        node.vy += dy * centerStrength
        node.vx *= damping
        node.vy *= damping
        node.x = Math.min(canvasSize - 40, Math.max(40, node.x + node.vx))
        node.y = Math.min(canvasSize - 40, Math.max(40, node.y + node.vy))
      }
    }

    return layout.map((node) => ({ ...node, canvasSize }))
  }

  function buildGraphLayout(nodes, edges, degreeMap) {
    const count = nodes.length
    if (count === 0) return []
    const orderedNodes = [...nodes].sort((a, b) => (degreeMap.get(b.id) || 0) - (degreeMap.get(a.id) || 0))

    if (count > 400) {
      const ringSize = 36
      const radiusBase = 120
      const ringGap = 80
      const maxRing = Math.ceil(count / ringSize)
      const canvasSize = Math.min(1800, Math.max(520, (radiusBase + maxRing * ringGap + 120) * 2))
      const center = canvasSize / 2
      return orderedNodes.map((node, index) => {
        const ringIndex = Math.floor(index / ringSize)
        const positionInRing = index % ringSize
        const countInRing = Math.min(ringSize, count - ringIndex * ringSize) || 1
        const angle = (positionInRing / countInRing) * Math.PI * 2
        const radius = radiusBase + ringIndex * ringGap
        const degree = degreeMap.get(node.id) || 0
        return {
          ...node,
          x: center + radius * Math.cos(angle),
          y: center + radius * Math.sin(angle),
          size: computeNodeSize(degree),
          canvasSize,
        }
      })
    }

    const adjacency = new Map()
    for (const node of orderedNodes) {
      adjacency.set(node.id, [])
    }
    for (const edge of edges) {
      if (adjacency.has(edge.source) && adjacency.has(edge.target)) {
        adjacency.get(edge.source).push(edge.target)
        adjacency.get(edge.target).push(edge.source)
      }
    }

    const components = []
    const visited = new Set()
    for (const node of orderedNodes) {
      if (visited.has(node.id)) continue
      const stack = [node.id]
      const component = []
      visited.add(node.id)
      while (stack.length) {
        const current = stack.pop()
        component.push(current)
        for (const next of adjacency.get(current) || []) {
          if (!visited.has(next)) {
            visited.add(next)
            stack.push(next)
          }
        }
      }
      components.push(component)
    }

    const componentLayouts = components.map((ids) => {
      const componentNodes = orderedNodes.filter((node) => ids.includes(node.id))
      const componentEdges = edges.filter((edge) => ids.includes(edge.source) && ids.includes(edge.target))
      const componentSize = Math.min(900, Math.max(320, 240 + Math.sqrt(componentNodes.length) * 160))
      const layout = runForceLayout(componentNodes, componentEdges, degreeMap, componentSize)
      return { ids, layout, size: componentSize }
    })

    const columns = Math.ceil(Math.sqrt(componentLayouts.length))
    const maxComponentSize = componentLayouts.reduce((max, comp) => Math.max(max, comp.size), 0)
    const cellSize = maxComponentSize + 140
    const rows = Math.ceil(componentLayouts.length / columns)
    const canvasSize = Math.max(cellSize * columns, cellSize * rows)

    const combined = []
    componentLayouts.forEach((component, index) => {
      const offsetX = (index % columns) * cellSize + (cellSize - component.size) / 2
      const offsetY = Math.floor(index / columns) * cellSize + (cellSize - component.size) / 2
      component.layout.forEach((node) => {
        combined.push({
          ...node,
          x: node.x + offsetX,
          y: node.y + offsetY,
          canvasSize,
        })
      })
    })

    return combined
  }

  function clamp(value, min, max) {
    return Math.min(Math.max(value, min), max)
  }

  function handleGraphWheel(event) {
    event.preventDefault()
    if (!graphCanvasSize) return
    const rect = event.currentTarget.getBoundingClientRect()
    const pointerX = (event.clientX - rect.left) / rect.width
    const pointerY = (event.clientY - rect.top) / rect.height
    const currentSize = graphViewBox.size
    const zoomFactor = event.deltaY > 0 ? 1.12 : 0.88
    const minSize = Math.max(220, graphCanvasSize * 0.35)
    const maxSize = graphCanvasSize * 2.2
    const newSize = clamp(currentSize * zoomFactor, minSize, maxSize)
    const svgX = graphViewBox.x + pointerX * currentSize
    const svgY = graphViewBox.y + pointerY * currentSize
    let nextX = svgX - pointerX * newSize
    let nextY = svgY - pointerY * newSize
    const padding = graphCanvasSize * 0.2
    nextX = clamp(nextX, -padding, graphCanvasSize - newSize + padding)
    nextY = clamp(nextY, -padding, graphCanvasSize - newSize + padding)
    graphViewBox = { x: nextX, y: nextY, size: newSize }
  }

  function handleGraphPointerDown(event) {
    isGraphPanning = true
    graphPanStart = {
      x: event.clientX,
      y: event.clientY,
      viewX: graphViewBox.x,
      viewY: graphViewBox.y,
    }
    event.currentTarget.setPointerCapture(event.pointerId)
  }

  function handleGraphPointerMove(event) {
    if (!isGraphPanning) return
    const rect = event.currentTarget.getBoundingClientRect()
    const scaleX = graphViewBox.size / rect.width
    const scaleY = graphViewBox.size / rect.height
    const dx = (event.clientX - graphPanStart.x) * scaleX
    const dy = (event.clientY - graphPanStart.y) * scaleY
    graphViewBox = {
      ...graphViewBox,
      x: graphPanStart.viewX - dx,
      y: graphPanStart.viewY - dy,
    }
  }

  function handleGraphPointerUp(event) {
    isGraphPanning = false
    try {
      event.currentTarget.releasePointerCapture(event.pointerId)
    } catch (error) {
      // ignore
    }
  }

  function resetGraphView() {
    graphViewBox = { x: 0, y: 0, size: graphCanvasSize }
  }

  $: hoveredNode = graphLayout.find((node) => node.id === hoveredNodeId)
  $: graphNodeCount = graphStats?.node_count ?? graphNodes.length
  $: graphEdgeCount = graphStats?.edge_count ?? graphEdges.length
  $: graphRelationshipCounts = graphStats?.relationship_counts || {
    references: graphEdges.filter((edge) => edge.relationship_type === 'references').length,
    cited_by: graphEdges.filter((edge) => edge.relationship_type === 'cited_by').length,
  }
  $: graphList = [...graphLayout].sort(
    (a, b) => (graphDegreeMap.get(b.id) || 0) - (graphDegreeMap.get(a.id) || 0)
  )
  $: graphDegreeMap = (() => {
    const map = new Map()
    for (const node of graphNodes) {
      map.set(node.id, 0)
    }
    for (const edge of graphEdges) {
      if (map.has(edge.source)) map.set(edge.source, map.get(edge.source) + 1)
      if (map.has(edge.target)) map.set(edge.target, map.get(edge.target) + 1)
    }
    return map
  })()

  function truncateLabel(label, max = 28) {
    if (!label) return ''
    return label.length > max ? `${label.slice(0, max - 1)}…` : label
  }

  onMount(async () => {
    await loadBibliographyList()
    await loadCorpus()
    await loadDownloads()
    await loadGraph()
    connectLogs()
  })
</script>

<div class="app-shell">
  <header class="app-header">
    <div>
      <p class="eyebrow">Korpus Builder</p>
      <h1>Corpus orchestration workspace</h1>
      <p class="subtitle">
        Build, enrich, and monitor literature pipelines end-to-end. API status: {apiStatus}.
      </p>
    </div>
    <div class="header-badge">
      <span>Milestone 2 prep</span>
      <strong>Frontend · Svelte</strong>
    </div>
  </header>

  <div class="layout">
    <aside class="side-nav" data-testid="side-nav">
      {#each tabs as tab}
        <button
          class:active={activeTab === tab.id}
          on:click={() => (activeTab = tab.id)}
          type="button"
          data-testid={`tab-${tab.id}`}
        >
          {tab.label}
        </button>
      {/each}
    </aside>

    <section class="content">
      {#if activeTab === 'dashboard'}
        <div class="grid">
          <div class="card highlight" data-testid="dashboard-overview">
            <h2>At a glance</h2>
            <p>Seed ingestion, keyword search, and corpus health in one view.</p>
            <div class="stat-row">
              <div>
                <span class="stat-label">Bibliographies</span>
                <strong>{bibliographyFiles.length}</strong>
              </div>
              <div>
                <span class="stat-label">Corpus works</span>
                <strong>{corpusItems.length}</strong>
              </div>
              <div>
                <span class="stat-label">Queue items</span>
                <strong>{downloads.length}</strong>
              </div>
            </div>
          </div>
          <div class="card">
            <h2>Recent activity</h2>
            <ul class="activity">
              {#each sampleActivity as item}
                <li>
                  <div>
                    <strong>{item.label}</strong>
                    <span>{item.detail}</span>
                  </div>
                  <time>{item.timestamp}</time>
                </li>
              {/each}
            </ul>
          </div>
          <div class="card">
            <h2>Pipeline health</h2>
            <p>Extraction, enrichment, and downloads are staged and recoverable.</p>
            <div class="pill-row">
              <span class="pill">Extraction ready</span>
              <span class="pill">Enrichment queued</span>
              <span class="pill">Downloads active</span>
            </div>
          </div>
        </div>
      {/if}

      {#if activeTab === 'ingest'}
        <div class="card" data-testid="ingest-panel">
          <h2>Seed ingestion</h2>
          <p>Upload PDFs, extract bibliographies, and consolidate reference files.</p>
          <div class="uploader">
            <label class="upload-drop">
              <input type="file" multiple accept=".pdf" on:change={handleFiles} />
              <span>Drop PDFs or click to add files</span>
            </label>
            <button class="primary" type="button" on:click={uploadAll}>Upload selected</button>
          </div>
          <div class="upload-list">
            {#if uploads.length === 0}
              <p class="muted">No files selected yet.</p>
            {:else}
              {#each uploads as item}
                <div class="upload-item">
                  <div>
                    <strong>{item.name}</strong>
                    <span>{formatBytes(item.size)}</span>
                  </div>
                  <div class="upload-actions">
                    <span class={`status ${item.status}`}>{item.status}</span>
                    <button type="button" on:click={() => uploadItem(item)} disabled={item.status !== 'ready'}>
                      Upload
                    </button>
                    <button
                      type="button"
                      on:click={() => extractForItem(item)}
                      disabled={!item.backendFilename}
                    >
                      Extract
                    </button>
                  </div>
                </div>
              {/each}
            {/if}
          </div>
        </div>

        <div class="card">
          <h3>Bibliography files</h3>
          <p class="muted">{bibliographyStatus} ({bibliographySource})</p>
          <div class="split">
            <button class="secondary" type="button" on:click={loadBibliographyList}>Refresh</button>
            <button class="primary" type="button" on:click={consolidate}>Consolidate</button>
            <span class="muted">{consolidateStatus}</span>
          </div>
          <ul class="file-list">
            {#each bibliographyFiles as file}
              <li>{file}</li>
            {/each}
          </ul>
        </div>
      {/if}

      {#if activeTab === 'search'}
        <div class="card" data-testid="search-panel">
          <h2>Keyword search</h2>
          <p>Compose OpenAlex queries and queue results for downloads.</p>
          <form class="search-form" on:submit|preventDefault={runSearch}>
            <input
              type="text"
              placeholder="e.g., institutional economics AND governance"
              bind:value={searchQuery}
              data-testid="search-query"
            />
            <select bind:value={searchField}>
              <option value="default">All fields</option>
              <option value="title">Title</option>
              <option value="abstract">Abstract</option>
              <option value="title_and_abstract">Title + abstract</option>
              <option value="fulltext">Full text</option>
            </select>
            <input type="number" placeholder="From" bind:value={yearFrom} />
            <input type="number" placeholder="To" bind:value={yearTo} />
            <button class="primary" type="submit">Search</button>
          </form>
          <p class="muted">{searchStatus} ({searchSource})</p>
          <div class="table">
            <div class="table-row header">
              <span>Title</span>
              <span>Authors</span>
              <span>Year</span>
              <span>Type</span>
            </div>
            {#each searchResults as result}
              <div class="table-row">
                <span>{result.title}</span>
                <span>{result.authors}</span>
                <span>{result.year}</span>
                <span>{result.type}</span>
              </div>
            {/each}
          </div>
        </div>
      {/if}

      {#if activeTab === 'corpus'}
        <div class="card" data-testid="corpus-panel">
          <h2>Corpus</h2>
          <p class="muted">{corpusSource === 'api' ? 'Live corpus from API.' : 'Sample corpus data.'}</p>
          <div class="table">
            <div class="table-row header">
              <span>Title</span>
              <span>Year</span>
              <span>Source</span>
              <span>Status</span>
            </div>
            {#each corpusItems as item}
              <div class="table-row">
                <span>{item.title}</span>
                <span>{item.year}</span>
                <span>{item.source}</span>
                <span class="tag">{item.status}</span>
              </div>
            {/each}
          </div>
        </div>
      {/if}

      {#if activeTab === 'downloads'}
        <div class="card" data-testid="downloads-panel">
          <h2>Download queue</h2>
          <p class="muted">{downloadsSource === 'api' ? 'Live queue from API.' : 'Sample queue data.'}</p>
          <div class="table">
            <div class="table-row header">
              <span>Work</span>
              <span>Status</span>
              <span>Attempts</span>
            </div>
            {#each downloads as item}
              <div class="table-row">
                <span>{item.title}</span>
                <span class={`tag ${item.status}`}>{item.status}</span>
                <span>{item.attempts}</span>
              </div>
            {/each}
          </div>
        </div>
      {/if}

      {#if activeTab === 'logs'}
        <div class="card" data-testid="logs-panel">
          <h2>Pipeline logs</h2>
          <p class="muted">WebSocket status: {logsStatus}</p>
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
      {/if}

      {#if activeTab === 'graph'}
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
            <label class="graph-toggle">
              <span>Hide isolates</span>
              <input type="checkbox" bind:checked={graphHideIsolates} />
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
                {#each graphEdges as edge}
                  {#if graphNodeMap.has(edge.source) && graphNodeMap.has(edge.target)}
                    <line
                      x1={graphNodeMap.get(edge.source).x}
                      y1={graphNodeMap.get(edge.source).y}
                      x2={graphNodeMap.get(edge.target).x}
                      y2={graphNodeMap.get(edge.target).y}
                      class={`edge ${edge.relationship_type === 'cited_by' ? 'edge--cited' : 'edge--ref'}`}
                    />
                  {/if}
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
              {#if hoveredNode}
                <div class="graph-hover">
                  <span class="eyebrow">Selected node</span>
                  <strong>{hoveredNode.title}</strong>
                </div>
              {/if}
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
                    <span>{truncateLabel(node.title)}</span>
                    <strong>{graphDegreeMap.get(node.id) ?? 0}</strong>
                  </button>
                {/each}
              </div>
            </div>
          </div>
        </div>
      {/if}
    </section>
  </div>
</div>
