<script>
  import { onDestroy, onMount, tick } from 'svelte'
  import { fetchGraph3D } from '../lib/api'

  export let autoLoad = true

  let THREE
  let OrbitControls
  let container
  let renderer
  let scene
  let camera
  let controls
  let points
  let edgeLines
  let clusterShells
  let raycaster
  let pointer
  let animationFrame = 0
  let settleFrame = 0
  let resizeObserver
  let handlePointerMove
  let handleClick
  let componentMounted = false
  let initialLoadStarted = false
  let graphData = { nodes: [], edges: [], stats: {} }
  let graphStatus = '3D graph not loaded.'
  let graphLoading = false
  let relationship = 'both'
  let statusFilter = 'all'
  let scope = 'all'
  let maxNodes = 120000
  let colorMode = 'cluster'
  let selectedNode = null
  let hoveredNode = null
  let selectedCluster = null

  const statusColors = {
    downloaded: 0x0f8b8d,
    matched: 0x3b82f6,
    queued_download: 0xf4a340,
    failed_download: 0xb91c1c,
    failed_enrichment: 0xb91c1c,
    enriching: 0x8b5cf6,
    raw: 0x6f7b84,
  }
  const palette = [
    0x0f8b8d,
    0xf4a340,
    0x3b82f6,
    0xe76f51,
    0x7c3aed,
    0x5f6f52,
    0xf59e0b,
    0x14b8a6,
    0xef4444,
    0x64748b,
    0x8a5a44,
    0x1f3b4d,
  ]

  function stableHash(value) {
    let hash = 2166136261
    for (const char of String(value || '')) {
      hash ^= char.charCodeAt(0)
      hash = Math.imul(hash, 16777619) >>> 0
    }
    return hash
  }

  function paletteColor(value) {
    return palette[stableHash(value) % palette.length]
  }

  function nodeColor(node) {
    if (colorMode === 'status') return statusColors[node.status] || 0x6f7b84
    if (colorMode === 'degree') {
      const degree = Number(node.degree || 0)
      if (degree > 100) return 0xfff1a8
      if (degree > 30) return 0xf4a340
      if (degree > 10) return 0x3b82f6
      return 0x0f8b8d
    }
    if (colorMode === 'year') {
      const year = Number(node.year || 0)
      if (!year) return 0x64748b
      return palette[Math.abs(Math.floor(year / 10)) % palette.length]
    }
    if (colorMode === 'area') return paletteColor(node.area || node.type || node.source || 'unknown')
    if (colorMode === 'component') return palette[Math.abs(Number(node.component || 0)) % palette.length]
    if (node.cluster_kind === 'low_signal') return 0x64747a
    return palette[Math.abs(Number(node.cluster || 0)) % palette.length]
  }

  function disposeObject(object) {
    if (!object) return
    for (const child of object.children || []) {
      disposeObject(child)
    }
    object.geometry?.dispose?.()
    if (Array.isArray(object.material)) {
      for (const material of object.material) material.dispose?.()
    } else {
      object.material?.dispose?.()
    }
    scene?.remove(object)
  }

  function renderGraph() {
    if (!scene || !THREE) return
    disposeObject(points)
    disposeObject(edgeLines)
    disposeObject(clusterShells)
    selectedNode = null
    hoveredNode = null

    const nodes = graphData.nodes || []
    const edges = graphData.edges || []
    const nodeIndex = new Map(nodes.map((node, index) => [node.id, index]))
    const positions = new Float32Array(nodes.length * 3)
    const colors = new Float32Array(nodes.length * 3)
    const color = new THREE.Color()

    nodes.forEach((node, index) => {
      positions[index * 3] = Number(node.x || 0)
      positions[index * 3 + 1] = Number(node.y || 0)
      positions[index * 3 + 2] = Number(node.z || 0)
      color.setHex(nodeColor(node))
      colors[index * 3] = color.r
      colors[index * 3 + 1] = color.g
      colors[index * 3 + 2] = color.b
    })

    const pointGeometry = new THREE.BufferGeometry()
    pointGeometry.setAttribute('position', new THREE.BufferAttribute(positions, 3))
    pointGeometry.setAttribute('color', new THREE.BufferAttribute(colors, 3))
    pointGeometry.computeBoundingSphere()
    points = new THREE.Points(
      pointGeometry,
      new THREE.PointsMaterial({
        size: 3,
        sizeAttenuation: false,
        vertexColors: true,
        transparent: true,
        opacity: 1,
        blending: THREE.AdditiveBlending,
        depthTest: false,
        depthWrite: false,
      })
    )
    points.userData.nodes = nodes
    scene.add(points)

    clusterShells = new THREE.Group()
    for (const cluster of clusters.slice(0, 80)) {
      const radius = Math.max(48, Number(cluster.radius || Math.sqrt(Number(cluster.size || 1)) * 8))
      const geometry = new THREE.SphereGeometry(radius, 24, 12)
      const material = new THREE.MeshBasicMaterial({
        color: cluster.kind === 'low_signal' ? 0x52636a : paletteColor(cluster.area || cluster.label || cluster.id),
        transparent: true,
        opacity: cluster.kind === 'low_signal' ? 0.055 : 0.15,
        wireframe: true,
        depthWrite: false,
      })
      const shell = new THREE.Mesh(geometry, material)
      shell.position.set(Number(cluster.x || 0), Number(cluster.y || 0), Number(cluster.z || 0))
      clusterShells.add(shell)
    }
    scene.add(clusterShells)

    const edgePositions = new Float32Array(edges.length * 2 * 3)
    const edgeColors = new Float32Array(edges.length * 2 * 3)
    let edgeOffset = 0
    let edgeColorOffset = 0
    for (const edge of edges) {
      const sourceIndex = nodeIndex.get(edge.s)
      const targetIndex = nodeIndex.get(edge.t)
      if (sourceIndex === undefined || targetIndex === undefined) continue
      edgePositions[edgeOffset++] = positions[sourceIndex * 3]
      edgePositions[edgeOffset++] = positions[sourceIndex * 3 + 1]
      edgePositions[edgeOffset++] = positions[sourceIndex * 3 + 2]
      edgePositions[edgeOffset++] = positions[targetIndex * 3]
      edgePositions[edgeOffset++] = positions[targetIndex * 3 + 1]
      edgePositions[edgeOffset++] = positions[targetIndex * 3 + 2]
      const edgeHex = edge.r === 'cited_by' ? 0xf4a340 : 0x7c8b95
      color.setHex(edgeHex)
      for (let i = 0; i < 2; i += 1) {
        edgeColors[edgeColorOffset++] = color.r
        edgeColors[edgeColorOffset++] = color.g
        edgeColors[edgeColorOffset++] = color.b
      }
    }
    const lineGeometry = new THREE.BufferGeometry()
    lineGeometry.setAttribute('position', new THREE.BufferAttribute(edgePositions.slice(0, edgeOffset), 3))
    lineGeometry.setAttribute('color', new THREE.BufferAttribute(edgeColors.slice(0, edgeColorOffset), 3))
    edgeLines = new THREE.LineSegments(
      lineGeometry,
      new THREE.LineBasicMaterial({
        vertexColors: true,
        transparent: true,
        opacity: 0.26,
      })
    )
    scene.add(edgeLines)

    resetCamera()
    renderNow()
    renderForFrames(45)
    requestRender()
  }

  function resetCamera() {
    if (!camera || !controls) return
    const sphere = points?.geometry?.boundingSphere
    const radius = sphere?.radius || 1600
    const center = sphere?.center || { x: 0, y: 0, z: 0 }
    const targetY = center.y - radius * 1.15
    camera.position.set(center.x + radius * 0.35, targetY + radius * 0.1, center.z + radius * 2.4)
    camera.near = Math.max(0.1, radius / 4000)
    camera.far = Math.max(6000, radius * 8)
    camera.updateProjectionMatrix()
    controls.target.set(center.x, targetY, center.z)
    controls.update()
  }

  function focusCluster(cluster) {
    if (!camera || !controls || !cluster) return
    selectedCluster = Number(cluster.id)
    const x = Number(cluster.x || 0)
    const y = Number(cluster.y || 0)
    const z = Number(cluster.z || 0)
    const radius = Math.max(280, Math.min(2200, 180 + Math.sqrt(Number(cluster.size || 1)) * 34))
    const visualRadius = Math.max(radius, Number(cluster.radius || 0) * 3.2)
    camera.position.set(x + visualRadius * 0.95, y + visualRadius * 0.35, z + visualRadius * 1.55)
    camera.near = Math.max(0.1, visualRadius / 2000)
    camera.far = Math.max(6000, visualRadius * 12)
    camera.updateProjectionMatrix()
    controls.target.set(x, y, z)
    controls.update()
    renderNow()
    renderForFrames(20)
  }

  async function load3DGraph() {
    graphLoading = true
    graphStatus = 'Building 3D graph payload...'
    try {
      const payload = await fetchGraph3D({
        maxNodes,
        relationship,
        status: statusFilter,
        scope,
      })
      graphData = payload || { nodes: [], edges: [], stats: {} }
      graphStatus = `Loaded ${graphData.nodes?.length || 0} nodes and ${graphData.edges?.length || 0} edges.`
      renderGraph()
    } catch (error) {
      graphStatus = error?.message || 'Failed to load 3D graph.'
    } finally {
      graphLoading = false
    }
  }

  function requestRender() {
    if (animationFrame) return
    animationFrame = requestAnimationFrame(() => {
      animationFrame = 0
      renderNow()
    })
  }

  function renderNow() {
    if (!renderer || !scene || !camera) return
    renderer.render(scene, camera)
  }

  function renderForFrames(count = 30) {
    if (settleFrame) cancelAnimationFrame(settleFrame)
    let remaining = count
    const draw = () => {
      renderNow()
      remaining -= 1
      if (remaining > 0) {
        settleFrame = requestAnimationFrame(draw)
      } else {
        settleFrame = 0
      }
    }
    settleFrame = requestAnimationFrame(draw)
  }

  function resize() {
    if (!container || !renderer || !camera) return
    const width = container.clientWidth || 800
    const height = container.clientHeight || 560
    camera.aspect = width / height
    camera.updateProjectionMatrix()
    renderer.setSize(width, height, false)
    requestRender()
  }

  function pointerFromEvent(event) {
    if (!pointer || !renderer) return
    const rect = renderer.domElement.getBoundingClientRect()
    pointer.x = ((event.clientX - rect.left) / rect.width) * 2 - 1
    pointer.y = -((event.clientY - rect.top) / rect.height) * 2 + 1
  }

  function pickNode(event, persist = false) {
    if (!raycaster || !points || !camera) return
    pointerFromEvent(event)
    raycaster.setFromCamera(pointer, camera)
    raycaster.params.Points.threshold = 10
    const hit = raycaster.intersectObject(points, false)[0]
    const nextNode = hit ? points.userData.nodes?.[hit.index] : null
    hoveredNode = nextNode
    if (persist && nextNode) selectedNode = nextNode
  }

  function formatNumber(value) {
    return new Intl.NumberFormat().format(Number(value || 0))
  }

  function compactList(items, fallback = 'unknown') {
    if (!Array.isArray(items) || !items.length) return fallback
    return items.slice(0, 3).map((item) => `${item.label} (${formatNumber(item.count)})`).join(', ')
  }

  onMount(async () => {
    componentMounted = true
    await tick()
    const [threeModule, controlsModule] = await Promise.all([
      import('three'),
      import('three/addons/controls/OrbitControls.js'),
    ])
    if (!componentMounted || !container) return
    THREE = threeModule
    OrbitControls = controlsModule.OrbitControls
    pointer = new THREE.Vector2(999, 999)
    scene = new THREE.Scene()
    scene.background = new THREE.Color(0x0b1f24)
    scene.fog = new THREE.Fog(0x0b1f24, 2400, 9000)
    camera = new THREE.PerspectiveCamera(58, 1, 0.1, 12000)
    renderer = new THREE.WebGLRenderer({ antialias: false, powerPreference: 'high-performance' })
    renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 1.5))
    container.appendChild(renderer.domElement)

    controls = new OrbitControls(camera, renderer.domElement)
    controls.enableDamping = false
    controls.addEventListener('change', requestRender)

    raycaster = new THREE.Raycaster()
    handlePointerMove = (event) => pickNode(event, false)
    handleClick = (event) => pickNode(event, true)
    renderer.domElement.addEventListener('pointermove', handlePointerMove)
    renderer.domElement.addEventListener('click', handleClick)

    resizeObserver = new ResizeObserver(resize)
    resizeObserver.observe(container)
    resetCamera()
    resize()

    requestRender()
    if (autoLoad && !initialLoadStarted) {
      initialLoadStarted = true
      void load3DGraph()
    }
  })

  onDestroy(() => {
    componentMounted = false
    if (animationFrame) cancelAnimationFrame(animationFrame)
    if (settleFrame) cancelAnimationFrame(settleFrame)
    resizeObserver?.disconnect()
    controls?.removeEventListener('change', requestRender)
    if (handlePointerMove) renderer?.domElement?.removeEventListener('pointermove', handlePointerMove)
    if (handleClick) renderer?.domElement?.removeEventListener('click', handleClick)
    controls?.dispose()
    disposeObject(points)
    disposeObject(edgeLines)
    disposeObject(clusterShells)
    renderer?.dispose()
    renderer?.domElement?.remove()
  })

  $: activeNode = selectedNode || hoveredNode
  $: clusters = graphData.clusters || []
  $: topClusters = clusters.slice(0, 16)
  $: selectedClusterData = clusters.find((cluster) => Number(cluster.id) === selectedCluster)
</script>

<div class="card graph-3d-panel" data-testid="graph-3d-panel">
  <div class="workspace-panel-header graph-3d-header">
    <div class="workspace-panel-title">
      <h2 class="workspace-section-title">3D Graph Explorer</h2>
      <p class="muted">
        Three.js/WebGL renderer for the existing corpus graph. Nodes are GPU points, edges are batched line segments, and layout is precomputed by the backend.
      </p>
    </div>
    <div class="graph-3d-actions">
      <button class="secondary" type="button" on:click={resetCamera} disabled={!graphData.nodes?.length}>Reset camera</button>
      <button type="button" on:click={load3DGraph} disabled={graphLoading}>
        {graphLoading ? 'Loading 3D graph...' : 'Load 3D graph'}
      </button>
    </div>
  </div>

  <div class="graph-3d-controls">
    <label>
      <span class="muted small">Scope</span>
      <select bind:value={scope} disabled={graphLoading}>
        <option value="all">Full library</option>
        <option value="corpus">Current corpus</option>
      </select>
    </label>
    <label>
      <span class="muted small">Relationship</span>
      <select bind:value={relationship} disabled={graphLoading}>
        <option value="both">References + cited by</option>
        <option value="references">References only</option>
        <option value="cited_by">Cited by only</option>
      </select>
    </label>
    <label>
      <span class="muted small">Status</span>
      <select bind:value={statusFilter} disabled={graphLoading}>
        <option value="all">All</option>
        <option value="downloaded">Downloaded</option>
        <option value="matched">Matched</option>
        <option value="raw">Raw</option>
      </select>
    </label>
    <label>
      <span class="muted small">Max nodes</span>
      <input type="number" min="1000" max="120000" step="1000" bind:value={maxNodes} disabled={graphLoading} />
    </label>
    <label>
      <span class="muted small">Color by</span>
      <select bind:value={colorMode} on:change={() => graphData.nodes?.length && renderGraph()}>
        <option value="cluster">Graph cluster</option>
        <option value="area">Science area</option>
        <option value="component">Connected component</option>
        <option value="status">Pipeline status</option>
        <option value="degree">Degree</option>
        <option value="year">Year decade</option>
      </select>
    </label>
  </div>

  <div class="graph-3d-layout">
    <div class="graph-3d-canvas" bind:this={container} aria-label="3D graph visualization" role="application"></div>
    <aside class="graph-3d-sidebar">
      <div class="graph-3d-summary">
        <div>
          <span class="eyebrow">Nodes</span>
          <strong>{formatNumber(graphData.stats?.node_count || graphData.nodes?.length)}</strong>
        </div>
        <div>
          <span class="eyebrow">Edges</span>
          <strong>{formatNumber(graphData.stats?.edge_count || graphData.edges?.length)}</strong>
        </div>
        <div>
          <span class="eyebrow">Clusters</span>
          <strong>{formatNumber(graphData.stats?.cluster_count)}</strong>
        </div>
        <div>
          <span class="eyebrow">Cited by</span>
          <strong>{formatNumber(graphData.stats?.relationship_counts?.cited_by)}</strong>
        </div>
      </div>

      <p class="muted small">{graphStatus}</p>
      <p class="muted small">
        {formatNumber(graphData.stats?.component_count)} connected components. Clusters are citation communities; tiny fragments are bucketed by type and decade.
      </p>

      <div class="graph-3d-clusters">
        <div>
          <span class="eyebrow">Top clusters</span>
          <p class="muted small">Jump to the largest communities.</p>
        </div>
        {#if topClusters.length}
          <div class="graph-3d-cluster-list">
            {#each topClusters as cluster}
              <button
                class:active={selectedCluster === Number(cluster.id)}
                type="button"
                on:click={() => focusCluster(cluster)}
                title={cluster.top_titles?.join('\n')}
              >
                <span>{cluster.label}</span>
                <small>{formatNumber(cluster.size)} works / {cluster.area || 'unknown area'} / {cluster.region || 'unknown region'}</small>
                <small>{cluster.kind === 'low_signal' ? 'Low-link bucket' : 'Citation community'} / {cluster.decade}</small>
              </button>
            {/each}
          </div>
        {:else}
          <p class="muted small">No cluster summaries loaded yet.</p>
        {/if}
      </div>

      {#if selectedClusterData}
        <div class="graph-3d-cluster-detail">
          <span class="eyebrow">Cluster profile</span>
          <strong>{selectedClusterData.label}</strong>
          <span class="muted small">{formatNumber(selectedClusterData.size)} works / radius {Math.round(selectedClusterData.radius || 0)}</span>
          <span class="muted small">Areas: {compactList(selectedClusterData.areas)}</span>
          <span class="muted small">Regions: {compactList(selectedClusterData.regions)}</span>
          <span class="muted small">Countries: {compactList(selectedClusterData.countries, 'unknown countries')}</span>
          <span class="muted small">Types: {compactList(selectedClusterData.types)}</span>
          {#if selectedClusterData.year_min || selectedClusterData.year_max}
            <span class="muted small">Years: {selectedClusterData.year_min || '?'}-{selectedClusterData.year_max || '?'}</span>
          {/if}
        </div>
      {/if}

      <div class="graph-3d-selected">
        <span class="eyebrow">Selected work</span>
        {#if activeNode}
          <strong>{activeNode.title}</strong>
          <span class="muted small">{activeNode.year || 'Year ?'} / {activeNode.type || 'Type ?'} / {activeNode.status}</span>
          <span class="muted small">Area: {activeNode.area || 'unknown'} / Region: {activeNode.region || 'unknown'}</span>
          <span class="muted small">Degree: {formatNumber(activeNode.degree)} / Cluster: {activeNode.cluster} ({formatNumber(activeNode.cluster_size)} nodes)</span>
          <span class="muted small">Component: {activeNode.component} ({formatNumber(activeNode.component_size)} nodes)</span>
          <span class="muted small">Source: {activeNode.source || 'unknown'}</span>
        {:else}
          <strong>No node selected</strong>
          <span class="muted small">Hover or click a point in the 3D graph.</span>
        {/if}
      </div>
    </aside>
  </div>
</div>
