<script>
  import { onDestroy, onMount, tick } from 'svelte'
  import {
    fetchGraph3DSnapshot,
    fetchGraph3DSnapshotResources,
    fetchGraph3DNodeDetail,
    fetchGraph3DClusterDetail,
  } from '../lib/api'

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
  let rotateFrame = 0
  let resizeObserver
  let handlePointerMove
  let handleClick
  let componentMounted = false
  let initialLoadStarted = false
  let graphData = { nodes: [], edges: [], edgeTriplets: null, positions: null, clusters: [], stats: {} }
  let graphStatus = '3D graph not loaded.'
  let graphLoading = false
  let relationship = 'both'
  let statusFilter = 'all'
  let scope = 'all'
  let maxNodes = 10000
  let colorMode = 'cluster'
  let depthScale = 1.6
  let autoRotate = false
  let selectedNode = null
  let hoveredNode = null
  let selectedCluster = null
  let selectedNodeDetail = null
  let selectedClusterDetail = null
  let nodeDetailLoading = false

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
    if (colorMode === 'region') return paletteColor(node.region || node.countries?.[0] || 'unknown')
    if (colorMode === 'component') return palette[Math.abs(Number(node.component || 0)) % palette.length]
    if (node.cluster_kind === 'low_signal') return 0x64747a
    return palette[Math.abs(Number(node.cluster || 0)) % palette.length]
  }

  function positionValue(positions, nodes, index, axis) {
    const base = index * 3 + axis
    if (positions && base < positions.length) {
      return Number(positions[base] || 0)
    }
    const node = nodes[index] || {}
    if (axis === 0) return Number(node.x || 0)
    if (axis === 1) return Number(node.y || 0)
    return Number(node.z || 0)
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
    hoveredNode = null

    const nodes = graphData.nodes || []
    const edges = graphData.edges || []
    const sourcePositions = graphData.positions
    const edgeTriplets = graphData.edgeTriplets
    const nodeIndex = new Map(nodes.map((node, index) => [node.id, index]))
    const positions = new Float32Array(nodes.length * 3)
    const colors = new Float32Array(nodes.length * 3)
    const color = new THREE.Color()

    nodes.forEach((node, index) => {
      positions[index * 3] = positionValue(sourcePositions, nodes, index, 0)
      positions[index * 3 + 1] = positionValue(sourcePositions, nodes, index, 1) * depthScale
      positions[index * 3 + 2] = positionValue(sourcePositions, nodes, index, 2)
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
        size: 4,
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
        opacity: cluster.kind === 'low_signal' ? 0.1 : 0.3,
        wireframe: true,
        depthTest: false,
        depthWrite: false,
      })
      const shell = new THREE.Mesh(geometry, material)
      shell.position.set(Number(cluster.x || 0), Number(cluster.y || 0) * depthScale, Number(cluster.z || 0))
      clusterShells.add(shell)
    }
    scene.add(clusterShells)

    const binaryEdgeCount = edgeTriplets?.length ? Math.floor(edgeTriplets.length / 3) : 0
    const edgeCount = binaryEdgeCount || edges.length
    const edgePositions = new Float32Array(edgeCount * 2 * 3)
    const edgeColors = new Float32Array(edgeCount * 2 * 3)
    let edgeOffset = 0
    let edgeColorOffset = 0
    const appendEdge = (sourceIndex, targetIndex, relationshipCode = 0) => {
      if (sourceIndex === undefined || targetIndex === undefined) return
      if (sourceIndex < 0 || targetIndex < 0 || sourceIndex >= nodes.length || targetIndex >= nodes.length) return
      edgePositions[edgeOffset++] = positions[sourceIndex * 3]
      edgePositions[edgeOffset++] = positions[sourceIndex * 3 + 1]
      edgePositions[edgeOffset++] = positions[sourceIndex * 3 + 2]
      edgePositions[edgeOffset++] = positions[targetIndex * 3]
      edgePositions[edgeOffset++] = positions[targetIndex * 3 + 1]
      edgePositions[edgeOffset++] = positions[targetIndex * 3 + 2]
      const edgeHex = relationshipCode === 1 ? 0xf4a340 : 0x7c8b95
      color.setHex(edgeHex)
      for (let i = 0; i < 2; i += 1) {
        edgeColors[edgeColorOffset++] = color.r
        edgeColors[edgeColorOffset++] = color.g
        edgeColors[edgeColorOffset++] = color.b
      }
    }
    if (binaryEdgeCount) {
      for (let i = 0; i < edgeTriplets.length; i += 3) {
        appendEdge(edgeTriplets[i], edgeTriplets[i + 1], edgeTriplets[i + 2])
      }
    } else {
      for (const edge of edges) {
        appendEdge(
          nodeIndex.get(edge.s || edge.source),
          nodeIndex.get(edge.t || edge.target),
          edge.r === 'cited_by' || edge.relationship_type === 'cited_by' ? 1 : 0
        )
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
    let center = points?.geometry?.boundingSphere?.center || { x: 0, y: 0, z: 0 }
    let radius = points?.geometry?.boundingSphere?.radius || 1600
    const framingClusters = clusters.slice(0, 24)
    if (framingClusters.length) {
      const bounds = framingClusters.reduce(
        (acc, cluster) => {
          const x = Number(cluster.x || 0)
          const y = Number(cluster.y || 0) * depthScale
          const z = Number(cluster.z || 0)
          const padding = Math.max(90, Number(cluster.radius || 0) * 1.7)
          acc.minX = Math.min(acc.minX, x - padding)
          acc.maxX = Math.max(acc.maxX, x + padding)
          acc.minY = Math.min(acc.minY, y - padding)
          acc.maxY = Math.max(acc.maxY, y + padding)
          acc.minZ = Math.min(acc.minZ, z - padding)
          acc.maxZ = Math.max(acc.maxZ, z + padding)
          return acc
        },
        { minX: Infinity, maxX: -Infinity, minY: Infinity, maxY: -Infinity, minZ: Infinity, maxZ: -Infinity }
      )
      center = {
        x: (bounds.minX + bounds.maxX) / 2,
        y: (bounds.minY + bounds.maxY) / 2,
        z: (bounds.minZ + bounds.maxZ) / 2,
      }
      const spanX = bounds.maxX - bounds.minX
      const spanY = bounds.maxY - bounds.minY
      const spanZ = bounds.maxZ - bounds.minZ
      radius = Math.max(520, Math.max(spanX, spanY, spanZ) * 0.62)
    }
    camera.position.set(center.x + radius * 1.05, center.y + radius * 0.72, center.z + radius * 1.55)
    camera.near = Math.max(0.1, radius / 4000)
    camera.far = Math.max(6000, radius * 8)
    camera.updateProjectionMatrix()
    controls.target.set(center.x, center.y, center.z)
    controls.update()
  }

  function focusCluster(cluster) {
    if (!camera || !controls || !cluster) return
    selectedCluster = Number(cluster.id)
    const x = Number(cluster.x || 0)
    const y = Number(cluster.y || 0) * depthScale
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
    void loadClusterDetail(cluster)
  }

  function setCameraPreset(preset) {
    if (!camera || !controls) return
    const center = controls.target || { x: 0, y: 0, z: 0 }
    const radius = points?.geometry?.boundingSphere?.radius || 1800
    if (preset === 'top') {
      camera.position.set(center.x, center.y + radius * 2.2, center.z + 1)
    } else if (preset === 'side') {
      camera.position.set(center.x + radius * 2.1, center.y + radius * 0.2, center.z)
    } else {
      camera.position.set(center.x + radius * 1.1, center.y + radius * 0.75, center.z + radius * 1.55)
    }
    camera.lookAt(center.x, center.y, center.z)
    controls.update()
    renderNow()
    renderForFrames(20)
  }

  function updateAutoRotate() {
    if (!controls) return
    controls.autoRotate = autoRotate
    controls.autoRotateSpeed = 0.65
    if (!autoRotate && rotateFrame) {
      cancelAnimationFrame(rotateFrame)
      rotateFrame = 0
      return
    }
    if (autoRotate && !rotateFrame) {
      const draw = () => {
        if (!autoRotate || !controls) {
          rotateFrame = 0
          return
        }
        controls.update()
        renderNow()
        rotateFrame = requestAnimationFrame(draw)
      }
      rotateFrame = requestAnimationFrame(draw)
    }
  }

  async function loadNodeDetail(node) {
    const id = node?.work_id || node?.id
    if (!id) return
    nodeDetailLoading = true
    selectedNodeDetail = null
    try {
      const detail = await fetchGraph3DNodeDetail(id)
      if ((selectedNode?.work_id || selectedNode?.id) === id) {
        selectedNodeDetail = detail
      }
    } catch {
      selectedNodeDetail = null
    } finally {
      nodeDetailLoading = false
    }
  }

  async function loadClusterDetail(cluster) {
    const snapshotKey = graphData.manifest?.snapshot_key
    if (!snapshotKey || cluster?.id === undefined || cluster?.id === null) {
      selectedClusterDetail = cluster || null
      return
    }
    selectedClusterDetail = cluster
    try {
      selectedClusterDetail = await fetchGraph3DClusterDetail(snapshotKey, cluster.id)
    } catch {
      selectedClusterDetail = cluster
    }
  }

  async function load3DGraph() {
    graphLoading = true
    graphStatus = 'Building 3D graph snapshot...'
    try {
      const manifest = await fetchGraph3DSnapshot({
        maxNodes,
        relationship,
        status: statusFilter,
        scope,
      })
      const snapshot = await fetchGraph3DSnapshotResources(manifest)
      const edgeCount = Number(snapshot.manifest?.buffers?.edges?.count || Math.floor((snapshot.edgeTriplets?.length || 0) / 3) || 0)
      graphData = {
        manifest,
        nodes: snapshot.nodes || [],
        edges: Array.isArray(snapshot.edgeTriplets) ? snapshot.edgeTriplets : [],
        edgeTriplets: snapshot.edgeTriplets instanceof Uint32Array ? snapshot.edgeTriplets : null,
        positions: snapshot.positions,
        clusters: snapshot.clusters || manifest.clusters || [],
        stats: {
          ...(manifest.stats || {}),
          node_count: Number(manifest.stats?.node_count || snapshot.nodes?.length || 0),
          edge_count: Number(manifest.stats?.edge_count || edgeCount),
        },
      }
      const totalWorks = Number(graphData.stats?.total_work_count || graphData.nodes?.length || 0)
      const loadedNodes = Number(graphData.nodes?.length || 0)
      const limitedNote = graphData.stats?.is_limited && totalWorks > loadedNodes
        ? ` Showing the ${formatNumber(loadedNodes)} most connected works out of ${formatNumber(totalWorks)}.`
        : ''
      graphStatus = `Loaded ${formatNumber(loadedNodes)} nodes and ${formatNumber(graphData.stats?.edge_count || 0)} edges from snapshot.${limitedNote}`
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
    if (persist && nextNode) {
      selectedNode = nextNode
      void loadNodeDetail(nextNode)
    }
  }

  function formatNumber(value) {
    return new Intl.NumberFormat().format(Number(value || 0))
  }

  function compactList(items, fallback = 'unknown') {
    if (!Array.isArray(items) || !items.length) return fallback
    return items.slice(0, 3).map((item) => `${item.label} (${formatNumber(item.count)})`).join(', ')
  }

  function coverageLabel(coverage) {
    const known = Number(coverage?.known || 0)
    const total = Number(coverage?.total || 0)
    if (!total) return 'unknown coverage'
    return `${formatNumber(known)} / ${formatNumber(total)} known`
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
    controls.enableRotate = true
    controls.enablePan = true
    controls.enableZoom = true
    controls.autoRotate = autoRotate
    controls.autoRotateSpeed = 0.65
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
    if (rotateFrame) cancelAnimationFrame(rotateFrame)
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
  $: activeNodeDetail = selectedNode && selectedNodeDetail ? { ...selectedNode, ...selectedNodeDetail } : activeNode
  $: clusters = graphData.clusters || []
  $: topClusters = clusters.slice(0, 16)
  $: selectedClusterData = clusters.find((cluster) => Number(cluster.id) === selectedCluster)
  $: selectedClusterPanel = selectedClusterDetail || selectedClusterData
</script>

<div class="card graph-3d-panel" data-testid="graph-3d-panel">
  <div class="workspace-panel-header graph-3d-header">
    <div class="workspace-panel-title">
      <h2 class="workspace-section-title">3D Graph Explorer</h2>
      <p class="muted">
        Fast WebGL overview of the most connected corpus works. Cluster summaries include inferred science areas and geographic regions when the metadata supports it.
      </p>
    </div>
    <div class="graph-3d-actions">
      <button class="secondary" type="button" on:click={() => setCameraPreset('angle')} disabled={!graphData.nodes?.length}>Angle</button>
      <button class="secondary" type="button" on:click={() => setCameraPreset('top')} disabled={!graphData.nodes?.length}>Top</button>
      <button class="secondary" type="button" on:click={() => setCameraPreset('side')} disabled={!graphData.nodes?.length}>Side</button>
      <button class:active={autoRotate} class="secondary" type="button" on:click={() => { autoRotate = !autoRotate; updateAutoRotate() }} disabled={!graphData.nodes?.length}>
        {autoRotate ? 'Stop rotate' : 'Auto rotate'}
      </button>
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
      <input type="number" min="1000" max="50000" step="1000" bind:value={maxNodes} disabled={graphLoading} />
    </label>
    <label>
      <span class="muted small">Color by</span>
      <select bind:value={colorMode} on:change={() => graphData.nodes?.length && renderGraph()}>
        <option value="cluster">Graph cluster</option>
        <option value="area">Science area</option>
        <option value="region">Region</option>
        <option value="component">Connected component</option>
        <option value="status">Pipeline status</option>
        <option value="degree">Degree</option>
        <option value="year">Year decade</option>
      </select>
    </label>
    <label>
      <span class="muted small">Depth</span>
      <select bind:value={depthScale} on:change={() => graphData.nodes?.length && renderGraph()}>
        <option value={1}>1x</option>
        <option value={1.6}>1.6x</option>
        <option value={2.4}>2.4x</option>
        <option value={3.2}>3.2x</option>
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
        {formatNumber(graphData.stats?.component_count)} connected components. Clusters are citation communities; tiny fragments are bucketed by area, region, and decade.
      </p>
      <p class="muted small">
        Areas: {compactList(graphData.stats?.top_areas)} ({coverageLabel(graphData.stats?.area_coverage)})
      </p>
      <p class="muted small">
        Regions: {compactList(graphData.stats?.top_regions)} ({coverageLabel(graphData.stats?.region_coverage)})
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

      {#if selectedClusterPanel}
        <div class="graph-3d-cluster-detail">
          <span class="eyebrow">Cluster profile</span>
          <strong>{selectedClusterPanel.label}</strong>
          <span class="muted small">{formatNumber(selectedClusterPanel.size)} works / radius {Math.round(selectedClusterPanel.radius || 0)}</span>
          <span class="muted small">Areas: {compactList(selectedClusterPanel.areas)}</span>
          <span class="muted small">Regions: {compactList(selectedClusterPanel.regions)}</span>
          <span class="muted small">Countries: {compactList(selectedClusterPanel.countries, 'unknown countries')}</span>
          <span class="muted small">Types: {compactList(selectedClusterPanel.types)}</span>
          {#if selectedClusterPanel.year_min || selectedClusterPanel.year_max}
            <span class="muted small">Years: {selectedClusterPanel.year_min || '?'}-{selectedClusterPanel.year_max || '?'}</span>
          {/if}
        </div>
      {/if}

      <div class="graph-3d-selected">
        <span class="eyebrow">Selected work</span>
        {#if activeNodeDetail}
          <strong>{activeNodeDetail.title}</strong>
          <span class="muted small">{activeNodeDetail.year || 'Year ?'} / {activeNodeDetail.type || 'Type ?'} / {activeNodeDetail.status}</span>
          <span class="muted small">Area: {activeNodeDetail.area || 'unknown'} / Region: {activeNodeDetail.region || 'unknown'}</span>
          <span class="muted small">Degree: {formatNumber(activeNodeDetail.degree)} / Cluster: {activeNodeDetail.cluster} ({formatNumber(activeNodeDetail.cluster_size)} nodes)</span>
          <span class="muted small">Component: {activeNodeDetail.component} ({formatNumber(activeNodeDetail.component_size)} nodes)</span>
          <span class="muted small">Source: {activeNodeDetail.source || 'unknown'}</span>
          {#if nodeDetailLoading}
            <span class="muted small">Loading metadata...</span>
          {/if}
          {#if selectedNodeDetail?.authors}
            <span class="muted small">Authors: {selectedNodeDetail.authors}</span>
          {/if}
          {#if selectedNodeDetail?.doi || selectedNodeDetail?.openalex_id}
            <span class="muted small">Identifiers: {selectedNodeDetail.doi || selectedNodeDetail.openalex_id}</span>
          {/if}
          {#if selectedNodeDetail?.publisher || selectedNodeDetail?.keywords}
            <span class="muted small">Metadata: {selectedNodeDetail.publisher || selectedNodeDetail.keywords}</span>
          {/if}
        {:else}
          <strong>No node selected</strong>
          <span class="muted small">Hover or click a point in the 3D graph.</span>
        {/if}
      </div>
    </aside>
  </div>
</div>
