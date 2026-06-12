<script>
  import { onDestroy, onMount, tick } from 'svelte'
  import {
    fetchGraph3DSnapshot,
    fetchGraph3DSnapshotResources,
    fetchGraph3DNodeDetail,
    fetchGraph3DClusterDetail,
  } from '../lib/api'
  import { createNodeMaterial, createEdgeMaterial } from '../lib/graphMaterials'

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
  let bridgeLines
  let clusterShells
  let raycaster
  let pointer
  let animationFrame = 0
  let settleFrame = 0
  let rotateFrame = 0
  let flyFrame = 0
  let resizeObserver
  let handlePointerMove
  let handleClick
  let handleContextMenu
  let handleKeydown
  let componentMounted = false
  let initialLoadStarted = false
  let clusterLabelLayer
  let clusterLabels = []
  let graphData = { nodes: [], edges: [], edgeTriplets: null, positions: null, clusters: [], stats: {} }
  let graphStatus = '3D graph not loaded.'
  let graphLoading = false
  let relationship = 'both'
  let statusFilter = 'downloaded'
  let scope = 'all'
  let maxNodes = 10000
  let groupMode = 'cluster'
  let loadedGroupBy = 'field'
  let depthScale = 1.6
  let autoRotate = false
  let selectedNode = null
  let hoveredNode = null
  let selectedCluster = null
  let selectedNodeDetail = null
  let selectedClusterDetail = null
  let nodeDetailLoading = false
  let highlightIndex = null
  let adjacency = null
  let edgeEndpoints = null
  let yearFrom = null
  let yearTo = null
  let visibleNodeCount = 0
  let searchQuery = ''
  let searchResults = []
  let searchTimer = 0
  let fadeStart = 0
  const nodeDetailCache = new Map()
  const clusterDetailCache = new Map()

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

  function colorCss(hex) {
    return `#${Number(hex || 0).toString(16).padStart(6, '0')}`
  }

  const UNKNOWN_FIELD_COLOR = 0x52636a
  let clusterColorById = new Map()

  function isUnknownField(kind) {
    return kind === 'unknown_field' || kind === 'low_signal'
  }

  function hslToHex(h, s, l) {
    const a = s * Math.min(l, 1 - l)
    const channel = (n) => {
      const k = (n + h / 30) % 12
      const value = l - a * Math.max(-1, Math.min(k - 3, 9 - k, 1))
      return Math.round(255 * value)
    }
    return (channel(0) << 16) | (channel(8) << 8) | channel(4)
  }

  // Give every territory (cluster) its own colour. A 12-entry palette collides
  // across the ~26+ groups a dimension can produce, so derive evenly separated
  // hues instead. The golden-angle stride keeps neighbouring territories (packed
  // by size) far apart on the colour wheel; a small lightness wobble separates
  // hues that land close together. "Unknown" territories are grey.
  function buildClusterColors(clusters) {
    const map = new Map()
    let index = 0
    for (const cluster of clusters || []) {
      if (isUnknownField(cluster.kind)) {
        map.set(cluster.id, UNKNOWN_FIELD_COLOR)
        continue
      }
      const hue = (index * 137.508) % 360
      const lightness = 0.54 + ((index % 3) - 1) * 0.07
      map.set(cluster.id, hslToHex(hue, 0.64, lightness))
      index += 1
    }
    clusterColorById = map
  }

  function clusterColor(cluster) {
    if (isUnknownField(cluster?.kind)) return UNKNOWN_FIELD_COLOR
    return clusterColorById.get(cluster?.id) ?? paletteColor(cluster?.field || cluster?.label || cluster?.id)
  }

  function nodeColor(node) {
    if (groupMode === 'degree') {
      const degree = Number(node.degree || 0)
      if (degree > 100) return 0xfff1a8
      if (degree > 30) return 0xf4a340
      if (degree > 10) return 0x3b82f6
      return 0x0f8b8d
    }
    // Every other mode arranges the graph into territories of that dimension, so
    // colour each point by its territory to match its shell and label.
    if (isUnknownField(node.cluster_kind)) return UNKNOWN_FIELD_COLOR
    return clusterColorById.get(node.cluster) ?? paletteColor(node.field || node.cluster || 'unknown')
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

  function nodeSize(degree) {
    return Math.min(14, Math.max(3, 3 + 2.4 * Math.log2(1 + Math.max(0, Number(degree || 0)))))
  }

  function renderGraph() {
    if (!scene || !THREE) return
    disposeObject(points)
    disposeObject(edgeLines)
    disposeObject(bridgeLines)
    disposeObject(clusterShells)
    clusterLabels = []
    hoveredNode = null

    const nodes = graphData.nodes || []
    const edges = graphData.edges || []
    const clusterData = graphData.clusters || []
    const sourcePositions = graphData.positions
    const edgeTriplets = graphData.edgeTriplets
    buildClusterColors(clusterData)
    const nodeIndex = new Map(nodes.map((node, index) => [node.id, index]))
    const positions = new Float32Array(nodes.length * 3)
    const colors = new Float32Array(nodes.length * 3)
    const sizes = new Float32Array(nodes.length)
    const nodeAlphas = new Float32Array(nodes.length).fill(1)
    const color = new THREE.Color()

    nodes.forEach((node, index) => {
      positions[index * 3] = positionValue(sourcePositions, nodes, index, 0)
      positions[index * 3 + 1] = positionValue(sourcePositions, nodes, index, 1) * depthScale
      positions[index * 3 + 2] = positionValue(sourcePositions, nodes, index, 2)
      const degree = Number(node.degree || 0)
      color.setHex(nodeColor(node))
      if (degree > 100) color.lerp(new THREE.Color(0xfff1a8), 0.4)
      colors[index * 3] = color.r
      colors[index * 3 + 1] = color.g
      colors[index * 3 + 2] = color.b
      sizes[index] = nodeSize(degree)
    })

    const pointGeometry = new THREE.BufferGeometry()
    pointGeometry.setAttribute('position', new THREE.BufferAttribute(positions, 3))
    pointGeometry.setAttribute('color', new THREE.BufferAttribute(colors, 3))
    pointGeometry.setAttribute('size', new THREE.BufferAttribute(sizes, 1))
    pointGeometry.setAttribute('alpha', new THREE.BufferAttribute(nodeAlphas, 1))
    pointGeometry.computeBoundingSphere()
    points = new THREE.Points(pointGeometry, createNodeMaterial(THREE, renderer?.getPixelRatio?.() || 1))
    points.userData.nodes = nodes
    scene.add(points)

    clusterShells = new THREE.Group()
    const visibleClusters = clusterData.slice(0, 32)
    for (const cluster of visibleClusters) {
      const radius = Math.max(48, Number(cluster.radius || Math.sqrt(Number(cluster.size || 1)) * 8))
      const geometry = new THREE.SphereGeometry(radius, 24, 12)
      const material = new THREE.MeshBasicMaterial({
        color: clusterColor(cluster),
        transparent: true,
        opacity: isUnknownField(cluster.kind) ? 0.08 : Math.min(0.2, 0.08 + Math.log1p(Number(cluster.size || 1)) * 0.018),
        wireframe: true,
        depthTest: false,
        depthWrite: false,
      })
      const shell = new THREE.Mesh(geometry, material)
      shell.position.set(Number(cluster.x || 0), Number(cluster.y || 0) * depthScale, Number(cluster.z || 0))
      clusterShells.add(shell)
    }
    scene.add(clusterShells)

    clusterLabels = visibleClusters.map((cluster, index) => ({
      id: cluster.id,
      label: cluster.field || cluster.label,
      detail: `${formatNumber(cluster.size)} works / ${cluster.region || 'unknown region'}`,
      color: colorCss(clusterColor(cluster)),
      radius: Math.max(24, Number(cluster.radius || 0)),
      size: Number(cluster.size || 1),
      vector: new THREE.Vector3(
        Number(cluster.x || 0),
        Number(cluster.y || 0) * depthScale + Math.max(62, Number(cluster.radius || 0) * 0.74),
        Number(cluster.z || 0)
      ),
      x: -9999,
      y: -9999,
      opacity: index < 8 ? 1 : 0.85,
      scale: Math.max(0.86, Math.min(1.12, 0.72 + Math.log1p(Number(cluster.size || 1)) * 0.062)),
    }))

    const binaryEdgeCount = edgeTriplets?.length ? Math.floor(edgeTriplets.length / 3) : 0
    const edgeCount = binaryEdgeCount || edges.length
    const edgePositions = new Float32Array(edgeCount * 2 * 3)
    const edgeColors = new Float32Array(edgeCount * 2 * 3)
    const endpointPairs = new Uint32Array(edgeCount * 2)
    let edgeOffset = 0
    let edgeColorOffset = 0
    let appendedEdges = 0
    const appendEdge = (sourceIndex, targetIndex, relationshipCode = 0) => {
      if (sourceIndex === undefined || targetIndex === undefined) return
      if (sourceIndex < 0 || targetIndex < 0 || sourceIndex >= nodes.length || targetIndex >= nodes.length) return
      edgePositions[edgeOffset++] = positions[sourceIndex * 3]
      edgePositions[edgeOffset++] = positions[sourceIndex * 3 + 1]
      edgePositions[edgeOffset++] = positions[sourceIndex * 3 + 2]
      edgePositions[edgeOffset++] = positions[targetIndex * 3]
      edgePositions[edgeOffset++] = positions[targetIndex * 3 + 1]
      edgePositions[edgeOffset++] = positions[targetIndex * 3 + 2]
      endpointPairs[appendedEdges * 2] = sourceIndex
      endpointPairs[appendedEdges * 2 + 1] = targetIndex
      appendedEdges += 1
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
    edgeEndpoints = endpointPairs.slice(0, appendedEdges * 2)
    adjacency = buildAdjacency(edgeEndpoints, nodes.length)
    const lineGeometry = new THREE.BufferGeometry()
    lineGeometry.setAttribute('position', new THREE.BufferAttribute(edgePositions.slice(0, edgeOffset), 3))
    lineGeometry.setAttribute('color', new THREE.BufferAttribute(edgeColors.slice(0, edgeColorOffset), 3))
    lineGeometry.setAttribute('alpha', new THREE.BufferAttribute(new Float32Array(appendedEdges * 2), 1))
    edgeLines = new THREE.LineSegments(lineGeometry, createEdgeMaterial(THREE))
    scene.add(edgeLines)

    const bridgeCandidates = []
    const bridgePositions = []
    const bridgeColors = []
    const bridgeColor = new THREE.Color(0xffcf70)
    const bridgeLimit = 700
    const collectBridge = (sourceIndex, targetIndex) => {
      if (sourceIndex === undefined || targetIndex === undefined) return
      const source = nodes[sourceIndex]
      const target = nodes[targetIndex]
      if (!source || !target) return
      const crossesCluster = Number(source.cluster) !== Number(target.cluster)
      const crossesArea = (source.area || 'unknown area') !== (target.area || 'unknown area')
      if (!crossesCluster && !crossesArea) return
      const sourceDegree = Number(source.degree || 0)
      const targetDegree = Number(target.degree || 0)
      if (!crossesArea && sourceDegree + targetDegree < 24) return
      bridgeCandidates.push({
        sourceIndex,
        targetIndex,
        crossesArea,
        score: sourceDegree + targetDegree + (crossesArea ? 42 : 0) + (crossesCluster ? 12 : 0),
      })
    }
    const appendBridge = ({ sourceIndex, targetIndex, crossesArea }) => {
      bridgePositions.push(
        positions[sourceIndex * 3],
        positions[sourceIndex * 3 + 1],
        positions[sourceIndex * 3 + 2],
        positions[targetIndex * 3],
        positions[targetIndex * 3 + 1],
        positions[targetIndex * 3 + 2]
      )
      const source = nodes[sourceIndex]
      const target = nodes[targetIndex]
      const hex = crossesArea ? paletteColor(`${source.area}->${target.area}`) : 0xffcf70
      bridgeColor.setHex(hex)
      for (let i = 0; i < 2; i += 1) {
        bridgeColors.push(bridgeColor.r, bridgeColor.g, bridgeColor.b)
      }
    }
    if (binaryEdgeCount) {
      for (let i = 0; i < edgeTriplets.length; i += 3) {
        collectBridge(edgeTriplets[i], edgeTriplets[i + 1])
      }
    } else {
      for (const edge of edges) {
        collectBridge(nodeIndex.get(edge.s || edge.source), nodeIndex.get(edge.t || edge.target))
      }
    }
    bridgeCandidates
      .sort((a, b) => b.score - a.score)
      .slice(0, bridgeLimit)
      .forEach(appendBridge)
    if (bridgePositions.length) {
      const bridgeGeometry = new THREE.BufferGeometry()
      bridgeGeometry.setAttribute('position', new THREE.BufferAttribute(new Float32Array(bridgePositions), 3))
      bridgeGeometry.setAttribute('color', new THREE.BufferAttribute(new Float32Array(bridgeColors), 3))
      bridgeLines = new THREE.LineSegments(
        bridgeGeometry,
        new THREE.LineBasicMaterial({
          vertexColors: true,
          transparent: true,
          opacity: 0.36,
          depthTest: false,
          depthWrite: false,
        })
      )
      scene.add(bridgeLines)
    }

    recomputeAlphas()
    fadeStart = performance.now()
    resetCamera()
    renderNow()
    renderForFrames(45)
    requestRender()
  }

  function buildAdjacency(endpoints, nodeCount) {
    const counts = new Uint32Array(nodeCount)
    for (let i = 0; i < endpoints.length; i += 2) {
      counts[endpoints[i]] += 1
      counts[endpoints[i + 1]] += 1
    }
    const offsets = new Uint32Array(nodeCount + 1)
    for (let i = 0; i < nodeCount; i += 1) {
      offsets[i + 1] = offsets[i] + counts[i]
    }
    const neighbors = new Uint32Array(offsets[nodeCount])
    const cursor = offsets.slice(0, nodeCount)
    for (let i = 0; i < endpoints.length; i += 2) {
      const source = endpoints[i]
      const target = endpoints[i + 1]
      neighbors[cursor[source]++] = target
      neighbors[cursor[target]++] = source
    }
    return { offsets, neighbors }
  }

  function passesYearFilter(node) {
    const year = Number(node.year || 0)
    if (!year) return true
    if (yearFrom && year < Number(yearFrom)) return false
    if (yearTo && year > Number(yearTo)) return false
    return true
  }

  function highlightNeighborhood(index) {
    const set = new Set([index])
    if (adjacency) {
      for (let i = adjacency.offsets[index]; i < adjacency.offsets[index + 1]; i += 1) {
        set.add(adjacency.neighbors[i])
      }
    }
    return set
  }

  function recomputeAlphas() {
    if (!points) return
    const nodes = graphData.nodes || []
    const nodeAlphaAttr = points.geometry.getAttribute('alpha')
    if (!nodeAlphaAttr) return
    const highlightSet = highlightIndex === null ? null : highlightNeighborhood(highlightIndex)
    let visible = 0
    for (let i = 0; i < nodes.length; i += 1) {
      let alpha = 0
      if (passesYearFilter(nodes[i])) {
        visible += 1
        alpha = highlightSet ? (highlightSet.has(i) ? 1 : 0.06) : 1
      }
      nodeAlphaAttr.array[i] = alpha
    }
    nodeAlphaAttr.needsUpdate = true
    visibleNodeCount = visible

    if (edgeLines && edgeEndpoints) {
      const edgeAlphaAttr = edgeLines.geometry.getAttribute('alpha')
      const totalEdges = edgeEndpoints.length / 2
      const lodActive = totalEdges > 60000
      const baseAlpha = lodActive ? 0.06 : 0.12
      for (let edge = 0; edge < totalEdges; edge += 1) {
        const source = edgeEndpoints[edge * 2]
        const target = edgeEndpoints[edge * 2 + 1]
        let alpha = 0
        if (nodeAlphaAttr.array[source] > 0 && nodeAlphaAttr.array[target] > 0) {
          if (highlightIndex !== null) {
            alpha = source === highlightIndex || target === highlightIndex ? 0.85 : 0.02
          } else if (
            lodActive
            && Number(nodes[source]?.degree || 0) <= 1
            && Number(nodes[target]?.degree || 0) <= 1
          ) {
            alpha = 0
          } else {
            alpha = baseAlpha
          }
        }
        edgeAlphaAttr.array[edge * 2] = alpha
        edgeAlphaAttr.array[edge * 2 + 1] = alpha
      }
      edgeAlphaAttr.needsUpdate = true
    }
    requestRender()
  }

  function applyHighlight(index) {
    highlightIndex = index
    recomputeAlphas()
  }

  function resetCamera() {
    if (!camera || !controls) return
    let center = points?.geometry?.boundingSphere?.center || { x: 0, y: 0, z: 0 }
    let radius = points?.geometry?.boundingSphere?.radius || 1600
    const framingClusters = (graphData.clusters || clusters).slice(0, 24)
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

  function flyTo({ target, position, duration = 700 }) {
    if (!camera || !controls) return
    if (flyFrame) {
      cancelAnimationFrame(flyFrame)
      flyFrame = 0
    }
    const startPosition = camera.position.clone()
    const startTarget = controls.target.clone()
    const endPosition = position.clone()
    const endTarget = target.clone()
    const startedAt = performance.now()
    const ease = (t) => (t < 0.5 ? 4 * t * t * t : 1 - Math.pow(-2 * t + 2, 3) / 2)
    const step = () => {
      const progress = Math.min(1, (performance.now() - startedAt) / duration)
      const eased = ease(progress)
      camera.position.lerpVectors(startPosition, endPosition, eased)
      controls.target.lerpVectors(startTarget, endTarget, eased)
      controls.update()
      renderNow()
      flyFrame = progress < 1 ? requestAnimationFrame(step) : 0
    }
    flyFrame = requestAnimationFrame(step)
  }

  function focusCluster(cluster) {
    if (!camera || !controls || !cluster) return
    selectedCluster = Number(cluster.id)
    const x = Number(cluster.x || 0)
    const y = Number(cluster.y || 0) * depthScale
    const z = Number(cluster.z || 0)
    const radius = Math.max(280, Math.min(2200, 180 + Math.sqrt(Number(cluster.size || 1)) * 34))
    const visualRadius = Math.max(radius, Number(cluster.radius || 0) * 3.2)
    camera.near = Math.max(0.1, visualRadius / 2000)
    camera.far = Math.max(6000, visualRadius * 12)
    camera.updateProjectionMatrix()
    flyTo({
      target: new THREE.Vector3(x, y, z),
      position: new THREE.Vector3(x + visualRadius * 0.95, y + visualRadius * 0.35, z + visualRadius * 1.55),
    })
    void loadClusterDetail(cluster)
  }

  function focusNode(index) {
    if (!camera || !controls || !points) return
    const positionAttr = points.geometry.getAttribute('position')
    if (!positionAttr || index === null || index === undefined || index * 3 >= positionAttr.array.length) return
    const target = new THREE.Vector3(
      positionAttr.array[index * 3],
      positionAttr.array[index * 3 + 1],
      positionAttr.array[index * 3 + 2]
    )
    const direction = camera.position.clone().sub(controls.target)
    if (direction.lengthSq() < 1) direction.set(1, 0.6, 1.4)
    direction.normalize().multiplyScalar(220)
    flyTo({ target, position: target.clone().add(direction) })
  }

  function setCameraPreset(preset) {
    if (!camera || !controls) return
    const center = controls.target.clone()
    const radius = points?.geometry?.boundingSphere?.radius || 1800
    let position
    if (preset === 'top') {
      position = new THREE.Vector3(center.x, center.y + radius * 2.2, center.z + 1)
    } else if (preset === 'side') {
      position = new THREE.Vector3(center.x + radius * 2.1, center.y + radius * 0.2, center.z)
    } else {
      position = new THREE.Vector3(center.x + radius * 1.1, center.y + radius * 0.75, center.z + radius * 1.55)
    }
    flyTo({ target: center, position })
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
    if (nodeDetailCache.has(id)) {
      selectedNodeDetail = nodeDetailCache.get(id)
      nodeDetailLoading = false
      return
    }
    nodeDetailLoading = true
    selectedNodeDetail = null
    try {
      const detail = await fetchGraph3DNodeDetail(id)
      nodeDetailCache.set(id, detail)
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
    const cacheKey = `${snapshotKey}:${cluster.id}`
    if (clusterDetailCache.has(cacheKey)) {
      selectedClusterDetail = clusterDetailCache.get(cacheKey)
      return
    }
    selectedClusterDetail = cluster
    try {
      selectedClusterDetail = await fetchGraph3DClusterDetail(snapshotKey, cluster.id)
      clusterDetailCache.set(cacheKey, selectedClusterDetail)
    } catch {
      selectedClusterDetail = cluster
    }
  }

  function runSearch() {
    const query = searchQuery.trim().toLowerCase()
    if (query.length < 2) {
      searchResults = []
      return
    }
    const nodes = graphData.nodes || []
    const results = []
    for (let index = 0; index < nodes.length && results.length < 12; index += 1) {
      const node = nodes[index]
      const haystack = `${node.title || ''} ${node.authors || ''} ${node.year || ''}`.toLowerCase()
      if (haystack.includes(query)) {
        results.push({ index, title: node.title || 'Untitled', year: node.year, authors: node.authors || '' })
      }
    }
    searchResults = results
  }

  function onSearchInput() {
    if (searchTimer) clearTimeout(searchTimer)
    searchTimer = setTimeout(() => {
      searchTimer = 0
      runSearch()
    }, 200)
  }

  function selectSearchResult(result) {
    const node = (graphData.nodes || [])[result.index]
    if (!node) return
    searchResults = []
    searchQuery = result.title
    selectedNode = node
    applyHighlight(result.index)
    focusNode(result.index)
    void loadNodeDetail(node)
  }

  function clearSelection() {
    selectedNode = null
    selectedNodeDetail = null
    hoveredNode = null
    if (highlightIndex !== null) applyHighlight(null)
  }

  function onYearFilterChange() {
    recomputeAlphas()
  }

  // Map a "Group by" selection to the server-side layout dimension. 'degree' is
  // a colour-only overlay (no re-arrangement), so it has no grouping dimension.
  function groupByForMode(mode) {
    if (mode === 'degree') return null
    return mode === 'cluster' ? 'field' : mode
  }

  function onGroupModeChange() {
    if (!graphData.nodes?.length) return
    const target = groupByForMode(groupMode)
    // A new grouping dimension re-arranges the graph (server re-layout); same
    // dimension or the degree overlay just recolours the current arrangement.
    if (target && target !== loadedGroupBy) {
      void load3DGraph()
    } else {
      renderGraph()
    }
  }

  async function load3DGraph() {
    graphLoading = true
    graphStatus = 'Building 3D graph snapshot...'
    highlightIndex = null
    selectedNode = null
    selectedNodeDetail = null
    selectedCluster = null
    selectedClusterDetail = null
    searchResults = []
    const groupBy = groupByForMode(groupMode) || loadedGroupBy || 'field'
    try {
      const manifest = await fetchGraph3DSnapshot({
        maxNodes,
        relationship,
        status: statusFilter,
        scope,
        groupBy,
      })
      loadedGroupBy = groupBy
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
      await tick()
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
    if (fadeStart) {
      const fade = Math.min(1, (performance.now() - fadeStart) / 400)
      if (points?.material?.uniforms?.uFade) points.material.uniforms.uFade.value = fade
      if (edgeLines?.material?.uniforms?.uFade) edgeLines.material.uniforms.uFade.value = fade
      if (fade >= 1) fadeStart = 0
    }
    renderer.render(scene, camera)
    updateClusterLabelPositions()
  }

  function updateClusterLabelPositions() {
    if (!camera || !renderer || !clusterLabels.length) return
    const rect = renderer.domElement.getBoundingClientRect()
    const placed = []
    const labelOffsets = [
      [0, 0],
      [-180, 38],
      [180, 38],
      [-160, -42],
      [160, -42],
      [0, 76],
    ]
    const cameraDistance = controls ? camera.position.distanceTo(controls.target) : 2600
    const maxLabels = cameraDistance < 1200 ? 20 : cameraDistance < 2600 ? 14 : 8
    const ranked = clusterLabels
      .map((label, index) => ({
        label,
        index,
        score: (label.radius * Math.log1p(label.size)) / Math.max(1, camera.position.distanceTo(label.vector)),
      }))
      .sort((a, b) => b.score - a.score)
    const updates = new Array(clusterLabels.length)
    let visibleCount = 0
    for (const { label, index } of ranked) {
      const projected = label.vector.clone().project(camera)
      const baseX = ((projected.x + 1) / 2) * rect.width
      const baseY = ((-projected.y + 1) / 2) * rect.height
      const width = 184 * Number(label.scale || 1)
      const height = 44 * Number(label.scale || 1)
      let x = baseX
      let y = baseY
      let visible = false
      if (visibleCount < maxLabels && projected.z > -1 && projected.z < 1) {
        for (const [offsetX, offsetY] of labelOffsets) {
          const candidateX = Math.min(rect.width - width / 2 - 12, Math.max(width / 2 + 12, baseX + offsetX))
          const candidateY = Math.min(rect.height - height / 2 - 12, Math.max(48 + height / 2, baseY + offsetY))
          const overlaps = placed.some((box) => (
            Math.abs(box.x - candidateX) < (box.width + width) * 0.48
            && Math.abs(box.y - candidateY) < (box.height + height) * 0.68
          ))
          if (!overlaps) {
            x = candidateX
            y = candidateY
            visible = true
            visibleCount += 1
            placed.push({ x, y, width, height })
            break
          }
        }
      }
      updates[index] = { ...label, x, y, visible }
    }
    clusterLabels = updates
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
    const cameraDistance = controls ? camera.position.distanceTo(controls.target) : 2000
    raycaster.params.Points.threshold = Math.min(24, Math.max(2, cameraDistance * 0.005))
    const alphaArray = points.geometry.getAttribute('alpha')?.array
    const hit = raycaster
      .intersectObject(points, false)
      .find((candidate) => !alphaArray || alphaArray[candidate.index] > 0.05)
    const nextNode = hit ? points.userData.nodes?.[hit.index] : null
    hoveredNode = nextNode
    if (!persist) return
    if (nextNode) {
      selectedNode = nextNode
      applyHighlight(hit.index)
      void loadNodeDetail(nextNode)
    } else {
      clearSelection()
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
    controls.screenSpacePanning = true
    controls.mouseButtons = {
      LEFT: THREE.MOUSE.ROTATE,
      MIDDLE: THREE.MOUSE.DOLLY,
      RIGHT: THREE.MOUSE.PAN,
    }
    controls.touches = {
      ONE: THREE.TOUCH.ROTATE,
      TWO: THREE.TOUCH.DOLLY_PAN,
    }
    controls.autoRotate = autoRotate
    controls.autoRotateSpeed = 0.65
    controls.addEventListener('change', requestRender)

    raycaster = new THREE.Raycaster()
    handlePointerMove = (event) => pickNode(event, false)
    handleClick = (event) => pickNode(event, true)
    handleContextMenu = (event) => event.preventDefault()
    handleKeydown = (event) => {
      if (event.key === 'Escape') {
        searchResults = []
        clearSelection()
      }
    }
    renderer.domElement.addEventListener('pointermove', handlePointerMove)
    renderer.domElement.addEventListener('click', handleClick)
    renderer.domElement.addEventListener('contextmenu', handleContextMenu)
    window.addEventListener('keydown', handleKeydown)

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
    if (flyFrame) cancelAnimationFrame(flyFrame)
    if (searchTimer) clearTimeout(searchTimer)
    resizeObserver?.disconnect()
    controls?.removeEventListener('change', requestRender)
    if (handlePointerMove) renderer?.domElement?.removeEventListener('pointermove', handlePointerMove)
    if (handleClick) renderer?.domElement?.removeEventListener('click', handleClick)
    if (handleContextMenu) renderer?.domElement?.removeEventListener('contextmenu', handleContextMenu)
    if (handleKeydown) window.removeEventListener('keydown', handleKeydown)
    controls?.dispose()
    disposeObject(points)
    disposeObject(edgeLines)
    disposeObject(bridgeLines)
    disposeObject(clusterShells)
    renderer?.dispose()
    renderer?.domElement?.remove()
  })

  $: activeNode = selectedNode || hoveredNode
  $: activeNodeDetail = selectedNode && selectedNodeDetail ? { ...selectedNode, ...selectedNodeDetail } : activeNode
  $: clusters = graphData.clusters || []
  $: selectedClusterData = clusters.find((cluster) => Number(cluster.id) === selectedCluster)
  $: selectedClusterPanel = selectedClusterDetail || selectedClusterData
</script>

<div class="card graph-3d-panel" data-testid="graph-3d-panel">
  <div class="workspace-panel-header graph-3d-header">
    <div class="workspace-panel-title">
      <h2 class="workspace-section-title">3D Graph Explorer</h2>
      <p class="muted">
        Fast WebGL overview of downloaded, metadata-ready works across the database. Choose a "Group by" dimension to re-arrange works into territories (academic field by default), with citation structure laid out inside each territory.
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
      <span class="muted small">Relationship</span>
      <select bind:value={relationship} disabled={graphLoading}>
        <option value="both">References + cited by</option>
        <option value="references">References only</option>
        <option value="cited_by">Cited by only</option>
      </select>
    </label>
    <label>
      <span class="muted small">Works shown</span>
      <select bind:value={maxNodes} disabled={graphLoading}>
        <option value={10000}>Top 10,000 (fastest)</option>
        <option value={25000}>Top 25,000</option>
        <option value={50000}>Top 50,000</option>
        <option value={0}>All works</option>
      </select>
    </label>
    <label>
      <span class="muted small">Group by</span>
      <select bind:value={groupMode} on:change={onGroupModeChange} disabled={graphLoading}>
        <option value="cluster">Academic field</option>
        <option value="source_path">Search path</option>
        <option value="type">Work type</option>
        <option value="region">Region</option>
        <option value="year">Year decade</option>
        <option value="component">Connected component</option>
        <option value="degree">Degree (colour only)</option>
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
    <label>
      <span class="muted small">Year from</span>
      <input type="number" data-testid="graph-3d-year-from" placeholder="any" bind:value={yearFrom} on:input={onYearFilterChange} disabled={!graphData.nodes?.length} />
    </label>
    <label>
      <span class="muted small">Year to</span>
      <input type="number" data-testid="graph-3d-year-to" placeholder="any" bind:value={yearTo} on:input={onYearFilterChange} disabled={!graphData.nodes?.length} />
    </label>
    <label class="graph-3d-search">
      <span class="muted small">Search</span>
      <input
        type="search"
        data-testid="graph-3d-search"
        placeholder="Title, author, year"
        bind:value={searchQuery}
        on:input={onSearchInput}
        disabled={!graphData.nodes?.length}
      />
      {#if searchResults.length}
        <ul class="graph-3d-search-results" data-testid="graph-3d-search-results">
          {#each searchResults as result (result.index)}
            <li>
              <button type="button" on:click={() => selectSearchResult(result)}>
                <span>{result.title}</span>
                <small>{result.authors ? `${result.authors} / ` : ''}{result.year || 'year ?'}</small>
              </button>
            </li>
          {/each}
        </ul>
      {/if}
    </label>
  </div>

  <div class="graph-3d-layout">
    <div class="graph-3d-canvas" bind:this={container} aria-label="3D graph visualization" role="application">
      <div class="graph-3d-map-key" aria-hidden="true">
        <span><i class="territory"></i>Territories</span>
        <span><i class="bridge"></i>Bridge links</span>
        <span><i class="canon"></i>Hubs (size = citations)</span>
      </div>
      <div class="graph-3d-label-layer" bind:this={clusterLabelLayer}>
        {#each clusterLabels as label (label.id)}
          {#if label.visible}
            <button
              type="button"
              class="graph-3d-cluster-label"
              style={`--label-x:${label.x}px;--label-y:${label.y}px;--label-color:${label.color};--label-opacity:${label.opacity};--label-scale:${label.scale}`}
              on:click={() => focusCluster(clusters.find((cluster) => Number(cluster.id) === Number(label.id)))}
            >
              <span>{label.label}</span>
              <small>{label.detail}</small>
            </button>
          {/if}
        {/each}
      </div>
    </div>
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
      {#if (yearFrom || yearTo) && graphData.nodes?.length}
        <p class="muted small" data-testid="graph-3d-filter-status">
          Showing {formatNumber(visibleNodeCount)} of {formatNumber(graphData.nodes.length)} nodes after year filter.
        </p>
      {/if}
      <p class="muted small">
        {formatNumber(graphData.stats?.component_count)} connected components. Territories follow the selected "Group by" dimension; oversized academic fields are split by subfield, and works with no value for the dimension sit in a grey "Unknown" territory.
      </p>
      <p class="muted small">
        Areas: {compactList(graphData.stats?.top_areas)} ({coverageLabel(graphData.stats?.area_coverage)})
      </p>
      <p class="muted small">
        Regions: {compactList(graphData.stats?.top_regions)} ({coverageLabel(graphData.stats?.region_coverage)})
      </p>

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
          <span class="muted small">Field: {activeNodeDetail.field || activeNodeDetail.area || 'unknown'} / Region: {activeNodeDetail.region || 'unknown'}</span>
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
