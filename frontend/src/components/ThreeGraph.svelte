<script>
  import { onDestroy, onMount, tick } from 'svelte'
  import {
    fetchGraph3DSnapshot,
    fetchGraph3DSnapshotResources,
    fetchGraph3DNodeDetail,
    fetchGraph3DClusterDetail,
  } from '../lib/api'
  import { createNodeQuadMaterial, createEdgeMaterial } from '../lib/graphMaterials'
  import { chooseEdgeSegments } from '../lib/graphGeometry'

  export let autoLoad = true

  let THREE
  let OrbitControls
  let container
  let renderer
  let scene
  let camera
  let controls
  let points
  let nodeQuads
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
  let handlePointerLeave
  let handleDownCapture
  let handleMoveCapture
  let handleUpCapture
  let handleContextMenu
  let handleKeydown
  let pendingDown = null
  let componentMounted = false
  let initialLoadStarted = false
  let clusterLabelLayer
  let clusterLabels = []
  let clusterLegend = []
  let hubLabels = []
  let tooltip = { visible: false, x: 0, y: 0, node: null }
  let halo = { visible: false, x: 0, y: 0 }
  let selectedIndex = null
  let isolatedCluster = null
  let pathStart = null
  let pathEnd = null
  let pathInfo = null
  let pathNodeSet = null
  let pathLine = null
  let selectionEdges = null
  let graphData = { nodes: [], edges: [], edgeTriplets: null, positions: null, clusters: [], stats: {} }
  let graphStatus = '3D graph not loaded.'
  let graphLoading = false
  let relationship = 'both'
  let statusFilter = 'downloaded'
  let scope = 'all'
  let maxNodes = 25000
  let groupMode = 'cluster'
  let loadedGroupBy = 'field'
  let depthScale = 1.6
  let autoRotate = false
  let cameraPreset = null
  let selectedNode = null
  let hoveredNode = null
  let selectedCluster = null
  let selectedNodeDetail = null
  let selectedClusterDetail = null
  let nodeDetailLoading = false
  let highlightIndex = null
  let adjacency = null
  let edgeEndpoints = null
  let edgeVertexStride = 2
  let yearFrom = null
  let yearTo = null
  let visibleNodeCount = 0
  let searchQuery = ''
  let searchResults = []
  let searchTimer = 0
  let yearFilterTimer = 0
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
  // Directional edge gradient: dim slate at the citing end → bright cyan at the
  // cited end, so an edge's colour shows which way the citation points.
  const EDGE_DIM_R = 0.20, EDGE_DIM_G = 0.27, EDGE_DIM_B = 0.30
  const EDGE_BRIGHT_R = 0.62, EDGE_BRIGHT_G = 0.86, EDGE_BRIGHT_B = 0.89
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

  function truncate(text, max) {
    const value = String(text || '').trim()
    return value.length > max ? `${value.slice(0, max - 1)}…` : value
  }

  function renderGraph() {
    if (!scene || !THREE) return
    disposeObject(points)
    disposeObject(nodeQuads)
    disposeObject(edgeLines)
    disposeObject(bridgeLines)
    disposeObject(clusterShells)
    // Node indices and positions change on re-render, so any path is stale.
    disposePathLine()
    disposeSelectionEdges()
    pathStart = null
    pathEnd = null
    pathInfo = null
    pathNodeSet = null
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

    // `points` is NOT added to the scene — it exists only as the raycast target
    // for hover/click picking. Nodes are rendered as instanced quads below to
    // avoid GL_POINTS driver bugs (intermittent dropping during camera motion).
    const pointGeometry = new THREE.BufferGeometry()
    pointGeometry.setAttribute('position', new THREE.BufferAttribute(positions, 3))
    pointGeometry.setAttribute('alpha', new THREE.BufferAttribute(nodeAlphas, 1))
    pointGeometry.computeBoundingSphere()
    // Plain material — this object is never rendered, only raycast for picking.
    points = new THREE.Points(pointGeometry, new THREE.PointsMaterial())
    points.userData.nodes = nodes
    points.frustumCulled = false

    // Visible nodes: one instanced camera-facing quad per node, sharing the same
    // position/colour/size/alpha buffers as the pick geometry.
    const quadGeometry = new THREE.InstancedBufferGeometry()
    const baseQuad = new THREE.PlaneGeometry(1, 1)
    quadGeometry.index = baseQuad.index
    quadGeometry.setAttribute('position', baseQuad.attributes.position)
    quadGeometry.setAttribute('aPosition', new THREE.InstancedBufferAttribute(positions, 3))
    quadGeometry.setAttribute('aColor', new THREE.InstancedBufferAttribute(colors, 3))
    quadGeometry.setAttribute('aSize', new THREE.InstancedBufferAttribute(sizes, 1))
    quadGeometry.setAttribute('aAlpha', new THREE.InstancedBufferAttribute(nodeAlphas, 1))
    quadGeometry.instanceCount = nodes.length
    quadGeometry.boundingSphere = pointGeometry.boundingSphere
    const dbSize = renderer?.getDrawingBufferSize
      ? renderer.getDrawingBufferSize(new THREE.Vector2())
      : { x: 1, y: 1 }
    nodeQuads = new THREE.Mesh(
      quadGeometry,
      createNodeQuadMaterial(THREE, renderer?.getPixelRatio?.() || 1, dbSize.x, dbSize.y)
    )
    nodeQuads.renderOrder = 3 // draw nodes on top of edges/shells
    nodeQuads.frustumCulled = false
    scene.add(nodeQuads)

    // Measure how far each cluster's nodes actually spread from its exported
    // centre. The exported radius understates the force-layout spread, so the
    // shell sphere was smaller than its own nodes — leaving them visibly
    // outside the cluster when zoomed in. Size the shell to contain ~92% of
    // its nodes instead.
    const clusterCenterById = new Map()
    for (const cluster of clusterData) {
      clusterCenterById.set(Number(cluster.id), {
        x: Number(cluster.x || 0),
        y: Number(cluster.y || 0) * depthScale,
        z: Number(cluster.z || 0),
      })
    }
    const clusterRadiusSamples = new Map()
    for (let i = 0; i < nodes.length; i += 1) {
      const id = Number(nodes[i].cluster)
      const c = clusterCenterById.get(id)
      if (!c) continue
      const dx = positions[i * 3] - c.x
      const dy = positions[i * 3 + 1] - c.y
      const dz = positions[i * 3 + 2] - c.z
      let arr = clusterRadiusSamples.get(id)
      if (!arr) { arr = []; clusterRadiusSamples.set(id, arr) }
      arr.push(Math.sqrt(dx * dx + dy * dy + dz * dz))
    }
    const shellRadiusById = new Map()
    const shellRadiusFor = (cluster) => {
      const exported = Math.max(48, Number(cluster.radius || Math.sqrt(Number(cluster.size || 1)) * 8))
      const samples = clusterRadiusSamples.get(Number(cluster.id))
      let contain = 0
      if (samples && samples.length) {
        samples.sort((a, b) => a - b)
        contain = samples[Math.min(samples.length - 1, Math.floor((samples.length - 1) * 0.92))] || 0
      }
      const radius = Math.max(exported, contain * 1.05)
      shellRadiusById.set(Number(cluster.id), radius)
      return radius
    }

    clusterShells = new THREE.Group()
    const visibleClusters = clusterData.slice(0, 32)
    for (const cluster of visibleClusters) {
      const radius = shellRadiusFor(cluster)
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

    clusterLabels = visibleClusters.map((cluster, index) => {
      const shellRadius = shellRadiusById.get(Number(cluster.id)) || Math.max(24, Number(cluster.radius || 0))
      return {
      id: cluster.id,
      label: cluster.field || cluster.label,
      detail: `${formatNumber(cluster.size)} works / ${cluster.region || 'unknown region'}`,
      color: colorCss(clusterColor(cluster)),
      radius: Math.max(24, shellRadius),
      size: Number(cluster.size || 1),
      vector: new THREE.Vector3(
        Number(cluster.x || 0),
        Number(cluster.y || 0) * depthScale + Math.max(62, shellRadius * 0.74),
        Number(cluster.z || 0)
      ),
      x: -9999,
      y: -9999,
      opacity: index < 8 ? 1 : 0.85,
      scale: Math.max(0.86, Math.min(1.12, 0.72 + Math.log1p(Number(cluster.size || 1)) * 0.062)),
      }
    })

    // Stable legend source: clusterLabels is rewritten every rendered frame by
    // the overlay positioner, so the legend reads from this instead to avoid
    // re-rendering 60×/s during auto-rotate.
    clusterLegend = visibleClusters.map((cluster) => ({
      id: cluster.id,
      label: cluster.field || cluster.label,
      color: colorCss(clusterColor(cluster)),
      size: Number(cluster.size || 1),
    }))

    // Always-on labels for the most-cited works ("hubs") so the map reads as a
    // citation landscape before any interaction. Highest-degree nodes only; the
    // overlay placer caps how many show by zoom and avoids cluster labels.
    hubLabels = nodes
      .map((node, index) => ({ node, index, degree: Number(node.degree || 0) }))
      .filter((entry) => entry.degree > 0)
      .sort((a, b) => b.degree - a.degree)
      .slice(0, 16)
      .map(({ node, index }) => ({
        index,
        label: truncate(node.title || node.label || 'Untitled', 32),
        degree: Number(node.degree || 0),
        vector: new THREE.Vector3(
          positions[index * 3],
          positions[index * 3 + 1],
          positions[index * 3 + 2]
        ),
        x: -9999,
        y: -9999,
        visible: false,
      }))

    // Hierarchical edge bundling: route every edge through its endpoints'
    // cluster centroids with a cubic Bézier, so edges that share the same two
    // territories converge onto a common trunk instead of forming a hairball.
    // Cheaper segment counts kick in as the edge population grows so the vertex
    // buffer stays bounded on huge ("All works") graphs.
    // Centroid per cluster for bundling. The snapshot only exports summaries
    // (with x/y/z) for the top-N clusters, so derive every cluster's centroid
    // from its node positions first, then override with the exported centroid
    // when present. Otherwise nodes in un-summarised clusters would all fall
    // back to the graph centre and over-bundle there.
    const clusterSums = new Map()
    for (let i = 0; i < nodes.length; i += 1) {
      const key = Number(nodes[i].cluster)
      if (Number.isNaN(key)) continue
      let acc = clusterSums.get(key)
      if (!acc) {
        acc = { x: 0, y: 0, z: 0, n: 0 }
        clusterSums.set(key, acc)
      }
      acc.x += positions[i * 3]
      acc.y += positions[i * 3 + 1]
      acc.z += positions[i * 3 + 2]
      acc.n += 1
    }
    const clusterCentroids = new Map()
    for (const [key, acc] of clusterSums) {
      clusterCentroids.set(key, { x: acc.x / acc.n, y: acc.y / acc.n, z: acc.z / acc.n })
    }
    for (const cluster of clusterData) {
      clusterCentroids.set(Number(cluster.id), {
        x: Number(cluster.x || 0),
        y: Number(cluster.y || 0) * depthScale,
        z: Number(cluster.z || 0),
      })
    }
    const graphCenter = pointGeometry.boundingSphere?.center
      ? { x: pointGeometry.boundingSphere.center.x, y: pointGeometry.boundingSphere.center.y, z: pointGeometry.boundingSphere.center.z }
      : { x: 0, y: 0, z: 0 }
    const BUNDLE_STRENGTH = 0.78

    // Collect every valid edge once. Adjacency (search / neighbourhood
    // highlight / path-finder) is built from the FULL set so connectivity is
    // complete, but only a curated subset becomes curve geometry: drawing all
    // ~437k edges is slow and bundles them into an indistinct mass. Rendering
    // the highest-degree edges keeps the major trunks while thinning the rest.
    const binaryEdgeCount = edgeTriplets?.length ? Math.floor(edgeTriplets.length / 3) : 0
    const rawCount = binaryEdgeCount || edges.length
    const srcArr = new Uint32Array(rawCount)
    const tgtArr = new Uint32Array(rawCount)
    const relArr = new Uint8Array(rawCount)
    let validCount = 0
    const pushEdge = (s, t, r) => {
      if (s === undefined || t === undefined) return
      if (s < 0 || t < 0 || s >= nodes.length || t >= nodes.length) return
      srcArr[validCount] = s
      tgtArr[validCount] = t
      relArr[validCount] = r
      validCount += 1
    }
    if (binaryEdgeCount) {
      for (let i = 0; i + 2 < edgeTriplets.length; i += 3) pushEdge(edgeTriplets[i], edgeTriplets[i + 1], edgeTriplets[i + 2])
    } else {
      for (const edge of edges) {
        pushEdge(
          nodeIndex.get(edge.s || edge.source),
          nodeIndex.get(edge.t || edge.target),
          edge.r === 'cited_by' || edge.relationship_type === 'cited_by' ? 1 : 0
        )
      }
    }

    const fullEndpoints = new Uint32Array(validCount * 2)
    for (let e = 0; e < validCount; e += 1) {
      fullEndpoints[e * 2] = srcArr[e]
      fullEndpoints[e * 2 + 1] = tgtArr[e]
    }
    adjacency = buildAdjacency(fullEndpoints, nodes.length)

    // Render every edge (no curation). Each is a directional colour gradient
    // (dim at the citing end → bright at the cited end), so you can see which
    // way a citation points.
    const renderOrder = null
    const renderCount = validCount

    const edgeSegments = chooseEdgeSegments(renderCount)
    edgeVertexStride = edgeSegments * 2
    const edgePositions = new Float32Array(renderCount * edgeVertexStride * 3)
    const edgeColors = new Float32Array(renderCount * edgeVertexStride * 3)
    const endpointPairs = new Uint32Array(renderCount * 2)
    let edgeOffset = 0
    let edgeColorOffset = 0
    let appendedEdges = 0
    const appendEdge = (sourceIndex, targetIndex, relationshipCode = 0) => {
      const sx = positions[sourceIndex * 3]
      const sy = positions[sourceIndex * 3 + 1]
      const sz = positions[sourceIndex * 3 + 2]
      const tx = positions[targetIndex * 3]
      const ty = positions[targetIndex * 3 + 1]
      const tz = positions[targetIndex * 3 + 2]
      // Control points: pull each endpoint toward its own cluster centroid (or
      // the graph centre when the cluster is unknown), so same-cluster edges bow
      // through that centroid and cross-cluster edges follow the centroid trunk.
      const ca = clusterCentroids.get(Number(nodes[sourceIndex].cluster)) || graphCenter
      const cb = clusterCentroids.get(Number(nodes[targetIndex].cluster)) || graphCenter
      const c1x = sx + (ca.x - sx) * BUNDLE_STRENGTH
      const c1y = sy + (ca.y - sy) * BUNDLE_STRENGTH
      const c1z = sz + (ca.z - sz) * BUNDLE_STRENGTH
      const c2x = tx + (cb.x - tx) * BUNDLE_STRENGTH
      const c2y = ty + (cb.y - ty) * BUNDLE_STRENGTH
      const c2z = tz + (cb.z - tz) * BUNDLE_STRENGTH
      // Direction: colour flows from the citing end (dim) to the cited end
      // (bright). source references target (rel 0) -> bright at target (t=1);
      // source cited_by target (rel 1) -> target cites source -> bright at the
      // source (t=0). `brightAtT1` is 1 when t=1 (target) is the cited end.
      const brightAtT1 = relationshipCode === 1 ? 0 : 1
      const writeColor = (t) => {
        const f = brightAtT1 ? t : 1 - t // 0 at citing end, 1 at cited end
        edgeColors[edgeColorOffset++] = EDGE_DIM_R + (EDGE_BRIGHT_R - EDGE_DIM_R) * f
        edgeColors[edgeColorOffset++] = EDGE_DIM_G + (EDGE_BRIGHT_G - EDGE_DIM_G) * f
        edgeColors[edgeColorOffset++] = EDGE_DIM_B + (EDGE_BRIGHT_B - EDGE_DIM_B) * f
      }
      let px = sx
      let py = sy
      let pz = sz
      let pt = 0
      for (let seg = 1; seg <= edgeSegments; seg += 1) {
        const u = seg / edgeSegments
        const mt = 1 - u
        const a = mt * mt * mt
        const b = 3 * mt * mt * u
        const c = 3 * mt * u * u
        const d = u * u * u
        const qx = a * sx + b * c1x + c * c2x + d * tx
        const qy = a * sy + b * c1y + c * c2y + d * ty
        const qz = a * sz + b * c1z + c * c2z + d * tz
        edgePositions[edgeOffset++] = px
        edgePositions[edgeOffset++] = py
        edgePositions[edgeOffset++] = pz
        edgePositions[edgeOffset++] = qx
        edgePositions[edgeOffset++] = qy
        edgePositions[edgeOffset++] = qz
        writeColor(pt)
        writeColor(u)
        px = qx
        py = qy
        pz = qz
        pt = u
      }
      endpointPairs[appendedEdges * 2] = sourceIndex
      endpointPairs[appendedEdges * 2 + 1] = targetIndex
      appendedEdges += 1
    }
    for (let k = 0; k < renderCount; k += 1) {
      const e = renderOrder ? renderOrder[k] : k
      appendEdge(srcArr[e], tgtArr[e], relArr[e])
    }
    edgeEndpoints = endpointPairs.slice(0, appendedEdges * 2)
    const lineGeometry = new THREE.BufferGeometry()
    lineGeometry.setAttribute('position', new THREE.BufferAttribute(edgePositions.slice(0, edgeOffset), 3))
    lineGeometry.setAttribute('color', new THREE.BufferAttribute(edgeColors.slice(0, edgeColorOffset), 3))
    lineGeometry.setAttribute('alpha', new THREE.BufferAttribute(new Float32Array(appendedEdges * edgeVertexStride), 1))
    edgeLines = new THREE.LineSegments(lineGeometry, createEdgeMaterial(THREE))
    edgeLines.frustumCulled = false
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
      bridgeLines.frustumCulled = false
      scene.add(bridgeLines)
    }

    recomputeAlphas()
    fadeStart = performance.now()
    resetCamera()
    renderNow()
    // The fade only needs to cover ~400ms; 24 frames is plenty and avoids
    // re-drawing the (now larger) curved-edge buffer dozens of extra times.
    renderForFrames(24)
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
    const pathActive = pathNodeSet !== null && pathNodeSet.size > 0
    // The year filter always drives the visible count, and path / highlight /
    // isolation compose on top of it rather than short-circuiting each other.
    let visible = 0
    for (let i = 0; i < nodes.length; i += 1) {
      const yearOk = passesYearFilter(nodes[i])
      if (yearOk) visible += 1
      let alpha = 0
      if (pathActive) {
        // An explicitly chosen path always shows, even outside the year range.
        alpha = pathNodeSet.has(i) ? 1 : 0.05
      } else if (yearOk) {
        const inIsolated = isolatedCluster === null || Number(nodes[i].cluster) === isolatedCluster
        if (highlightSet) {
          // Highlight composes with isolation: a neighbour outside the isolated
          // territory stays dim.
          alpha = highlightSet.has(i) && inIsolated ? 1 : 0.06
        } else {
          alpha = inIsolated ? 1 : 0.07
        }
      }
      nodeAlphaAttr.array[i] = alpha
    }
    nodeAlphaAttr.needsUpdate = true
    // The quad instances share this alpha buffer; flag the rendered attribute.
    const quadAlpha = nodeQuads?.geometry?.getAttribute('aAlpha')
    if (quadAlpha) quadAlpha.needsUpdate = true
    visibleNodeCount = visible

    if (edgeLines && edgeEndpoints) {
      const edgeAlphaAttr = edgeLines.geometry.getAttribute('alpha')
      const edgeAlphaArray = edgeAlphaAttr.array
      const totalEdges = edgeEndpoints.length / 2
      // Per-edge opacity, scaled down as the edge count grows so that drawing
      // the full set reads as a faint directional web rather than a solid mass
      // (normal blending, so dense areas still layer up).
      const baseAlpha = totalEdges > 200000 ? 0.05 : totalEdges > 60000 ? 0.12 : 0.24
      const isolating = isolatedCluster !== null
      for (let edge = 0; edge < totalEdges; edge += 1) {
        const source = edgeEndpoints[edge * 2]
        const target = edgeEndpoints[edge * 2 + 1]
        const sourceAlpha = nodeAlphaAttr.array[source]
        const targetAlpha = nodeAlphaAttr.array[target]
        let alpha = 0
        if (sourceAlpha > 0 && targetAlpha > 0) {
          const dimmedEndpoint = sourceAlpha < 0.5 || targetAlpha < 0.5
          if (pathActive) {
            // The bright amber overlay line carries the path; dim the base graph.
            alpha = 0.02
          } else if (isolating && dimmedEndpoint) {
            // Isolation wins over highlight for edges leaving the territory.
            alpha = 0.015
          } else if (highlightIndex !== null) {
            alpha = source === highlightIndex || target === highlightIndex ? 0.85 : 0.02
          } else {
            alpha = baseAlpha
          }
        }
        // Each edge owns `edgeVertexStride` consecutive curve vertices.
        const base = edge * edgeVertexStride
        edgeAlphaArray.fill(alpha, base, base + edgeVertexStride)
      }
      edgeAlphaAttr.needsUpdate = true
    }
    requestRender()
  }

  function applyHighlight(index) {
    highlightIndex = index
    recomputeAlphas()
  }

  // Click a legend swatch to isolate that territory (dim everything else);
  // click the active one again to clear.
  function toggleIsolate(clusterId) {
    isolatedCluster = isolatedCluster === clusterId ? null : Number(clusterId)
    recomputeAlphas()
  }

  // Breadth-first shortest path over the undirected citation adjacency.
  function bfsPath(startIndex, endIndex) {
    if (!adjacency) return null
    const count = (graphData.nodes || []).length
    if (startIndex === endIndex) return [startIndex]
    const prev = new Int32Array(count).fill(-1)
    const visited = new Uint8Array(count)
    const queue = [startIndex]
    visited[startIndex] = 1
    let head = 0
    while (head < queue.length) {
      const current = queue[head++]
      for (let i = adjacency.offsets[current]; i < adjacency.offsets[current + 1]; i += 1) {
        const next = adjacency.neighbors[i]
        if (!visited[next]) {
          visited[next] = 1
          prev[next] = current
          if (next === endIndex) {
            const path = []
            for (let c = endIndex; c !== -1; c = prev[c]) path.push(c)
            return path.reverse()
          }
          queue.push(next)
        }
      }
    }
    return null
  }

  function disposePathLine() {
    if (!pathLine) return
    pathLine.geometry?.dispose?.()
    pathLine.material?.dispose?.()
    scene?.remove(pathLine)
    pathLine = null
  }

  function buildPathLine(indices) {
    disposePathLine()
    if (!indices || indices.length < 2 || !points || !THREE) return
    const posAttr = points.geometry.getAttribute('position')
    const linePoints = indices.map((idx) => new THREE.Vector3(
      posAttr.getX(idx), posAttr.getY(idx), posAttr.getZ(idx)
    ))
    const geometry = new THREE.BufferGeometry().setFromPoints(linePoints)
    pathLine = new THREE.Line(geometry, new THREE.LineBasicMaterial({
      color: 0xffd166,
      transparent: true,
      opacity: 0.96,
      depthTest: false,
      depthWrite: false,
    }))
    pathLine.renderOrder = 6
    scene.add(pathLine)
  }

  function disposeSelectionEdges() {
    if (!selectionEdges) return
    selectionEdges.geometry?.dispose?.()
    selectionEdges.material?.dispose?.()
    scene?.remove(selectionEdges)
    selectionEdges = null
  }

  // Highlight every edge adjacent to the selected node as a bright overlay,
  // drawn from the FULL adjacency (so it shows connections even when the edge
  // isn't part of the curated rendered subset). Doesn't dim anything else.
  function buildSelectionEdges(index) {
    disposeSelectionEdges()
    if (index === null || index === undefined || !adjacency || !points || !THREE) return
    const posAttr = points.geometry.getAttribute('position')
    const start = adjacency.offsets[index]
    const end = adjacency.offsets[index + 1]
    const count = end - start
    if (count <= 0) return
    const nx = posAttr.getX(index)
    const ny = posAttr.getY(index)
    const nz = posAttr.getZ(index)
    const pts = new Float32Array(count * 2 * 3)
    let o = 0
    for (let i = start; i < end; i += 1) {
      const nb = adjacency.neighbors[i]
      pts[o++] = nx; pts[o++] = ny; pts[o++] = nz
      pts[o++] = posAttr.getX(nb); pts[o++] = posAttr.getY(nb); pts[o++] = posAttr.getZ(nb)
    }
    const geometry = new THREE.BufferGeometry()
    geometry.setAttribute('position', new THREE.BufferAttribute(pts, 3))
    selectionEdges = new THREE.LineSegments(geometry, new THREE.LineBasicMaterial({
      color: 0x7fe9e3,
      transparent: true,
      opacity: 0.85,
      depthTest: false,
      depthWrite: false,
    }))
    selectionEdges.renderOrder = 5
    selectionEdges.frustumCulled = false
    scene.add(selectionEdges)
    requestRender()
  }

  function computePath() {
    if (pathStart === null || pathEnd === null) return
    const nodes = graphData.nodes || []
    const indices = bfsPath(pathStart, pathEnd)
    if (!indices) {
      pathInfo = { found: false, steps: [], hops: 0 }
      pathNodeSet = null
      disposePathLine()
      recomputeAlphas()
      return
    }
    pathNodeSet = new Set(indices)
    pathInfo = {
      found: true,
      hops: indices.length - 1,
      steps: indices.map((idx) => ({
        title: truncate(nodes[idx]?.title || nodes[idx]?.label || 'Untitled', 60),
        year: nodes[idx]?.year || null,
      })),
    }
    buildPathLine(indices)
    recomputeAlphas()
  }

  function setPathPoint(which) {
    if (selectedIndex === null) return
    if (which === 'start') pathStart = pathStart === selectedIndex ? null : selectedIndex
    else pathEnd = pathEnd === selectedIndex ? null : selectedIndex
    if (pathStart !== null && pathEnd !== null) {
      computePath()
    } else {
      pathInfo = null
      pathNodeSet = null
      disposePathLine()
      recomputeAlphas()
    }
  }

  function clearPath() {
    pathStart = null
    pathEnd = null
    pathInfo = null
    pathNodeSet = null
    disposePathLine()
    recomputeAlphas()
  }

  // Single reset for every "dim the rest" state: a selected node's neighbourhood
  // highlight, a territory isolation, or a citation path.
  function showAllNodes() {
    selectedNode = null
    selectedNodeDetail = null
    selectedIndex = null
    hoveredNode = null
    tooltip = { ...tooltip, visible: false }
    if (renderer) renderer.domElement.style.cursor = ''
    halo = { ...halo, visible: false }
    highlightIndex = null
    isolatedCluster = null
    pathStart = null
    pathEnd = null
    pathInfo = null
    pathNodeSet = null
    disposePathLine()
    disposeSelectionEdges()
    recomputeAlphas()
  }

  function resetCamera() {
    if (!camera || !controls) return
    // Reset/re-render frames the whole graph, which is none of the presets, so
    // clear the segmented-control highlight to keep it in sync with the camera.
    cameraPreset = null
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
    cameraPreset = preset
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
    selectedIndex = result.index
    buildSelectionEdges(result.index)
    focusNode(result.index)
    void loadNodeDetail(node)
  }

  function clearSelection() {
    selectedNode = null
    selectedNodeDetail = null
    selectedIndex = null
    hoveredNode = null
    halo = { ...halo, visible: false }
    disposeSelectionEdges()
    requestRender()
  }

  function onYearFilterChange() {
    // Debounced: recomputeAlphas walks every edge, so coalesce keystrokes
    // instead of running a full pass on each digit typed.
    if (yearFilterTimer) clearTimeout(yearFilterTimer)
    yearFilterTimer = setTimeout(() => {
      yearFilterTimer = 0
      recomputeAlphas()
    }, 200)
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
    selectedIndex = null
    isolatedCluster = null
    halo = { ...halo, visible: false }
    tooltip = { ...tooltip, visible: false }
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
      if (nodeQuads?.material?.uniforms?.uFade) nodeQuads.material.uniforms.uFade.value = fade
      if (edgeLines?.material?.uniforms?.uFade) edgeLines.material.uniforms.uFade.value = fade
      if (fade >= 1) fadeStart = 0
    }
    // Fade edges out as the camera moves in close so the bundle convergence at a
    // cluster's centre stops veiling its nodes; full strength in the overview.
    if (edgeLines?.material?.uniforms?.uViewDim && controls) {
      const dist = camera.position.distanceTo(controls.target)
      // Smooth ramp 0.12 (at/below 350) → 1.0 (at/above 1500): compute t in
      // [0,1] first, then lerp, so it doesn't sit pinned at the floor.
      const t = Math.min(1, Math.max(0, (dist - 350) / (1500 - 350)))
      edgeLines.material.uniforms.uViewDim.value = 0.12 + t * 0.88
    }
    renderer.render(scene, camera)
    updateClusterLabelPositions()
  }

  function updateClusterLabelPositions() {
    if (!camera || !renderer || !clusterLabels.length) return
    const rect = renderer.domElement.getBoundingClientRect()
    const placed = []
    // Vertical-only nudges so a label always stays horizontally on its cluster.
    // If no nudge is collision-free, the label is hidden rather than relocated
    // far from its cluster (the old horizontal offsets could fling it ~180px
    // away, which read as detached labels when zoomed out).
    const labelOffsets = [0, -26, 28, -52, 54]
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
      const halfW = width / 2
      const halfH = height / 2
      let x = baseX
      let y = baseY
      let visible = false
      // The cluster anchor must be in front of the camera and on-screen. Hide
      // off-screen labels instead of clamping them to an edge (which detached
      // them from their cluster).
      const onScreen =
        projected.z > -1 && projected.z < 1 &&
        baseX >= 0 && baseX <= rect.width &&
        baseY >= 0 && baseY <= rect.height
      if (visibleCount < maxLabels && onScreen) {
        for (const offsetY of labelOffsets) {
          const candidateX = baseX
          const candidateY = baseY + offsetY
          // The label is rendered centred (translate -50%,-50%) inside an
          // overflow-hidden canvas, so require the whole box on-screen — skip
          // offsets that would clip it rather than show a half-cut label.
          const inBounds =
            candidateX - halfW >= 0 && candidateX + halfW <= rect.width &&
            candidateY - halfH >= 0 && candidateY + halfH <= rect.height
          if (!inBounds) continue
          const overlaps = placed.some((box) => (
            Math.abs(box.x - candidateX) < (box.width + width) * 0.5
            && Math.abs(box.y - candidateY) < (box.height + height) * 0.6
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

    // Hub labels are placed into the same occupancy map so they never collide
    // with territory labels. Fewer show when zoomed out, and a hub whose node is
    // filtered/dimmed out is skipped.
    if (hubLabels.length) {
      const nodeAlphaAttr = points?.geometry?.getAttribute('alpha')?.array
      const hubMax = cameraDistance < 1200 ? 12 : cameraDistance < 2600 ? 7 : 3
      const hubOffsets = [-16, -34, 14, -52]
      let hubVisible = 0
      const hubUpdates = new Array(hubLabels.length)
      for (let h = 0; h < hubLabels.length; h += 1) {
        const hub = hubLabels[h]
        const dimmed = nodeAlphaAttr ? nodeAlphaAttr[hub.index] < 0.5 : false
        const projected = hub.vector.clone().project(camera)
        const baseX = ((projected.x + 1) / 2) * rect.width
        const baseY = ((-projected.y + 1) / 2) * rect.height
        const width = 150
        const height = 22
        let x = baseX
        let y = baseY
        let visible = false
        const onScreen =
          projected.z > -1 && projected.z < 1 &&
          baseX >= 0 && baseX <= rect.width && baseY >= 0 && baseY <= rect.height
        if (!dimmed && hubVisible < hubMax && onScreen) {
          for (const offsetY of hubOffsets) {
            const candidateY = baseY + offsetY
            const overlaps = placed.some((box) => (
              Math.abs(box.x - baseX) < (box.width + width) * 0.5
              && Math.abs(box.y - candidateY) < (box.height + height) * 0.6
            ))
            if (!overlaps) {
              x = baseX
              y = candidateY
              visible = true
              hubVisible += 1
              placed.push({ x, y, width, height })
              break
            }
          }
        }
        hubUpdates[h] = { ...hub, x, y, visible }
      }
      hubLabels = hubUpdates
    }

    // Selection halo follows the picked node in screen space.
    if (selectedIndex !== null && points) {
      const posAttr = points.geometry.getAttribute('position')
      const v = new THREE.Vector3(
        posAttr.getX(selectedIndex),
        posAttr.getY(selectedIndex),
        posAttr.getZ(selectedIndex)
      ).project(camera)
      const onScreen = v.z > -1 && v.z < 1
      halo = {
        visible: onScreen,
        x: ((v.x + 1) / 2) * rect.width,
        y: ((-v.y + 1) / 2) * rect.height,
      }
    } else if (halo.visible) {
      halo = { ...halo, visible: false }
    }
  }

  function renderForFrames(count = 30) {
    if (settleFrame) cancelAnimationFrame(settleFrame)
    let remaining = count
    const draw = () => {
      renderNow()
      remaining -= 1
      // Keep rendering until the min frame count AND any in-progress fade are
      // done. A fixed count alone left uFade < 1 on high-refresh displays
      // (24 frames < the 400ms fade), leaving nodes/edges stuck semi-transparent
      // until the next interaction.
      if (remaining > 0 || fadeStart) {
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
    // Node quads are sized in screen pixels, so they need the drawing-buffer size.
    if (nodeQuads?.material?.uniforms?.uViewport) {
      renderer.getDrawingBufferSize(nodeQuads.material.uniforms.uViewport.value)
    }
    requestRender()
  }

  function pointerFromEvent(event) {
    if (!pointer || !renderer) return
    const rect = renderer.domElement.getBoundingClientRect()
    pointer.x = ((event.clientX - rect.left) / rect.width) * 2 - 1
    pointer.y = -((event.clientY - rect.top) / rect.height) * 2 + 1
  }

  // HTML overlays on the canvas (cluster labels, legend) need pointer-events to
  // be clickable, which also makes them swallow wheel events so the graph won't
  // zoom while the cursor is over them. Forward the wheel to the canvas so
  // OrbitControls still zooms — unless the element itself can actually scroll.
  function forwardWheel(node) {
    const handler = (event) => {
      if (!renderer) return
      // Only let the element keep the wheel if it's a real scroll container
      // that actually has overflow (e.g. a long legend); plain text labels
      // whose content is a hair taller than the box must still forward to zoom.
      const overflowY = getComputedStyle(node).overflowY
      const scrollable = (overflowY === 'auto' || overflowY === 'scroll')
        && node.scrollHeight > node.clientHeight + 2
      if (scrollable) return
      event.preventDefault()
      // Re-dispatch a faithful copy: preserve modifier keys (trackpad pinch is
      // ctrl+wheel, shift+wheel pans, etc.) and let it bubble like a real wheel.
      renderer.domElement.dispatchEvent(new WheelEvent('wheel', {
        deltaX: event.deltaX,
        deltaY: event.deltaY,
        deltaZ: event.deltaZ,
        deltaMode: event.deltaMode,
        clientX: event.clientX,
        clientY: event.clientY,
        screenX: event.screenX,
        screenY: event.screenY,
        ctrlKey: event.ctrlKey,
        shiftKey: event.shiftKey,
        altKey: event.altKey,
        metaKey: event.metaKey,
        bubbles: true,
        cancelable: true,
      }))
    }
    node.addEventListener('wheel', handler, { passive: false })
    return { destroy() { node.removeEventListener('wheel', handler) } }
  }

  function pickNode(event, persist = false) {
    if (!raycaster || !points || !camera) return
    pointerFromEvent(event)
    raycaster.setFromCamera(pointer, camera)
    const cameraDistance = controls ? camera.position.distanceTo(controls.target) : 2000
    raycaster.params.Points.threshold = Math.min(24, Math.max(2, cameraDistance * 0.005))
    const alphaArray = points.geometry.getAttribute('alpha')?.array
    // Pick the node nearest the cursor (smallest perpendicular distance to the
    // ray), not the first one along the ray — so a click selects the dot under
    // the pointer rather than a nearby one that happens to be closer to camera.
    let hit = null
    let bestDist = Infinity
    for (const candidate of raycaster.intersectObject(points, false)) {
      if (alphaArray && !(alphaArray[candidate.index] > 0.05)) continue
      const d = candidate.distanceToRay ?? candidate.distance ?? 0
      if (d < bestDist) {
        bestDist = d
        hit = candidate
      }
    }
    const nextNode = hit ? points.userData.nodes?.[hit.index] : null
    hoveredNode = nextNode
    if (renderer) renderer.domElement.style.cursor = nextNode ? 'pointer' : ''
    if (nextNode && !persist) {
      const rect = renderer.domElement.getBoundingClientRect()
      tooltip = { visible: true, x: event.clientX - rect.left, y: event.clientY - rect.top, node: nextNode }
    } else if (!nextNode) {
      tooltip = { ...tooltip, visible: false }
    }
    if (!persist) return
    if (nextNode) {
      // Selection just spotlights the node (halo + sidebar detail); it no
      // longer dims the rest of the graph.
      selectedNode = nextNode
      selectedIndex = hit.index
      tooltip = { ...tooltip, visible: false }
      buildSelectionEdges(hit.index)
      void loadNodeDetail(nextNode)
      requestRender()
    } else {
      clearSelection()
    }
  }

  function formatNumber(value) {
    return new Intl.NumberFormat().format(Number(value || 0))
  }

  // Resolve a clickable URL for the selected work, so a researcher can open the
  // actual paper. Each accepts the raw stored value (id or full URL).
  function doiHref(doi) {
    const s = String(doi || '').trim()
    if (!s) return null
    if (/^https?:\/\//i.test(s)) return s
    return `https://doi.org/${s.replace(/^doi:/i, '')}`
  }
  function openAlexHref(id) {
    const s = String(id || '').trim()
    if (!s) return null
    if (/^https?:\/\//i.test(s)) return s
    return `https://openalex.org/${s}`
  }
  function externalHref(url) {
    const s = String(url || '').trim()
    return /^https?:\/\//i.test(s) ? s : null
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
    // preserveDrawingBuffer keeps the last rendered frame in the buffer. The
    // graph renders on demand (only on interaction); without this the browser
    // can composite a cleared buffer between renders, blanking the canvas
    // intermittently — the "nodes vanish sometimes" report.
    renderer = new THREE.WebGLRenderer({ antialias: false, powerPreference: 'high-performance', preserveDrawingBuffer: true })
    renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 1.5))
    container.appendChild(renderer.domElement)

    controls = new OrbitControls(camera, renderer.domElement)
    controls.enableDamping = false
    controls.enableRotate = true
    controls.enablePan = true
    controls.enableZoom = true
    // Zoom toward the cursor, not the fixed orbit target — otherwise scrolling
    // only ever approaches the graph centre (with proportional, diminishing
    // steps) and you can never reach an off-centre cluster.
    controls.zoomToCursor = true
    controls.minDistance = 1
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
    // Faster than the default 1.0: the default view sits ~3600 units out and a
    // cluster is only a few hundred across, so at 1.0 the proportional dolly
    // took ~20 scrolls to get in and felt like it stopped making progress.
    controls.zoomSpeed = 2.5
    controls.autoRotate = autoRotate
    controls.autoRotateSpeed = 0.65
    controls.addEventListener('change', requestRender)

    raycaster = new THREE.Raycaster()
    handlePointerMove = (event) => pickNode(event, false)
    handlePointerLeave = () => {
      hoveredNode = null
      tooltip = { ...tooltip, visible: false }
      if (renderer) renderer.domElement.style.cursor = ''
    }
    // Left-button dead-zone so a click selects without the camera rotating.
    // OrbitControls rotates on ANY movement once it sees the pointerdown, so we
    // hold the left pointerdown back (capture phase) and only hand it to
    // OrbitControls once the pointer crosses a small threshold — i.e. it's a
    // real drag. Below the threshold it's a click: select on pointerup.
    const DRAG_DEADZONE = 5
    handleDownCapture = (event) => {
      if (event.button !== 0 || event.__fromGraph) return
      pendingDown = { pointerId: event.pointerId, x: event.clientX, y: event.clientY }
      event.stopImmediatePropagation() // withhold from OrbitControls for now
    }
    handleMoveCapture = (event) => {
      if (!pendingDown || event.pointerId !== pendingDown.pointerId) return
      const moved = Math.hypot(event.clientX - pendingDown.x, event.clientY - pendingDown.y)
      if (moved > DRAG_DEADZONE) {
        // Promote to a rotate: hand OrbitControls a fresh pointerdown.
        const synth = new PointerEvent('pointerdown', {
          pointerId: pendingDown.pointerId,
          clientX: pendingDown.x,
          clientY: pendingDown.y,
          button: 0,
          buttons: 1,
          bubbles: true,
          cancelable: true,
        })
        synth.__fromGraph = true
        pendingDown = null
        renderer.domElement.dispatchEvent(synth)
      }
    }
    handleUpCapture = (event) => {
      if (pendingDown && event.pointerId === pendingDown.pointerId) {
        pendingDown = null
        // never crossed the dead-zone → a click selects (a cancel just clears)
        if (event.type === 'pointerup') pickNode(event, true)
      }
    }
    handleContextMenu = (event) => event.preventDefault()
    handleKeydown = (event) => {
      if (event.key === 'Escape') {
        searchResults = []
        clearSelection()
      }
    }
    renderer.domElement.addEventListener('pointermove', handlePointerMove)
    renderer.domElement.addEventListener('pointerleave', handlePointerLeave)
    renderer.domElement.addEventListener('pointerdown', handleDownCapture, { capture: true })
    renderer.domElement.addEventListener('pointermove', handleMoveCapture, { capture: true })
    renderer.domElement.addEventListener('pointerup', handleUpCapture, { capture: true })
    renderer.domElement.addEventListener('pointercancel', handleUpCapture, { capture: true })
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
    if (yearFilterTimer) clearTimeout(yearFilterTimer)
    resizeObserver?.disconnect()
    controls?.removeEventListener('change', requestRender)
    if (handlePointerMove) renderer?.domElement?.removeEventListener('pointermove', handlePointerMove)
    if (handlePointerLeave) renderer?.domElement?.removeEventListener('pointerleave', handlePointerLeave)
    if (handleDownCapture) renderer?.domElement?.removeEventListener('pointerdown', handleDownCapture, { capture: true })
    if (handleMoveCapture) renderer?.domElement?.removeEventListener('pointermove', handleMoveCapture, { capture: true })
    if (handleUpCapture) {
      renderer?.domElement?.removeEventListener('pointerup', handleUpCapture, { capture: true })
      renderer?.domElement?.removeEventListener('pointercancel', handleUpCapture, { capture: true })
    }
    if (handleContextMenu) renderer?.domElement?.removeEventListener('contextmenu', handleContextMenu)
    if (handleKeydown) window.removeEventListener('keydown', handleKeydown)
    controls?.dispose()
    disposeObject(points)
    disposeObject(nodeQuads)
    disposeObject(edgeLines)
    disposeObject(bridgeLines)
    disposeObject(clusterShells)
    disposePathLine()
    disposeSelectionEdges()
    renderer?.dispose()
    renderer?.domElement?.remove()
  })

  $: activeNode = selectedNode || hoveredNode
  $: activeNodeDetail = selectedNode && selectedNodeDetail ? { ...selectedNode, ...selectedNodeDetail } : activeNode
  $: clusters = graphData.clusters || []
  $: selectedClusterData = clusters.find((cluster) => Number(cluster.id) === selectedCluster)
  $: selectedClusterPanel = selectedClusterDetail || selectedClusterData
  $: legendItems = clusterLegend.slice(0, 12)
  // The "Show all nodes" reset is only for states that actually dim the graph
  // (territory isolation, a citation path) — a plain selection no longer does.
  $: viewFiltered = isolatedCluster !== null || Boolean(pathInfo)
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
      <div class="graph-3d-segment" role="group" aria-label="Camera angle">
        <button class="graph-3d-btn" class:active={cameraPreset === 'angle'} aria-pressed={cameraPreset === 'angle'} type="button" on:click={() => setCameraPreset('angle')} disabled={!graphData.nodes?.length}>Angle</button>
        <button class="graph-3d-btn" class:active={cameraPreset === 'top'} aria-pressed={cameraPreset === 'top'} type="button" on:click={() => setCameraPreset('top')} disabled={!graphData.nodes?.length}>Top</button>
        <button class="graph-3d-btn" class:active={cameraPreset === 'side'} aria-pressed={cameraPreset === 'side'} type="button" on:click={() => setCameraPreset('side')} disabled={!graphData.nodes?.length}>Side</button>
      </div>
      <button class="graph-3d-btn" class:active={autoRotate} type="button" on:click={() => { autoRotate = !autoRotate; updateAutoRotate() }} disabled={!graphData.nodes?.length}>
        {autoRotate ? '⏸ Rotating' : '↻ Auto rotate'}
      </button>
      <button class="graph-3d-btn" type="button" on:click={resetCamera} disabled={!graphData.nodes?.length}>Reset</button>
      <button class="graph-3d-btn graph-3d-btn-primary" type="button" on:click={load3DGraph} disabled={graphLoading}>
        {graphLoading ? 'Loading…' : 'Load 3D graph'}
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
      {#if viewFiltered}
        <button type="button" class="graph-3d-showall" data-testid="graph-3d-showall" on:click={showAllNodes}>
          ✕ Show all nodes
        </button>
      {/if}
      <div class="graph-3d-label-layer" bind:this={clusterLabelLayer}>
        {#each clusterLabels as label (label.id)}
          {#if label.visible}
            <button
              type="button"
              class="graph-3d-cluster-label"
              use:forwardWheel
              style={`--label-x:${label.x}px;--label-y:${label.y}px;--label-color:${label.color};--label-opacity:${label.opacity};--label-scale:${label.scale}`}
              on:click={() => focusCluster(clusters.find((cluster) => Number(cluster.id) === Number(label.id)))}
            >
              <span>{label.label}</span>
              <small>{label.detail}</small>
            </button>
          {/if}
        {/each}
        {#each hubLabels as hub (hub.index)}
          {#if hub.visible}
            <span class="graph-3d-hub-label" style={`--label-x:${hub.x}px;--label-y:${hub.y}px`}>
              {hub.label}
            </span>
          {/if}
        {/each}
        {#if halo.visible}
          <span class="graph-3d-halo" style={`--label-x:${halo.x}px;--label-y:${halo.y}px`} aria-hidden="true"></span>
        {/if}
      </div>
      {#if tooltip.visible && tooltip.node}
        <div class="graph-3d-tooltip" style={`--label-x:${tooltip.x}px;--label-y:${tooltip.y}px`} aria-hidden="true">
          <strong>{truncate(tooltip.node.title || tooltip.node.label || 'Untitled', 90)}</strong>
          <span>{tooltip.node.authors ? `${truncate(tooltip.node.authors, 60)} · ` : ''}{tooltip.node.year || 'year ?'}</span>
          <span>{formatNumber(tooltip.node.degree)} citations · {tooltip.node.field || tooltip.node.area || 'unknown field'}</span>
        </div>
      {/if}
      {#if legendItems.length}
        <div class="graph-3d-legend" data-testid="graph-3d-legend" use:forwardWheel>
          <span class="graph-3d-legend-title">Territories</span>
          {#each legendItems as item (item.id)}
            <button
              type="button"
              class="graph-3d-legend-item"
              class:active={isolatedCluster === Number(item.id)}
              class:dimmed={isolatedCluster !== null && isolatedCluster !== Number(item.id)}
              data-testid="graph-3d-legend-item"
              aria-pressed={isolatedCluster === Number(item.id)}
              on:click={() => toggleIsolate(Number(item.id))}
            >
              <i style={`background:${item.color}`}></i>
              <span>{item.label}</span>
            </button>
          {/each}
          {#if isolatedCluster !== null}
            <button type="button" class="graph-3d-legend-clear" data-testid="graph-3d-legend-clear" on:click={() => toggleIsolate(isolatedCluster)}>
              Clear isolation
            </button>
          {/if}
        </div>
      {/if}
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
          {#if selectedNodeDetail && (doiHref(selectedNodeDetail.doi) || openAlexHref(selectedNodeDetail.openalex_id) || externalHref(selectedNodeDetail.source_pdf))}
            <div class="graph-3d-links" data-testid="graph-3d-links">
              <span class="muted small">Open:</span>
              {#if doiHref(selectedNodeDetail.doi)}
                <a href={doiHref(selectedNodeDetail.doi)} target="_blank" rel="noopener noreferrer">DOI ↗</a>
              {/if}
              {#if openAlexHref(selectedNodeDetail.openalex_id)}
                <a href={openAlexHref(selectedNodeDetail.openalex_id)} target="_blank" rel="noopener noreferrer">OpenAlex ↗</a>
              {/if}
              {#if externalHref(selectedNodeDetail.source_pdf)}
                <a href={externalHref(selectedNodeDetail.source_pdf)} target="_blank" rel="noopener noreferrer">PDF ↗</a>
              {/if}
            </div>
          {/if}
          {#if selectedNodeDetail?.publisher || selectedNodeDetail?.keywords}
            <span class="muted small">Metadata: {selectedNodeDetail.publisher || selectedNodeDetail.keywords}</span>
          {/if}
          {#if selectedIndex !== null}
            <div class="graph-3d-path-actions" data-testid="graph-3d-path-actions">
              <button type="button" class:active={pathStart === selectedIndex} on:click={() => setPathPoint('start')}>
                {pathStart === selectedIndex ? '✓ Path start' : 'Set as path start'}
              </button>
              <button type="button" class:active={pathEnd === selectedIndex} on:click={() => setPathPoint('end')}>
                {pathEnd === selectedIndex ? '✓ Path end' : 'Set as path end'}
              </button>
            </div>
          {/if}
        {:else}
          <strong>No node selected</strong>
          <span class="muted small">Hover or click a point in the 3D graph.</span>
        {/if}
      </div>

      {#if pathStart !== null || pathEnd !== null || pathInfo}
        <div class="graph-3d-path" data-testid="graph-3d-path">
          <span class="eyebrow">Citation path</span>
          <span class="muted small">From: {pathStart !== null ? truncate(graphData.nodes[pathStart]?.title || 'Untitled', 48) : 'pick a work and "Set as path start"'}</span>
          <span class="muted small">To: {pathEnd !== null ? truncate(graphData.nodes[pathEnd]?.title || 'Untitled', 48) : 'pick a work and "Set as path end"'}</span>
          {#if pathInfo && pathInfo.found}
            <span class="muted small" data-testid="graph-3d-path-status">{pathInfo.hops} {pathInfo.hops === 1 ? 'hop' : 'hops'} along the citation graph.</span>
            <ol class="graph-3d-path-steps">
              {#each pathInfo.steps as step, i (i)}
                <li><span>{step.title}</span><small>{step.year || ''}</small></li>
              {/each}
            </ol>
          {:else if pathInfo && !pathInfo.found}
            <span class="muted small" data-testid="graph-3d-path-status">No citation path connects these two works.</span>
          {/if}
          <button type="button" class="graph-3d-path-clear" data-testid="graph-3d-path-clear" on:click={clearPath}>Clear path</button>
        </div>
      {/if}
    </aside>
  </div>
</div>
