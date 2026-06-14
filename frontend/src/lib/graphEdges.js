// Deterministic edge-geometry construction for the 3D graph: samples a cubic
// Bézier per edge (bundled through each endpoint's cluster centroid) and writes
// a directional colour gradient along it. Pure number-crunching with no DOM or
// Three.js dependency, so the exact same code runs in a Web Worker (off the
// main thread, the common case) or inline as a fallback.
//
// Inputs are flat typed arrays so the whole thing is allocation-free in the hot
// loop and cheap to clone across the worker boundary:
//   src/tgt/rel   per-edge source index, target index, relationship code (1 = cited_by)
//   positions     node positions, length nodeCount*3 (already depth-scaled)
//   nodeCentroid  per-node bundling centroid, length nodeCount*3 (cluster
//                 centroid resolved on the main thread, graph centre as fallback)
//   dim           [r,g,b] colour at the citing end
//   grad          [dr,dg,db] = brightColour - dim (added toward the cited end)
//
// Returns the three buffers the LineSegments geometry needs, plus the vertex
// stride (segments*2). Every edge is emitted, so the buffers are exactly full.
export function buildEdgeBuffers({
  src,
  tgt,
  rel,
  count,
  positions,
  nodeCentroid,
  edgeSegments,
  bundleStrength,
  dim,
  grad,
}) {
  const stride = edgeSegments * 2
  const edgePositions = new Float32Array(count * stride * 3)
  const edgeColors = new Float32Array(count * stride * 3)
  const endpointPairs = new Uint32Array(count * 2)
  const dimR = dim[0]
  const dimG = dim[1]
  const dimB = dim[2]
  const gradR = grad[0]
  const gradG = grad[1]
  const gradB = grad[2]
  let eo = 0
  let co = 0
  for (let e = 0; e < count; e += 1) {
    const s = src[e]
    const t = tgt[e]
    const sx = positions[s * 3]
    const sy = positions[s * 3 + 1]
    const sz = positions[s * 3 + 2]
    const tx = positions[t * 3]
    const ty = positions[t * 3 + 1]
    const tz = positions[t * 3 + 2]
    // Control points pull each endpoint toward its own cluster centroid, so
    // same-cluster edges bow through that centroid and cross-cluster edges
    // follow the centroid trunk.
    const c1x = sx + (nodeCentroid[s * 3] - sx) * bundleStrength
    const c1y = sy + (nodeCentroid[s * 3 + 1] - sy) * bundleStrength
    const c1z = sz + (nodeCentroid[s * 3 + 2] - sz) * bundleStrength
    const c2x = tx + (nodeCentroid[t * 3] - tx) * bundleStrength
    const c2y = ty + (nodeCentroid[t * 3 + 1] - ty) * bundleStrength
    const c2z = tz + (nodeCentroid[t * 3 + 2] - tz) * bundleStrength
    // Direction: colour runs from the citing end (dim) to the cited end
    // (bright) via t in [0,1] along source->target. references (rel 0): source
    // cites target, so cited is at t=1. cited_by (rel 1): invert t.
    const forward = rel[e] !== 1
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
      edgePositions[eo++] = px
      edgePositions[eo++] = py
      edgePositions[eo++] = pz
      edgePositions[eo++] = qx
      edgePositions[eo++] = qy
      edgePositions[eo++] = qz
      const f0 = forward ? pt : 1 - pt
      const f1 = forward ? u : 1 - u
      edgeColors[co++] = dimR + gradR * f0
      edgeColors[co++] = dimG + gradG * f0
      edgeColors[co++] = dimB + gradB * f0
      edgeColors[co++] = dimR + gradR * f1
      edgeColors[co++] = dimG + gradG * f1
      edgeColors[co++] = dimB + gradB * f1
      px = qx
      py = qy
      pz = qz
      pt = u
    }
    endpointPairs[e * 2] = s
    endpointPairs[e * 2 + 1] = t
  }
  return { edgePositions, edgeColors, endpointPairs, stride }
}
