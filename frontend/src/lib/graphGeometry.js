// Pure geometry helpers for the 3D graph's curved/bundled edges. Kept free of
// Three.js and DOM so they can be unit-tested directly (see
// tests/graph-geometry.spec.ts) — the edge-segment count in particular shipped
// a regression once when it silently collapsed to 2, so it is worth pinning.

// Sampled segments per bundled edge. Fewer as the edge population grows so the
// vertex buffer stays bounded on the whole-corpus view, but never so few that a
// cubic Bézier degenerates into a visible straight kink.
export function chooseEdgeSegments(edgeCount) {
  if (edgeCount > 500000) return 4
  if (edgeCount > 200000) return 6
  return 8
}

// Control points for one bundled edge: pull each endpoint toward its own
// cluster centroid (hierarchical edge bundling) by `strength`. `start`/`end` and
// the centroids are plain {x,y,z}. Returns the two cubic-Bézier control points.
export function edgeControlPoints(start, end, startCentroid, endCentroid, strength) {
  return {
    c1: {
      x: start.x + (startCentroid.x - start.x) * strength,
      y: start.y + (startCentroid.y - start.y) * strength,
      z: start.z + (startCentroid.z - start.z) * strength,
    },
    c2: {
      x: end.x + (endCentroid.x - end.x) * strength,
      y: end.y + (endCentroid.y - end.y) * strength,
      z: end.z + (endCentroid.z - end.z) * strength,
    },
  }
}

// Point at parameter u in [0,1] on the cubic Bézier p0->c1->c2->p3.
export function cubicBezierPoint(p0, c1, c2, p3, u) {
  const mt = 1 - u
  const a = mt * mt * mt
  const b = 3 * mt * mt * u
  const c = 3 * mt * u * u
  const d = u * u * u
  return {
    x: a * p0.x + b * c1.x + c * c2.x + d * p3.x,
    y: a * p0.y + b * c1.y + c * c2.y + d * p3.y,
    z: a * p0.z + b * c1.z + c * c2.z + d * p3.z,
  }
}
