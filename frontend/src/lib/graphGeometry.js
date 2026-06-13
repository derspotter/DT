// Pure geometry helpers for the 3D graph's curved/bundled edges. Kept free of
// Three.js and DOM so they can be unit-tested directly (see
// tests/graph-geometry.spec.ts) — the edge-segment count in particular shipped
// a regression once when it silently collapsed to 2, so it is worth pinning.

// Sampled segments per bundled edge. Fewer as the edge population grows so the
// vertex buffer (built synchronously and uploaded to the GPU on every load)
// stays small enough to paint quickly, but never so few that a cubic Bézier
// degenerates into a visible straight kink. 3 chords already read as a curve;
// 2 (a single mid-point kink) does not — that was the regression we guard.
export function chooseEdgeSegments(edgeCount) {
  if (edgeCount > 400000) return 3
  if (edgeCount > 120000) return 4
  return 6
}
