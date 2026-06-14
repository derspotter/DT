import { expect, test } from '@playwright/test'
import { buildEdgeBuffers } from '../src/lib/graphEdges.js'

const DIM = [0.96, 0.62, 0.27] // citing end (amber)
const BRIGHT = [0.3, 0.8, 1.0] // cited end (cyan)
const GRAD = [BRIGHT[0] - DIM[0], BRIGHT[1] - DIM[1], BRIGHT[2] - DIM[2]]

function build(rel: number) {
  // Two nodes 10 apart on X. Centroids == node positions, so the Bézier control
  // points collapse onto the endpoints and the curve is a straight line —
  // predictable to assert against.
  const positions = new Float32Array([0, 0, 0, 10, 0, 0])
  return buildEdgeBuffers({
    src: new Uint32Array([0]),
    tgt: new Uint32Array([1]),
    rel: new Uint8Array([rel]),
    count: 1,
    positions,
    nodeCentroid: positions.slice(),
    edgeSegments: 3,
    bundleStrength: 0.78,
    dim: DIM,
    grad: GRAD,
  })
}

test('buildEdgeBuffers fills buffers exactly and records endpoints', () => {
  const out = build(0)
  expect(out.stride).toBe(6) // segments * 2
  expect(out.edgePositions.length).toBe(1 * 6 * 3)
  expect(out.edgeColors.length).toBe(1 * 6 * 3)
  expect(Array.from(out.endpointPairs)).toEqual([0, 1])
  // Straight line: first vertex at the source, last at the target.
  expect(Array.from(out.edgePositions.slice(0, 3))).toEqual([0, 0, 0])
  expect(Array.from(out.edgePositions.slice(15, 18))).toEqual([10, 0, 0])
})

test('references (rel 0) run amber at the citing end to cyan at the cited end', () => {
  const out = build(0)
  const last = (6 - 1) * 3
  expect(out.edgeColors[0]).toBeCloseTo(DIM[0], 5) // first vertex (citing) = amber
  expect(out.edgeColors[2]).toBeCloseTo(DIM[2], 5)
  expect(out.edgeColors[last]).toBeCloseTo(BRIGHT[0], 5) // last vertex (cited) = cyan
  expect(out.edgeColors[last + 2]).toBeCloseTo(BRIGHT[2], 5)
})

test('cited_by (rel 1) inverts the gradient direction', () => {
  const out = build(1)
  const last = (6 - 1) * 3
  expect(out.edgeColors[0]).toBeCloseTo(BRIGHT[0], 5) // citing is now the far end
  expect(out.edgeColors[last]).toBeCloseTo(DIM[0], 5)
})
