import { expect, test } from '@playwright/test'
import { chooseEdgeSegments, cubicBezierPoint, edgeControlPoints } from '../src/lib/graphGeometry'

test('chooseEdgeSegments never collapses curves to a straight kink', () => {
  // The 25k default loads ~437k edges; keep it curved but light enough to
  // paint quickly. 2 segments (a single kink) is the regression we guard.
  expect(chooseEdgeSegments(437570)).toBe(3)
  expect(chooseEdgeSegments(184100)).toBe(4)
  expect(chooseEdgeSegments(655000)).toBe(3)
  // Boundaries.
  expect(chooseEdgeSegments(120000)).toBe(6)
  expect(chooseEdgeSegments(120001)).toBe(4)
  expect(chooseEdgeSegments(400000)).toBe(4)
  expect(chooseEdgeSegments(400001)).toBe(3)
  // Always at least 3 so a cubic Bézier never degenerates to one kink.
  for (const n of [0, 1, 1e6, 5e6]) expect(chooseEdgeSegments(n)).toBeGreaterThanOrEqual(3)
})

test('edgeControlPoints pull endpoints toward their cluster centroids', () => {
  const start = { x: 0, y: 0, z: 0 }
  const end = { x: 100, y: 0, z: 0 }
  const cs = { x: 0, y: 50, z: 0 }
  const ce = { x: 100, y: 50, z: 0 }
  const { c1, c2 } = edgeControlPoints(start, end, cs, ce, 0.8)
  // 80% of the way from each endpoint toward its centroid.
  expect(c1).toEqual({ x: 0, y: 40, z: 0 })
  expect(c2).toEqual({ x: 100, y: 40, z: 0 })
})

test('cubicBezierPoint returns endpoints at u=0/u=1 and bows at the middle', () => {
  const p0 = { x: 0, y: 0, z: 0 }
  const p3 = { x: 100, y: 0, z: 0 }
  const { c1, c2 } = edgeControlPoints(p0, p3, { x: 0, y: 50, z: 0 }, { x: 100, y: 50, z: 0 }, 0.8)
  expect(cubicBezierPoint(p0, c1, c2, p3, 0)).toEqual(p0)
  expect(cubicBezierPoint(p0, c1, c2, p3, 1)).toEqual(p3)
  // Midpoint must deflect off the straight line (y > 0), i.e. the edge curves.
  const mid = cubicBezierPoint(p0, c1, c2, p3, 0.5)
  expect(mid.x).toBeCloseTo(50, 5)
  expect(mid.y).toBeGreaterThan(20)
})
