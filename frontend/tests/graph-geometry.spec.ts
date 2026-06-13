import { expect, test } from '@playwright/test'
import { chooseEdgeSegments } from '../src/lib/graphGeometry.js'

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
