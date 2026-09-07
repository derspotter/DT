import { expect, test } from '@playwright/test'
import { columnsForTable, defaultVisibility, loadVisibility, visibleColumns } from '../src/lib/tableColumns.js'

function withStorage(value: string | null, fn: () => void) {
  const g = globalThis as any
  const prev = g.localStorage
  g.localStorage = { getItem: () => value, setItem: () => {} }
  try { fn() } finally { g.localStorage = prev }
}

test('loadVisibility ignores stale keys when deciding whether any column is visible', () => {
  const allOff: Record<string, boolean> = {}
  for (const def of columnsForTable('corpus')) allOff[def.key] = false
  // A stale key from an older release is the only "true" — must not count.
  withStorage(JSON.stringify({ ...allOff, legacy_column: true }), () => {
    const vis = loadVisibility('corpus')
    expect(vis).toEqual(defaultVisibility('corpus'))
    expect(visibleColumns('corpus', vis).length).toBeGreaterThan(0)
  })
})

test('loadVisibility keeps a stored choice and drops unknown keys', () => {
  withStorage(JSON.stringify({ year: false, cited_by: true, legacy_column: true }), () => {
    const vis = loadVisibility('seed')
    expect(vis.year).toBe(false)
    expect(vis.cited_by).toBe(true)
    expect('legacy_column' in vis).toBe(false)
    expect(vis.title).toBe(true)
  })
})
