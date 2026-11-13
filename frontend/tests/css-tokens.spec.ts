import { readFileSync, readdirSync, statSync } from 'node:fs'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { expect, test } from '@playwright/test'

// Every custom property used without a fallback must be defined somewhere:
// an undefined `var(--name)` silently falls back to the inherited value.
// `var(--name, fallback)` counts as safe on purpose (--border, --row-checked).
// Definitions include inline style declarations (ThreeGraph's `--label-x:`).

const srcDir = fileURLToPath(new URL('../src', import.meta.url))

function svelteFiles(dir: string): string[] {
  return readdirSync(dir).flatMap((name) => {
    const path = join(dir, name)
    if (statSync(path).isDirectory()) return svelteFiles(path)
    return name.endsWith('.svelte') ? [path] : []
  })
}

test('every var(--name) without a fallback is defined somewhere', () => {
  const text = [join(srcDir, 'app.css'), ...svelteFiles(srcDir)]
    .map((path) => readFileSync(path, 'utf8'))
    .join('\n')
  const used = new Set([...text.matchAll(/var\(--([a-z0-9-]+)\)/g)].map((match) => match[1]))
  const defined = new Set([...text.matchAll(/--([a-z0-9-]+)\s*:/g)].map((match) => match[1]))
  expect(used.size).toBeGreaterThan(0)
  expect([...used].filter((name) => !defined.has(name))).toEqual([])
})
