import { test, expect } from '@playwright/test'
import { execFileSync } from 'node:child_process'
import { fileURLToPath } from 'node:url'

// No page: this checks the file watcher Vite actually builds from
// vite.config.js (chokidar's CHOKIDAR_USEPOLLING override included), not the
// config object. The server is created without listen() and closed again.
const frontendDir = fileURLToPath(new URL('..', import.meta.url))
const probe =
  "import { createServer } from 'vite'; const s = await createServer({ configFile: 'vite.config.js', logLevel: 'silent', optimizeDeps: { noDiscovery: true } }); console.log(JSON.stringify({ usePolling: s.watcher.options.usePolling, option: s.config.server.watch?.usePolling })); await s.close()"

function effectivePolling(value?: string): boolean {
  const { usePolling, option } = probeWatcher(value)
  // The option passed to chokidar must agree with the watcher, or macOS picks
  // FSEvents from the option before the environment override applies.
  expect(option).toBe(usePolling)
  return usePolling
}

function probeWatcher(value?: string): { usePolling: boolean; option: boolean } {
  const env = { ...process.env }
  delete env.CHOKIDAR_USEPOLLING
  if (value !== undefined) env.CHOKIDAR_USEPOLLING = value
  const out = execFileSync(process.execPath, ['--input-type=module', '-e', probe], {
    cwd: frontendDir,
    env,
    encoding: 'utf8',
  })
  return JSON.parse(out.trim().split('\n').pop()!)
}

test('the watcher Vite builds polls only when CHOKIDAR_USEPOLLING turns it on', () => {
  expect(effectivePolling()).toBe(false)
  expect(effectivePolling('1')).toBe(true)
  expect(effectivePolling(' TRUE ')).toBe(true)
  expect(effectivePolling('0')).toBe(false)
  expect(effectivePolling('')).toBe(false)
})
