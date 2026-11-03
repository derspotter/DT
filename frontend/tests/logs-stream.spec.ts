import { readFileSync } from 'node:fs'
import { expect, test, type Page, type Route, type WebSocketRoute } from '@playwright/test'
import { mockLogSocket } from './helpers/mockApi'

// The log socket's lines are buffered and flushed into `logs` at most once per
// 250 ms; JSON frames are parsed once (extraction events drive the extraction
// card, other JSON frames are ignored) and never become log rows.

const maxLogHistory = Number(
  readFileSync(new URL('../src/App.svelte', import.meta.url), 'utf8').match(/const maxLogHistory = (\d+)/)![1]
)

function fulfil(route: Route, body: unknown) {
  return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(body) })
}

type TailBody = { entries: string[]; next_cursor: number; has_more: boolean }

async function boot(page: Page, hash: string) {
  const handle: { socket: WebSocketRoute | null; connects: number } = { socket: null, connects: 0 }
  const errors: string[] = []
  // `tail.answer`, when set, answers the next /api/logs/tail request (and is
  // then cleared); every tail request's cursor is recorded.
  const tail: { answer: (() => Promise<TailBody>) | null; cursors: string[] } = { answer: null, cursors: [] }
  page.on('pageerror', (error) => errors.push(error.message))
  await mockLogSocket(page, (ws) => {
    handle.socket = ws
    handle.connects += 1
  })
  await page.addInitScript(() => {
    window.localStorage.setItem('rag_feeder_token', 'playwright-token')
  })
  await page.route('**/api/**', async (route) => {
    const url = new URL(route.request().url())
    const path = url.pathname
    if (path === '/api/auth/me') {
      return fulfil(route, {
        user: { id: 1, username: 'admin', last_corpus_id: 1, is_admin: true },
        corpora: [{ id: 1, name: 'Local research corpus', role: 'owner', owner_username: 'admin' }],
      })
    }
    if (path === '/api/logs/tail') {
      tail.cursors.push(url.searchParams.get('cursor') || '')
      const answer = tail.answer
      tail.answer = null
      return fulfil(route, answer ? await answer() : { entries: [], next_cursor: 0, has_more: false })
    }
    if (path === '/api/corpus') return fulfil(route, { items: [], total: 0, source: 'api', stage_totals: {} })
    if (path === '/api/seed/sources') return fulfil(route, { sources: [], total: 0, source: 'api' })
    if (path === '/api/ingest/runs') return fulfil(route, { runs: [], total: 0, source: 'api' })
    if (path === '/api/ingest/stats') return fulfil(route, { stats: {} })
    if (path === '/api/recursion-config') return fulfil(route, { keyword: {} })
    if (path === '/api/downloads') return fulfil(route, { items: [], total: 0, source: 'api' })
    if (path === '/api/pipeline/daemon/status') return fulfil(route, { daemon: { workers: {} }, backlog: {} })
    return fulfil(route, {})
  })
  const firstTail = page.waitForResponse((res) => new URL(res.url()).pathname === '/api/logs/tail')
  await page.goto(`/${hash}`)
  await expect.poll(() => handle.socket !== null).toBe(true)
  // The connect-time tail hydration replaces `logs`; let it land before sending.
  await firstTail
  await page.waitForTimeout(200)
  return { handle, errors, tail }
}

const panel = (page: Page) => page.getByTestId('logs-panel')

test('a burst of 200 lines is one flush', async ({ page }) => {
  const { handle } = await boot(page, '#/logs')
  await expect(panel(page)).toHaveAttribute('data-log-count', '0')
  await page.evaluate(() => {
    const el = document.querySelector('[data-testid="logs-panel"]')!
    ;(window as any).__logCountChanges = 0
    new MutationObserver((records) => {
      ;(window as any).__logCountChanges += records.length
    }).observe(el, { attributes: true, attributeFilter: ['data-log-count'] })
  })
  for (let i = 1; i <= 200; i += 1) handle.socket!.send(`[pipeline] burst line ${i}`)
  await page.waitForTimeout(600)
  await expect(panel(page)).toHaveAttribute('data-log-count', '200')
  const changes = await page.evaluate(() => (window as any).__logCountChanges)
  expect(changes).toBeGreaterThanOrEqual(1)
  expect(changes).toBeLessThanOrEqual(2)
  await expect(page.locator('.log-line').last()).toHaveText('[pipeline] burst line 200')
})

test('lines received on another tab appear when the logs tab opens, capped at the history', async ({ page }) => {
  expect(maxLogHistory).toBe(5000)
  const { handle } = await boot(page, '#/workspace')
  for (let i = 1; i <= 6000; i += 1) handle.socket!.send(`[pipeline] line ${i}`)
  // Let every frame reach the page while the logs tab is closed.
  await page.waitForTimeout(1000)
  await page.evaluate(() => { window.location.hash = '#/logs' })
  await expect(panel(page)).toHaveAttribute('data-log-count', String(maxLogHistory))
  await expect(page.locator('.log-line').first()).toHaveText(`[pipeline] line ${6000 - maxLogHistory + 1}`)
  await expect(page.getByText('[pipeline] line 6000', { exact: true })).toBeVisible()
})

test('a JSON extraction frame drives the extraction card and a promotion frame is ignored', async ({ page }) => {
  const { handle, errors } = await boot(page, '#/logs')
  handle.socket!.send('[pipeline] before frames')
  await expect(panel(page)).toHaveAttribute('data-log-count', '1')

  handle.socket!.send(JSON.stringify({ kind: 'promotion', corpus_id: 1, promoted: 3 }))
  handle.socket!.send(JSON.stringify({ kind: 'extraction', event: 'started', corpus_id: 1, base_name: 'paper', filename: 'paper.pdf' }))
  await page.waitForTimeout(600)
  await expect(panel(page)).toHaveAttribute('data-log-count', '1')
  await expect(page.locator('.log-line')).toHaveCount(1)

  await page.evaluate(() => { window.location.hash = '#/workspace' })
  await expect(page.getByRole('progressbar', { name: 'Extraction progress' })).toBeVisible()
  const progress = page.getByRole('progressbar', { name: 'Extraction progress' })
  // The card is the run for paper / paper.pdf: a completion for another file
  // leaves it running, the completion for paper.pdf ends it.
  handle.socket!.send(JSON.stringify({ kind: 'extraction', event: 'done', status: 'failed', corpus_id: 1, base_name: 'other', filename: 'other.pdf' }))
  await page.waitForTimeout(400)
  await expect(progress).toBeVisible()
  await expect(page.getByText('Extraction queued...')).toBeVisible()
  handle.socket!.send(JSON.stringify({ kind: 'extraction', event: 'done', status: 'failed', corpus_id: 1, base_name: 'paper', filename: 'paper.pdf' }))
  await expect(progress).toHaveCount(0)
  expect(errors).toEqual([])
})

test('lines buffered before a socket reconnect survive it', async ({ page }) => {
  const { handle } = await boot(page, '#/workspace')
  for (const n of [1, 2, 3]) handle.socket!.send(`[pipeline] kept line ${n}`)
  await page.waitForTimeout(300)
  // The server drops the connection; the app reconnects after 3 s.
  handle.socket!.close({ code: 4000, reason: 'transient' })
  await expect.poll(() => handle.connects, { timeout: 8000 }).toBe(2)
  await page.waitForTimeout(300)
  await page.evaluate(() => { window.location.hash = '#/logs' })
  await expect(panel(page)).toHaveAttribute('data-log-count', '3')
  for (const n of [1, 2, 3]) {
    await expect(page.getByText(`[pipeline] kept line ${n}`, { exact: true })).toHaveCount(1)
  }
})

test('a tail refresh that already holds a buffered line shows it once and keeps the cursor', async ({ page }) => {
  const { handle, tail } = await boot(page, '#/logs')
  let release!: () => void
  const released = new Promise<void>((resolve) => { release = resolve })
  tail.answer = async () => {
    await released
    // The backend persists a line before broadcasting it: the tail has it.
    return { entries: ['[pipeline] old a', '[pipeline] old b', '[pipeline] live line'], next_cursor: 3, has_more: true }
  }
  await page.getByRole('button', { name: 'Refresh from file' }).click()
  await expect.poll(() => tail.cursors.length).toBe(2)
  // While the refresh is in flight the logs tab closes, so the live line stays buffered.
  await page.evaluate(() => { window.location.hash = '#/workspace' })
  handle.socket!.send('[pipeline] live line')
  await page.waitForTimeout(300)
  const answered = page.waitForResponse((res) => new URL(res.url()).pathname === '/api/logs/tail')
  release()
  await answered
  await page.waitForTimeout(100)
  await page.evaluate(() => { window.location.hash = '#/logs' })
  await expect(panel(page)).toHaveAttribute('data-log-count', '3')
  await expect(page.getByText('[pipeline] live line', { exact: true })).toHaveCount(1)
  // "Load older" continues from the tail's cursor: no line was counted twice.
  await page.getByRole('button', { name: 'Load older' }).click()
  await expect.poll(() => tail.cursors.length).toBe(3)
  expect(tail.cursors[2]).toBe('3')
})
