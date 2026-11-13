import { readFileSync } from 'node:fs'
import { expect, test, type Page, type WebSocketRoute } from '@playwright/test'
import { fulfil, mePayload, mockLogSocket } from './helpers/mockApi'

// The log socket's lines are buffered and flushed into `logs` at most once per
// 250 ms; JSON frames are parsed once (extraction events drive the extraction
// card, other JSON frames are ignored) and never become log rows.

const maxLogHistory = Number(
  readFileSync(new URL('../src/App.svelte', import.meta.url), 'utf8').match(/const maxLogHistory = (\d+)/)![1]
)

type TailBody = { entries: string[]; next_cursor: number; has_more: boolean }

// `file`, when given, is the persisted log: every tail request is answered
// from it by offset (cursor = newest lines to skip), as the backend pages.
async function boot(page: Page, hash: string, file: string[] | null = null) {
  const handle: { socket: WebSocketRoute | null; connects: number } = { socket: null, connects: 0 }
  const errors: string[] = []
  // `tail.answer`, when set, answers the next /api/logs/tail request (and is
  // then cleared); every tail request's cursor is recorded.
  // `tail.hold`, when set, delays the next file-backed answer until it resolves.
  const tail: {
    answer: (() => Promise<TailBody>) | null
    cursors: string[]
    lines: string[]
    file: string[] | null
    hold: Promise<void> | null
  } = { answer: null, cursors: [], lines: [], file, hold: null }
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
      return fulfil(route, mePayload())
    }
    if (path === '/api/logs/tail') {
      tail.cursors.push(url.searchParams.get('cursor') || '')
      tail.lines.push(url.searchParams.get('lines') || '')
      if (tail.file) {
        const skip = Number(url.searchParams.get('cursor') || 0)
        const lines = Number(url.searchParams.get('lines') || 200)
        const end = Math.max(0, tail.file.length - skip)
        const start = Math.max(0, end - lines)
        const body = { entries: tail.file.slice(start, end), next_cursor: skip + (end - start), has_more: start > 0 }
        const hold = tail.hold
        tail.hold = null
        if (hold) await hold
        return fulfil(route, body)
      }
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
const gapText = /^Connection lost \d\d:\d\d:\d\d–\d\d:\d\d:\d\d\. Lines from that period may be missing\.$/
// File lines and gap markers in view order.
const renderedTexts = (page: Page) => page.locator('.log-box').locator('.log-line, .log-gap__text').allTextContents()

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

async function reconnect(page: Page, handle: { socket: WebSocketRoute | null; connects: number }) {
  const before = handle.connects
  // The server drops the connection; the app reconnects after 3 s.
  handle.socket!.close({ code: 4000, reason: 'transient' })
  await expect.poll(() => handle.connects, { timeout: 8000 }).toBe(before + 1)
  await page.waitForTimeout(300)
}

test('lines buffered before a socket reconnect survive it and are not re-hydrated', async ({ page }) => {
  const { handle, tail } = await boot(page, '#/workspace')
  for (const n of [1, 2, 3]) handle.socket!.send(`[pipeline] kept line ${n}`)
  await page.waitForTimeout(300)
  await reconnect(page, handle)
  // Only the first connect hydrated: the reconnect found buffered content.
  expect(tail.cursors).toHaveLength(1)
  await page.evaluate(() => { window.location.hash = '#/logs' })
  // The three lines plus the reconnect's gap marker.
  await expect(panel(page)).toHaveAttribute('data-log-count', '4')
  for (const n of [1, 2, 3]) {
    await expect(page.getByText(`[pipeline] kept line ${n}`, { exact: true })).toHaveCount(1)
  }
  await expect(page.locator('.log-gap')).toHaveCount(1)
  await expect(page.locator('.log-gap__text')).toHaveText(gapText)
  expect(tail.cursors).toHaveLength(1)
})

test('500 buffered lines survive a reconnect whole', async ({ page }) => {
  const { handle, tail } = await boot(page, '#/workspace')
  for (let i = 1; i <= 500; i += 1) handle.socket!.send(`[pipeline] buffered ${i}`)
  await page.waitForTimeout(500)
  // A tail answer, if one were requested, would be the last 200 file lines.
  tail.answer = async () => ({
    entries: Array.from({ length: 200 }, (_, i) => `[pipeline] buffered ${301 + i}`),
    next_cursor: 200,
    has_more: true,
  })
  await reconnect(page, handle)
  await page.evaluate(() => { window.location.hash = '#/logs' })
  // The 500 lines plus the reconnect's gap marker after them.
  await expect(panel(page)).toHaveAttribute('data-log-count', '501')
  await expect(page.locator('.log-line')).toHaveCount(500)
  await expect(page.locator('.log-line').first()).toHaveText('[pipeline] buffered 1')
  await expect(page.locator('.log-line').last()).toHaveText('[pipeline] buffered 500')
  await expect(page.locator('.log-gap')).toHaveCount(1)
  expect(tail.cursors).toHaveLength(1)
})

test('Refresh from file replaces the rendered lines with the tail and continues from its cursor', async ({ page }) => {
  const { handle, tail } = await boot(page, '#/logs')
  handle.socket!.send('[pipeline] live line')
  await expect(panel(page)).toHaveAttribute('data-log-count', '1')
  // The backend persists a line before broadcasting it: the tail has it.
  tail.answer = async () => ({
    entries: ['[pipeline] old a', '[pipeline] old b', '[pipeline] live line'],
    next_cursor: 3,
    has_more: true,
  })
  await page.getByRole('button', { name: 'Refresh from file' }).click()
  await expect(panel(page)).toHaveAttribute('data-log-count', '3')
  await expect(page.getByText('[pipeline] live line', { exact: true })).toHaveCount(1)
  await page.getByRole('button', { name: 'Load older' }).click()
  await expect.poll(() => tail.cursors.length).toBe(3)
  expect(tail.cursors[2]).toBe('3')
})

test('Load older pages through the file without gaps or duplicates while lines arrive', async ({ page }) => {
  const line = (n: number) => `[pipeline] file line ${n}`
  const file = Array.from({ length: 450 }, (_, i) => line(i + 1))
  const { handle, tail } = await boot(page, '#/logs', file)
  await expect(panel(page)).toHaveAttribute('data-log-count', '200')
  await expect(page.locator('.log-line').first()).toHaveText(line(251))

  // Line 451 is persisted and broadcast; it is still buffered when
  // "Load older" is clicked.
  let release: () => void = () => {}
  tail.hold = new Promise<void>((resolve) => { release = resolve })
  file.push(line(451))
  handle.socket!.send(line(451))
  await page.getByRole('button', { name: 'Load older' }).click()
  await expect.poll(() => tail.cursors.length).toBe(2)
  // Line 452 arrives (and is flushed) while the older page is in flight.
  file.push(line(452))
  handle.socket!.send(line(452))
  await page.waitForTimeout(500)
  release()
  await expect(panel(page)).toHaveAttribute('data-log-count', '402')

  await page.getByRole('button', { name: 'Load older' }).click()
  await expect(panel(page)).toHaveAttribute('data-log-count', '452')
  await expect(page.getByRole('button', { name: 'Load older' })).toBeDisabled()

  expect(tail.cursors).toEqual(['0', '201', '402'])
  const texts = await page.locator('.log-line').allTextContents()
  expect(texts).toEqual(Array.from({ length: 452 }, (_, i) => line(i + 1)))
})

test('a tail answer requested before a log type change is dropped', async ({ page }) => {
  const { tail } = await boot(page, '#/logs')
  let release: () => void = () => {}
  const held = new Promise<void>((resolve) => { release = resolve })
  tail.answer = async () => {
    await held
    return { entries: ['[pipeline] stale line'], next_cursor: 1, has_more: true }
  }
  await page.getByRole('button', { name: 'Refresh from file' }).click()
  await expect.poll(() => tail.cursors.length).toBe(2)
  await page.getByTestId('logs-type-select').selectOption('app')
  await expect.poll(() => tail.cursors.length).toBe(3)
  await page.waitForTimeout(200)
  release()
  await page.waitForTimeout(500)
  await expect(page.getByText('[pipeline] stale line', { exact: true })).toHaveCount(0)
  await expect(panel(page)).toHaveAttribute('data-log-count', '0')
})

// A view of `shown` lines: the 200-line tail of a 6,000-line file, then live
// lines up to `shown`. The cursor then counts every shown line.
async function fillView(page: Page, shown: number) {
  const fileLine = (n: number) => `[pipeline] file line ${n}`
  const file = Array.from({ length: 6000 }, (_, i) => fileLine(i + 1))
  const booted = await boot(page, '#/logs', file)
  await expect(panel(page)).toHaveAttribute('data-log-count', '200')
  for (let i = 1; i <= shown - 200; i += 1) booted.handle.socket!.send(`[pipeline] live ${i}`)
  await expect(panel(page)).toHaveAttribute('data-log-count', String(shown))
  await expect(panel(page)).toHaveAttribute('data-logs-cursor', String(shown))
  return { ...booted, fileLine }
}

const fullViewText = 'The log view holds 5,000 lines. Older lines stay in the log file.'

test('Load older sends nothing when the view is full and says so', async ({ page }) => {
  const { tail } = await fillView(page, maxLogHistory)
  const loadOlder = page.getByRole('button', { name: 'Load older' })
  await expect(loadOlder).toBeEnabled()
  await loadOlder.click()
  await expect(panel(page)).toContainText(fullViewText)
  await page.waitForTimeout(300)
  expect(tail.cursors).toEqual(['0'])
  await expect(panel(page)).toHaveAttribute('data-log-count', String(maxLogHistory))
})

test('Load older admits only what still fits when the answer arrives', async ({ page }) => {
  const { handle, tail, fileLine } = await fillView(page, 4950)
  let release: () => void = () => {}
  tail.hold = new Promise<void>((resolve) => { release = resolve })
  await page.getByRole('button', { name: 'Load older' }).click()
  await expect.poll(() => tail.cursors.length).toBe(2)
  expect(tail.cursors[1]).toBe('4950')
  expect(tail.lines[1]).toBe('50')
  for (let i = 1; i <= 30; i += 1) handle.socket!.send(`[pipeline] late ${i}`)
  await expect(panel(page)).toHaveAttribute('data-log-count', '4980')
  await expect(panel(page)).toHaveAttribute('data-logs-cursor', '4980')
  release()
  await expect(panel(page)).toContainText('Loaded 20 of 50 older log lines; the view holds 5,000 lines.')
  await expect(panel(page)).toHaveAttribute('data-log-count', '5000')
  // The answer was file lines 1001-1050; the newest 20 sit next to the view.
  await expect(page.locator('.log-line').first()).toHaveText(fileLine(1031))
  await expect(page.getByText(fileLine(1030), { exact: true })).toHaveCount(0)
  await expect(page.locator('.log-line').last()).toHaveText('[pipeline] late 30')
  // 30 flushed live lines + 20 admitted, not + 50.
  await expect(panel(page)).toHaveAttribute('data-logs-cursor', '5000')
})

test('Load older admits nothing when live lines fill the view before the answer', async ({ page }) => {
  const { handle, tail, fileLine } = await fillView(page, 4950)
  let release: () => void = () => {}
  tail.hold = new Promise<void>((resolve) => { release = resolve })
  await page.getByRole('button', { name: 'Load older' }).click()
  await expect.poll(() => tail.cursors.length).toBe(2)
  expect(tail.lines[1]).toBe('50')
  for (let i = 1; i <= 50; i += 1) handle.socket!.send(`[pipeline] late ${i}`)
  await expect(panel(page)).toHaveAttribute('data-log-count', '5000')
  release()
  await expect(panel(page)).toContainText(fullViewText)
  await expect(panel(page)).toHaveAttribute('data-log-count', '5000')
  await expect(page.getByText(fileLine(1050), { exact: true })).toHaveCount(0)
  await expect(page.locator('.log-line').last()).toHaveText('[pipeline] late 50')
  await expect(panel(page)).toHaveAttribute('data-logs-cursor', '5000')
})

test('a reconnect keeps the view and adds one gap marker with Reload logs', async ({ page }) => {
  const { handle, tail } = await boot(page, '#/logs')
  for (const text of ['a', 'b', 'c']) handle.socket!.send(text)
  await expect(panel(page)).toHaveAttribute('data-log-count', '3')
  const before = handle.connects
  handle.socket!.close({ code: 1006 })
  await expect.poll(() => handle.connects, { timeout: 8000 }).toBe(before + 1)
  handle.socket!.send('d')
  await expect(panel(page)).toHaveAttribute('data-log-count', '5')
  const texts = await renderedTexts(page)
  expect(texts.slice(0, 3)).toEqual(['a', 'b', 'c'])
  expect(texts[3]).toMatch(gapText)
  expect(texts[4]).toBe('d')
  expect(texts).toHaveLength(5)
  expect(tail.cursors).toEqual(['0'])

  tail.answer = async () => ({ entries: ['tail 1', 'tail 2'], next_cursor: 2, has_more: false })
  await page.getByTestId('logs-reload').click()
  await expect(panel(page)).toHaveAttribute('data-log-count', '2')
  expect(await renderedTexts(page)).toEqual(['tail 1', 'tail 2'])
  await expect(page.locator('.log-gap')).toHaveCount(0)
  expect(tail.cursors).toEqual(['0', '0'])
})

test('after a reconnect gap Load older waits for Reload logs (D7, K-M1)', async ({ page }) => {
  const line = (n: number) => `[pipeline] file line ${n}`
  const file = Array.from({ length: 450 }, (_, i) => line(i + 1))
  const { handle, tail } = await boot(page, '#/logs', file)
  await expect(panel(page)).toHaveAttribute('data-log-count', '200')
  const older = page.getByRole('button', { name: 'Load older' })
  await expect(older).toBeEnabled()
  await expect(page.getByTestId('logs-older-hint')).toHaveCount(0)
  await reconnect(page, handle)
  file.push(line(451))
  handle.socket!.send(line(451))
  // 201 file lines and the marker; the cursor cannot count lines written during the gap.
  await expect(panel(page)).toHaveAttribute('data-log-count', '202')
  await expect(page.locator('.log-gap')).toHaveCount(1)
  await expect(older).toBeDisabled()
  await expect(page.getByTestId('logs-older-hint')).toHaveText('Reload logs first')
  expect(tail.cursors).toHaveLength(1)

  await page.getByTestId('logs-reload').click()
  await expect(panel(page)).toHaveAttribute('data-log-count', '200')
  await expect(page.locator('.log-gap')).toHaveCount(0)
  await expect(page.getByTestId('logs-older-hint')).toHaveCount(0)
  await expect(older).toBeEnabled()
  await older.click()
  await expect.poll(() => tail.cursors.length).toBe(3)
  expect(tail.cursors[2]).toBe('200')
  await expect(panel(page)).toHaveAttribute('data-log-count', '400')
  // No line twice: the reload's tail (252-451) and the older page (52-251) meet.
  await expect(page.locator('.log-line').first()).toHaveText(line(52))
  await expect(page.getByText(line(252), { exact: true })).toHaveCount(1)
})

test('a tail requested before a reconnect gap does not clear the gap (K-M1 round 2)', async ({ page }) => {
  const line = (n: number) => `[pipeline] file line ${n}`
  const file = Array.from({ length: 450 }, (_, i) => line(i + 1))
  const { handle, tail } = await boot(page, '#/logs', file)
  await expect(panel(page)).toHaveAttribute('data-log-count', '200')
  const older = page.getByRole('button', { name: 'Load older' })
  let release: () => void = () => {}
  tail.hold = new Promise<void>((resolve) => { release = resolve })
  await page.getByRole('button', { name: 'Refresh from file' }).click()
  await expect.poll(() => tail.cursors.length).toBe(2)
  // The gap opens while that tail request is still out.
  await reconnect(page, handle)
  await expect(older).toBeDisabled()
  release()
  await expect(panel(page)).toContainText('Loaded 200 recent log lines.')
  await page.waitForTimeout(300)
  await expect(older).toBeDisabled()
  await expect(page.getByTestId('logs-older-hint')).toHaveText('Reload logs first')
  // A reload started after the gap clears it.
  await page.getByRole('button', { name: 'Refresh from file' }).click()
  await expect.poll(() => tail.cursors.length).toBe(3)
  await expect(older).toBeEnabled()
  await expect(page.getByTestId('logs-older-hint')).toHaveCount(0)
})
