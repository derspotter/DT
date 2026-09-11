import { expect, test, type Route } from '@playwright/test'

// The Topic field: type-ahead against the (mocked) topics proxy, chips for the
// chosen topics, and a topic-only run that is priced at 1 credit a page.

const TOPICS = [
  { id: 'T10208', label: 'Labor market dynamics and wage inequality', hint: 'Wages, employment and inequality.', works_count: 91061 },
  { id: 'T11421', label: 'Labor Movements and Unions', hint: 'Union revitalisation and labour relations.', works_count: 210535 },
]

function json(route: Route, body: unknown, status = 200) {
  return route.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) })
}

function mockApi(route: Route, state: { searchBody: any; previewBody: any; topicQueries: string[]; corpusId?: number }) {
  const url = new URL(route.request().url())
  const path = url.pathname
  if (path === '/api/auth/me') {
    return json(route, {
      user: { id: 1, username: 'admin', last_corpus_id: state.corpusId || 1, is_admin: true },
      corpora: [1, 2].map(id => ({ id, name: `Research corpus ${id}`, role: 'owner', owner_username: 'admin' })),
    })
  }
  if (path === '/api/corpora/2/select') {
    state.corpusId = 2
    return json(route, { selected: 2 })
  }
  if (path === '/api/auth/login') return json(route, { token: 'new-session-token' })
  if (path === '/api/openalex/topics') {
    const q = (url.searchParams.get('q') || '').toLowerCase()
    state.topicQueries.push(q)
    return json(route, { topics: TOPICS.filter((t) => t.label.toLowerCase().includes(q)) })
  }
  if (path === '/api/openalex/quota') {
    return json(route, { available: true, stale: false, api_key_present: true, remaining: 50, limit: 10000, reset_in_seconds: 3600 })
  }
  if (path === '/api/keyword-search/preview') {
    state.previewBody = route.request().postDataJSON()
    const creditsPerPage = String(state.previewBody?.query || '').trim() ? 10 : 1
    return json(route, { count: 301596, threshold: 100000, creditsPerPage })
  }
  if (path === '/api/keyword-search' && route.request().method() === 'POST') {
    state.searchBody = route.request().postDataJSON()
    return json(route, { runId: 77, status: 'running' }, 202)
  }
  if (path === '/api/corpus') return json(route, { items: [], total: 0, source: 'api', stage_totals: {} })
  if (path === '/api/recursion-config') return json(route, { keyword: {} })
  if (path === '/api/seed/sources') return json(route, { sources: [], total: 0, source: 'api' })
  if (path === '/api/ingest/runs') return json(route, { runs: [], total: 0, source: 'api' })
  if (path === '/api/ingest/stats') return json(route, { stats: {} })
  if (path.startsWith('/api/downloads')) return json(route, { items: [], total: 0, source: 'api' })
  return json(route, {})
}

test('topics are picked from the type-ahead and a topic-only run is priced at 1 credit a page', async ({ page }) => {
  const state = { searchBody: null as any, previewBody: null as any, topicQueries: [] as string[] }
  await page.addInitScript(() => { window.localStorage.setItem('rag_feeder_token', 'playwright-token') })
  await page.route('**/api/**', (route) => mockApi(route, state))
  await page.goto('/#/workspace')

  const card = page.locator('.seed-intake-card--search')
  const topicInput = card.getByRole('combobox', { name: /^Topic/ })
  await topicInput.fill('labor')
  const listbox = card.getByRole('listbox', { name: 'Matching topics' })
  await expect(listbox.getByRole('option')).toHaveCount(2)
  await expect(listbox).toContainText('91,061 works')
  await listbox.getByRole('option').filter({ hasText: 'wage inequality' }).getByRole('button').click()
  await expect(card.getByTestId('topic-chip')).toHaveCount(1)
  await expect(card.getByTestId('topic-chip')).toContainText('Labor market dynamics and wage inequality')

  // Keyboard path: type, Enter picks the highlighted suggestion.
  await topicInput.fill('union')
  await expect(listbox.getByRole('option')).toHaveCount(1)
  await topicInput.press('Enter')
  await expect(card.getByTestId('topic-chip')).toHaveCount(2)
  // A picked topic is not offered again.
  await topicInput.fill('labor')
  await expect(listbox.getByRole('option')).toHaveCount(0)
  await topicInput.fill('')

  // Topic-only: relevance is meaningless without text, so the sort falls back.
  await card.getByRole('spinbutton', { name: 'Max results' }).fill('0')
  await expect(card.getByRole('combobox', { name: 'Sort' })).toHaveValue('cited_by_count')
  await expect(card.getByRole('combobox', { name: 'Sort' }).getByRole('option', { name: /Relevance/ })).toHaveCount(0)

  await card.getByRole('button', { name: 'Search', exact: true }).click()
  await expect.poll(() => state.previewBody?.topics).toEqual([
    { id: 'T10208', label: 'Labor market dynamics and wage inequality' },
    { id: 'T11421', label: 'Labor Movements and Unions' },
  ])
  const warning = page.getByTestId('search-warning')
  // 301,596 / 200 = 1,508 pages at 1 credit each.
  await expect(warning).toContainText('1,508 OpenAlex requests (1,508 credits)')
  await expect(warning).toContainText('50 of 10,000 credits left')
  await warning.getByTestId('search-cap-at-budget').click()
  // 50 credits pay for 50 topic pages x 200.
  await expect.poll(() => state.searchBody?.maxResults).toBe(10000)
  expect(state.searchBody.topics).toHaveLength(2)
  expect(state.searchBody.sort).toBe('cited_by_count')
  expect(state.searchBody.query).toBe('')
})

test('a chip can be removed and Reset clears the topics', async ({ page }) => {
  const state = { searchBody: null as any, previewBody: null as any, topicQueries: [] as string[] }
  await page.addInitScript(() => { window.localStorage.setItem('rag_feeder_token', 'playwright-token') })
  await page.route('**/api/**', (route) => mockApi(route, state))
  await page.goto('/#/workspace')
  const card = page.locator('.seed-intake-card--search')
  const topicInput = card.getByRole('combobox', { name: /^Topic/ })
  await topicInput.fill('labor')
  const listbox = card.getByRole('listbox', { name: 'Matching topics' })
  await listbox.getByRole('option').first().getByRole('button').click()
  await topicInput.fill('union')
  await expect(listbox.getByRole('option')).toHaveCount(1)
  await topicInput.press('Enter')
  await expect(card.getByTestId('topic-chip')).toHaveCount(2)
  await card.getByRole('button', { name: /^Remove topic Labor Movements/ }).click()
  await expect(card.getByTestId('topic-chip')).toHaveCount(1)
  await card.getByRole('button', { name: 'Reset' }).click()
  await expect(card.getByTestId('topic-chip')).toHaveCount(0)
  // With no query and no topic, relevance is offered again.
  await expect(card.getByRole('combobox', { name: 'Sort' }).getByRole('option', { name: /Relevance/ })).toHaveCount(1)
})

// Hold an actual request open so the response arrives AFTER the UI action.
// Immediate mocks cannot expose this race.
for (const action of ['reset', 'clear', 'escape', 'blur', 'workspace'] as const) {
  test(`a late topic response cannot reopen suggestions after ${action}`, async ({ page }) => {
    const state = { searchBody: null as any, previewBody: null as any, topicQueries: [] as string[] }
    await page.addInitScript(() => localStorage.setItem('rag_feeder_token', 'playwright-token'))
    await page.route('**/api/**', route => mockApi(route, state))
    let pending: Route | undefined
    await page.route('**/api/openalex/topics?*', route => { pending = route })
    await page.goto('/#/workspace')
    const card = page.locator('.seed-intake-card--search')
    const input = card.getByRole('combobox', { name: /^Topic/ })
    await input.fill('labor')
    await expect.poll(() => Boolean(pending)).toBe(true)
    if (action === 'reset') await card.getByRole('button', { name: 'Reset', exact: true }).click()
    if (action === 'clear') await input.fill('')
    if (action === 'escape') await input.press('Escape')
    if (action === 'blur') await card.getByRole('textbox', { name: 'Query', exact: true }).focus()
    if (action === 'workspace') await page.locator('.header-corpus-picker select').selectOption('2')
    // Let the intentionally delayed blur-close run before delivering the reply.
    await page.waitForTimeout(250)
    const response = page.waitForResponse(r => r.url().includes('/api/openalex/topics?'))
    await json(pending!, { topics: TOPICS })
    await response
    await page.waitForTimeout(100)
    await expect(card.getByRole('listbox', { name: 'Matching topics' })).toHaveCount(0)
    if (['reset', 'clear', 'workspace'].includes(action)) await expect(input).toHaveValue('')
  })
}

test('a stale unauthorized topic response cannot log out the current session', async ({ page }) => {
  const state = { searchBody: null as any, previewBody: null as any, topicQueries: [] as string[] }
  await page.addInitScript(() => localStorage.setItem('rag_feeder_token', 'playwright-token'))
  await page.route('**/api/**', route => mockApi(route, state))
  let pending: Route | undefined
  await page.route('**/api/openalex/topics?*', route => { pending = route })
  await page.goto('/#/workspace')
  const card = page.locator('.seed-intake-card--search')
  await card.getByRole('combobox', { name: /^Topic/ }).fill('labor')
  await expect.poll(() => Boolean(pending)).toBe(true)
  await card.getByRole('button', { name: 'Reset', exact: true }).click()
  await json(pending!, { error: 'old request unauthorized' }, 401)
  await page.waitForTimeout(250)
  await expect(card).toBeVisible()
  expect(await page.evaluate(() => localStorage.getItem('rag_feeder_token'))).toBe('playwright-token')
})

for (const action of ['workspace', 'logout'] as const) {
  test(`topic selection is cleared on ${action}`, async ({ page }) => {
    const state = { searchBody: null as any, previewBody: null as any, topicQueries: [] as string[] }
    await page.addInitScript(() => localStorage.setItem('rag_feeder_token', 'playwright-token'))
    await page.route('**/api/**', route => mockApi(route, state))
    await page.goto('/#/workspace')
    const card = page.locator('.seed-intake-card--search')
    const input = card.getByRole('combobox', { name: /^Topic/ })
    await input.fill('labor')
    await card.getByRole('listbox').getByRole('option').first().getByRole('button').click()
    await expect(card.getByTestId('topic-chip')).toHaveCount(1)
    await input.fill('u')
    if (action === 'workspace') {
      await page.locator('.header-corpus-picker select').selectOption('2')
      await expect(page.locator('.header-corpus-picker select')).toHaveValue('2')
    } else {
      await page.getByRole('button', { name: 'Log out', exact: true }).click()
      await page.getByLabel('Username', { exact: true }).fill('admin')
      await page.getByLabel('Password', { exact: true }).fill('test-password')
      await page.getByRole('button', { name: 'Sign in', exact: true }).click()
    }
    await expect(card).toBeVisible()
    await expect(card.getByTestId('topic-chip')).toHaveCount(0)
    await expect(input).toHaveValue('')
  })
}
