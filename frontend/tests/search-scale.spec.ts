import { expect, test, type Route } from '@playwright/test'

// Exercises the preflight match-count preview and the 100k warning in the
// keyword-search card with the backend mocked (no auth/DB available in CI):
// a search whose preview count is at/above the threshold shows the warning
// with "Cap at" / "Fetch all" actions, and "Cap at" starts a background run
// with the capped maxResults.

async function mockApi(route: Route) {
  const url = new URL(route.request().url())
  const path = url.pathname

  if (path === '/api/auth/me') {
    return route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({
        user: { id: 1, username: 'admin', last_corpus_id: 1, is_admin: true },
        corpora: [{ id: 1, name: 'Local research corpus', role: 'owner', owner_username: 'admin' }],
      }),
    })
  }

  // Permissive defaults so the workspace renders without real data.
  if (path === '/api/corpus') {
    return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ items: [], total: 0, source: 'api', stage_totals: {} }) })
  }
  if (path === '/api/recursion-config') {
    return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ keyword: {} }) })
  }
  if (path === '/api/openalex/quota') {
    return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ available: false }) })
  }
  if (path.startsWith('/api/pipeline/')) {
    return route.fulfill({ status: 200, contentType: 'application/json', body: '{}' })
  }
  if (path.startsWith('/api/downloads')) {
    return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ items: [], total: 0, source: 'api' }) })
  }
  if (path.startsWith('/api/corpora/')) {
    return route.fulfill({ status: 200, contentType: 'application/json', body: '{}' })
  }
  const arrayKeyed: Record<string, string> = {
    '/api/seed/sources': 'sources',
    '/api/ingest/runs': 'runs',
  }
  if (arrayKeyed[path]) {
    return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ [arrayKeyed[path]]: [], total: 0, source: 'api' }) })
  }
  if (path === '/api/ingest/stats') {
    return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ stats: {} }) })
  }

  return route.fulfill({ status: 200, contentType: 'application/json', body: '{}' })
}

test('a search above the threshold shows the warning and "Cap at" starts a capped run', async ({ page }) => {
  let searchBody: any = null
  await page.addInitScript(() => {
    window.localStorage.setItem('rag_feeder_token', 'playwright-token')
  })
  await page.route('**/api/**', async (route) => {
    const url = new URL(route.request().url())
    if (url.pathname === '/api/keyword-search/preview') {
      return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ count: 342118, threshold: 100000 }) })
    }
    if (url.pathname === '/api/keyword-search' && route.request().method() === 'POST') {
      searchBody = route.request().postDataJSON()
      return route.fulfill({ status: 202, contentType: 'application/json', body: JSON.stringify({ runId: 55, status: 'running' }) })
    }
    return mockApi(route)
  })
  await page.goto('/#/workspace')
  const card = page.locator('.seed-intake-card--search')
  await card.getByRole('textbox', { name: 'Query' }).fill('economics')
  await card.getByRole('button', { name: 'Search', exact: true }).click()
  const warning = page.getByTestId('search-warning')
  await expect(warning).toContainText('This search matches 342,118 works')
  await warning.getByRole('button', { name: 'Cap at 10,000' }).click()
  await expect.poll(() => searchBody?.maxResults).toBe(10000)
  await expect(card.locator('p.muted').first()).toContainText('Fetching in the background')
})

test('Reset clears the warning instead of leaving stale "Cap at" / "Fetch all" actions live', async ({ page }) => {
  let searchRequests = 0
  await page.addInitScript(() => {
    window.localStorage.setItem('rag_feeder_token', 'playwright-token')
  })
  await page.route('**/api/**', async (route) => {
    const url = new URL(route.request().url())
    if (url.pathname === '/api/keyword-search/preview') {
      return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ count: 342118, threshold: 100000 }) })
    }
    if (url.pathname === '/api/keyword-search' && route.request().method() === 'POST') {
      searchRequests += 1
      return route.fulfill({ status: 202, contentType: 'application/json', body: JSON.stringify({ runId: 55, status: 'running' }) })
    }
    return mockApi(route)
  })
  await page.goto('/#/workspace')
  const card = page.locator('.seed-intake-card--search')
  await card.getByRole('textbox', { name: 'Query' }).fill('economics')
  await card.getByRole('button', { name: 'Search', exact: true }).click()
  const warning = page.getByTestId('search-warning')
  await expect(warning).toContainText('This search matches 342,118 works')

  await card.getByRole('button', { name: 'Reset' }).click()
  await expect(page.getByTestId('search-warning')).toHaveCount(0)
  await expect(page.getByTestId('search-preview')).toHaveCount(0)
  expect(searchRequests).toBe(0)
})
