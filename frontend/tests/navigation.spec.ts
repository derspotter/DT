import { expect, test, type Route } from '@playwright/test'

function deferred<T = void>() {
  let resolve!: (value: T | PromiseLike<T>) => void
  const promise = new Promise<T>((nextResolve) => {
    resolve = nextResolve
  })
  return { promise, resolve }
}

test('does not snap back to workspace when navigating during post-login load', async ({ page }) => {
  const seedSourcesGate = deferred()
  let seedSourcesRequested = false
  let loggedIn = false

  async function fulfillJson(route: Route, body: unknown) {
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify(body),
    })
  }

  await page.route('**/api/**', async (route) => {
    const url = new URL(route.request().url())
    const path = url.pathname

    if (path === '/api/auth/login') {
      loggedIn = true
      return fulfillJson(route, {
        token: 'playwright-token',
        user: { id: 1, username: 'admin', last_corpus_id: 1, is_admin: true },
      })
    }
    if (path === '/api/auth/me') {
      if (!loggedIn) {
        return route.fulfill({
          status: 401,
          contentType: 'application/json',
          body: JSON.stringify({ error: 'Missing Authorization header' }),
        })
      }
      return fulfillJson(route, {
        user: { id: 1, username: 'admin', last_corpus_id: 1, is_admin: true },
        corpora: [{ id: 1, name: 'Local research corpus', role: 'owner', owner_username: 'admin' }],
      })
    }
    if (path === '/api/seed/sources') {
      seedSourcesRequested = true
      await seedSourcesGate.promise
      return fulfillJson(route, { sources: [] })
    }
    if (path === '/api/recursion-config') {
      return fulfillJson(route, { keyword: { includeDownstream: false, includeUpstream: false, relatedDepthDownstream: 0, relatedDepthUpstream: 0, maxRelated: 30 } })
    }
    if (path === '/api/ingest/stats') return fulfillJson(route, { stats: {} })
    if (path === '/api/ingest/runs') return fulfillJson(route, { runs: [] })
    if (path === '/api/corpus') return fulfillJson(route, { items: [], total: 0, source: 'api', stage_totals: {} })
    if (path === '/api/downloads') return fulfillJson(route, { items: [], total: 0, source: 'api' })
    if (path === '/api/graph') return fulfillJson(route, { nodes: [], edges: [], stats: { node_count: 0, edge_count: 0, relationship_counts: { references: 0, cited_by: 0 } } })
    if (path === '/api/corpora/1/kantropos-assignment') return fulfillJson(route, { assignment: null })

    return fulfillJson(route, {})
  })

  await page.goto('/#/login')
  await page.getByRole('textbox', { name: 'Username' }).fill('admin')
  await page.getByRole('textbox', { name: 'Password' }).fill('password')
  await page.getByRole('button', { name: 'Sign in' }).click()

  await expect(page.getByTestId('side-nav')).toBeVisible()
  await expect.poll(() => seedSourcesRequested).toBeTruthy()

  await page.getByTestId('tab-graph').click()
  await expect(page).toHaveURL(/#\/graph$/)

  seedSourcesGate.resolve()

  await expect(page).toHaveURL(/#\/graph$/)
  await expect(page.getByTestId('tab-graph')).toHaveClass(/active/)
})
