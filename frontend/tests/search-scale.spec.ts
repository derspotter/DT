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

test('search defaults to 50, allows a larger cap, and Reset restores 50', async ({ page }) => {
  let searchBody: any = null
  await page.addInitScript(() => window.localStorage.setItem('rag_feeder_token', 'playwright-token'))
  await page.route('**/api/**', async (route) => {
    const pathname = new URL(route.request().url()).pathname
    if (pathname === '/api/keyword-search/preview') {
      return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ count: 342118, threshold: 100000 }) })
    }
    if (pathname === '/api/keyword-search') {
      searchBody = route.request().postDataJSON()
      return route.fulfill({ status: 202, contentType: 'application/json', body: JSON.stringify({ runId: 55, status: 'running' }) })
    }
    return mockApi(route)
  })
  await page.goto('/#/workspace')
  const card = page.locator('.seed-intake-card--search')
  const cap = card.getByRole('spinbutton', { name: 'Max results' })
  await expect(cap).toHaveValue('50')
  await card.getByRole('textbox', { name: 'Query' }).fill('economics')
  await card.getByRole('button', { name: 'Search', exact: true }).click()
  await expect.poll(() => searchBody?.maxResults).toBe(50)
  await expect(page.getByTestId('search-warning')).toHaveCount(0)
  await cap.fill('1000')
  await card.getByRole('button', { name: 'Search', exact: true }).click()
  await expect.poll(() => searchBody?.maxResults).toBe(1000)
  await cap.fill('')
  await card.getByRole('button', { name: 'Search', exact: true }).click()
  await expect.poll(() => searchBody?.maxResults).toBe(50)
  await card.getByRole('button', { name: 'Reset' }).click()
  await expect(cap).toHaveValue('50')
})

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
  await card.getByRole('spinbutton', { name: 'Max results' }).fill('0')
  await card.getByRole('button', { name: 'Search', exact: true }).click()
  const warning = page.getByTestId('search-warning')
  await expect(warning).toContainText('This search matches 342,118 works')
  await warning.getByRole('button', { name: 'Cap at 10,000' }).click()
  await expect.poll(() => searchBody?.maxResults).toBe(10000)
  await expect(card.locator('p.muted').first()).toContainText('Fetching in the background')
})

test('the warning names the OpenAlex budget and "Cap at budget" caps to it', async ({ page }) => {
  let searchBody: any = null
  await page.addInitScript(() => {
    window.localStorage.setItem('rag_feeder_token', 'playwright-token')
  })
  await page.route('**/api/**', async (route) => {
    const url = new URL(route.request().url())
    if (url.pathname === '/api/openalex/quota') {
      return route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          available: true, stale: false, api_key_present: true,
          remaining: 50, limit: 100000, reset_in_seconds: 3600,
        }),
      })
    }
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
  await card.getByRole('spinbutton', { name: 'Max results' }).fill('0')
  await card.getByRole('button', { name: 'Search', exact: true }).click()
  const warning = page.getByTestId('search-warning')
  // 342,118 / 200 = 1,711 requests; a text search costs 10 credits a request,
  // so 17,110 credits; at 1.1s each that is ~31 minutes, and it is far more
  // than the 50 credits left in today's budget.
  await expect(warning).toContainText('1,711 OpenAlex requests (17,110 credits) and roughly 31.4 minutes')
  await expect(warning).toContainText("That is more than today's remaining OpenAlex budget: 50 of 100,000 credits left.")
  await warning.getByTestId('search-cap-at-budget').click()
  // 50 credits pay for 5 text-search pages x 200 results.
  await expect.poll(() => searchBody?.maxResults).toBe(1000)
})

test('no budget button when the quota is unknown or stale', async ({ page }) => {
  await page.addInitScript(() => {
    window.localStorage.setItem('rag_feeder_token', 'playwright-token')
  })
  await page.route('**/api/**', async (route) => {
    const url = new URL(route.request().url())
    if (url.pathname === '/api/openalex/quota') {
      // A stale snapshot must not drive a cap: it may describe yesterday.
      return route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ available: true, stale: true, api_key_present: true, remaining: 50, limit: 100000, reset_in_seconds: 3600 }),
      })
    }
    if (url.pathname === '/api/keyword-search/preview') {
      return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ count: 342118, threshold: 100000 }) })
    }
    return mockApi(route)
  })
  await page.goto('/#/workspace')
  const card = page.locator('.seed-intake-card--search')
  await card.getByRole('textbox', { name: 'Query' }).fill('economics')
  await card.getByRole('spinbutton', { name: 'Max results' }).fill('0')
  await card.getByRole('button', { name: 'Search', exact: true }).click()
  const warning = page.getByTestId('search-warning')
  await expect(warning).toContainText('This search matches 342,118 works')
  await expect(warning).not.toContainText("today's remaining OpenAlex budget")
  await expect(warning.getByTestId('search-cap-at-budget')).toHaveCount(0)
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
  await card.getByRole('spinbutton', { name: 'Max results' }).fill('0')
  await card.getByRole('button', { name: 'Search', exact: true }).click()
  const warning = page.getByTestId('search-warning')
  await expect(warning).toContainText('This search matches 342,118 works')

  await card.getByRole('button', { name: 'Reset' }).click()
  await expect(page.getByTestId('search-warning')).toHaveCount(0)
  await expect(page.getByTestId('search-preview')).toHaveCount(0)
  expect(searchRequests).toBe(0)
})

function makeSeedCandidates(count: number, startIndex = 0) {
  return Array.from({ length: count }, (_, i) => {
    const n = startIndex + i
    return {
      candidate_key: `c${n}`,
      title: `Economics paper ${n}`,
      authors: [{ name: 'A. Author' }],
      year: 2020,
      source: 'openalex',
      state: 'pending',
      in_corpus: false,
      refs_count: n,
      cited_by_count: n,
    }
  })
}

test('seed table pages through search results, shows every-item selection, and dismisses all', async ({ page }) => {
  const candidatesOffsets: number[] = []
  let dismissBody: any = null
  await page.route('**/api/**', mockApi)
  await page.route('**/api/seed/sources**', async (route) => {
    return route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({
        sources: [{
          id: 'search:77', source_type: 'search', seed_kind: 'search', source_key: '77', label: 'economics',
          subtitle: '', created_at: '2026-09-06T10:00:00Z', candidate_count: 300, state_counts: null,
          removable: true, meta: {}, run: null,
        }],
      }),
    })
  })
  await page.route('**/api/seed/sources/search/77/candidates**', async (route) => {
    const url = new URL(route.request().url())
    const offset = Number(url.searchParams.get('offset') || 0)
    const limit = Number(url.searchParams.get('limit') || 200)
    candidatesOffsets.push(offset)
    // Honour limit/offset like the real route: the workspace now polls seeds
    // continuously, and a background reload re-requests everything loaded so
    // far in one page.
    const count = Math.max(0, Math.min(limit, 300 - offset))
    return route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({ candidates: makeSeedCandidates(count, offset), total: 300, offset, limit }),
    })
  })
  await page.route('**/api/seed/candidates/dismiss', async (route) => {
    dismissBody = route.request().postDataJSON()
    return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ success: true, dismissed: 300 }) })
  })

  await page.goto('/')
  await page.getByText('economics', { exact: false }).click()

  const footer = page.locator('.seed-table-footer')
  await expect(footer).toContainText('Showing 200 of 300')
  await footer.getByRole('button', { name: 'Show more' }).click()
  // A background poll can re-request offset 0 at any time, so assert the page
  // was requested, not that it was the most recent request.
  await expect.poll(() => candidatesOffsets.includes(200)).toBe(true)
  await expect(footer).toHaveCount(0)

  const selectAllCheckbox = page.locator('.seed-source__select input[type="checkbox"]')
  await selectAllCheckbox.check()
  await expect(page.locator('.seed-source__body .table-toolbar-left')).toContainText('All 300 items selected')

  await page.getByRole('button', { name: 'Dismiss selected' }).click()
  await expect.poll(() => dismissBody?.all).toBe(true)
})

test('Show more does not duplicate rows when the second page overlaps the first', async ({ page }) => {
  // A run still filling in shifts rows down under `sr.id DESC`, so the page at
  // offset=200 can repeat 50 rows the table already shows.
  await page.route('**/api/**', mockApi)
  await page.route('**/api/seed/sources**', async (route) => {
    return route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({
        sources: [{
          id: 'search:99', source_type: 'search', seed_kind: 'search', source_key: '99', label: 'overlapping',
          subtitle: '', created_at: '2026-09-06T10:00:00Z', candidate_count: 250, state_counts: null,
          removable: true, meta: {}, run: null,
        }],
      }),
    })
  })
  await page.route('**/api/seed/sources/search/99/candidates**', async (route) => {
    const url = new URL(route.request().url())
    const offset = Number(url.searchParams.get('offset') || 0)
    const limit = Number(url.searchParams.get('limit') || 200)
    // Page 1: c0..c199. Page 2 (offset 200) starts 50 rows earlier: c150..c249.
    const start = offset === 0 ? 0 : offset - 50
    const count = Math.max(0, Math.min(limit, 250 - start))
    return route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({ candidates: makeSeedCandidates(count, start), total: 250, offset, limit }),
    })
  })

  await page.goto('/')
  await page.getByText('overlapping', { exact: false }).click()
  const rows = page.locator('.seed-candidate-table .table-row.clickable')
  await expect(rows).toHaveCount(200)
  await page.locator('.seed-table-footer').getByRole('button', { name: 'Show more' }).click()
  // Union of c0..c199 and c150..c249 is 250 rows, not 200 + 100 = 300.
  await expect(rows).toHaveCount(250)
  // And each of the 50 overlapping rows renders exactly once.
  await expect(rows.filter({ hasText: 'Economics paper 150' })).toHaveCount(1)
  await expect(rows.filter({ hasText: 'Economics paper 199' })).toHaveCount(1)
})

test('the toolbar "Select all" selects the whole seed, like the header checkbox', async ({ page }) => {
  await page.route('**/api/**', mockApi)
  await page.route('**/api/seed/sources**', async (route) => {
    return route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({
        sources: [{
          id: 'search:66', source_type: 'search', seed_kind: 'search', source_key: '66', label: 'toolbar seed',
          subtitle: '', created_at: '2026-09-06T10:00:00Z', candidate_count: 300, state_counts: null,
          removable: true, meta: {}, run: null,
        }],
      }),
    })
  })
  await page.route('**/api/seed/sources/search/66/candidates**', async (route) => {
    const url = new URL(route.request().url())
    const offset = Number(url.searchParams.get('offset') || 0)
    const limit = Number(url.searchParams.get('limit') || 200)
    const count = Math.max(0, Math.min(limit, 300 - offset))
    return route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({ candidates: makeSeedCandidates(count, offset), total: 300, offset, limit }),
    })
  })

  await page.goto('/')
  await page.getByText('toolbar seed', { exact: false }).click()
  await page.locator('.seed-source__body').getByRole('button', { name: 'Select all', exact: true }).click()
  // The whole seed (300), not just the loaded page (200).
  await expect(page.locator('.seed-source__body .table-toolbar-left')).toContainText('All 300 items selected')
})

test('promoting while a seed filter is active forwards q to the promote request', async ({ page }) => {
  let promoteBody: any = null
  await page.route('**/api/**', mockApi)
  await page.route('**/api/seed/sources**', async (route) => {
    return route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({
        sources: [{
          id: 'search:88', source_type: 'search', seed_kind: 'search', source_key: '88', label: 'sociology',
          subtitle: '', created_at: '2026-09-06T10:00:00Z', candidate_count: 5, state_counts: null,
          removable: true, meta: {}, run: null,
        }],
      }),
    })
  })
  await page.route('**/api/seed/sources/search/88/candidates**', async (route) => {
    return route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({ candidates: makeSeedCandidates(5), total: 5, offset: 0, limit: 200 }),
    })
  })
  await page.route('**/api/seed/sources/search/88/promote', async (route) => {
    promoteBody = route.request().postDataJSON()
    return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ success: true }) })
  })

  await page.goto('/')
  await page.getByLabel('Filter seed items by title, author or publication').fill('kinship')
  await page.locator('.seed-source__action--promote').click()
  await expect.poll(() => promoteBody?.q).toBe('kinship')
  expect(promoteBody.candidateKeys).toEqual([])
})

test('an idle expanded seed is not re-requested on every workspace poll', async ({ page }) => {
  // The workspace polls the (cheap) seed list continuously. The expanded
  // seed's candidates must NOT be re-pulled with it while nothing is running:
  // that route resolves the whole seed's state, so a forever-poll was four
  // full-seed passes per cycle.
  let candidateRequests = 0
  const summaryFlags: (string | null)[] = []
  let sourceRequests = 0
  await page.route('**/api/**', mockApi)
  await page.route('**/api/seed/sources**', async (route) => {
    sourceRequests += 1
    return route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({
        sources: [{
          id: 'search:41', source_type: 'search', seed_kind: 'search', source_key: '41', label: 'idle seed',
          subtitle: '', created_at: '2026-09-06T10:00:00Z', candidate_count: 3, state_counts: null,
          removable: true, meta: {}, run: { status: 'done', fetched_count: 3, expected_count: 3, error: null },
        }],
      }),
    })
  })
  await page.route('**/api/seed/sources/search/41/candidates**', async (route) => {
    candidateRequests += 1
    summaryFlags.push(new URL(route.request().url()).searchParams.get('summary'))
    return route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({ candidates: makeSeedCandidates(3), total: 3, offset: 0, limit: 200 }),
    })
  })

  await page.goto('/')
  await page.getByText('idle seed', { exact: false }).click()
  await expect(page.locator('.seed-candidate-table .table-row.clickable')).toHaveCount(3)
  const afterExpand = candidateRequests
  const sourcesAfterExpand = sourceRequests

  await page.waitForTimeout(7000)
  // The seed list itself kept polling (that is the I2 fix)...
  expect(sourceRequests).toBeGreaterThan(sourcesAfterExpand + 1)
  // ...but the expensive candidates route was not touched again.
  expect(candidateRequests).toBe(afterExpand)
  // And the pills survived without it: reconcileSeedSourceCandidates derives
  // them from the rows already loaded, not from source_summary.
  await expect(page.locator('.pill-row').first()).toContainText('3 items')
})

test('an expanded seed with a running run keeps refreshing, with a light summary', async ({ page }) => {
  let candidateRequests = 0
  const summaryFlags: (string | null)[] = []
  await page.route('**/api/**', mockApi)
  await page.route('**/api/seed/sources**', async (route) => {
    return route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({
        sources: [{
          id: 'search:42', source_type: 'search', seed_kind: 'search', source_key: '42', label: 'live seed',
          subtitle: '', created_at: '2026-09-06T10:00:00Z', candidate_count: 3, state_counts: null,
          removable: true, meta: {}, run: { status: 'running', fetched_count: 3, expected_count: 999, error: null },
        }],
      }),
    })
  })
  await page.route('**/api/seed/sources/search/42/candidates**', async (route) => {
    candidateRequests += 1
    summaryFlags.push(new URL(route.request().url()).searchParams.get('summary'))
    return route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({ candidates: makeSeedCandidates(3), total: 3, offset: 0, limit: 200 }),
    })
  })

  await page.goto('/')
  await page.getByText('live seed', { exact: false }).click()
  await expect(page.locator('.seed-candidate-table .table-row.clickable')).toHaveCount(3)
  const afterExpand = candidateRequests
  // A running run keeps the rows current.
  await expect.poll(() => candidateRequests, { timeout: 10_000 }).toBeGreaterThan(afterExpand)
  // The initial expand asks for the full summary; the background polls do not.
  expect(summaryFlags[0]).toBeNull()
  expect(summaryFlags.slice(afterExpand)).toContain('light')
})

test('a running seed shows progress and settles to done', async ({ page }) => {
  // The transition is driven by the test, not by how many times the 2s poll
  // happens to have fired: a request-count trigger made this flake whenever a
  // poll landed early or late.
  const PHASES = {
    first: { status: 'running', fetched_count: 400, expected_count: 1200, error: null },
    second: { status: 'running', fetched_count: 800, expected_count: 1200, error: null },
    done: { status: 'done', fetched_count: 1200, expected_count: 1200, error: null },
  }
  let phase: keyof typeof PHASES = 'first'
  const served: string[] = []
  await page.route('**/api/**', mockApi)
  await page.route('**/api/seed/sources**', async (route) => {
    const run = PHASES[phase]
    served.push(`${run.status}:${run.fetched_count}`)
    return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ sources: [{
      id: 'search:55', source_type: 'search', seed_kind: 'search', source_key: '55', label: 'economics', subtitle: '',
      created_at: '2026-09-06T10:00:00Z', candidate_count: run.fetched_count, state_counts: null, removable: true, meta: {}, run,
    }] }) })
  })
  await page.goto('/')
  const status = page.getByTestId('seed-run-status')
  await expect(status).toContainText('fetching 400 of 1,200')
  phase = 'second'
  await expect(status).toContainText('fetching 800 of 1,200', { timeout: 10_000 })
  phase = 'done'
  await expect(status).toHaveCount(0, { timeout: 10_000 })
  // The sequence, not the intermediate counts: it started running, stayed
  // running while the run advanced, and the last thing served was 'done'.
  expect(served[0]).toBe('running:400')
  expect(served).toContain('running:800')
  expect(served[served.length - 1]).toBe('done:1200')
})
