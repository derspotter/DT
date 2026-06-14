import { expect, test, type Route } from '@playwright/test'

const graph3dPayload = {
  nodes: [
    {
      id: 'W1',
      work_id: 1,
      title: 'Graph seed work',
      authors: 'Ada Lovelace',
      year: 2024,
      type: 'journal-article',
      status: 'downloaded',
      source: 'seed',
      area: 'Economic sociology',
      region: 'Europe',
      countries: ['DE'],
      degree: 5,
      component: 0,
      component_size: 2,
      cluster: 0,
      cluster_size: 2,
      cluster_kind: 'community',
      x: -120,
      y: 0,
      z: 0,
    },
    {
      id: 'W2',
      work_id: 2,
      title: 'Referenced work',
      authors: 'Jane Doe',
      year: 2023,
      type: 'book',
      status: 'matched',
      source: 'seed',
      area: 'Economic sociology',
      region: 'Europe',
      countries: ['DE'],
      degree: 1,
      component: 0,
      component_size: 2,
      cluster: 0,
      cluster_size: 2,
      cluster_kind: 'community',
      x: 120,
      y: 0,
      z: 0,
    },
  ],
  edges: [{ s: 'W1', t: 'W2', r: 'references' }],
  clusters: [
    {
      id: 0,
      label: 'Economic sociology / seed',
      kind: 'community',
      size: 2,
      degree: 2,
      source: 'seed',
      area: 'Economic sociology',
      region: 'Europe',
      country: 'DE',
      decade: '2020s',
      year_min: 2023,
      year_max: 2024,
      radius: 64,
      x: 0,
      y: 0,
      z: 0,
      areas: [{ label: 'Economic sociology', count: 2 }],
      regions: [{ label: 'Europe', count: 2 }],
      countries: [{ label: 'DE', count: 2 }],
      types: [{ label: 'journal-article', count: 1 }, { label: 'book', count: 1 }],
      top_titles: ['Graph seed work'],
    },
  ],
  stats: {
    node_count: 2,
    edge_count: 1,
    component_count: 1,
    cluster_count: 1,
    relationship_counts: { references: 1, cited_by: 0 },
  },
}

const graph3dNodesMeta = graph3dPayload.nodes.map(({ x: _x, y: _y, z: _z, ...node }) => node)
const graph3dPositions = new Float32Array(graph3dPayload.nodes.flatMap((node) => [node.x, node.y, node.z]))
const graph3dEdgeTriplets = new Uint32Array([0, 1, 0])
const graph3dSnapshotManifest = {
  source: 'snapshot',
  snapshot_key: 'playwrightsnapshot',
  schema_version: 2,
  layout: { algorithm: 'igraph-fr3d', seed: 42 },
  stats: graph3dPayload.stats,
  files: {
    nodes: '/api/graph/3d/snapshot/playwrightsnapshot/nodes.bin',
    edges: '/api/graph/3d/snapshot/playwrightsnapshot/edges.bin',
    nodes_meta: '/api/graph/3d/snapshot/playwrightsnapshot/nodes_meta.json',
    clusters: '/api/graph/3d/snapshot/playwrightsnapshot/clusters.json',
  },
  buffers: {
    nodes: { count: 2, array: 'float32', stride: 3 },
    edges: { count: 1, array: 'uint32', stride: 3 },
  },
  clusters: graph3dPayload.clusters,
}

async function mockApi(route: Route) {
  const url = new URL(route.request().url())
  const path = url.pathname
  let body: unknown = {}

  if (path === '/api/auth/me') {
    body = {
      user: { id: 1, username: 'admin', last_corpus_id: 1, is_admin: true },
      corpora: [{ id: 1, name: 'Local research corpus', role: 'owner', owner_username: 'admin' }],
    }
  } else if (path === '/api/graph/3d/snapshot') {
    body = graph3dSnapshotManifest
  } else if (path === '/api/graph/3d/snapshot/playwrightsnapshot/nodes.bin') {
    await route.fulfill({
      status: 200,
      contentType: 'application/octet-stream',
      body: Buffer.from(graph3dPositions.buffer),
    })
    return
  } else if (path === '/api/graph/3d/snapshot/playwrightsnapshot/edges.bin') {
    await route.fulfill({
      status: 200,
      contentType: 'application/octet-stream',
      body: Buffer.from(graph3dEdgeTriplets.buffer),
    })
    return
  } else if (path === '/api/graph/3d/snapshot/playwrightsnapshot/nodes_meta.json') {
    body = graph3dNodesMeta
  } else if (path === '/api/graph/3d/snapshot/playwrightsnapshot/clusters.json') {
    body = graph3dPayload.clusters
  } else if (path.startsWith('/api/graph/3d/node/')) {
    body = {
      id: 2,
      title: 'Referenced work',
      authors: 'Jane Doe',
      doi: '10.1000/example',
      publisher: 'Example Press',
    }
  } else if (path === '/api/graph/3d') {
    body = graph3dPayload
  } else if (path === '/api/recursion-config') {
    body = { keyword: { includeDownstream: false, includeUpstream: false, relatedDepthDownstream: 0, relatedDepthUpstream: 0, maxRelated: 30 } }
  } else if (path === '/api/seed/sources') {
    body = { sources: [] }
  } else if (path === '/api/ingest/stats') {
    body = { stats: { raw_pending: 0, matched: 0, queued_download: 0, downloaded: 0 } }
  } else if (path === '/api/ingest/runs') {
    body = { runs: [] }
  } else if (path === '/api/corpus') {
    body = { items: [], total: 0, source: 'api', stage_totals: {} }
  } else if (path === '/api/downloads') {
    body = { items: [], total: 0, source: 'api' }
  }

  await route.fulfill({
    status: 200,
    contentType: 'application/json',
    body: JSON.stringify(body),
  })
}

test('graph tab is available to non-admin users', async ({ page }) => {
  await page.addInitScript(() => {
    window.localStorage.setItem('rag_feeder_token', 'playwright-token')
  })
  await page.route('**/api/**', mockApi)
  // Override auth to a non-admin researcher (registered last, so it wins).
  await page.route('**/api/auth/me', (route) =>
    route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({
        user: { id: 2, username: 'researcher', last_corpus_id: 1, is_admin: false },
        corpora: [{ id: 1, name: 'Local research corpus', role: 'editor', owner_username: 'admin' }],
      }),
    })
  )

  await page.goto('/#/graph')
  await expect(page.getByTestId('tab-graph')).toBeVisible()
  await page.getByTestId('tab-graph').click()
  await expect(page.getByTestId('graph-3d-panel')).toBeVisible()
  await expect(page.getByText(/Loaded 2 nodes and 1 edges? from snapshot\./)).toBeVisible()
})

test('loads the Three.js 3D graph panel from the graph API', async ({ page }) => {
  await page.addInitScript(() => {
    window.localStorage.setItem('rag_feeder_token', 'playwright-token')
  })
  await page.route('**/api/**', mockApi)

  const graphResponse = page.waitForResponse((res) => {
    const url = new URL(res.url())
    if (url.pathname !== '/api/graph/3d/snapshot' || res.request().method() !== 'GET') return false
    // The initial load defaults to 25,000 works (status/scope are fixed
    // server-side and no longer sent as params).
    return url.searchParams.get('max_nodes') === '25000'
  })
  await page.goto('/#/graph')
  await page.getByTestId('tab-graph').click()

  const panel = page.getByTestId('graph-3d-panel')
  await expect(panel).toBeVisible()
  await expect(panel.getByRole('heading', { name: '3D Graph Explorer' })).toBeVisible()

  // The initial load defaults to 25,000 works — guard against a silent revert.
  const initialSnapshot = await graphResponse
  expect(new URL(initialSnapshot.url()).searchParams.get('max_nodes')).toBe('25000')

  await expect(panel.getByText(/Loaded 2 nodes and 1 edges? from snapshot\./)).toBeVisible()
  await expect(panel.getByLabel('3D graph visualization')).toBeVisible()
  const mapKey = panel.locator('.graph-3d-map-key')
  await expect(mapKey.getByText('Territories')).toBeVisible()
  await expect(mapKey.getByText('Hubs (size = citations)')).toBeVisible()
  await expect(panel.getByText('No node selected')).toBeVisible()
})

async function openLoadedGraphPanel(page) {
  await page.addInitScript(() => {
    window.localStorage.setItem('rag_feeder_token', 'playwright-token')
  })
  const loaded = page.waitForResponse((res) =>
    new URL(res.url()).pathname === '/api/graph/3d/snapshot/playwrightsnapshot/nodes_meta.json'
  )
  await page.goto('/#/graph')
  await page.getByTestId('tab-graph').click()
  const panel = page.getByTestId('graph-3d-panel')
  await expect(panel).toBeVisible()
  await loaded
  await expect(panel.getByText(/Loaded 2 nodes and 1 edges? from snapshot\./)).toBeVisible()
  return panel
}

test('changing the grouping re-requests the snapshot with group_by', async ({ page }) => {
  await page.route('**/api/**', mockApi)
  const panel = await openLoadedGraphPanel(page)

  const regrouped = page.waitForRequest((req) => {
    const url = new URL(req.url())
    return url.pathname === '/api/graph/3d/snapshot' && url.searchParams.get('group_by') === 'type'
  })
  await panel.getByText('Group by').locator('..').locator('select').selectOption('type')
  await regrouped
  await expect(panel.getByText(/Loaded 2 nodes and 1 edges? from snapshot\./)).toBeVisible()
})

test('search finds a work and selects it', async ({ page }) => {
  await page.route('**/api/**', mockApi)
  const panel = await openLoadedGraphPanel(page)

  await panel.getByTestId('graph-3d-search').fill('Referenced')
  const results = panel.getByTestId('graph-3d-search-results')
  await expect(results).toBeVisible()
  await results.getByRole('button', { name: /Referenced work/ }).click()

  await expect(results).toBeHidden()
  await expect(panel.locator('.graph-3d-selected strong')).toHaveText('Referenced work')
})

test('year filter reports visible node count', async ({ page }) => {
  await page.route('**/api/**', mockApi)
  const panel = await openLoadedGraphPanel(page)

  await panel.getByTestId('graph-3d-year-from').fill('2024')
  await expect(panel.getByTestId('graph-3d-filter-status')).toHaveText(
    'Showing 1 of 2 nodes after year filter.'
  )

  await panel.getByTestId('graph-3d-year-from').fill('')
  await expect(panel.getByTestId('graph-3d-filter-status')).toBeHidden()
})

test('legend isolates a territory and clears it', async ({ page }) => {
  await page.route('**/api/**', mockApi)
  const panel = await openLoadedGraphPanel(page)

  const legend = panel.getByTestId('graph-3d-legend')
  await expect(legend).toBeVisible()
  const item = panel.getByTestId('graph-3d-legend-item').first()
  await expect(item).toHaveAttribute('aria-pressed', 'false')

  await item.click()
  await expect(item).toHaveAttribute('aria-pressed', 'true')
  const clear = panel.getByTestId('graph-3d-legend-clear')
  await expect(clear).toBeVisible()

  await clear.click()
  await expect(item).toHaveAttribute('aria-pressed', 'false')
  await expect(clear).toBeHidden()
})

test('path-finder connects two works via search selection', async ({ page }) => {
  await page.route('**/api/**', mockApi)
  const panel = await openLoadedGraphPanel(page)

  const search = panel.getByTestId('graph-3d-search')
  const results = panel.getByTestId('graph-3d-search-results')

  // Pick the first work and mark it as the path start.
  await search.fill('Graph seed')
  await results.getByRole('button', { name: /Graph seed work/ }).click()
  await panel.getByTestId('graph-3d-path-actions').getByRole('button', { name: /path start/i }).click()

  // Pick the second work and mark it as the path end.
  await search.fill('Referenced')
  await results.getByRole('button', { name: /Referenced work/ }).click()
  await panel.getByTestId('graph-3d-path-actions').getByRole('button', { name: /path end/i }).click()

  await expect(panel.getByTestId('graph-3d-path-status')).toHaveText(
    '1 hop along the citation graph.'
  )

  await panel.getByTestId('graph-3d-path-clear').click()
  await expect(panel.getByTestId('graph-3d-path')).toBeHidden()
})

test('isolating a territory shows a reset control that restores all nodes', async ({ page }) => {
  await page.route('**/api/**', mockApi)
  const panel = await openLoadedGraphPanel(page)

  // No filtering yet → no reset control.
  await expect(panel.getByTestId('graph-3d-showall')).toBeHidden()

  // Isolating a territory dims the rest; the reset control appears.
  await panel.getByTestId('graph-3d-legend-item').first().click()
  const showAll = panel.getByTestId('graph-3d-showall')
  await expect(showAll).toBeVisible()

  await showAll.click()
  await expect(showAll).toBeHidden()
})

test('selecting a work does not dim the rest (no reset control)', async ({ page }) => {
  await page.route('**/api/**', mockApi)
  const panel = await openLoadedGraphPanel(page)

  await panel.getByTestId('graph-3d-search').fill('Referenced')
  const results = panel.getByTestId('graph-3d-search-results')
  await expect(results).toBeVisible()
  await results.getByRole('button', { name: /Referenced work/ }).click()

  // The work is selected (shown in the sidebar) but nothing is dimmed, so the
  // "Show all nodes" reset control must not appear.
  await expect(panel.locator('.graph-3d-selected strong')).toHaveText('Referenced work')
  await expect(panel.getByTestId('graph-3d-showall')).toBeHidden()
})

test('rotating with a left-drag does not change the selection', async ({ page }) => {
  await page.route('**/api/**', mockApi)
  const panel = await openLoadedGraphPanel(page)

  // Select a work first (deterministic, via search).
  await panel.getByTestId('graph-3d-search').fill('Referenced')
  const results = panel.getByTestId('graph-3d-search-results')
  await expect(results).toBeVisible()
  await results.getByRole('button', { name: /Referenced work/ }).click()
  await expect(panel.locator('.graph-3d-selected strong')).toHaveText('Referenced work')

  // A left-drag on the canvas (rotate) ends in a click; it must NOT re-select.
  const canvas = panel.locator('.graph-3d-canvas')
  const box = await canvas.boundingBox()
  const cx = box!.x + box!.width / 2
  const cy = box!.y + box!.height / 2
  await page.mouse.move(cx - 80, cy)
  await page.mouse.down()
  await page.mouse.move(cx + 80, cy + 40, { steps: 8 })
  await page.mouse.up()

  // Selection unchanged: the originally selected work is still shown.
  await expect(panel.locator('.graph-3d-selected strong')).toHaveText('Referenced work')
})

test('wheeling over a canvas overlay still reaches the canvas to zoom', async ({ page }) => {
  await page.route('**/api/**', mockApi)
  const panel = await openLoadedGraphPanel(page)

  await page.evaluate(() => {
    const canvas = document.querySelector('.graph-3d-canvas canvas') as HTMLElement
    ;(window as unknown as { __wheels: number }).__wheels = 0
    canvas?.addEventListener('wheel', () => { (window as unknown as { __wheels: number }).__wheels += 1 })
  })

  // The interactive legend sits on top of the canvas; a wheel over it must be
  // forwarded so the graph still zooms instead of the gesture being swallowed.
  await panel.getByTestId('graph-3d-legend').hover()
  await page.mouse.wheel(0, -120)

  await expect
    .poll(() => page.evaluate(() => (window as unknown as { __wheels: number }).__wheels))
    .toBeGreaterThan(0)
})

test('node detail responses are cached per work', async ({ page }) => {
  let nodeDetailRequests = 0
  await page.route('**/api/**', async (route) => {
    if (new URL(route.request().url()).pathname === '/api/graph/3d/node/2') {
      nodeDetailRequests += 1
    }
    await mockApi(route)
  })
  const panel = await openLoadedGraphPanel(page)

  const search = panel.getByTestId('graph-3d-search')
  const results = panel.getByTestId('graph-3d-search-results')
  await search.fill('Referenced')
  await results.getByRole('button', { name: /Referenced work/ }).click()
  await expect(panel.getByText('Identifiers: 10.1000/example')).toBeVisible()

  await search.fill('Referenced')
  await results.getByRole('button', { name: /Referenced work/ }).click()
  await expect(panel.locator('.graph-3d-selected strong')).toHaveText('Referenced work')

  expect(nodeDetailRequests).toBe(1)
})

test('selected work exposes a clickable link to open the paper', async ({ page }) => {
  await page.route('**/api/**', mockApi)
  const panel = await openLoadedGraphPanel(page)

  await panel.getByTestId('graph-3d-search').fill('Referenced')
  const results = panel.getByTestId('graph-3d-search-results')
  await expect(results).toBeVisible()
  await results.getByRole('button', { name: /Referenced work/ }).click()

  const links = panel.getByTestId('graph-3d-links')
  await expect(links).toBeVisible()
  await expect(links.getByRole('link', { name: /DOI/ })).toHaveAttribute(
    'href',
    'https://doi.org/10.1000/example'
  )
})
