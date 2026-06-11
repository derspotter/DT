import { expect, test, type Route } from '@playwright/test'

const graph2dPayload = {
  nodes: [
    {
      id: 'W1',
      title: 'Graph seed work',
      year: 2024,
      type: 'journal-article',
      status: 'downloaded',
      source_path: 'seed',
    },
    {
      id: 'W2',
      title: 'Referenced work',
      year: 2023,
      type: 'book',
      status: 'matched',
      source_path: 'seed',
    },
  ],
  edges: [{ source: 'W1', target: 'W2', relationship_type: 'references' }],
  stats: { node_count: 2, edge_count: 1, relationship_counts: { references: 1, cited_by: 0 } },
}

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
  } else if (path === '/api/graph') {
    body = graph2dPayload
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

test('loads the Three.js 3D graph panel from the graph API', async ({ page }) => {
  await page.addInitScript(() => {
    window.localStorage.setItem('rag_feeder_token', 'playwright-token')
  })
  await page.route('**/api/**', mockApi)

  const graphResponse = page.waitForResponse((res) => {
    const url = new URL(res.url())
    if (url.pathname !== '/api/graph/3d/snapshot' || res.request().method() !== 'GET') return false
    return url.searchParams.get('scope') === 'all'
  })
  await page.goto('/#/graph')
  await page.getByTestId('tab-graph').click()

  const panel = page.getByTestId('graph-3d-panel')
  await expect(panel).toBeVisible()
  await expect(panel.getByRole('heading', { name: '3D Graph Explorer' })).toBeVisible()

  await graphResponse

  await expect(panel.getByText('Loaded 2 nodes and 1 edges from snapshot.')).toBeVisible()
  await expect(panel.getByLabel('3D graph visualization')).toBeVisible()
  await expect(panel.getByText('Territories')).toBeVisible()
  await expect(panel.getByText('Hubs (size = citations)')).toBeVisible()
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
  await expect(panel.getByText('Loaded 2 nodes and 1 edges from snapshot.')).toBeVisible()
  return panel
}

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
