import { expect, test, type Locator, type Page, type Route } from '@playwright/test'
import { afterBodyRead, fetchStats, fulfil, mePayload, mockLogSocket, trackFetches } from './helpers/mockApi'

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
    body = mePayload()
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
  await mockLogSocket(page)
  await trackFetches(page)
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
  await mockLogSocket(page)
  await trackFetches(page)
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
  await mockLogSocket(page)
  await trackFetches(page)
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

test('selecting a cluster shows its detail without a request', async ({ page }) => {
  let clusterRequests = 0
  await page.route('**/api/**', async (route) => {
    if (new URL(route.request().url()).pathname.startsWith('/api/graph/3d/cluster/')) clusterRequests += 1
    await mockApi(route)
  })
  const panel = await openLoadedGraphPanel(page)
  await panel.locator('.graph-3d-cluster-label').first().click()
  await expect(panel.getByText('Economic sociology / seed').first()).toBeVisible()
  await expect(panel.getByText('Countries: DE')).toBeVisible()
  expect(clusterRequests).toBe(0)
})

// ---------------------------------------------------------------------------
// Load ownership (BE-54) and node-detail ownership (FE-61).
//
// `graphMock` answers like `mockApi`, plus per-test state:
// - the manifest version: `versions` by manifest request order (the last one
//   repeats), or `versionsByCorpus` by `corpus_id`; every resource URL of a
//   manifest carries `?v=<version>`;
// - resource gates by `<file>?v=<version>` (`hold`), released with an answer
//   (`{ status, body }`) or `undefined` (the normal file);
// - per-request gates for `/api/graph/3d/node/<id>` (`holdNode(id, index)`)
//   and for the manifest (`holdManifest(index)`, by manifest request order);
// - a mutable `lastCorpusId` that `/api/corpora/<id>/select` sets and
//   `/api/auth/me` returns.

const SNAPSHOT_BASE = '/api/graph/3d/snapshot/playwrightsnapshot'
const MANIFEST = '/api/graph/3d/snapshot'
const NODE_1 = '/api/graph/3d/node/1'
const NODE_2 = '/api/graph/3d/node/2'
const REBUILT = { error: 'The graph snapshot was rebuilt; reload the graph.', code: 'snapshot_rebuilt' }
const SECOND_CORPUS = { id: 2, name: 'Second corpus', role: 'owner', owner_username: 'admin' }
const FIRST_CORPUS = { id: 1, name: 'Local research corpus', role: 'owner', owner_username: 'admin' }

type Answer = { status: number; body?: unknown } | undefined
type Gate = { promise: Promise<Answer>; release: (answer?: Answer) => void }

function gate(): Gate {
  let release: (answer?: Answer) => void = () => {}
  const promise = new Promise<Answer>((resolve) => { release = resolve })
  return { promise, release }
}

function versionedManifest(version: string, edgeCount = 1) {
  const v = `?v=${encodeURIComponent(version)}`
  return {
    ...graph3dSnapshotManifest,
    stats: { ...graph3dPayload.stats, edge_count: edgeCount },
    files: {
      nodes: `${SNAPSHOT_BASE}/nodes.bin${v}`,
      edges: `${SNAPSHOT_BASE}/edges.bin${v}`,
      nodes_meta: `${SNAPSHOT_BASE}/nodes_meta.json${v}`,
      clusters: `${SNAPSHOT_BASE}/clusters.json${v}`,
      manifest: `${SNAPSHOT_BASE}/manifest.json${v}`,
    },
  }
}

const RESOURCE_FILES = ['nodes.bin', 'edges.bin', 'nodes_meta.json', 'clusters.json']

async function graphMock(page: Page, {
  versions = ['A'],
  versionsByCorpus = null as Record<string, string> | null,
  edgeCountByCorpus = {} as Record<string, number>,
} = {}) {
  const state = {
    lastCorpusId: 1,
    manifestCount: 0,
    resourceGates: new Map<string, Gate>(),
    nodeGates: new Map<string, Gate>(),
    manifestGates: new Map<number, Gate>(),
    nodeRequests: {} as Record<string, number>,
  }
  const hold = (key: string) => {
    const held = gate()
    state.resourceGates.set(key, held)
    return held
  }
  const holdNode = (id: number, index: number) => {
    const held = gate()
    state.nodeGates.set(`${id}#${index}`, held)
    return held
  }
  const holdManifest = (index: number) => {
    const held = gate()
    state.manifestGates.set(index, held)
    return held
  }
  await page.addInitScript(() => {
    window.localStorage.setItem('rag_feeder_token', 'playwright-token')
  })
  await mockLogSocket(page)
  await trackFetches(page)
  await page.route('**/api/**', async (route) => {
    const url = new URL(route.request().url())
    const path = url.pathname
    if (path === '/api/auth/me') {
      return fulfil(route, mePayload({ corpusId: state.lastCorpusId, corpora: [FIRST_CORPUS, SECOND_CORPUS] }))
    }
    const select = path.match(/^\/api\/corpora\/(\d+)\/select$/)
    if (select && route.request().method() === 'POST') {
      state.lastCorpusId = Number(select[1])
      return fulfil(route, { ok: true })
    }
    if (path === MANIFEST) {
      const corpus = url.searchParams.get('corpus_id') || ''
      const version = versionsByCorpus
        ? versionsByCorpus[corpus]
        : versions[Math.min(state.manifestCount, versions.length - 1)]
      const held = state.manifestGates.get(state.manifestCount)
      state.manifestCount += 1
      const answer = held ? await held.promise : undefined
      if (answer) return fulfil(route, answer.body ?? {}, answer.status)
      return fulfil(route, versionedManifest(version, edgeCountByCorpus[corpus] ?? 1))
    }
    if (path.startsWith(`${SNAPSHOT_BASE}/`)) {
      const file = path.slice(SNAPSHOT_BASE.length + 1)
      const held = state.resourceGates.get(`${file}?v=${url.searchParams.get('v')}`)
      const answer = held ? await held.promise : undefined
      if (answer) return fulfil(route, answer.body ?? {}, answer.status)
      return mockApi(route)
    }
    const node = path.match(/^\/api\/graph\/3d\/node\/(\d+)$/)
    if (node) {
      const index = state.nodeRequests[node[1]] ?? 0
      state.nodeRequests[node[1]] = index + 1
      const held = state.nodeGates.get(`${node[1]}#${index}`)
      const answer = held ? await held.promise : undefined
      if (answer) return fulfil(route, answer.body ?? {}, answer.status)
      if (node[1] === '1') return fulfil(route, { id: 1, title: 'Graph seed work', doi: '10.1000/seed' })
      return mockApi(route)
    }
    return mockApi(route)
  })
  return { state, hold, holdNode, holdManifest }
}

// The keys of every fetch the app called after its first body read of a
// request matching `readMatch` (page-side order, so causal).
async function callsAfterRead(page: Page, readMatch: RegExp): Promise<string[] | null> {
  return page.evaluate(({ source, flags }) => {
    const re = new RegExp(source, flags)
    const events = (window as any).__dtFetch.events as { kind: string; key: string }[]
    const index = events.findIndex((event) => event.kind === 'read' && re.test(event.key))
    if (index < 0) return null
    return events.slice(index + 1).filter((event) => event.kind === 'call').map((event) => event.key)
  }, { source: readMatch.source, flags: readMatch.flags })
}

const calls = async (page: Page, match: string | RegExp) => (await fetchStats(page, match)).calls
const reads = async (page: Page, match: string | RegExp) => (await fetchStats(page, match)).reads

async function openGraph(page: Page) {
  await page.goto('/#/graph')
  await page.getByTestId('tab-graph').click()
  const panel = page.getByTestId('graph-3d-panel')
  await expect(panel).toBeVisible()
  return panel
}

async function selectWork(panel: Locator, title: string, query: string) {
  await panel.getByTestId('graph-3d-search').fill(query)
  const results = panel.getByTestId('graph-3d-search-results')
  await results.getByRole('button', { name: new RegExp(title) }).click()
  await expect(panel.locator('.graph-3d-selected strong')).toHaveText(title)
}

const selectSeed = (panel: Locator) => selectWork(panel, 'Graph seed work', 'Graph seed')
const selectReferenced = (panel: Locator) => selectWork(panel, 'Referenced work', 'Referenced')

const NODES_A = /nodes\.bin\?v=A$/

test('a 409 on a snapshot file reloads the manifest once and loads build B', async ({ page }) => {
  const { hold } = await graphMock(page, { versions: ['A', 'B'] })
  hold('nodes.bin?v=A').release({ status: 409, body: REBUILT })
  const panel = await openGraph(page)

  await expect(panel.getByText(/Loaded 2 nodes and 1 edges? from snapshot\./)).toBeVisible()
  expect(await calls(page, MANIFEST)).toBe(2)
  const after = await callsAfterRead(page, NODES_A)
  expect(after).not.toBeNull()
  const resources = after!.filter((key) => key.startsWith(`${SNAPSHOT_BASE}/`))
  expect(resources.length).toBe(RESOURCE_FILES.length)
  for (const key of resources) expect(key).toMatch(/\?v=B$/)
})

test('a 409 after the graph unmounts starts no request', async ({ page }) => {
  const { hold, holdManifest } = await graphMock(page, { versions: ['A', 'R'] })
  const nodesA = hold('nodes.bin?v=A')
  // A retry would be manifest request 1: held, so it can start nothing more.
  const secondManifest = holdManifest(1)
  await openGraph(page)
  await expect.poll(() => calls(page, NODES_A)).toBe(1)

  await page.getByTestId('tab-workspace').click()
  await expect(page.getByTestId('graph-3d-panel')).toBeHidden()
  const before = await reads(page, NODES_A)
  nodesA.release({ status: 409, body: REBUILT })
  await afterBodyRead(page, NODES_A, before)
  expect(await calls(page, MANIFEST)).toBe(1)

  // The reopened graph is a new instance; its manifest is request 1.
  await page.getByTestId('tab-graph').click()
  await expect.poll(() => calls(page, MANIFEST)).toBe(2)
  secondManifest.release()
  const panel = page.getByTestId('graph-3d-panel')
  await expect(panel.getByText(/Loaded 2 nodes and 1 edges? from snapshot\./)).toBeVisible()
  expect(await calls(page, MANIFEST)).toBe(2)
  for (const file of RESOURCE_FILES) {
    expect([file, await calls(page, `${SNAPSHOT_BASE}/${file}`)]).toEqual([file, 2])
    expect([file, await calls(page, new RegExp(`${file.replace('.', '\\.')}\\?v=R$`))]).toEqual([file, 1])
  }
})

test('a 409 after a header corpus switch neither retries nor publishes the old corpus', async ({ page }) => {
  const { hold, holdManifest } = await graphMock(page, {
    versionsByCorpus: { 1: 'A', 2: 'C' },
    edgeCountByCorpus: { 2: 7 },
  })
  const nodesA = hold('nodes.bin?v=A')
  const corpus2 = RESOURCE_FILES.map((file) => hold(`${file}?v=C`))
  // Manifest 0 is corpus 1's, 1 is corpus 2's; a retry would be request 2: held.
  const retriedManifest = holdManifest(2)
  await openGraph(page)
  await expect.poll(() => calls(page, NODES_A)).toBe(1)

  await page.locator('.header-corpus-picker select').selectOption('2')
  // Commit marker: the new instance's corpus-2 manifest. The Graph tab skips the
  // seed reads, so the global "first /api/seed/sources after the switch's /me"
  // marker never comes here; this is the graph-specific checkpoint (owner ruling).
  await expect.poll(() => calls(page, /^\/api\/graph\/3d\/snapshot\?corpus_id=2&/)).toBe(1)

  const before = await reads(page, NODES_A)
  nodesA.release({ status: 409, body: REBUILT })
  await afterBodyRead(page, NODES_A, before)
  // One manifest per corpus; a retry under either corpus makes 3.
  expect(await calls(page, MANIFEST)).toBe(2)

  for (const held of corpus2) held.release()
  await expect(page.getByTestId('graph-3d-panel').getByText('Loaded 2 nodes and 7 edges from snapshot.')).toBeVisible()
  expect(await calls(page, MANIFEST)).toBe(2)
  retriedManifest.release()
})

for (const late of ['fails', 'succeeds'] as const) {
  test(`an older detail answer that ${late} does not end the selected node's loading`, async ({ page }) => {
    const { holdNode } = await graphMock(page)
    const node1 = holdNode(1, 0)
    const node2 = holdNode(2, 0)
    const panel = await openGraph(page)
    await expect(panel.getByText(/Loaded 2 nodes and 1 edges? from snapshot\./)).toBeVisible()

    await selectSeed(panel)
    await expect.poll(() => calls(page, NODE_1)).toBe(1)
    await selectReferenced(panel)
    await expect.poll(() => calls(page, NODE_2)).toBe(1)
    await expect(panel.getByText('Loading metadata...')).toBeVisible()

    const before = await reads(page, NODE_1)
    node1.release(late === 'fails'
      ? { status: 500, body: { error: 'boom' } }
      : { status: 200, body: { id: 1, title: 'Graph seed work', doi: '10.1000/seed' } })
    await afterBodyRead(page, NODE_1, before)
    await expect(panel.getByText('Loading metadata...')).toBeVisible()

    node2.release()
    await expect(panel.getByText('Identifiers: 10.1000/example', { exact: true })).toBeVisible()
    await expect(panel.getByText('Loading metadata...')).toBeHidden()
  })
}

test('an older answer for the same node neither shows nor caches', async ({ page }) => {
  const { holdNode } = await graphMock(page)
  const a1 = holdNode(1, 0)
  const a2 = holdNode(1, 1)
  const panel = await openGraph(page)
  await expect(panel.getByText(/Loaded 2 nodes and 1 edges? from snapshot\./)).toBeVisible()

  await selectSeed(panel)
  await expect.poll(() => calls(page, NODE_1)).toBe(1)
  await selectReferenced(panel)
  await expect(panel.getByText('Identifiers: 10.1000/example', { exact: true })).toBeVisible()
  await selectSeed(panel)
  await expect.poll(() => calls(page, NODE_1)).toBe(2)

  a2.release({ status: 200, body: { id: 1, title: 'Graph seed work', doi: '10.1000/seed-v2' } })
  await expect(panel.getByText('Identifiers: 10.1000/seed-v2', { exact: true })).toBeVisible()

  const before = await reads(page, NODE_1)
  a1.release({ status: 200, body: { id: 1, title: 'Graph seed work', doi: '10.1000/seed-v1' } })
  await afterBodyRead(page, NODE_1, before)
  await expect(panel.getByText('Identifiers: 10.1000/seed-v2', { exact: true })).toBeVisible()

  await selectReferenced(panel)
  await expect(panel.getByText('Identifiers: 10.1000/example', { exact: true })).toBeVisible()
  await selectSeed(panel)
  await expect(panel.getByText('Identifiers: 10.1000/seed-v2', { exact: true })).toBeVisible()
  expect(await calls(page, NODE_1)).toBe(2)
})

test('Escape ends the pending detail request', async ({ page }) => {
  const { holdNode } = await graphMock(page)
  const first = holdNode(1, 0)
  const panel = await openGraph(page)
  await expect(panel.getByText(/Loaded 2 nodes and 1 edges? from snapshot\./)).toBeVisible()

  await selectSeed(panel)
  await expect.poll(() => calls(page, NODE_1)).toBe(1)
  await page.keyboard.press('Escape')
  await expect(panel.getByText('No node selected')).toBeVisible()

  const before = await reads(page, NODE_1)
  first.release({ status: 200, body: { id: 1, doi: '10.1000/seed' } })
  await afterBodyRead(page, NODE_1, before)

  await selectSeed(panel)
  await expect.poll(() => calls(page, NODE_1)).toBe(2)
  await expect(panel.getByText('Identifiers: 10.1000/seed', { exact: true })).toBeVisible()
})
