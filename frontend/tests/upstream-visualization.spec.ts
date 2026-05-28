import { expect, test, type Route } from '@playwright/test'

const browsePayload = {
  source: 'api',
  target: {
    id: 'target-a',
    name: 'Target A',
    path: '/tmp/target-a',
    metadata_bib_exists: true,
    metadata_bib_path: '/tmp/target-a/metadata.bib',
  },
  summary: {
    current_items_count: 1,
    pending_items_count: 1,
    assigned_corpora_count: 1,
    metadata_bib_modified_at: '2026-05-12T10:00:00.000Z',
    metadata_bib_exists: true,
    metadata_bib_path: '/tmp/target-a/metadata.bib',
    metadata_entry_count: 1,
    has_imported_current: true,
    pending_is_provisional: false,
    current_items_loaded: 1,
    pending_items_loaded: 1,
    current_items_has_more: false,
    pending_items_has_more: false,
  },
  assigned_corpora: [
    {
      id: 1,
      name: 'Local research corpus',
      owner_username: 'admin',
      pending_count: 1,
      downloaded_count: 1,
    },
  ],
  current_items: [
    {
      id: 10,
      work_id: 10,
      graph_node_id: 'W100000001',
      title: 'Current upstream graph node',
      authors: 'Current Author',
      year: 2024,
      type: 'journal-article',
      status: 'downloaded',
      upstream_state: 'current',
      source: '/tmp/target-a/metadata.bib',
      openalex_id: 'https://openalex.org/W100000001',
      source_corpora: [],
      source_corpus_ids: [],
    },
  ],
  pending_items: [
    {
      id: 11,
      work_id: 11,
      graph_node_id: 'W100000002',
      title: 'Pending local graph node',
      authors: 'Pending Author',
      year: 2025,
      type: 'book',
      status: 'downloaded',
      upstream_state: 'pending',
      source: 'Local research corpus',
      openalex_id: 'https://openalex.org/W100000002',
      source_corpora: [{ id: 1, name: 'Local research corpus', owner_username: 'admin' }],
      source_corpus_ids: [1],
      source_corpora_count: 1,
    },
  ],
  graph: {
    nodes: [
      {
        id: 'W100000001',
        work_id: 10,
        title: 'Current upstream graph node',
        year: 2024,
        type: 'journal-article',
        status: 'downloaded',
        upstream_state: 'current',
        source: '/tmp/target-a/metadata.bib',
        openalex_id: 'https://openalex.org/W100000001',
        source_corpora: [],
        source_corpora_count: 0,
      },
      {
        id: 'W100000002',
        work_id: 11,
        title: 'Pending local graph node',
        year: 2025,
        type: 'book',
        status: 'downloaded',
        upstream_state: 'pending',
        source: 'Local research corpus',
        openalex_id: 'https://openalex.org/W100000002',
        source_corpora: [{ id: 1, name: 'Local research corpus', owner_username: 'admin' }],
        source_corpora_count: 1,
      },
    ],
    edges: [{ source: 'W100000002', target: 'W100000001', relationship_type: 'references' }],
    stats: {
      node_count: 2,
      edge_count: 1,
      relationship_counts: { references: 1, cited_by: 0 },
    },
  },
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
  } else if (path === '/api/recursion-config') {
    body = { keyword: { includeDownstream: true, includeUpstream: false, relatedDepthDownstream: 1, relatedDepthUpstream: 0, maxRelated: 25 } }
  } else if (path === '/api/seed/sources') {
    body = { sources: [] }
  } else if (path === '/api/ingest/stats') {
    body = { stats: { raw_pending: 0, matched: 0, queued_download: 0, downloaded: 0 } }
  } else if (path === '/api/ingest/runs') {
    body = { runs: [] }
  } else if (path === '/api/corpus') {
    body = { items: [], total: 0, source: 'api', stage_totals: {} }
  } else if (path === '/api/corpora/1/kantropos-assignment') {
    body = {
      assignment: { corpus_id: 1, target_id: 'target-a', target_name: 'Target A' },
      corpora: [{ id: 'target-a', name: 'Target A', path: '/tmp/target-a' }],
      root_path: '/tmp',
      can_edit: true,
    }
  } else if (path === '/api/kantropos/corpora') {
    body = {
      root_path: '/tmp',
      corpora: [
        {
          id: 'target-a',
          name: 'Target A',
          path: '/tmp/target-a',
          metadata_bib_exists: true,
          metadata_bib_path: '/tmp/target-a/metadata.bib',
          metadata_entry_count: 1,
          assignment_count: 1,
          assigned_corpora: [{ corpus_id: 1, corpus_name: 'Local research corpus', owner_username: 'admin' }],
        },
      ],
    }
  } else if (path === '/api/kantropos/corpora/target-a/browse') {
    body = browsePayload
  } else {
    body = {}
  }

  await route.fulfill({
    status: 200,
    contentType: 'application/json',
    body: JSON.stringify(body),
  })
}

test('renders upstream corpus visualization from browse graph data', async ({ page }) => {
  await page.addInitScript(() => {
    window.localStorage.setItem('rag_feeder_token', 'playwright-token')
  })
  await page.route('**/api/**', mockApi)

  await page.goto('/#/upstream')
  await expect(page.getByRole('heading', { name: 'Keep local corpora aligned with the upstream baseline.' })).toBeVisible()

  const map = page.getByTestId('upstream-visualization')
  await expect(map).toBeVisible()
  await expect(page.getByLabel('Upstream corpus visualization')).toBeVisible()
  await expect(map.getByText('Visible nodes')).toBeVisible()
  await expect(map.getByText('Citation edges', { exact: true })).toBeVisible()
  await expect(map.locator('.upstream-map__metrics strong').nth(0)).toHaveText('2')
  await expect(map.locator('.upstream-map__metrics strong').nth(1)).toHaveText('1')

  await map.getByLabel('Color by').selectOption('source')
  await expect(map.getByText('Local research corpus').first()).toBeVisible()
  await map.getByLabel('Show').selectOption('pending')
  await expect(map.locator('.upstream-map__metrics strong').nth(0)).toHaveText('1')
  await expect(map.locator('.upstream-map__selected strong', { hasText: 'Pending local graph node' })).toBeVisible()
})
