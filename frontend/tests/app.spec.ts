import { test, expect, type Page } from '@playwright/test'

async function ensureSignedIn(page: Page) {
  const signInButton = page.getByRole('button', { name: 'Sign in' })
  if (await signInButton.isVisible().catch(() => false)) {
    await page.getByRole('textbox', { name: 'Username' }).fill('admin')
    await page.getByRole('textbox', { name: 'Password' }).fill('admin')
    await signInButton.click()
  }
  await expect(page.getByRole('heading', { name: 'Corpus orchestration workspace' })).toBeVisible()
}

test.describe('Korpus Builder UI', () => {
  test('dashboard loads', async ({ page }) => {
    await page.goto('/')
    await ensureSignedIn(page)
    await expect(page.getByTestId('dashboard-overview')).toBeVisible()
    await expect(page.getByTestId('tab-dashboard')).toBeVisible()
  })

  test('ingest panel shows uploader and extracted entries section', async ({ page }) => {
    await page.goto('/')
    await ensureSignedIn(page)
    await page.getByTestId('tab-ingest').click()
    await expect(page.getByTestId('ingest-panel')).toBeVisible()
    await expect(page.getByText('Upload selected')).toBeVisible()
    await expect(page.getByRole('heading', { name: 'Extracted entries' })).toBeVisible()
  })

  test('ingest select-all checks all extracted entry rows', async ({ page }) => {
    await page.route('**/api/ingest/runs*', async (route) => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          runs: [
            {
              ingest_source: 'demo-ingest-source',
              entry_count: 2,
              last_created_at: '2026-02-12 10:00:00',
              source_pdf: '/tmp/demo.pdf',
            },
          ],
        }),
      })
    })
    await page.route('**/api/ingest/latest*', async (route) => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          ingest_source: 'demo-ingest-source',
          items: [
            { id: 101, title: 'First Entry', authors: 'A. Author', year: 2024, source: 'Demo', doi: '' },
            { id: '102', title: 'Second Entry', authors: 'B. Author', year: 2025, source: 'Demo', doi: '' },
          ],
        }),
      })
    })

    await page.goto('/')
    await ensureSignedIn(page)
    await page.getByTestId('tab-ingest').click()
    await page.getByRole('button', { name: 'demo-ingest-source' }).click()
    await expect(page.getByText('Loaded 2 entries.')).toBeVisible()

    await page.getByRole('button', { name: 'Select all' }).click()

    const selectionState = await page.evaluate(() => {
      const boxes = Array.from(document.querySelectorAll('input[type="checkbox"][aria-label^="Select"]'))
      const rowBoxes = boxes.filter((el) => el.getAttribute('aria-label') !== 'Select all')
      return {
        rows: rowBoxes.length,
        checkedRows: rowBoxes.filter((el) => el.checked).length,
      }
    })

    expect(selectionState.rows).toBe(2)
    expect(selectionState.checkedRows).toBe(2)
    await expect(page.getByText('Selected: 2')).toBeVisible()
  })
  test('keyword search renders mocked results', async ({ page }) => {
    await page.route('**/api/keyword-search', async (route) => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          runId: 777,
          source: 'openalex',
          results: [
            {
              id: 'W1',
              openalex_id: 'W1',
              title: 'Mocked Search Result',
              authors: 'Test Author',
              year: 2024,
              type: 'article',
              doi: null,
            },
          ],
        }),
      })
    })

    await page.goto('/')
    await ensureSignedIn(page)
    await page.getByTestId('tab-search').click()
    await page.getByTestId('search-query').fill('institutional economics')
    await page.getByTestId('search-submit').click()
    await expect(page.getByText('Mocked Search Result')).toBeVisible()
  })

  test('corpus table renders', async ({ page }) => {
    await page.goto('/')
    await ensureSignedIn(page)
    await page.getByTestId('tab-corpus').click()
    await expect(page.getByTestId('corpus-panel')).toBeVisible()
    await expect(page.getByTestId('corpus-panel').getByRole('heading', { name: 'Corpus' })).toBeVisible()
  })

  test('downloads queue renders with export controls', async ({ page }) => {
    await page.goto('/')
    await ensureSignedIn(page)
    await page.getByTestId('tab-downloads').click()
    await expect(page.getByTestId('downloads-panel')).toBeVisible()
    await expect(page.getByTestId('exports-panel')).toBeVisible()
    await expect(page.getByRole('button', { name: 'JSON' })).toBeVisible()
    await expect(page.getByRole('button', { name: 'BibTeX' })).toBeVisible()
    await expect(page.getByRole('button', { name: 'PDFs ZIP' })).toBeVisible()
    await expect(page.getByRole('button', { name: 'Bundle ZIP' })).toBeVisible()
    await expect(page.getByTestId('export-filter-status')).toBeVisible()
  })

  test('logs and graph views render', async ({ page }) => {
    await page.goto('/')
    await ensureSignedIn(page)

    await page.getByTestId('tab-logs').click()
    await expect(page.getByTestId('logs-panel')).toBeVisible()
    await expect(page.getByText('WebSocket status')).toBeVisible()

    await page.getByTestId('tab-graph').click()
    await expect(page.getByTestId('graph-panel')).toBeVisible()
    await expect(page.getByLabel('Graph visualization')).toBeVisible()
  })

  test('logs tab supports type switch and older paging controls', async ({ page }) => {
    await page.route('**/api/logs/tail*', async (route) => {
      const url = new URL(route.request().url())
      const type = url.searchParams.get('type') || 'pipeline'
      const cursor = Number(url.searchParams.get('cursor') || '0')
      if (type === 'app') {
        await route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify({
            type: 'app',
            entries: ['[app] recent line'],
            count: 1,
            lines: 200,
            cursor,
            next_cursor: cursor + 1,
            has_more: false,
            total_lines: 1,
            include_rotated: true,
            source_file: '/tmp/backend-app.log',
            source_exists: true,
          }),
        })
        return
      }
      const payload = cursor === 0
        ? {
          type: 'pipeline',
          entries: ['[pipeline] latest line'],
          count: 1,
          lines: 200,
          cursor: 0,
          next_cursor: 1,
          has_more: true,
          total_lines: 2,
          include_rotated: true,
          source_file: '/tmp/backend-pipeline.log',
          source_exists: true,
        }
        : {
          type: 'pipeline',
          entries: ['[pipeline] older line'],
          count: 1,
          lines: 200,
          cursor,
          next_cursor: cursor + 1,
          has_more: false,
          total_lines: 2,
          include_rotated: true,
          source_file: '/tmp/backend-pipeline.log',
          source_exists: true,
        }
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify(payload),
      })
    })

    await page.goto('/')
    await ensureSignedIn(page)
    await page.getByTestId('tab-logs').click()
    await expect(page.getByTestId('logs-panel')).toBeVisible()
    await expect(page.getByTestId('logs-type-select')).toBeVisible()

    await page.getByRole('button', { name: 'Load older' }).click()
    await expect(page.getByTestId('logs-panel').getByText('[pipeline] older line')).toBeVisible()

    await page.getByTestId('logs-type-select').selectOption('app')
    await expect(page.getByTestId('logs-panel').getByText('[app] recent line')).toBeVisible()
  })
})
