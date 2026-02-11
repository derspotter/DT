import { test, expect, type Page } from '@playwright/test'

async function ensureSignedIn(page: Page) {
  const signInButton = page.getByRole('button', { name: 'Sign in' })
  if (await signInButton.isVisible().catch(() => false)) {
    await page.getByRole('textbox', { name: 'Username' }).fill('admin')
    await page.getByRole('textbox', { name: 'Password' }).fill('admin')
    await signInButton.click()
  }
}

test.describe('Korpus Builder UI', () => {
  test('dashboard loads', async ({ page }) => {
    await page.goto('/')
    await expect(page.getByTestId('dashboard-overview')).toBeVisible()
    await expect(page.getByText('Corpus orchestration workspace')).toBeVisible()
  })

  test('ingest panel shows uploader and bibliography list', async ({ page }) => {
    await page.goto('/')
    await page.getByTestId('tab-ingest').click()
    await expect(page.getByTestId('ingest-panel')).toBeVisible()
    await expect(page.getByText('Upload selected')).toBeVisible()
    await expect(page.getByText('Bibliography files')).toBeVisible()
  })

  test('keyword search returns sample results', async ({ page }) => {
    await page.goto('/')
    await ensureSignedIn(page)
    await page.getByTestId('tab-search').click()
    const query = page.getByTestId('search-query')
    await query.fill('institutional economics')
    await page
      .getByTestId('search-panel')
      .getByRole('button', { name: 'Search' })
      .click()
    await expect(page.getByText('Sample results loaded.')).toBeVisible()
    await expect(page.getByText('The New Institutional Economics')).toBeVisible()
  })

  test('keyword search supports query mode and JSON seed mode', async ({ page }) => {
    const seenBodies: Array<Record<string, unknown>> = []
    const seedPayload = '[{"doi":"10.1111/j.1468-0335.1937.tb00002.x"}]'

    await page.route('**/api/keyword-search', async (route) => {
      const request = route.request()
      if (request.method() !== 'POST') {
        await route.fulfill({
          status: 204,
          headers: { 'access-control-allow-origin': '*' },
          body: '',
        })
        return
      }

      const body = request.postDataJSON() as Record<string, unknown>
      seenBodies.push(body)
      const isSeedMode = typeof body.seedJson === 'string' && body.seedJson.length > 0

      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        headers: { 'access-control-allow-origin': '*' },
        body: JSON.stringify({
          runId: isSeedMode ? 222 : 111,
          source: 'openalex',
          mode: isSeedMode ? 'seed' : 'query',
          expansion: { added: isSeedMode ? 1 : 0, processed: 1 },
          results: [
            {
              id: isSeedMode ? 'W2015930340' : 'W2136823593',
              openalex_id: isSeedMode ? 'W2015930340' : 'W2136823593',
              title: isSeedMode ? 'Seed Mode Result' : 'Query Mode Result',
              authors: 'Test Author',
              year: isSeedMode ? 1937 : 2000,
              type: 'article',
              doi: isSeedMode ? '10.1111/j.1468-0335.1937.tb00002.x' : null,
            },
          ],
        }),
      })
    })

    await page.goto('/')
    await ensureSignedIn(page)
    await page.getByTestId('tab-search').click()

    await page.getByTestId('search-mode').selectOption('query')
    await page.getByTestId('search-query').fill('institutional economics')
    await page.getByTestId('search-submit').click()
    await expect(page.getByText('Query Mode Result')).toBeVisible()
    expect(seenBodies[0]).toMatchObject({
      query: 'institutional economics',
      seedJson: '',
    })

    await page.getByTestId('search-mode').selectOption('seed')
    await page.getByTestId('search-seed-json').fill(seedPayload)
    await page.getByTestId('search-submit').click()
    await expect(page.getByText('Seed Mode Result')).toBeVisible()
    expect(seenBodies[1]).toMatchObject({
      query: '',
      seedJson: seedPayload,
    })
  })

  test('corpus table renders', async ({ page }) => {
    await page.goto('/')
    await page.getByTestId('tab-corpus').click()
    await expect(page.getByTestId('corpus-panel')).toBeVisible()
    await expect(page.getByText('Sample corpus data.')).toBeVisible()
  })

  test('downloads queue renders', async ({ page }) => {
    await page.goto('/')
    await page.getByTestId('tab-downloads').click()
    await expect(page.getByTestId('downloads-panel')).toBeVisible()
    await expect(page.getByText('Sample queue data.')).toBeVisible()
  })

  test('logs view renders', async ({ page }) => {
    await page.goto('/')
    await page.getByTestId('tab-logs').click()
    await expect(page.getByTestId('logs-panel')).toBeVisible()
    await expect(page.getByText('WebSocket status')).toBeVisible()
  })

  test('graph panel renders', async ({ page }) => {
    await page.goto('/')
    await page.getByTestId('tab-graph').click()
    await expect(page.getByTestId('graph-panel')).toBeVisible()
    await expect(page.getByLabel('Graph visualization')).toBeVisible()
    await expect(page.getByText('Relationship')).toBeVisible()
    await expect(page.getByTestId('graph-panel').getByText('Status')).toBeVisible()
  })
})
