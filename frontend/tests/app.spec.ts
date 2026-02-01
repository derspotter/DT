import { test, expect } from '@playwright/test'

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
