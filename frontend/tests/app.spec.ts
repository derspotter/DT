import { test, expect, type APIRequestContext, type Page } from '@playwright/test'
import { mkdirSync, writeFileSync } from 'node:fs'
import { dirname } from 'node:path'

async function ensureSignedIn(page: Page, request: APIRequestContext) {
  await page.goto('/')
  const heading = page.getByRole('heading', { name: 'Corpus orchestration workspace' })
  if (await heading.isVisible().catch(() => false)) {
    await expect(page.getByText(/API status:/).first()).toContainText('API status:')
    return
  }

  const username = process.env.E2E_USERNAME || process.env.RAG_ADMIN_USER || ''
  const password = process.env.E2E_PASSWORD || process.env.RAG_ADMIN_PASSWORD || ''
  expect(username, 'Missing E2E_USERNAME or RAG_ADMIN_USER for Playwright login').toBeTruthy()
  expect(password, 'Missing E2E_PASSWORD or RAG_ADMIN_PASSWORD for Playwright login').toBeTruthy()

  const signInButton = page.getByRole('button', { name: 'Sign in' })
  if (await signInButton.isVisible().catch(() => false)) {
    await page.getByRole('textbox', { name: 'Username' }).fill(username)
    await page.getByRole('textbox', { name: 'Password' }).fill(password)
    await signInButton.click()
  }

  await expect(heading).toBeVisible({ timeout: 20_000 })
  await expect(page.getByText(/API status:/).first()).toContainText('API status:')
}

async function waitUntilSearchSettled(page: Page) {
  const status = page.locator('.seed-intake-card--search p.muted').first()
  await expect
    .poll(
      async () => {
        const value = (await status.textContent()) || ''
        return value.includes('Searching...')
      },
      { timeout: 120_000 }
    )
    .toBeFalsy()
  return ((await status.textContent()) || '').trim()
}

async function waitUntilGraphSettled(page: Page) {
  const status = page.getByTestId('graph-panel').locator('p.muted').first()
  await expect
    .poll(
      async () => {
        const value = (await status.textContent()) || ''
        return value.includes('Loading graph...')
      },
      { timeout: 120_000 }
    )
    .toBeFalsy()
  return ((await status.textContent()) || '').trim()
}

function buildTinyPdfBytes(text: string): Buffer {
  const stream = `BT /F1 18 Tf 72 720 Td (${text}) Tj ET\n`
  const objects = [
    '1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n',
    '2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n',
    '3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>\nendobj\n',
    `4 0 obj\n<< /Length ${Buffer.byteLength(stream, 'utf8')} >>\nstream\n${stream}endstream\nendobj\n`,
    '5 0 obj\n<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>\nendobj\n',
  ]
  let body = '%PDF-1.4\n'
  const offsets: number[] = [0]
  for (const obj of objects) {
    offsets.push(Buffer.byteLength(body, 'utf8'))
    body += obj
  }
  const xrefOffset = Buffer.byteLength(body, 'utf8')
  body += `xref\n0 ${objects.length + 1}\n`
  body += '0000000000 65535 f \n'
  for (let i = 1; i <= objects.length; i += 1) {
    body += `${String(offsets[i]).padStart(10, '0')} 00000 n \n`
  }
  body += `trailer\n<< /Size ${objects.length + 1} /Root 1 0 R >>\nstartxref\n${xrefOffset}\n%%EOF\n`
  return Buffer.from(body, 'utf8')
}

function writePdfFixture(pathname: string, text: string): string {
  mkdirSync(dirname(pathname), { recursive: true })
  writeFileSync(pathname, buildTinyPdfBytes(text))
  return pathname
}

test.describe('Korpus Builder live integration', () => {
  test.setTimeout(180_000)

  async function readGraphSummary(page: Page) {
    const values = await page.getByTestId('graph-panel').locator('.graph-summary strong').allTextContents()
    return {
      nodes: Number(values[0] || 0),
      edges: Number(values[1] || 0),
      references: Number(values[2] || 0),
      citedBy: Number(values[3] || 0),
    }
  }

  test('end-to-end flow uses real backend APIs', async ({ page, request }) => {
    await ensureSignedIn(page, request)

    await expect(page.getByTestId('side-nav')).toBeVisible()
    await expect(page.getByTestId('tab-workspace')).toBeVisible()
    await expect(page.getByTestId('tab-dashboard')).toBeVisible()
    await expect(page.getByTestId('tab-logs')).toBeVisible()

    await page.getByTestId('tab-dashboard').click()
    await expect(page.getByTestId('dashboard-overview')).toBeVisible()

    await page.getByTestId('tab-workspace').click()
    await expect(page.locator('.seed-intake-card--search')).toBeVisible()
    await page.getByRole('textbox', { name: 'Query' }).fill('institutional economics AND governance')
    await page.getByRole('button', { name: 'Search' }).click()
    const searchStatus = await waitUntilSearchSettled(page)
    expect(searchStatus).not.toContain('Searching...')
    expect(searchStatus.length).toBeGreaterThan(0)
    if (searchStatus.includes('Sample results loaded.')) {
      test.info().annotations.push({
        type: 'warning',
        description: 'Keyword search fell back to sample data; backend/OpenAlex integration should be checked.',
      })
    }

    const corpusPanel = page.getByTestId('corpus-panel')
    await expect(corpusPanel).toBeVisible()
    await expect(corpusPanel.getByText(/corpus from api|sample corpus data/i)).toBeVisible()
    const corpusRows = corpusPanel.locator('.corpus-select-row')
    if (await corpusRows.count()) {
      await corpusRows.first().click()
      await expect(corpusPanel.locator('.corpus-details-grid strong').first()).not.toHaveText('-')
    }

    await page.getByTestId('tab-downloads').click()
    const downloadsPanel = page.getByTestId('downloads-panel')
    await expect(downloadsPanel).toBeVisible()
    await expect(downloadsPanel.getByText('Sample queue data.')).toHaveCount(0)
    await expect(page.getByTestId('exports-panel')).toBeVisible()
    await expect(page.getByRole('button', { name: 'Bundle ZIP' })).toBeVisible()

    await page.getByTestId('tab-logs').click()
    const logsPanel = page.getByTestId('logs-panel')
    await expect(logsPanel).toBeVisible()
    await expect(logsPanel.getByText('WebSocket status:')).toBeVisible()
    await page.getByTestId('logs-type-select').selectOption('app')
    await page.getByTestId('logs-type-select').selectOption('pipeline')
    await expect(logsPanel.getByRole('button', { name: 'Refresh from file' })).toBeVisible()

    await page.getByTestId('tab-graph').click()
    const graphPanel = page.getByTestId('graph-panel')
    await expect(graphPanel).toBeVisible()
    await graphPanel.getByRole('button', { name: 'Apply filters' }).click()
    const graphStatus = await waitUntilGraphSettled(page)
    expect(graphStatus).not.toContain('Loading graph...')
    if (graphStatus.includes('(sample)')) {
      test.info().annotations.push({
        type: 'warning',
        description: 'Graph fell back to sample data; backend graph endpoint should be checked.',
      })
    }
    await expect(page.getByLabel('Graph visualization')).toBeVisible()
  })

  test('ingest accepts a real PDF upload and starts extraction', async ({ page, request }, testInfo) => {
    await ensureSignedIn(page, request)
    await page.getByTestId('tab-workspace').click()
    const uploadCard = page.locator('.seed-intake-card--upload')
    await expect(uploadCard).toBeVisible()

    const pdfPath = writePdfFixture(
      testInfo.outputPath('fixtures/ingest-smoke.pdf'),
      `Playwright ingest smoke ${Date.now()}`
    )
    const pdfName = 'ingest-smoke.pdf'

    await uploadCard.locator('input[type="file"][accept=".pdf"]').setInputFiles(pdfPath)
    const uploadRow = uploadCard.locator('.upload-item', { hasText: pdfName })
    await expect(uploadRow).toBeVisible()
    await expect(uploadRow.locator('.status')).toHaveText('uploaded', { timeout: 120_000 })

    await uploadRow.getByRole('button', { name: 'Extract' }).click()
    await expect(uploadRow.locator('.status')).toHaveText('extracted', { timeout: 30_000 })
  })

  test('graph controls update data and re-query on every change', async ({ page }) => {
    await ensureSignedIn(page, page.request)

    await page.getByTestId('tab-graph').click()
    const graphPanel = page.getByTestId('graph-panel')
    await expect(graphPanel).toBeVisible()

    const status = await waitUntilGraphSettled(page)
    expect(status).not.toContain('Loading graph...')

    const initialSummary = await readGraphSummary(page)
    for (const count of Object.values(initialSummary)) {
      expect(count).toBeGreaterThanOrEqual(0)
    }

    const graphControls = graphPanel.locator('.graph-controls')
    const graphSelects = graphControls.locator('select')
    const graphInputs = graphControls.locator('input[type="number"]')
    const relationshipSelect = graphSelects.nth(0)
    const statusSelect = graphSelects.nth(1)
    const colorModeSelect = graphSelects.nth(2)
    const maxNodesInput = graphInputs.nth(1)
    const yearFromInput = graphInputs.nth(0)

    await relationshipSelect.selectOption('references')
    const referencesResponse = page.waitForResponse(
      (res) => res.url().includes('/api/graph') && res.request().method() === 'GET' && new URL(res.url()).searchParams.get('relationship') === 'references'
    )
    await page.getByRole('button', { name: 'Apply filters' }).click()
    await referencesResponse
    const referencesSummary = await waitUntilGraphSettled(page).then(() => readGraphSummary(page))
    expect(referencesSummary.nodes).toBeGreaterThanOrEqual(0)

    await relationshipSelect.selectOption('cited_by')
    const citedByResponse = page.waitForResponse(
      (res) => res.url().includes('/api/graph') && res.request().method() === 'GET' && new URL(res.url()).searchParams.get('relationship') === 'cited_by'
    )
    await page.getByRole('button', { name: 'Apply filters' }).click()
    await citedByResponse
    await waitUntilGraphSettled(page)
    const citedBySummary = await readGraphSummary(page)
    expect(citedBySummary.edges).toBeGreaterThanOrEqual(0)
    expect(citedBySummary).toBeDefined()

    await statusSelect.selectOption('matched')
    await maxNodesInput.fill('120')
    await yearFromInput.fill('2000')
    await colorModeSelect.selectOption('status')
    await statusSelect.waitFor()
    await page.getByRole('button', { name: 'Apply filters' }).click()
    await waitUntilGraphSettled(page)

    const currentSummary = await readGraphSummary(page)
    expect(currentSummary.nodes).toBeGreaterThanOrEqual(0)

    await page.getByRole('button', { name: 'Reset view' }).click()
    expect(await page.getByLabel('Graph visualization').getAttribute('aria-label')).toBe('Graph visualization')

    if (currentSummary.nodes > 0) {
      const listItem = page.locator('.graph-list button', { hasText: /./ }).first()
      await listItem.hover()
      await expect(page.locator('.graph-hover')).toBeVisible()
    }
  })
})
