import { test, expect, type APIRequestContext, type Page } from '@playwright/test'
import { mkdirSync, writeFileSync } from 'node:fs'
import { dirname } from 'node:path'

async function ensureSignedIn(page: Page, request: APIRequestContext) {
  await page.goto('/')
  const heading = page.getByRole('heading', { name: 'Corpus orchestration workspace' })
  if (await heading.isVisible().catch(() => false)) {
    await expect(page.locator('.subtitle').first()).toContainText('API status:')
    return
  }

  const username = `pw_e2e_${Date.now()}_${Math.floor(Math.random() * 10_000)}`
  const password = `pw_e2e_${Math.floor(Math.random() * 1_000_000)}`
  const register = await request.post('http://localhost:4000/api/auth/register', {
    data: { username, password },
  })
  expect([201, 409]).toContain(register.status())

  const signInButton = page.getByRole('button', { name: 'Sign in' })
  if (await signInButton.isVisible().catch(() => false)) {
    await page.getByRole('textbox', { name: 'Username' }).fill(username)
    await page.getByRole('textbox', { name: 'Password' }).fill(password)
    await signInButton.click()
  }

  await expect(heading).toBeVisible({ timeout: 20_000 })
  await expect(page.locator('.subtitle').first()).toContainText('API status:')
}

async function waitUntilSearchSettled(page: Page) {
  const status = page.getByTestId('search-panel').locator('p.muted').first()
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

  test('end-to-end flow uses real backend APIs', async ({ page, request }) => {
    await ensureSignedIn(page, request)

    await expect(page.getByTestId('dashboard-overview')).toBeVisible()
    await expect(page.getByTestId('header-log-stream')).toBeVisible()

    const globalPipeline = page.locator('.header-pipeline')
    await expect(globalPipeline).toBeVisible()
    const stateText = ((await globalPipeline.locator('.header-pipeline__state strong').textContent()) || '').trim()
    if (stateText.toLowerCase() === 'paused') {
      await page.getByRole('button', { name: 'Start / Continue' }).click()
      await expect(globalPipeline.locator('.header-pipeline__state strong')).toHaveText(/Running/i, { timeout: 20_000 })
      await page.getByRole('button', { name: 'Pause', exact: true }).click()
      await expect(page.getByRole('button', { name: 'Pause', exact: true })).toBeVisible()
    }

    await page.getByTestId('tab-ingest').click()
    await expect(page.getByTestId('ingest-panel')).toBeVisible()
    await page.getByRole('button', { name: 'Refresh stats' }).click()
    await page.getByRole('button', { name: 'Refresh runs' }).click()
    await expect(page.getByText('Raw:')).toBeVisible()
    await expect(page.getByText('With metadata:')).toBeVisible()
    await expect(page.getByText('Downloaded:')).toBeVisible()

    const runButtons = page.locator('button.link.truncate-line')
    if (await runButtons.count()) {
      await runButtons.first().click()
      const selectAll = page.getByRole('button', { name: 'Select all' })
      if (await selectAll.isVisible().catch(() => false)) {
        await selectAll.click()
        const selectedLabel = page.locator('.table-toolbar-left .muted').first()
        await expect(selectedLabel).toContainText('Selected:')
      }
    }

    await page.getByTestId('tab-search').click()
    await expect(page.getByTestId('search-panel')).toBeVisible()
    await page.getByTestId('search-mode').selectOption('query')
    await page.getByTestId('search-query').fill('institutional economics AND governance')
    await page.getByTestId('search-submit').click()
    const searchStatus = await waitUntilSearchSettled(page)
    expect(searchStatus).not.toContain('Searching...')
    expect(searchStatus.length).toBeGreaterThan(0)
    if (searchStatus.includes('Sample results loaded.')) {
      test.info().annotations.push({
        type: 'warning',
        description: 'Keyword search fell back to sample data; backend/OpenAlex integration should be checked.',
      })
    }

    await page.getByTestId('tab-corpus').click()
    const corpusPanel = page.getByTestId('corpus-panel')
    await expect(corpusPanel).toBeVisible()
    await expect(corpusPanel.getByText('Live corpus from API.')).toBeVisible()
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
    await page.getByTestId('tab-ingest').click()
    await expect(page.getByTestId('ingest-panel')).toBeVisible()

    const pdfPath = writePdfFixture(
      testInfo.outputPath('fixtures/ingest-smoke.pdf'),
      `Playwright ingest smoke ${Date.now()}`
    )
    const pdfName = 'ingest-smoke.pdf'

    await page.locator('input[type="file"][accept=".pdf"]').setInputFiles(pdfPath)
    const uploadRow = page.locator('.upload-item', { hasText: pdfName })
    await expect(uploadRow).toBeVisible()

    await page.getByRole('button', { name: 'Upload selected' }).click()
    await expect(uploadRow.locator('.status')).toHaveText('uploaded', { timeout: 120_000 })

    await uploadRow.getByRole('button', { name: 'Extract' }).click()
    await expect(uploadRow.locator('.status')).toHaveText('extracted', { timeout: 30_000 })
  })
})
