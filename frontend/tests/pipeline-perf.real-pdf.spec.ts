import { expect, test, type APIRequestContext, type Page } from '@playwright/test'
import { existsSync } from 'node:fs'
import { basename, resolve } from 'node:path'

const API_BASE = process.env.E2E_API_BASE || 'http://localhost:4000'
const RUN_PERF = process.env.RUN_REAL_PDF_PERF === '1'
const EXTRACT_TIMEOUT_MS = Number(process.env.PERF_EXTRACT_TIMEOUT_MS || 12 * 60_000)
const POLL_INTERVAL_MS = 2_000
const EXTRACT_STABLE_WINDOW_MS = Number(process.env.PERF_EXTRACT_STABLE_WINDOW_MS || 20_000)
const INGEST_RUNS_LIMIT = Number(process.env.PERF_INGEST_RUNS_LIMIT || 300)

function realPdfCandidates(): string[] {
  const fromEnv = process.env.REAL_PDF_PATH
  const fromRepo = [
    '../dl_lit/testout/downloads/2005_Institutionalists at the limits of institutionalis.pdf',
    '../dl_lit/bibliographies/done_bibs/1770319824237-768760587-institutional impossibility guild socialism.pdf',
    '../dl_lit_project/pdf_library/1998_New Institutional Economics.pdf',
    '../dl_lit_project/pdf_library/1998_New Institutional Economics in the PostSocialist.pdf',
  ].map((value) => resolve(process.cwd(), value))
  return [fromEnv || '', ...fromRepo].filter(Boolean)
}

function pickRealPdfPath(): string | null {
  const candidates = realPdfCandidates()
  return candidates.find((value) => existsSync(value)) || null
}

async function ensureSignedIn(page: Page, request: APIRequestContext): Promise<string> {
  await page.goto('/')
  const heading = page.getByRole('heading', { name: 'Corpus orchestration workspace' })
  if (await heading.isVisible().catch(() => false)) {
    const existingToken = await page.evaluate(() => localStorage.getItem('rag_feeder_token') || '')
    if (existingToken) return existingToken
  }

  const username = `perf_e2e_${Date.now()}_${Math.floor(Math.random() * 10_000)}`
  const password = `perf_${Math.floor(Math.random() * 1_000_000)}`
  const register = await request.post(`${API_BASE}/api/auth/register`, { data: { username, password } })
  expect([201, 409]).toContain(register.status())

  await page.getByRole('textbox', { name: 'Username' }).fill(username)
  await page.getByRole('textbox', { name: 'Password' }).fill(password)
  await page.getByRole('button', { name: 'Sign in' }).click()
  await expect(heading).toBeVisible({ timeout: 20_000 })

  const token = await page.evaluate(() => localStorage.getItem('rag_feeder_token') || '')
  expect(token).toBeTruthy()
  return token
}

async function authedJson(
  request: APIRequestContext,
  token: string,
  path: string,
  method: 'GET' | 'POST' = 'GET',
  body?: unknown
): Promise<any> {
  const attempts = method === 'GET' ? 6 : 2
  let lastError: unknown = null
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      const response = await request.fetch(`${API_BASE}${path}`, {
        method,
        headers: {
          Authorization: `Bearer ${token}`,
          ...(body ? { 'Content-Type': 'application/json' } : {}),
        },
        data: body ? JSON.stringify(body) : undefined,
      })
      const text = await response.text()
      if (!response.ok()) {
        throw new Error(`${method} ${path} failed (${response.status()}): ${text}`)
      }
      try {
        return JSON.parse(text)
      } catch {
        return text
      }
    } catch (error) {
      lastError = error
      if (attempt < attempts) {
        await new Promise((resolveSleep) => setTimeout(resolveSleep, 1_500))
      }
    }
  }
  throw lastError instanceof Error ? lastError : new Error(`Failed request ${method} ${path}`)
}

async function pollFor<T>(
  fn: () => Promise<T>,
  ok: (value: T) => boolean,
  timeoutMs: number,
  intervalMs = POLL_INTERVAL_MS
): Promise<{ value: T; elapsedMs: number }> {
  const start = Date.now()
  let lastError: unknown = null
  while (Date.now() - start < timeoutMs) {
    try {
      const value = await fn()
      if (ok(value)) {
        return { value, elapsedMs: Date.now() - start }
      }
      lastError = null
    } catch (error) {
      lastError = error
    }
    await new Promise((resolveSleep) => setTimeout(resolveSleep, intervalMs))
  }
  if (lastError) {
    throw lastError instanceof Error ? lastError : new Error(String(lastError))
  }
  throw new Error(`Timed out after ${Math.round(timeoutMs / 1000)}s`)
}

async function getRunEntryCount(request: APIRequestContext, token: string, runName: string): Promise<number> {
  const payload = await authedJson(request, token, `/api/ingest/runs?limit=${INGEST_RUNS_LIMIT}`)
  const runs = Array.isArray(payload?.runs) ? payload.runs : []
  const run = runs.find((item: any) => String(item.ingest_source || '') === runName)
  return Number(run?.entry_count || 0)
}

async function waitForStableRunEntryCount(
  request: APIRequestContext,
  token: string,
  runName: string,
  timeoutMs: number,
  stableWindowMs: number
): Promise<{ finalCount: number; elapsedMs: number; ttfrMs: number }> {
  const start = Date.now()
  let lastCount = -1
  let lastChangeAt = start
  let firstNonZeroAt = 0

  while (Date.now() - start < timeoutMs) {
    const count = await getRunEntryCount(request, token, runName)
    const now = Date.now()

    if (count !== lastCount) {
      lastCount = count
      lastChangeAt = now
      if (count > 0 && !firstNonZeroAt) {
        firstNonZeroAt = now
      }
    }

    const stableForMs = now - lastChangeAt
    if (lastCount > 0 && stableForMs >= stableWindowMs) {
      return {
        finalCount: lastCount,
        elapsedMs: now - start,
        ttfrMs: firstNonZeroAt ? firstNonZeroAt - start : now - start,
      }
    }

    await new Promise((resolveSleep) => setTimeout(resolveSleep, POLL_INTERVAL_MS))
  }

  throw new Error(
    `Timed out waiting for stable extraction count for run ${runName}. Last count=${Math.max(lastCount, 0)}`
  )
}

test.describe('Real PDF pipeline performance', () => {
  test.skip(!RUN_PERF, 'Set RUN_REAL_PDF_PERF=1 to run real PDF speed benchmark.')
  test.setTimeout(30 * 60_000)

  test('measures extraction, enrichment, and download throughput on a real PDF', async ({ page, request }, testInfo) => {
    const pdfPath = pickRealPdfPath()
    test.skip(!pdfPath, 'No real PDF fixture found. Set REAL_PDF_PATH=/abs/path/to/file.pdf')

    const token = await ensureSignedIn(page, request)
    const sourceName = basename(pdfPath!)

    const baselineRunsPayload = await authedJson(request, token, '/api/ingest/runs?limit=100')
    const knownRunNames = new Set(
      (Array.isArray(baselineRunsPayload?.runs) ? baselineRunsPayload.runs : []).map((run: any) => String(run.ingest_source || ''))
    )

    await page.getByTestId('tab-ingest').click()
    await expect(page.getByTestId('ingest-panel')).toBeVisible()
    await page.locator('input[type="file"][accept=".pdf"]').setInputFiles(pdfPath!)
    const uploadRow = page.locator('.upload-item', { hasText: sourceName })
    await expect(uploadRow).toBeVisible()

    await page.getByRole('button', { name: 'Upload selected' }).click()
    await expect(uploadRow.locator('.status')).toHaveText('uploaded', { timeout: 180_000 })

    const extractStart = Date.now()
    await uploadRow.getByRole('button', { name: 'Extract' }).click()
    await expect(uploadRow.locator('.status')).toHaveText('extracted', { timeout: 45_000 })

    const discoveredRun = await pollFor(
      async () => {
        const payload = await authedJson(request, token, '/api/ingest/runs?limit=100')
        const runs = Array.isArray(payload?.runs) ? payload.runs : []
        return runs.find((run: any) => {
          const ingestSource = String(run.ingest_source || '')
          return !knownRunNames.has(ingestSource)
        }) || null
      },
      (run) => Boolean(run),
      EXTRACT_TIMEOUT_MS
    )
    const run = discoveredRun.value
    const runName = String(run.ingest_source)

    const extractionSettled = await waitForStableRunEntryCount(
      request,
      token,
      runName,
      EXTRACT_TIMEOUT_MS,
      EXTRACT_STABLE_WINDOW_MS
    )
    const extractCompleteSeconds = (Date.now() - extractStart) / 1000
    const extractFirstResultSeconds = extractionSettled.ttfrMs / 1000

    const entriesPayload = await authedJson(
      request,
      token,
      `/api/ingest/latest?baseName=${encodeURIComponent(runName)}&limit=1000`
    )
    const entries = Array.isArray(entriesPayload?.items) ? entriesPayload.items : []
    const entryIds = entries
      .map((item: any) => Number(item.id))
      .filter((value: number) => Number.isFinite(value))
    const extractedCount = Math.max(entryIds.length, extractionSettled.finalCount)
    expect(extractedCount).toBeGreaterThan(0)

    const requestedBatch = Math.min(10, entryIds.length)
    const enqueuePayload = await authedJson(request, token, '/api/ingest/enqueue', 'POST', {
      entryIds: entryIds.slice(0, requestedBatch),
    })
    const markedBatch = Number(enqueuePayload?.marked || 0)

    let enrichPayload: any = { processed: 0, promoted: 0, failed: 0, queued: 0 }
    let enrichSeconds = 0
    if (markedBatch > 0) {
      const enrichStart = Date.now()
      enrichPayload = await authedJson(request, token, '/api/ingest/process-marked', 'POST', {
        limit: markedBatch,
        workers: 6,
        fetchReferences: false,
      })
      enrichSeconds = (Date.now() - enrichStart) / 1000
    }

    const statsBeforeDownload = await authedJson(request, token, '/api/ingest/stats')
    const downloadStart = Date.now()
    const runOncePayload = await authedJson(request, token, '/api/downloads/worker/run-once', 'POST', {
      batchSize: 5,
    })
    const downloadSeconds = (Date.now() - downloadStart) / 1000
    const statsAfterDownload = await authedJson(request, token, '/api/ingest/stats')

    const downloadedBefore = Number(statsBeforeDownload?.stats?.downloaded_references || 0)
    const downloadedAfter = Number(statsAfterDownload?.stats?.downloaded_references || 0)
    const downloadedDelta = Math.max(0, downloadedAfter - downloadedBefore)

    const metrics = {
      pdf: pdfPath,
      runName,
      extraction: {
        firstResultSeconds: Number(extractFirstResultSeconds.toFixed(2)),
        completeSeconds: Number(extractCompleteSeconds.toFixed(2)),
        stabilityWindowSeconds: Number((EXTRACT_STABLE_WINDOW_MS / 1000).toFixed(2)),
        entries: extractedCount,
        entriesPerMinute: Number(((extractedCount / Math.max(extractCompleteSeconds, 1)) * 60).toFixed(2)),
      },
      enrichment: {
        requestedBatch,
        markedBatch,
        alreadyProcessedAtEnqueue: Number(enqueuePayload?.already_processed || 0),
        stagedAtEnqueue: Number(enqueuePayload?.staged || 0),
        duplicatesAtEnqueue: Number(enqueuePayload?.duplicates || 0),
        seconds: Number(enrichSeconds.toFixed(2)),
        processed: Number(enrichPayload?.processed || 0),
        promoted: Number(enrichPayload?.promoted || 0),
        failed: Number(enrichPayload?.failed || 0),
        processedPerMinute: Number(((Number(enrichPayload?.processed || 0) / Math.max(enrichSeconds, 1)) * 60).toFixed(2)),
      },
      download: {
        seconds: Number(downloadSeconds.toFixed(2)),
        runOnceDownloaded: Number(runOncePayload?.downloaded || runOncePayload?.last_result?.downloaded || 0),
        downloadedDelta,
        downloadedPerMinute: Number(((downloadedDelta / Math.max(downloadSeconds, 1)) * 60).toFixed(2)),
      },
    }

    console.log('\n[perf] real-pdf pipeline metrics:\n' + JSON.stringify(metrics, null, 2))
    testInfo.annotations.push({
      type: 'benchmark',
      description: JSON.stringify(metrics),
    })
    await testInfo.attach('pipeline-perf-metrics', {
      body: JSON.stringify(metrics, null, 2),
      contentType: 'application/json',
    })

    expect(metrics.extraction.entries).toBeGreaterThan(0)
  })
})
