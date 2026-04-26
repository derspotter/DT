import { expect, test, type APIRequestContext, type Page } from '@playwright/test'
import { createRequire } from 'node:module'
import { existsSync, rmSync } from 'node:fs'
import { join, resolve } from 'node:path'

const require = createRequire(import.meta.url)
const Database = require('../../backend/node_modules/better-sqlite3')

const API_BASE = process.env.E2E_API_BASE || 'http://localhost:4000'
const DB_PATH =
  process.env.E2E_DB_PATH || '/home/jay/DT/dl_lit_project/data/literature.db'
const RUN_SCRAPER_E2E = process.env.RUN_SCRAPER_E2E === '1'
const ARTIFACTS_ROOT =
  process.env.E2E_ARTIFACTS_ROOT || '/home/jay/DT/dl_lit_project/artifacts'

async function ensureSignedIn(page: Page): Promise<string> {
  await page.goto('/')
  const heading = page.getByRole('heading', { name: 'Corpus orchestration workspace' })
  if (await heading.isVisible().catch(() => false)) {
    const existingToken = await page.evaluate(() => localStorage.getItem('rag_feeder_token') || '')
    if (existingToken) return existingToken
  }

  const username = process.env.E2E_USERNAME || process.env.RAG_ADMIN_USER || ''
  const password = process.env.E2E_PASSWORD || process.env.RAG_ADMIN_PASSWORD || ''
  expect(username, 'Missing E2E_USERNAME or RAG_ADMIN_USER for Playwright login').toBeTruthy()
  expect(password, 'Missing E2E_PASSWORD or RAG_ADMIN_PASSWORD for Playwright login').toBeTruthy()

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
  path: string
): Promise<any> {
  const response = await request.get(`${API_BASE}${path}`, {
    headers: { Authorization: `Bearer ${token}` },
  })
  const text = await response.text()
  if (!response.ok()) {
    throw new Error(`GET ${path} failed (${response.status()}): ${text}`)
  }
  return text ? JSON.parse(text) : null
}

async function waitForScraperRun(
  request: APIRequestContext,
  token: string,
  runId: number,
  timeoutMs = 90_000
) {
  const startedAt = Date.now()
  let latest: any = null
  while (Date.now() - startedAt < timeoutMs) {
    latest = await authedJson(request, token, `/api/scraper/runs/${runId}`)
    const status = String(latest?.run?.status || '')
    if (['completed', 'failed'].includes(status)) return latest
    await new Promise((resolvePromise) => setTimeout(resolvePromise, 2_000))
  }
  throw new Error(`Scraper run ${runId} did not finish within ${timeoutMs}ms. Latest: ${JSON.stringify(latest?.run || null)}`)
}

function cleanupScraperRun(dbPath: string, runId: number) {
  if (!existsSync(dbPath) || !runId) return
  const db = new Database(dbPath)
  db.prepare('DELETE FROM scraper_artifacts WHERE run_id = ?').run(runId)
  db.prepare("DELETE FROM pipeline_jobs WHERE parameters_json LIKE ?").run(`%"run_id":${runId}%`)
  db.prepare("DELETE FROM pipeline_jobs WHERE parameters_json LIKE ?").run(`%"run_id": ${runId}%`)
  db.prepare('DELETE FROM scraper_runs WHERE id = ?').run(runId)
  db.close()
}

function rmBestEffort(pathname: string) {
  try {
    rmSync(pathname, { force: true, recursive: true })
  } catch {
    // The Docker worker may own generated artifact directories.
  }
}

test.describe('scraper lab', () => {
  test.skip(!RUN_SCRAPER_E2E, 'Set RUN_SCRAPER_E2E=1 to run this mutating scraper test.')
  test.setTimeout(120_000)

  test('queues an EUR-Lex scrape from the UI and records the worker result', async ({ page, request }) => {
    test.skip(!existsSync(DB_PATH), `Missing database for scraper cleanup: ${DB_PATH}`)

    const token = await ensureSignedIn(page)
    let runId = 0

    try {
      await page.goto('/#/scraper')
      await expect(page.getByText('Collect EU source material without touching the literature corpus.')).toBeVisible()

      await page.getByLabel('CELEX number').fill('32024R1689')
      const createResponsePromise = page.waitForResponse((response) =>
        response.url().endsWith('/api/scraper/runs') && response.request().method() === 'POST'
      )
      await page.getByRole('button', { name: 'Start EUR-Lex scrape' }).click()
      const createResponse = await createResponsePromise
      expect(createResponse.status()).toBe(201)
      const createPayload = await createResponse.json()
      runId = Number(createPayload?.run?.id || 0)
      expect(runId).toBeGreaterThan(0)

      const result = await waitForScraperRun(request, token, runId)
      const status = String(result?.run?.status || '')
      if (status === 'failed') {
        expect(String(result?.run?.error || '')).toContain('Missing EURLEX_USERNAME/EURLEX_PASSWORD')
      } else {
        expect(status).toBe('completed')
        expect(Number(result?.run?.total_count || 0)).toBeGreaterThan(0)
        expect(Number(result?.run?.downloaded_count || 0) + Number(result?.run?.failed_count || 0)).toBeGreaterThan(0)
      }
    } finally {
      if (runId) {
        cleanupScraperRun(DB_PATH, runId)
        rmBestEffort(join(resolve(ARTIFACTS_ROOT), 'scraper_lab', `run_${runId}`))
      }
    }
  })
})
