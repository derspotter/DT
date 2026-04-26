import { expect, test, type APIRequestContext, type Page } from '@playwright/test'
import { createRequire } from 'node:module'
import {
  copyFileSync,
  existsSync,
  mkdirSync,
  readFileSync,
  rmSync,
  statSync,
  writeFileSync,
} from 'node:fs'
import { basename, dirname, join, resolve } from 'node:path'

const require = createRequire(import.meta.url)
const Database = require('../../backend/node_modules/better-sqlite3')

const API_BASE = process.env.E2E_API_BASE || 'http://localhost:4000'
const METADATA_BIB_PATH =
  process.env.E2E_METADATA_BIB_PATH || '/home/jay/DT/dl_lit_project/data/metadata.bib'
const DB_PATH =
  process.env.E2E_DB_PATH || '/home/jay/DT/dl_lit_project/data/literature.db'
const RUN_UPSTREAM_METADATA_E2E = process.env.RUN_UPSTREAM_METADATA_E2E === '1'
const CONTAINER_WORKDIR = process.env.E2E_CONTAINER_WORKDIR || '/usr/src/app'
const HOST_WORKDIR = process.env.E2E_HOST_WORKDIR || resolve(process.cwd(), '..')

type KantroposTarget = {
  id: string
  name: string
  path: string
  metadata_bib_path?: string
}

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
  path: string,
  method: 'GET' | 'POST' | 'PUT' | 'DELETE' = 'GET',
  body?: unknown
): Promise<any> {
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
  return text ? JSON.parse(text) : null
}

function firstBibEntries(sourcePath: string, count = 2): string {
  const content = readFileSync(sourcePath, 'utf8')
  const starts = [...content.matchAll(/^@[A-Za-z]+\s*\{/gm)].map((match) => Number(match.index))
  if (starts.length === 0) {
    throw new Error(`No BibTeX entries found in ${sourcePath}`)
  }
  const end = starts[Math.min(count, starts.length)] ?? content.length
  return `${content.slice(starts[0], end).trim()}\n`
}

function prepareMetadataBib(targetPath: string, sourcePath: string, testRunId: string) {
  mkdirSync(targetPath, { recursive: true })
  const metadataPath = join(targetPath, 'metadata.bib')
  const backupPath = join(targetPath, `.metadata.bib.${testRunId}.bak`)
  if (existsSync(metadataPath)) {
    copyFileSync(metadataPath, backupPath)
  }
  writeFileSync(metadataPath, firstBibEntries(sourcePath), 'utf8')
  return { metadataPath, backupPath }
}

function hostPathForTargetPath(targetPath: string): string {
  const normalized = resolve(targetPath)
  const normalizedContainerRoot = resolve(CONTAINER_WORKDIR)
  if (normalized === normalizedContainerRoot || normalized.startsWith(`${normalizedContainerRoot}/`)) {
    return join(HOST_WORKDIR, normalized.slice(normalizedContainerRoot.length))
  }
  return normalized
}

function containerPathForHostPath(hostPath: string): string {
  const normalized = resolve(hostPath)
  const normalizedHostRoot = resolve(HOST_WORKDIR)
  if (normalized === normalizedHostRoot || normalized.startsWith(`${normalizedHostRoot}/`)) {
    return join(CONTAINER_WORKDIR, normalized.slice(normalizedHostRoot.length))
  }
  return normalized
}

function restoreMetadataBib(metadataPath: string, backupPath: string) {
  if (existsSync(backupPath)) {
    copyFileSync(backupPath, metadataPath)
    rmSync(backupPath, { force: true })
    return
  }
  rmSync(metadataPath, { force: true })
}

function seedDownloadedScraperArtifact(dbPath: string, sourceFile: string, testRunId: string) {
  const db = new Database(dbPath)
  const run = db
    .prepare(
      `INSERT INTO scraper_runs (
         source_type, query_json, status, total_count, downloaded_count, failed_count,
         created_at, started_at, finished_at
       ) VALUES (
         'eurlex', ?, 'completed', 1, 1, 0,
         CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
       )`
    )
    .run(JSON.stringify({ playwright_test: testRunId }))
  const runId = Number(run.lastInsertRowid)
  const key = `playwright_upstream_${testRunId}`
  const artifact = db
    .prepare(
      `INSERT INTO scraper_artifacts (
         run_id, source_type, source_id, title, date, year, source_url, download_url,
         local_artifact_path, file_name, raw_metadata_json, bibtex_key, metadata_completeness,
         status, created_at, updated_at
       ) VALUES (?, 'eurlex', ?, ?, '2026-04-24', 2026, ?, ?, ?, ?, ?, ?, 'minimal', 'downloaded', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`
    )
    .run(
      runId,
      `PW-${testRunId}`,
      `Playwright upstream update ${testRunId}`,
      'https://example.test/playwright-source',
      'https://example.test/playwright-artifact.pdf',
      sourceFile,
      basename(sourceFile),
      JSON.stringify({ playwright_test: testRunId }),
      key
    )
  db.close()
  return { runId, artifactId: Number(artifact.lastInsertRowid), bibtexKey: key }
}

function cleanupScraperRows(dbPath: string, runId: number) {
  const db = new Database(dbPath)
  db.prepare('DELETE FROM scraper_artifacts WHERE run_id = ?').run(runId)
  db.prepare('DELETE FROM scraper_runs WHERE id = ?').run(runId)
  db.close()
}

function rmBestEffort(pathname: string) {
  try {
    rmSync(pathname, { force: true, recursive: true })
  } catch {
    // Files copied by the backend container can be owned by that container user.
  }
}

test.describe('upstream metadata.bib assignment and update', () => {
  test.skip(!RUN_UPSTREAM_METADATA_E2E, 'Set RUN_UPSTREAM_METADATA_E2E=1 to run this mutating upstream metadata.bib test.')
  test.setTimeout(120_000)

  test('assigns a corpus to an upstream target and applies a downloaded artifact', async ({ page, request }, testInfo) => {
    test.skip(!existsSync(METADATA_BIB_PATH), `Missing metadata fixture: ${METADATA_BIB_PATH}`)
    test.skip(!existsSync(DB_PATH), `Missing database for scraper artifact setup: ${DB_PATH}`)

    const token = await ensureSignedIn(page)
    const targetPayload = await authedJson(request, token, '/api/kantropos/corpora')
    const targets = (targetPayload?.corpora || []) as KantroposTarget[]
    const requestedTargetId = process.env.E2E_KANTROPOS_TARGET_ID || ''
    const target =
      targets.find((entry) => requestedTargetId && String(entry.id) === requestedTargetId) ||
      targets.find((entry) => resolve(entry.metadata_bib_path || '') === resolve(METADATA_BIB_PATH)) ||
      targets[0]
    test.skip(!target, 'No upstream/Kantropos target is configured in the backend.')
    test.skip(!target.path, `Selected upstream target ${target?.id || ''} has no path.`)

    const testRunId = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
    const corpusName = `Playwright upstream ${testRunId}`
    const targetPath = hostPathForTargetPath(target.path)
    const artifactSource = join(targetPath, `.tmp-playwright-artifacts/artifact-${testRunId}.pdf`)
    mkdirSync(dirname(artifactSource), { recursive: true })
    writeFileSync(artifactSource, 'playwright artifact placeholder\n', 'utf8')

    let corpusId = 0
    let metadataPath = ''
    let backupPath = ''
    let runId = 0
    let bibtexKey = ''

    try {
      ;({ metadataPath, backupPath } = prepareMetadataBib(targetPath, METADATA_BIB_PATH, testRunId))
      const beforeSize = statSync(metadataPath).size

      const createdCorpus = await authedJson(request, token, '/api/corpora', 'POST', { name: corpusName })
      corpusId = Number(createdCorpus.id)
      await authedJson(request, token, `/api/corpora/${corpusId}/select`, 'POST')
      ;({ runId, bibtexKey } = seedDownloadedScraperArtifact(DB_PATH, containerPathForHostPath(artifactSource), testRunId))

      await page.reload()
      await ensureSignedIn(page)
      await page.goto('/#/upstream')
      await page.getByTestId('tab-upstream').click()
      await expect(page.getByText('Keep local corpora aligned with the upstream baseline.')).toBeVisible()

      await authedJson(request, token, `/api/corpora/${corpusId}/kantropos-assignment`, 'PUT', {
        targetId: target.id,
      })
      await page.reload()
      await page.goto('/#/upstream')
      await expect(page.getByText(`Assigned to: ${target.name}`)).toBeVisible({ timeout: 15_000 })

      const browseTargetSelect = page
        .locator('.upstream-browser__target-select')
        .locator('select')
      await browseTargetSelect.selectOption(target.id)
      await page.getByRole('button', { name: /Refresh|Refreshing/ }).click()
      await expect(page.getByText('metadata.bib modified')).toBeVisible()

      await page.goto('/#/scraper')
      await expect(page.getByText('Collect EU source material without touching the literature corpus.')).toBeVisible()
      await page.getByRole('button', { name: new RegExp(`Run #${runId}`) }).click()

      const applyTargetSelect = page
        .locator('.apply-target')
        .locator('select')
      await applyTargetSelect.selectOption(target.id)
      await page.getByRole('button', { name: 'Select ready' }).click()
      await page.getByRole('button', { name: 'Apply to upstream corpus' }).click()
      await expect(page.getByText('Applied 1; skipped 0; errors 0.')).toBeVisible({ timeout: 15_000 })

      const updatedBib = readFileSync(metadataPath, 'utf8')
      expect(updatedBib).toContain(`@misc{${bibtexKey},`)
      expect(statSync(metadataPath).size).toBeGreaterThan(beforeSize)
      expect(existsSync(join(targetPath, 'scraper', 'eurlex', basename(artifactSource)))).toBeTruthy()
    } finally {
      if (runId) cleanupScraperRows(DB_PATH, runId)
      if (corpusId) {
        await authedJson(request, token, `/api/corpora/${corpusId}`, 'DELETE').catch(() => null)
      }
      if (metadataPath) restoreMetadataBib(metadataPath, backupPath)
      if (targetPath) {
        rmBestEffort(join(targetPath, 'scraper', 'eurlex', basename(artifactSource)))
        rmBestEffort(join(targetPath, 'scraper', 'eurlex', `${basename(artifactSource, '.pdf')}.json`))
      }
      rmBestEffort(artifactSource)
    }
  })
})
