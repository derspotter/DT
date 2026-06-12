import { test, expect, type APIRequestContext, type Page } from '@playwright/test'

// Real authenticated round-trip for the BibTeX seed upload against the live
// backend + DB. Uploads a uniquely-marked probe entry; the surrounding shell
// script pre-/post-cleans it from the database so the corpus is not polluted.
// Run explicitly: npx playwright test tests/seed-upload.real.spec.ts

// Opt-in: this hits the live backend/DB and creates a probe work that must be
// cleaned up afterwards (see the surrounding script), so it is skipped unless
// explicitly enabled.
test.skip(process.env.RUN_SEED_UPLOAD_REAL !== '1', 'set RUN_SEED_UPLOAD_REAL=1 to run the live round-trip')

const PROBE_DOI = '10.5555/playwright-e2e-seed-probe'
const PROBE_TITLE = 'Playwright E2E Seed Probe Work'

async function ensureSignedIn(page: Page) {
  await page.goto('/')
  const heading = page.getByRole('heading', { name: 'Corpus orchestration workspace' })
  if (await heading.isVisible().catch(() => false)) return

  const username = process.env.E2E_USERNAME || process.env.RAG_ADMIN_USER || ''
  const password = process.env.E2E_PASSWORD || process.env.RAG_ADMIN_PASSWORD || ''
  expect(username, 'Missing RAG_ADMIN_USER').toBeTruthy()
  expect(password, 'Missing RAG_ADMIN_PASSWORD').toBeTruthy()

  const signInButton = page.getByRole('button', { name: 'Sign in' })
  if (await signInButton.isVisible().catch(() => false)) {
    await page.getByRole('textbox', { name: 'Username' }).fill(username)
    await page.getByRole('textbox', { name: 'Password' }).fill(password)
    await signInButton.click()
  }
  await expect(heading).toBeVisible({ timeout: 20_000 })
}

test('BibTeX seed upload imports into the live corpus (real round-trip)', async ({ page }) => {
  test.setTimeout(90_000)
  await ensureSignedIn(page)

  const fileInput = page.locator('input[type="file"]').first()
  await expect(fileInput).toBeAttached()

  const uploadResponse = page.waitForResponse(
    (res) => new URL(res.url()).pathname === '/api/process_pdf' && res.request().method() === 'POST',
    { timeout: 60_000 }
  )
  const importResponse = page.waitForResponse(
    (res) => new URL(res.url()).pathname === '/api/ingest/import-seed' && res.request().method() === 'POST',
    { timeout: 60_000 }
  )
  await fileInput.setInputFiles({
    name: 'playwright-e2e-seed.bib',
    mimeType: 'application/x-bibtex',
    buffer: Buffer.from(
      `@article{probe, title={${PROBE_TITLE}}, author={Probe, Pat}, year={2024}, doi={${PROBE_DOI}}}\n`
    ),
  })

  // The .bib uploaded, then the client imported it into the corpus.
  expect((await uploadResponse).status()).toBe(200)
  const res = await importResponse
  expect(res.status()).toBe(200)
  const body = await res.json()
  // Pre-clean removes any prior probe, so the live import adds exactly one work
  // and links it to the corpus.
  expect(body).toMatchObject({ added: 1, corpus_linked: 1 })
})
