// Capture the responsive screenshot matrix for the DT app.
//
// Usage:
//   node scripts/responsive-shots.mjs <phase> [tab1,tab2,...] [width1,width2,...]
//
//   phase   - subdir under artifacts/responsive/ (e.g. baseline, after, task4)
//   tabs    - optional comma list (default: all)
//   widths  - optional comma list (default: 390,768,1024)
//
// Requires: backend on :4000, frontend dev server on :5175, and a valid token
// in /tmp/dt-token.txt (write it via the API login). Run from repo root.
import pw from '/home/jay/DT/frontend/node_modules/@playwright/test/index.js'
const { chromium } = pw
import { mkdirSync, readFileSync } from 'node:fs'

const BASE = 'http://localhost:5175'
const phase = process.argv[2] || 'baseline'
const ALL_TABS = ['workspace', 'upstream', 'scraper', 'graph', 'dashboard', 'downloads', 'logs', 'admin']
const tabsArg = process.argv[3]
const tabs = (!tabsArg || tabsArg === 'all' ? ALL_TABS : tabsArg.split(',')).map((t) => t.trim()).filter(Boolean)
const widths = (process.argv[4] ? process.argv[4].split(',') : ['390', '768', '1024']).map(Number)
const HEIGHTS = { 390: 844, 768: 1024, 1024: 768, 1366: 768 }

const token = readFileSync('/tmp/dt-token.txt', 'utf8').trim()
if (!token) throw new Error('no token in /tmp/dt-token.txt')

const outDir = `/home/jay/DT/artifacts/responsive/${phase}`
mkdirSync(outDir, { recursive: true })

const browser = await chromium.launch({ args: ['--enable-unsafe-swiftshader', '--no-sandbox'] })
const ctx = await browser.newContext()
const page = await ctx.newPage()

// Seed the auth token, then a full reload so the app boots authenticated
// (the module reads localStorage at load time).
await page.goto(BASE)
await page.evaluate((t) => localStorage.setItem('rag_feeder_token', t), token)
await page.reload()
await page.waitForSelector('[data-testid="side-nav"], .side-nav', { timeout: 20000 })

const overflow = []
for (const w of widths) {
  await page.setViewportSize({ width: w, height: HEIGHTS[w] || 800 })
  for (const tab of tabs) {
    // Hash routing is handled in-app via hashchange (no reload needed once authed).
    await page.evaluate((t) => { window.location.hash = `#/${t}` }, tab)
    await page.waitForTimeout(1400) // let data + layout settle
    const scrollW = await page.evaluate(() => document.documentElement.scrollWidth)
    const innerW = await page.evaluate(() => window.innerWidth)
    if (scrollW > innerW + 1) overflow.push(`${tab}@${w}: page scrollWidth ${scrollW} > ${innerW}`)
    // The graph's auto-rotating WebGL canvas never settles for a full-page
    // capture, so shoot its viewport instead; everything else is full-page.
    await page.screenshot({
      path: `${outDir}/${tab}-${w}.png`,
      fullPage: tab !== 'graph',
      animations: 'disabled',
      timeout: 60000,
    })
    process.stdout.write(`  ${tab}@${w} (scrollW=${scrollW}/${innerW})\n`)
  }
}

await browser.close()
console.log(`\nSaved ${tabs.length * widths.length} shots to ${outDir}`)
if (overflow.length) {
  console.log('\n⚠ PAGE-LEVEL HORIZONTAL OVERFLOW:')
  for (const o of overflow) console.log('  - ' + o)
} else {
  console.log('\n✓ no page-level horizontal overflow at any width')
}
