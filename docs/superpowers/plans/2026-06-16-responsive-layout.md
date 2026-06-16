# Responsive Layout Overhaul Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the whole DT app usable on phone and tablet by consolidating the scattered breakpoints into documented tablet (`≤1024px`) / phone (`≤640px`) tiers and closing the known layout gaps.

**Architecture:** A single delimited "Responsive" section at the **end of `frontend/src/app.css`** owns all tier rules (so it wins the cascade). Scattered media-query rules are migrated into the two tier blocks **incrementally**, deleting each old block as its rules move, with screenshot validation at each step. Desktop defaults (`>1024px`) are never edited. Data tables get a horizontal-scroll wrapper (present at both tiers) plus a phone `min-width` so columns scroll sideways instead of squashing.

**Tech Stack:** Svelte 5 + Vite (frontend), plain CSS (`app.css`), Playwright (e2e regression), Playwright MCP browser tools (screenshots). Backend: Node/Express on `:4000`.

**Spec:** `docs/superpowers/specs/2026-06-16-responsive-layout-design.md`

---

## Verification model (read first)

This is visual CSS work. The per-task loop is **not** unit-test-first TDD. It is:

1. Make the CSS/markup change.
2. **Re-screenshot the affected view(s)** at the relevant widths via Playwright MCP and visually confirm the intended change + no regression.
3. Run the **Playwright e2e** smoke (`graph-3d`, `navigation`, `app`) as an automated regression guard.
4. Commit.

**Screenshot widths:** `390` (phone portrait), `768` (tablet portrait), `1024` (tablet landscape / small laptop). Always include `1366` spot-checks on tasks that touch desktop-adjacent rules to prove desktop is unchanged.

**Screenshot how:** Use the Playwright MCP tools — `mcp__playwright__browser_navigate`, `mcp__playwright__browser_resize` (width, height e.g. 844), `mcp__playwright__browser_take_screenshot` (save to `artifacts/responsive/<phase>/<view>-<width>.png`). Load schemas first with `ToolSearch` query `select:mcp__playwright__browser_navigate,mcp__playwright__browser_resize,mcp__playwright__browser_take_screenshot,mcp__playwright__browser_snapshot,mcp__playwright__browser_click,mcp__playwright__browser_type`.

**Views to capture (the matrix):** workspace, corpus pipeline (the Raw/Metadata/Downloaded columns), upstream (Korpus Management), graph, dashboard (Pipeline Status — admin), downloads (admin), logs (admin), admin, scraper lab.

---

## File structure

| File | Responsibility | Change |
|---|---|---|
| `frontend/src/app.css` | All global layout + the new "Responsive" tier section | Heavily modified: add end-of-file Responsive section; migrate + delete scattered media blocks |
| `frontend/src/App.svelte` | App shell markup; the corpus/seed/search/downloads/logs `.table` blocks live here | Add `.table-scroll-x` wrappers around tables lacking a horizontal-scroll container |
| `frontend/src/components/ScraperLab.svelte` | Component-scoped `@media (max-width:760px)` | Align its breakpoint to the tier system |
| `frontend/src/components/*.svelte` (Dashboard, Logs, AdminPanel, UpstreamBrowser, UpstreamCorpusMap) | Render tables/cards using global classes | Add scroll wrappers only where a `.table` block lacks one |
| `artifacts/responsive/` | Screenshot baseline + after sets (gitignored or committed as evidence) | New dir |

---

## Task 0: Stand up the app + capture the baseline screenshot matrix

**Files:** none modified (infra + screenshots only).

- [ ] **Step 1: Start the backend**

Run (background): `cd /home/jay/DT && node backend/src/index.js`
The backend reads `/home/jay/DT/.env` (already present) and bootstraps the admin from `RAG_ADMIN_USER` / `RAG_ADMIN_PASSWORD`. It listens on `:4000`.
Expected log: `HTTP server listening on port 4000`. If it exits complaining about a DB path, set `RAG_FEEDER_DB_PATH` / the documented DB env var to `dl_lit_project/data/literature.db` (grep `backend/src/app.js` for the DB-path env name) and restart.

- [ ] **Step 2: Start the frontend dev server**

Run (background): `cd /home/jay/DT/frontend && npm run dev`
Expected: Vite serving on `http://localhost:5175`, proxying `/api` → `:4000`.

- [ ] **Step 3: Read admin credentials (do not echo secrets into the transcript)**

Read `RAG_ADMIN_USER` and `RAG_ADMIN_PASSWORD` values from `/home/jay/DT/.env` for use in the login form. Keep them out of any committed file or printed output.

- [ ] **Step 4: Log in via Playwright MCP**

Navigate to `http://localhost:5175`, fill Username / Password, click "Sign in". Confirm the heading `Corpus orchestration workspace` is visible.

- [ ] **Step 5: Capture the baseline matrix**

For each view in the matrix and each width in {390, 768, 1024}: resize the browser, navigate to the view's tab (use the side-nav / hash), and screenshot to `artifacts/responsive/baseline/<view>-<width>.png`. Note in a scratch list every view where you observe: horizontal page scroll, overlapping/cut-off controls, the 3-up pipeline columns crammed, or unreadable tables. This list is the concrete punch-list the later tasks must clear.

- [ ] **Step 6: Confirm the e2e baseline is green**

Run: `cd /home/jay/DT/frontend && E2E_USERNAME=<user> E2E_PASSWORD=<pass> npx playwright test navigation graph-3d app`
Expected: PASS (these need the running servers; `reuseExistingServer` is on). This is the regression baseline.

- [ ] **Step 7: Commit the baseline evidence**

```bash
git add artifacts/responsive/baseline
git commit -m "test: capture responsive baseline screenshots (390/768/1024)"
```

---

## Task 1: Scaffold the "Responsive" section + tier convention

**Files:** Modify `frontend/src/app.css` (append at end).

- [ ] **Step 1: Append the delimited Responsive section**

Add at the very end of `app.css`:

```css
/* ==========================================================================
   RESPONSIVE  —  single source of truth for adaptive layout
   Tiers (CSS pixels; width=device-width means high-DPI phones report ~390,
   tablets ~768 portrait / ~1024 landscape):
     Desktop  > 1024px   default rules above; never edited here
     Tablet   <= 1024px  .responsive-tablet block
     Phone    <= 640px   .responsive-phone block (also matches tablet rules)
   Auxiliary (justified): short-desktop max-height rule; narrow-phone <=400.
   Rules are migrated here from the old scattered @media blocks; the old
   blocks are deleted as their rules move. Phone rules assume the tablet
   block already applied (cascade), so only add phone-specific overrides.
   ========================================================================== */

@media (max-width: 1024px) {
  /* tablet tier — populated by later tasks */
}

@media (max-width: 640px) {
  /* phone tier — populated by later tasks */
}
```

- [ ] **Step 2: Build to confirm no syntax error**

Run: `cd /home/jay/DT/frontend && npm run build`
Expected: build succeeds (empty media blocks are valid CSS; no visual change yet).

- [ ] **Step 3: Commit**

```bash
git add frontend/src/app.css
git commit -m "style: scaffold consolidated responsive tier section"
```

---

## Task 2: Consolidate the side-nav into one source of truth

The side-nav is currently collapsed by **both** the `980px` block (`app.css:3340`) and the `780px` block (`app.css:5044+`). Unify into the tablet tier.

**Files:** Modify `frontend/src/app.css`.

- [ ] **Step 1: Move side-nav collapse into the tablet tier**

In the new `@media (max-width: 1024px)` block, add:

```css
  .layout { grid-template-columns: 1fr; gap: 14px; }
  .side-nav {
    flex-direction: row; flex-wrap: wrap; gap: 8px;
    padding: 10px 12px; position: static; top: auto;
  }
  .nav-group { flex-direction: row; flex-wrap: wrap; gap: 8px; }
  .nav-group-title { display: none; }
  .side-nav button { padding: 12px 16px; font-size: 0.9rem; white-space: nowrap; min-height: 44px; }
```

(`min-height: 44px` fixes the small-touch-target gap.)

- [ ] **Step 2: Delete the now-duplicated side-nav rules from the old blocks**

Remove the side-nav / `.layout` / `.nav-group` rules from the `980px` block (`app.css:3336-3344`) and the entire side-nav portion of the `780px` block (`app.css:5044-5081`). Leave non-side-nav rules in those blocks untouched for now (later tasks handle them).

- [ ] **Step 3: Re-screenshot the nav at 390 / 768 / 1024**

Capture `artifacts/responsive/after/nav-<width>.png` on the workspace + graph tabs. Confirm: one pill row, no doubled/conflicting spacing, buttons ≥44px tall, all tabs reachable without scrolling past the menu.

- [ ] **Step 4: Spot-check desktop unchanged**

Screenshot at 1366; confirm the sidebar is still a vertical 220px column.

- [ ] **Step 5: e2e regression**

Run: `npx playwright test navigation`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add frontend/src/app.css
git commit -m "style: unify side-nav collapse into tablet tier"
```

---

## Task 3: Consolidate header + app-shell into tiers

Header rules are spread across `980px` (3286-3334), `1100px` (5026-5042), `760px` (4798-4817), and the `max-height` rule (4884+).

**Files:** Modify `frontend/src/app.css`.

- [ ] **Step 1: Migrate header rules into the tablet tier**

Move the header-stacking rules (`.app-header` column, `.header-panel` full width + top border, `.header-corpus`/`.header-invite`/`.header-user` single column, `.header-main--workspace` single column, `.header-corpus-picker` width) into the `@media (max-width: 1024px)` block. Keep them byte-equivalent to the originals; only the breakpoint changes (980/1100 → 1024).

- [ ] **Step 2: Migrate phone-density header rules into the phone tier**

Move the `760px` header rules (smaller `h1`, 14px padding, full-width corpus actions) into `@media (max-width: 640px)`, plus:

```css
  .app-shell { padding: 12px clamp(12px,3vw,18px) 16px; gap: 12px; }
```

- [ ] **Step 3: Delete the migrated rules from the old 980 / 1100 / 760 blocks**

Remove exactly the header rules moved in Steps 1–2 from blocks at 3286-3334, 5026-5042, 4798-4817. Keep the `max-height: 860px` short-desktop block (4884) as a documented auxiliary — add a one-line comment above it: `/* auxiliary: short landscape desktop, not a width tier */`.

- [ ] **Step 4: Re-screenshot header at 390 / 768 / 1024 + desktop 1366**

Confirm header stacks cleanly, corpus picker full-width on tablet/phone, no overflow; desktop unchanged.

- [ ] **Step 5: e2e regression**

Run: `npx playwright test navigation app`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add frontend/src/app.css
git commit -m "style: consolidate header + app-shell into tiers"
```

---

## Task 4: Data tables — horizontal-scroll wrapper + phone min-width

This is the core change. Replace the `.table-row → 1fr` stack (`app.css:3362-3364`) with horizontal scroll that preserves columns.

**Files:** Modify `frontend/src/app.css`, `frontend/src/App.svelte`, and the component files that render `.table` blocks without a scroll wrapper.

- [ ] **Step 1: Define the scroll wrapper utility**

In `app.css` (near the existing `.table` rules, ~line 1404), add:

```css
.table-scroll-x { overflow-x: auto; -webkit-overflow-scrolling: touch; }
```

There is an existing `.table.table-scroll` concept (`app.css:696`, `1404`). Inspect it: if it already provides horizontal overflow, reuse it and skip adding `.table-scroll-x`; otherwise add the utility above. Document which you chose in the commit message.

- [ ] **Step 2: Wrap tables that lack a horizontal-scroll container**

For each `.table` block in `App.svelte` (and Dashboard/Logs/AdminPanel/UpstreamBrowser) that is NOT already inside a `.table.table-scroll` / `.table-scroll-x`, wrap the `<div class="table ...">…</div>` in `<div class="table-scroll-x">…</div>`. Identify them by grepping: `grep -n 'class="table' frontend/src/App.svelte frontend/src/components/*.svelte`. Do not wrap the `.corpus-columns` container itself — wrap the individual tables inside each column.

- [ ] **Step 3: Remove the `1fr` stack and add phone min-width**

In `app.css`, delete the `.table-row { grid-template-columns: 1fr; }` rule from the `980px` block (3362-3364). In the `@media (max-width: 640px)` tier, add a `min-width` to each table-row variant so columns keep proportions and the wrapper scrolls. Starting values (derived from each `grid-template-columns` track sum; tune against screenshots in Step 4):

```css
  .table-row.cols-3 { min-width: 420px; }
  .table-row.cols-4 { min-width: 520px; }
  .table-row.cols-5 { min-width: 640px; }
  .table-row.cols-6 { min-width: 720px; }
  .table-row.cols-7 { min-width: 760px; }
  .table-row.cols-search { min-width: 680px; }
  .table-row.cols-runs { min-width: 560px; }
  .table-row.cols-corpus,
  .table-row.cols-corpus-main,
  .table-row.cols-corpus-workspace { min-width: 640px; }
  .table-row.cols-upstream-current { min-width: 700px; }
```

Header and body rows share these variants, so they stay column-aligned while scrolling.

- [ ] **Step 4: Re-screenshot every table view at 390 / 768**

Capture corpus pipeline, downloads, logs, search results, upstream tables. Confirm: tables scroll sideways within their panel (not the page), header aligns with body columns, the most-important column (title/name) is visible before scrolling. Adjust the `min-width` values where a table feels too wide or columns collide.

- [ ] **Step 5: Confirm no page-level horizontal scroll**

At 390px on each view, verify `document.documentElement.scrollWidth <= window.innerWidth` via `mcp__playwright__browser_evaluate`. Expected: true (overflow is confined to `.table-scroll-x`).

- [ ] **Step 6: e2e regression**

Run: `npx playwright test app navigation`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add frontend/src/app.css frontend/src/App.svelte frontend/src/components
git commit -m "style: horizontal-scroll data tables on small screens (keep columns)"
```

---

## Task 5: Pipeline `.corpus-columns` → 1-up on tablet/phone

`.corpus-columns` (`app.css:3385`) is `repeat(3, 1fr)` and never collapses — the worst phone offender.

**Files:** Modify `frontend/src/app.css`.

- [ ] **Step 1: Stack the pipeline columns in the tablet tier**

In `@media (max-width: 1024px)` add:

```css
  .corpus-columns { grid-template-columns: 1fr; }
```

(Each of Raw / Metadata / Downloaded now spans full width; the tables inside keep their own column grid and scroll via Task 4 if needed.)

- [ ] **Step 2: Re-screenshot the corpus pipeline at 390 / 768 / 1024 + desktop 1366**

Confirm: one column per stage stacked vertically on tablet/phone, each readable; desktop still 3-up.

- [ ] **Step 3: e2e regression**

Run: `npx playwright test navigation`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add frontend/src/app.css
git commit -m "style: stack pipeline columns 1-up on tablet/phone"
```

---

## Task 6: Workspace seed/search grids → tiers

Currently at `1280px` (4256), `800px` (1312), `760px` (4819-4835).

**Files:** Modify `frontend/src/app.css`.

- [ ] **Step 1: Migrate into tiers**

Move `.seed-corpus-toolbar .seed-intake-grid → 1fr` and the search-row/scope/submit stacking into the tablet tier; move the phone-density seed-search rules (`760px`) into the phone tier. Keep rule bodies byte-equivalent.

- [ ] **Step 2: Delete migrated rules from the 1280 / 800 / 760 blocks**

Remove exactly what was moved. If a block becomes empty, delete the whole `@media` block.

- [ ] **Step 3: Re-screenshot workspace at 390 / 768 / 1024 + 1366**

Confirm search + seed cards stack cleanly; desktop unchanged.

- [ ] **Step 4: e2e regression**

Run: `npx playwright test app`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add frontend/src/app.css
git commit -m "style: fold workspace search/seed grids into tiers"
```

---

## Task 7: 3D graph controls + canvas → tiers

Currently at `980px` (3346-3352 graph-layout) and `780px` (5083+ graph-3d-controls) and `760px` (4845-4851 canvas/summary).

**Files:** Modify `frontend/src/app.css`.

- [ ] **Step 1: Migrate graph rules into tiers**

Tablet tier: `.graph-layout`, `.graph-3d-layout { grid-template-columns: 1fr; }` and the 2-col controls packing. Phone tier: `.graph-3d-canvas { min-height: 420px; }`, `.graph-3d-summary { grid-template-columns: 1fr; }`, full-width stacked controls. Optionally add a landscape rule for more canvas height: `@media (max-width: 1024px) and (orientation: landscape) { .graph-3d-canvas { min-height: 70vh; } }` — document it as an auxiliary.

- [ ] **Step 2: Delete migrated graph rules from old blocks**

- [ ] **Step 3: Re-screenshot graph at 390 / 768 / 1024 + 1366**

Confirm controls reachable, canvas usable, detail panel stacks under canvas; desktop unchanged. The `graph-3d` mock e2e renders here.

- [ ] **Step 4: e2e regression**

Run: `npx playwright test graph-3d`
Expected: PASS (update only assertions that legitimately change layout, e.g. a phone-width column assertion).

- [ ] **Step 5: Commit**

```bash
git add frontend/src/app.css
git commit -m "style: consolidate 3D graph controls + canvas into tiers"
```

---

## Task 8: Upstream / corpus SVG maps → tiers

Currently at `1450px` (4250 summary grid) and `760px` (4853-4865 map canvas/metrics).

**Files:** Modify `frontend/src/app.css`.

- [ ] **Step 1: Migrate map rules into tiers**

Tablet tier: `.upstream-summary-grid { grid-template-columns: repeat(2, minmax(0,1fr)); }`. Phone tier: `.upstream-map` padding, `.upstream-map__canvas/svg { min-height: 300px; }`, `.upstream-map__details/__metrics { grid-template-columns: 1fr; }`.

- [ ] **Step 2: Delete migrated rules from the 1450 / 760 blocks**

- [ ] **Step 3: Re-screenshot upstream (Korpus Management) at 390 / 768 / 1024 + 1366**

Confirm map + metrics stack, SVG has height, no overflow; desktop unchanged.

- [ ] **Step 4: e2e regression**

Run: `npx playwright test upstream-metadata upstream-visualization`
Expected: PASS (these may need the running backend).

- [ ] **Step 5: Commit**

```bash
git add frontend/src/app.css
git commit -m "style: consolidate upstream/corpus maps into tiers"
```

---

## Task 9: ScraperLab block + final breakpoint sweep

**Files:** Modify `frontend/src/components/ScraperLab.svelte`; audit `frontend/src/app.css`.

- [ ] **Step 1: Align ScraperLab's component `@media (max-width:760px)` to the tier system**

In `ScraperLab.svelte:769`, change the breakpoint to `640px` (phone) if its rules are phone-density, or `1024px` (tablet) if they are layout-collapse. Screenshot Scraper Lab at 390/768 to decide. Keep the rules scoped to the component.

- [ ] **Step 2: Audit remaining old breakpoints**

Run: `grep -nE "@media" frontend/src/app.css`
Expected after migration: only `@media (max-width: 1024px)`, `@media (max-width: 640px)`, the documented `max-height: 860px` auxiliary, `prefers-reduced-motion`, and any explicitly-justified landscape/`≤400` auxiliary. Any leftover 1450/1280/1100/980/800/780/760/560 block must be either migrated or, if its rules are already covered by a tier, deleted. Resolve each.

- [ ] **Step 3: Re-screenshot Scraper Lab + a full matrix pass**

Capture every view at 390 / 768 / 1024 into `artifacts/responsive/after/`. Compare against `artifacts/responsive/baseline/`. Confirm the Task 0 punch-list is fully cleared and nothing regressed.

- [ ] **Step 4: e2e regression**

Run: `npx playwright test scraper-lab navigation`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add frontend/src/components/ScraperLab.svelte frontend/src/app.css artifacts/responsive/after
git commit -m "style: align ScraperLab breakpoint + finish breakpoint sweep"
```

---

## Task 10: Full verification + PR

**Files:** none (verification + PR).

- [ ] **Step 1: Full build**

Run: `cd /home/jay/DT/frontend && npm run build`
Expected: clean build.

- [ ] **Step 2: Full e2e suite**

Run: `npx playwright test` (with `E2E_USERNAME` / `E2E_PASSWORD` and servers up)
Expected: PASS. Investigate and fix any failure before proceeding.

- [ ] **Step 3: No-page-overflow assertion across the matrix**

For each view at 390px, assert `document.documentElement.scrollWidth <= window.innerWidth` via `mcp__playwright__browser_evaluate`. Expected: true everywhere.

- [ ] **Step 4: Final breakpoint audit**

Run: `grep -nE "@media" frontend/src/app.css`
Expected: only the two tiers + documented auxiliaries (no stray legacy breakpoints).

- [ ] **Step 5: Open the PR**

```bash
git push -u origin responsive-layout
gh pr create --title "Responsive layout: consolidate breakpoints into tablet/phone tiers" --body "$(cat <<'EOF'
Consolidates the scattered breakpoints (1450/1280/1100/980/800/780/760/640/560,
with overlapping duplicates) into documented tablet (<=1024) / phone (<=640)
tiers, and closes the known gaps:

- pipeline columns (.corpus-columns) now stack 1-up on tablet/phone (were 3-up
  down to 320px)
- data tables scroll horizontally keeping columns, instead of stacking to an
  ambiguous label-less single column
- single source of truth for the side-nav collapse (was collapsed by two
  conflicting blocks)
- tablet tier added (layouts no longer jump desktop -> phone)
- touch targets >= 44px

Verified by screenshotting every view at 390 / 768 / 1024 before and after
(artifacts/responsive/) and running the Playwright e2e suite.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 6: Check Copilot review before merge**

Run: `bash scripts/gh-copilot-comments.sh --wait` and address valid comments before squash-merging (per project standard).

---

## Self-review notes

- **Spec coverage:** tiers (Task 1), side-nav single-source (Task 2), header (Task 3), table horizontal-scroll + remove 1fr stack (Task 4), `.corpus-columns` 1-up (Task 5), workspace grids (Task 6), graph (Task 7), maps (Task 8), ScraperLab + sweep/delete-old-blocks (Task 9), verification matrix + e2e (Tasks 0 & 10). All spec sections map to a task.
- **Touch targets** (spec gap #5) handled in Task 2 Step 1.
- **No-page-overflow edge case** asserted in Task 4 Step 5 and Task 10 Step 3.
- **Desktop-untouched** guaranteed by appending tier rules at end of cascade + 1366 spot-checks in Tasks 2/3/5/6/7/8.
