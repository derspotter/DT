# Responsive Layout Overhaul — Design

**Date:** 2026-06-16
**Status:** Approved (design), pending implementation plan
**Scope:** `frontend/src/app.css`, `frontend/src/App.svelte`, and component `.svelte` files where tables/grids need a scroll wrapper. Pure CSS plus minimal markup wrappers — no app logic/state changes.

## Goal

Make the whole DT app usable across all viewports, with **phone** and **tablet** as the primary targets. The app already ships a responsive layer (viewport meta + ~13 media queries), but it is scattered, has overlapping/duplicate breakpoints, and has concrete gaps. This work consolidates the breakpoint system and closes the gaps.

## Context: current state (as of this design)

Verified by reading `frontend/src/app.css` and `App.svelte`.

**Already works:**
- `<meta name="viewport" content="width=device-width, initial-scale=1.0">` is present (`frontend/index.html`).
- Side-nav collapses to a horizontal pill row; app header stacks; all `.table-row.cols-*` data tables collapse to a single `1fr` column at `≤980px`; workspace search/seed grids stack; graph-3d controls pack two-per-row; the upstream SVG map stacks.

**Problems / gaps:**
1. **Ad-hoc, overlapping breakpoints**: 1450 / 1280 / 1100 / 980 / 800 / 780 / 760 / 640 / 560, including **two** separate `980px` blocks, **two** `780px` blocks, and **two** `1100px` blocks whose rules overlap and partially fight (e.g. the side-nav is collapsed by both the 980 and 780 rules). No documented tablet vs phone tiers.
2. **`.corpus-columns` never collapses** — the Raw / Metadata / Downloaded pipeline stays 3 equal columns down to 320px. Worst phone offender.
3. **Data tables stack to a single `1fr` column at `≤980px`** — loses the column layout early on tablets, and on phones the cells stack with no field labels (the header row also becomes `1fr`), producing ambiguous data.
4. **No tablet tier** — layouts jump from desktop (`>980`) straight to phone-ish. Tablet portrait (~768) / landscape (~1024) fall into half-broken states; fixed-px multi-column rows (e.g. `cols-corpus-main` with 132px columns) may overflow horizontally in the 800–980 band.
5. **Touch targets** shrink to ~8px padding / 0.85rem on phone — below the ~44px guideline.

## Decisions (locked with user)

- **Tiers (CSS pixels):** Desktop `>1024px` (default, untouched) · **Tablet `≤1024px`** · **Phone `≤640px`**. CSS-pixel widths are correct despite high-DPI panels because `width=device-width` lays out in device-independent pixels (e.g. iPhone 15 Pro = 393 CSS px, iPad portrait = 820 CSS px). Width is the primary lever; orientation manifests as width.
- **Phone data tables:** **horizontal scroll, keep columns.** Wrap each table in an `overflow-x:auto` container and give `.table-row` a `min-width` so columns keep proportions and the table scrolls sideways. **Remove** the current `.table-row → 1fr` stack. Reuse the existing `.table.table-scroll` concept rather than adding a parallel mechanism.
- **Breakpoints:** **consolidate** into the two tiers in one delimited "Responsive" section, **deleting** the redundant old media-query blocks as their rules are migrated. Keep at most two justified auxiliaries (the existing `max-height: 860px` short-desktop rule; optionally a `≤400px` narrow-phone nicety), each with an explanatory comment.
- **Verification:** stand up the real backend + frontend and **screenshot every view at 390 / 768 / 1024px** as a baseline, change, re-screenshot the same matrix, diff.

## Architecture / approach

A single, clearly delimited **"Responsive"** section is added at the **end of `app.css`** so its tier rules win the cascade. The scattered media-query rules are migrated into the two tier blocks **incrementally**, deleting each old block as its rules move, with screenshot validation after each migration step. Desktop default rules (`>1024px`) are never edited.

### Per-area behavior

- **App shell + header:** stack at tablet, denser padding at phone (mostly exists; consolidate into tiers).
- **Side-nav:** desktop sidebar → tablet/phone horizontal wrapping pill row, sticky top, **one** source of truth (removes the duplicated 980+780 definitions). Touch targets ≥ ~44px.
- **Data tables (`.table-row.cols-*`):** The horizontal-scroll wrapper is present at **both** tiers (it only scrolls when content exceeds the panel width), which also fixes the tablet-band (800–980) overflow from fixed-px multi-column rows.
  - *Tablet:* keep columns; `.corpus-columns` drops to **1-up** so each table gets full width; the table keeps its column grid and scrolls horizontally only if it would otherwise overflow.
  - *Phone:* `.table-row` gets a `min-width` so columns keep proportions and scroll sideways; **the `1fr` stack is removed**; header and body rows share the min-width grid so they stay aligned. Add `-webkit-overflow-scrolling: touch` and a subtle edge-fade scroll affordance. Tune `min-width` so the most-important columns are visible first.
- **3D graph view:** controls wrap to 2-col (tablet) / full-width stack (phone); canvas min-height; detail panel stacks under canvas. Keep usable; do not chase phone-gesture polish. Optional: more canvas height in landscape via orientation/aspect rule.
- **Upstream / corpus SVG maps:** stack metrics/details with min-heights (exists; fold into tiers).
- **Dashboard / ScraperLab / Admin / Logs:** inherit the global table/card/grid tiers; align ScraperLab's component-scoped `760px` block to the tiers.

### Markup changes

Pure CSS, except: add a wrapping `<div class="table-scroll-x">` (or reuse the existing `.table.table-scroll` wrapper) around any `.table` block that lacks a horizontal-scroll container. No state/logic changes.

### Edge cases

- No horizontal overflow of the **page** at any width — scrolling is contained to table panels, not `body`. Verify `min-width:0` on flex/grid children (mostly present).
- Preserve landscape-phone and short-viewport desktop behavior (existing `max-height` rule).
- `prefers-reduced-motion` already handled; keep.

## Testing / verification

- **Baseline + after:** stand up backend + frontend; screenshot every view (workspace, corpus pipeline, upstream, graph, dashboard, downloads, logs, admin, scraper lab) at **390 / 768 / 1024px** before and after; diff visually. Confirm: no horizontal page overflow, tables scroll within panels, nav reachable, touch targets ≥ ~40px.
- **Regression:** run existing Playwright e2e (`graph-3d`, `navigation`, `app`). Adjust only assertions that legitimately change (e.g. a phone-width test asserting `.corpus-columns` stacks). Keep additions minimal.

## Risks

- **Desktop regression** from touching shared CSS → mitigated by additive end-of-file tier blocks, incremental screenshot validation, and never editing `>1024px` defaults.
- **Hidden columns behind horizontal scroll** → tune `min-width` so key columns show first; add an edge-fade affordance.

## Out of scope

- 3D-graph phone-gesture/interaction polish beyond "chrome doesn't break."
- Any backend, data, or app-logic changes.
- A third "large tablet / small laptop" tier (considered, declined — keep 1024/640).
