# Jochen Round 2 — Phase 4: Expansion Safety — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A promotion with expansion on can no longer explode silently: the user sees a confirmation with the ceiling before a big promote, and every expansion path stops at a per-promotion total cap that an admin ceiling bounds.

**Architecture:** One new promotion setting, `maxExpansionTotal` (UI default 1000), flows through the promote route, which clamps it to the admin ceiling `RAG_FEEDER_EXPANSION_HARD_CAP` (default 5000). The three expansion paths honour it: `seed_expand.py` (new-seed mode) via `--max-total`, `keyword_search.py` (search-time expansion) via `--max-total`, and the enrich worker (download-everything mode) via a job budget that shrinks `max_related_per_source` per work. Each reports `truncated`, which the UI surfaces. The confirmation is frontend-only.

**Tech Stack:** Svelte (`frontend/src/App.svelte`), Node/Express (`backend/src/app.js`), Python (`backend/scripts/seed_expand.py`, `backend/scripts/keyword_search.py`, `backend/scripts/daemon/worker.py`), Jest, pytest, Playwright.

**Spec:** `docs/superpowers/specs/2026-09-03-jochen-round-2-design.md`, section "Phase 4 — Expansion safety (items 6, 8a)". Item 8b (recursive PDF extraction) is out of scope by decision.

## Global Constraints

- Line anchors verified at `b0c7b43` on 2026-09-06; this phase is independent of Phase 3 and may land before or after it.
- Setting names: frontend `maxExpansionTotal` (number, min 1, default 1000); request body `maxExpansionTotal`; env `RAG_FEEDER_EXPANSION_HARD_CAP` (default 5000), admin key `expansion_hard_cap`; Python flag `--max-total`; enrich job payload key `maxTotal`.
- Confirmation triggers when expansion is enabled and `items > 25 || ceiling > 500`, where `directions = includeDownstream + includeUpstream` (0..2), `perItem = maxRelated * directions * maxRelated^(depth-1)` with `depth = max(relatedDepthDownstream, relatedDepthUpstream)`, `ceiling = min(items * perItem, maxExpansionTotal)`, `requests ≈ items * directions * (1 + ceil(maxRelated / 50))`.
- Copy, verbatim: `Promote {items} items with {directionsLabel} expansion (depth {depth}, up to {maxRelated} related per item)? This can add up to {ceiling} works and takes about {requests} OpenAlex requests. The expansion stops at the total cap of {maxExpansionTotal}.` Buttons `Promote anyway` and `Cancel`. `directionsLabel` is `downstream`, `upstream` or `downstream and upstream`. Truncation message appended to the promote result: `Expansion stopped at the total cap of {maxExpansionTotal} works.`
- No `window.confirm`: the confirmation is an inline panel in the seed row.
- Commit trailer: `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>` and `Claude-Session: https://claude.ai/code/session_01U3c4gfg7jQvvmPdpLNBN5T`.
- Tests: `cd backend && npm test`; `cd dl_lit_project && /home/jay/DT/venv/bin/python -m pytest tests -q`; `cd frontend && set -a && . ../.env && set +a && npx playwright test`.

---

## File structure

| File | Responsibility |
|---|---|
| `backend/scripts/seed_expand.py` | `--max-total`; stop collecting at the cap; `truncated` + `total_added` in output |
| `backend/scripts/keyword_search.py` | `--max-total` for search-time expansion; `expansion.truncated` |
| `backend/scripts/daemon/worker.py` | `maxTotal` budget across the enrich job; `expansion_truncated` in the result |
| `backend/src/app.js` | `buildUploadedDocsExpansion` + `buildKeywordSearchExpansion` read `maxExpansionTotal`, clamp to the ceiling; pass `--max-total` / `maxTotal`; admin setting `expansion_hard_cap`; `expansion_truncated` in the promote response |
| `frontend/src/App.svelte` | `maxExpansionTotal` state + pill; `expansionEstimate()`; inline confirmation; truncation message |
| `frontend/src/components/AdminPanel.svelte` | `expansion_hard_cap` field |

---

## Task 1: `--max-total` in `seed_expand.py`

**Files:**
- Modify: `backend/scripts/seed_expand.py:84-200` (`main`)
- Test: `dl_lit_project/tests/test_seed_expand_cap.py` (new; loads the script with `importlib` and `monkeypatch.syspath_prepend(scripts dir)` like `tests/test_keyword_search_script.py` in the Phase 3 plan, or like `tests/test_corpus_list_sort.py` today)

**Interfaces:**
- Produces: `--max-total N` (default 0 = unlimited). Output JSON gains `"truncated": bool, "total_added": int`. Items are counted across all seeds and directions; once `total_added >= N` no further run is created and remaining seeds/directions are skipped. A direction whose candidate list would cross the cap is cut to the remaining budget (ranking already happened, so the cut keeps the top of the list).

- [ ] **Step 1: Write the failing test**

```python
import importlib.util, json, sys
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / 'backend' / 'scripts' / 'seed_expand.py'


def _load(monkeypatch):
    monkeypatch.syspath_prepend(str(SCRIPT.parent))
    spec = importlib.util.spec_from_file_location('seed_expand_script', SCRIPT)
    mod = importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)
    return mod


def _fake_work(i):
    return {'id': f'https://openalex.org/W{i}', 'display_name': f'w{i}', 'referenced_works': []}


def test_max_total_cuts_across_seeds(monkeypatch, tmp_path, capsys):
    mod = _load(monkeypatch)
    monkeypatch.setattr(mod, '_resolve_work', lambda seed, mailto: _fake_work(seed['n']))
    monkeypatch.setattr(mod, '_collect_for_direction', lambda work, d, max_related, rs, m, rl: [f'W{1000 + k}' for k in range(30)])
    monkeypatch.setattr(mod, 'fetch_referenced_work_details', lambda ids, rl, mailto=None, include_links=False: [{'id': f'https://openalex.org/{i}', 'title': i} for i in ids])
    monkeypatch.setattr(sys, 'argv', ['x', '--db-path', str(tmp_path / 't.db'), '--seed-json', json.dumps([{'n': 1}, {'n': 2}, {'n': 3}]),
                                       '--include-downstream', '--max-related', '30', '--max-total', '45'])
    mod.main()
    out = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert out['truncated'] is True
    assert out['total_added'] == 45
    assert [r['count'] for r in out['runs']] == [30, 15]   # third seed skipped entirely


def test_no_cap_by_default(monkeypatch, tmp_path, capsys):
    mod = _load(monkeypatch)
    monkeypatch.setattr(mod, '_resolve_work', lambda seed, mailto: _fake_work(seed['n']))
    monkeypatch.setattr(mod, '_collect_for_direction', lambda *a, **k: ['W5', 'W6'])
    monkeypatch.setattr(mod, 'fetch_referenced_work_details', lambda ids, rl, mailto=None, include_links=False: [{'id': f'https://openalex.org/{i}', 'title': i} for i in ids])
    monkeypatch.setattr(sys, 'argv', ['x', '--db-path', str(tmp_path / 't.db'), '--seed-json', json.dumps([{'n': 1}]), '--include-downstream'])
    mod.main()
    out = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert out['truncated'] is False and out['total_added'] == 2
```

- [ ] **Step 2: Run and confirm failure**

Run: `cd dl_lit_project && /home/jay/DT/venv/bin/python -m pytest tests/test_seed_expand_cap.py -q`
Expected: FAIL (`--max-total` unrecognised).

- [ ] **Step 3: Implement**

Argparse: `parser.add_argument('--max-total', type=int, default=0, help='Stop after this many related works across all seeds and directions (0 = unlimited)')`.

In `main`, before the loop: `budget = int(args.max_total or 0); total_added = 0; truncated = False`. Inside the per-direction block, after `candidate_ids` is collected and before `fetch_referenced_work_details`:

```python
                if budget > 0:
                    remaining = budget - total_added
                    if remaining <= 0:
                        truncated = True
                        break
                    if len(candidate_ids) > remaining:
                        candidate_ids = candidate_ids[:remaining]
                        truncated = True
```

After `runs.append(...)`: `total_added += len(records)`. The `break` above exits the directions loop; add the same `if budget > 0 and total_added >= budget: truncated = True; break` check at the top of the seed loop so later seeds are skipped. Final print: `print(json.dumps({'runs': runs, 'truncated': truncated, 'total_added': total_added}))`. Also include `'max_total': budget` in each run's `filters` dict.

- [ ] **Step 4: Run the Python suite, commit**

```bash
cd dl_lit_project && /home/jay/DT/venv/bin/python -m pytest tests -q
git add backend/scripts/seed_expand.py dl_lit_project/tests/test_seed_expand_cap.py
git commit -m "seed_expand.py: --max-total caps the expansion across seeds and directions"
```

---

## Task 2: `--max-total` for search-time expansion in `keyword_search.py`

**Files:**
- Modify: `backend/scripts/keyword_search.py` — `_crawl_direction` (~line 370), `expand_references_recursive` (~line 460), argparse, payload
- Test: `dl_lit_project/tests/test_keyword_search_script.py` (Phase 3 file; if Phase 3 has not landed, create the file with the `_load` helper shown there)

**Interfaces:**
- Produces: `expand_references_recursive(..., max_total=0)` returns stats with `truncated: bool`; `--max-total` CLI flag; payload `expansion.truncated`.

- [ ] **Step 1: Write the failing test**

```python
def test_expansion_stops_at_max_total(monkeypatch):
    mod = _load(monkeypatch)
    base = [{'id': f'https://openalex.org/W{i}', 'referenced_works': [f'W{i * 100 + k}' for k in range(10)]} for i in range(1, 4)]
    monkeypatch.setattr(mod, '_rank_candidate_ids', lambda ids, sort, cap, mailto: ids[:cap] if cap else ids)
    monkeypatch.setattr(mod, 'fetch_referenced_work_details', lambda ids, rl, mailto=None, include_links=False: [{'id': f'https://openalex.org/{i}', 'title': i} for i in ids])
    items, stats = mod.expand_references_recursive(base, related_depth_downstream=1, related_depth_upstream=0, max_related=10,
                                                   mailto=None, include_downstream=True, include_upstream=False, max_total=15)
    assert stats['truncated'] is True
    assert stats['added'] == 15
    assert len(items) == 3 + 15
```

- [ ] **Step 2: Run and confirm failure**

Expected: FAIL (`unexpected keyword argument 'max_total'`).

- [ ] **Step 3: Implement**

`_crawl_direction(..., budget=None)` where `budget` is a one-element list `[remaining]` shared across directions (mutable). Inside the loop, before adding resolved items:

```python
            if budget is not None:
                if budget[0] <= 0:
                    return {'added': added, 'processed': processed, 'matched': matched, 'truncated': True}
                if len(resolved_items) > budget[0]:
                    resolved_items = resolved_items[:budget[0]]
                    stats_truncated = True
```

and decrement `budget[0]` by 1 for each item actually appended. Return `truncated` in the stats dict. `expand_references_recursive(..., max_total=0)` builds `budget = [max_total] if max_total > 0 else None`, passes it to both directions, and returns `'truncated': any(direction_stats.get('truncated'))`. `main`: `parser.add_argument('--max-total', type=int, default=0)`, pass `max_total=max(0, int(args.max_total or 0))`.

- [ ] **Step 4: Run, commit**

```bash
cd dl_lit_project && /home/jay/DT/venv/bin/python -m pytest tests -q
git add backend/scripts/keyword_search.py dl_lit_project/tests/test_keyword_search_script.py
git commit -m "keyword_search.py: --max-total bounds search-time expansion"
```

---

## Task 3: Job budget in the enrich worker (download-everything mode)

**Files:**
- Modify: `backend/scripts/daemon/worker.py:517-600` (`do_enrich`)
- Test: `dl_lit_project/tests/test_pipeline_daemon.py` (existing; add a case that drives `do_enrich` with a fake db — follow the existing tests' fake-db pattern in that file)

**Interfaces:**
- Consumes: `expansion.maxTotal` (int, 0 = unlimited) in the enrich job payload.
- Produces: the result dict gains `expansion_truncated: bool` and `expansion_budget_used: int`. Per work, `max_related_per_source = min(max_rel, remaining)`; after `apply_enriched_metadata`, `remaining -= min(len(enriched.get('referenced_work_ids') or []), max_related_per_source)` when `expand_related` is on. When `remaining <= 0`, later works are enriched with `expand_related=False`.

- [ ] **Step 1: Write the failing test**

Read the top of `dl_lit_project/tests/test_pipeline_daemon.py` for how a `Worker` (or the daemon class) is constructed with a stub db and `process_single_reference` monkeypatched; add:

```python
def test_enrich_job_budget_caps_related_across_works(monkeypatch, daemon_with_fake_db):
    daemon, fake_db = daemon_with_fake_db  # fixture from this file
    seen = []
    def fake_apply(work_id, enriched, *, expand_related=False, max_related_per_source=40):
        seen.append((work_id, expand_related, max_related_per_source)); return work_id, None
    fake_db.apply_enriched_metadata = fake_apply
    monkeypatch.setattr(daemon, '_enrich_single', lambda entry, *a, **k: (entry['id'], {'referenced_work_ids': ['a'] * 30}, {'status': 'matched'}))
    fake_db.pending = [{'id': 1, 'title': 't1'}, {'id': 2, 'title': 't2'}, {'id': 3, 'title': 't3'}]
    result = daemon.do_enrich(corpus_id=1, limit=3, workers=1,
                              expansion={'includeDownstream': True, 'relatedDepthDownstream': 2, 'maxRelated': 30, 'maxTotal': 45},
                              pending_work_ids=[1, 2, 3])
    assert [(e, m) for _, e, m in seen] == [(True, 30), (True, 15), (False, 0)]
    assert result['expansion_truncated'] is True and result['expansion_budget_used'] == 45
```

Adapt fixture/attribute names to what the file actually provides; the assertion shape is what matters.

- [ ] **Step 2: Run and confirm failure**

Run: `cd dl_lit_project && /home/jay/DT/venv/bin/python -m pytest tests/test_pipeline_daemon.py -q -k budget`
Expected: FAIL (`expansion_truncated` missing; third call still `expand_related=True`).

- [ ] **Step 3: Implement**

In `do_enrich`, after `max_rel = expansion.get("maxRelated", 30)`:

```python
        max_total = int(expansion.get("maxTotal", 0) or 0)
        budget_remaining = max_total if max_total > 0 else None
        budget_used = 0
        expansion_truncated = False
```

At the `apply_enriched_metadata` call:

```python
                    expand_this = (rel_down > 1 and fetch_refs)
                    per_source = max_rel
                    if expand_this and budget_remaining is not None:
                        if budget_remaining <= 0:
                            expand_this = False
                            per_source = 0
                            expansion_truncated = True
                        else:
                            per_source = min(max_rel, budget_remaining)
                    wid, err = self.db.apply_enriched_metadata(
                        pending_work_id, enriched, expand_related=expand_this, max_related_per_source=per_source
                    )
                    if expand_this and budget_remaining is not None:
                        used = min(len(enriched.get('referenced_work_ids') or []), per_source)
                        budget_remaining -= used
                        budget_used += used
                        if used < len(enriched.get('referenced_work_ids') or []):
                            expansion_truncated = True
```

Add `"expansion_truncated": expansion_truncated, "expansion_budget_used": budget_used` to the returned result dict.

- [ ] **Step 4: Run, commit**

```bash
cd dl_lit_project && /home/jay/DT/venv/bin/python -m pytest tests -q
git add backend/scripts/daemon/worker.py dl_lit_project/tests/test_pipeline_daemon.py
git commit -m "Enrich worker: per-job expansion budget (maxTotal)"
```

---

## Task 4: Backend plumbing and the admin ceiling

**Files:**
- Modify: `backend/src/app.js` — `buildUploadedDocsExpansion` (grep `^function buildUploadedDocsExpansion`), `buildKeywordSearchExpansion`, `applyKeywordSearchExpansionArgs`, promote route (`expandArgs` and the `enrichExpansion` payload, ~line 5060-5110), `APP_SETTING_DEFS` + PUT validation, promote response
- Test: `backend/tests/recursion-args.test.js` (existing; extend), `backend/tests/keyword-search.test.js`

**Interfaces:**
- Produces: both expansion builders return `maxExpansionTotal` (positive int, default 1000, clamped to `expansionHardCap()`); `expansionHardCap()` reads admin setting/env `RAG_FEEDER_EXPANSION_HARD_CAP` (default 5000, positive int). `applyKeywordSearchExpansionArgs` appends `--max-total <n>`. The promote route passes `--max-total` to `seed_expand.py` and `maxTotal` in the enrich job's `expansion`. Promote response gains `expansion_truncated: boolean` (from `expandResult.truncated` in new-seed mode; `false` otherwise — the enrich job reports its own flag asynchronously in the job result) and the message suffix ` Expansion stopped at the total cap of {n} works.` when true.

- [ ] **Step 1: Write the failing tests**

In `backend/tests/recursion-args.test.js` (see how it imports the builders; export them if they are not yet exported):

```js
test('maxExpansionTotal defaults to 1000 and is clamped to the hard cap', () => {
  process.env.RAG_FEEDER_EXPANSION_HARD_CAP = '3000'
  try {
    expect(buildUploadedDocsExpansion({}).maxExpansionTotal).toBe(1000)
    expect(buildUploadedDocsExpansion({ maxExpansionTotal: 250 }).maxExpansionTotal).toBe(250)
    expect(buildUploadedDocsExpansion({ maxExpansionTotal: 99999 }).maxExpansionTotal).toBe(3000)
    expect(buildUploadedDocsExpansion({ maxExpansionTotal: '-4' }).maxExpansionTotal).toBe(1000)
  } finally { delete process.env.RAG_FEEDER_EXPANSION_HARD_CAP }
})

test('keyword search args carry --max-total', () => {
  const args = []
  applyKeywordSearchExpansionArgs(args, buildKeywordSearchExpansion({ maxExpansionTotal: 700 }))
  expect(args).toEqual(expect.arrayContaining(['--max-total', '700']))
})
```

- [ ] **Step 2: Run and confirm failure**

Run: `cd backend && npm test -- recursion-args`
Expected: FAIL.

- [ ] **Step 3: Implement**

```js
function expansionHardCap() {
  const raw = appSettingsEnv().RAG_FEEDER_EXPANSION_HARD_CAP || process.env.RAG_FEEDER_EXPANSION_HARD_CAP || '';
  const parsed = Number(raw);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : 5000;
}

function coerceExpansionTotal(value) {
  const parsed = Number(value);
  const wanted = Number.isInteger(parsed) && parsed > 0 ? parsed : 1000;
  return Math.min(wanted, expansionHardCap());
}
```

Add `maxExpansionTotal: coerceExpansionTotal(body?.maxExpansionTotal)` to both builders' returned objects. In `applyKeywordSearchExpansionArgs`, push `'--max-total', String(expansion.maxExpansionTotal)`. In the promote route: `expandArgs.push('--max-total', String(expansion.maxExpansionTotal))` and `enrichExpansion.maxTotal = expansion.maxExpansionTotal` (add to the object literal; in new-seed mode the enrich crawl is disabled anyway). After `expandResult`, set `const expansionTruncated = Boolean(expandResult?.truncated)`; include `expansion_truncated: expansionTruncated` in the JSON and append the suffix to `message` when true. `APP_SETTING_DEFS`: `{ key: 'expansion_hard_cap', env: 'RAG_FEEDER_EXPANSION_HARD_CAP', secret: false }` with the positive-integer PUT validation.

- [ ] **Step 4: Run, commit**

```bash
cd backend && npm test
git add backend/src/app.js backend/tests/recursion-args.test.js
git commit -m "Expansion total cap: request setting clamped to an admin ceiling, threaded to all three paths"
```

---

## Task 5: Confirmation panel, setting pill and truncation message (frontend)

**Files:**
- Modify: `frontend/src/App.svelte` — state near `let maxRelated = 30` (~line 247); promotion settings row (grep `Max Related / Paper`, first occurrence ~line 4679); `promoteSeedCandidateKeys` (~3119), `handlePromoteWholeSeedSource` (~3184); seed row markup for the inline panel (inside `.seed-source__summary-main`, after the subtitle); `frontend/src/app.css`
- Modify: `frontend/src/lib/api.js` `promoteSeedCandidates` to send `maxExpansionTotal`
- Modify: `frontend/src/components/AdminPanel.svelte` — `expansion_hard_cap` field next to the OpenAlex group
- Test: `frontend/tests/search-scale.spec.ts` or a new `frontend/tests/expansion-safety.spec.ts` (mocked)

**Interfaces:**
- Produces: `let maxExpansionTotal = 1000`; `expansionEstimate(items)` → `{ directions, directionsLabel, depth, perItem, ceiling, requests }`; `pendingPromotion = { sourceId, items, keys | null } | null` drives the panel; `confirmPromotion()` / `cancelPromotion()`.

- [ ] **Step 1: Implement**

```js
  let maxExpansionTotal = 1000
  let pendingPromotion = null

  function expansionEstimate(items) {
    const directions = (includeDownstream && relatedDepthDownstream >= 1 ? 1 : 0) + (includeUpstream && relatedDepthUpstream >= 1 ? 1 : 0)
    const depth = Math.max(includeDownstream ? relatedDepthDownstream : 0, includeUpstream ? relatedDepthUpstream : 0)
    const perItem = maxRelated * directions * Math.pow(maxRelated, Math.max(0, depth - 1))
    const ceiling = Math.min(items * perItem, maxExpansionTotal)
    const requests = items * directions * (1 + Math.ceil(maxRelated / 50))
    const directionsLabel = directions === 2 ? 'downstream and upstream' : (includeDownstream && relatedDepthDownstream >= 1 ? 'downstream' : 'upstream')
    return { directions, directionsLabel, depth, perItem, ceiling, requests }
  }

  function needsPromotionConfirmation(items) {
    if (!expansionEnabled) return false
    const est = expansionEstimate(items)
    return items > 25 || est.ceiling > 500
  }
```

Wrap the two entry points: `handlePromoteSeedSource(source)` computes `items = seedAllSelected?.[sourceId] ? seedPage(source).total : candidateKeys.length` (fall back to `candidateKeys.length` if Phase 3 is not present) and, when `needsPromotionConfirmation(items)`, sets `pendingPromotion = { sourceId, items, run: () => promoteSeedCandidateKeys(source, candidateKeys, { clearSelection: true }) }` and returns; `handlePromoteWholeSeedSource` does the same with `items = estimatedSelectableSeedCount(source)` and `run: () => promoteWholeSeedSourceNow(source)` (rename the current body to `promoteWholeSeedSourceNow`). `confirmPromotion()` runs `pendingPromotion.run()` then clears; `cancelPromotion()` clears.

Panel markup, inside the seed row's summary main block (after the subtitle), rendered when `pendingPromotion?.sourceId === sourceId`:

```svelte
                      {#if pendingPromotion?.sourceId === sourceId}
                        {@const est = expansionEstimate(pendingPromotion.items)}
                        <div class="promotion-confirm" role="alertdialog" data-testid="promotion-confirm" on:click|stopPropagation on:keydown|stopPropagation>
                          <p>Promote {pendingPromotion.items.toLocaleString('en-US')} items with {est.directionsLabel} expansion (depth {est.depth}, up to {maxRelated} related per item)? This can add up to {est.ceiling.toLocaleString('en-US')} works and takes about {est.requests.toLocaleString('en-US')} OpenAlex requests. The expansion stops at the total cap of {maxExpansionTotal.toLocaleString('en-US')}.</p>
                          <div class="promotion-confirm__actions">
                            <button class="secondary" type="button" on:click={cancelPromotion}>Cancel</button>
                            <button class="primary" type="button" on:click={confirmPromotion}>Promote anyway</button>
                          </div>
                        </div>
                      {/if}
```

Settings pill, after the `Max Related / Paper` pill (both occurrences if the legacy search tab is kept; the workspace one at ~4679 is the required one):

```svelte
            <div class="seed-expansion-pill" class:opacity-50={!expansionEnabled}>
              <span class="muted small" title="Hard stop for one promotion's expansion, across all items and directions">Max expansion total</span>
              <input type="number" min="1" step="100" bind:value={maxExpansionTotal} class="short-input" disabled={!expansionEnabled} />
            </div>
```

Pass `maxExpansionTotal` in both `promoteSeedCandidates(...)` calls and in `api.js` `promoteSeedCandidates` body. Truncation: after a promote resolves, if `result?.expansion_truncated` append ` Expansion stopped at the total cap of ${maxExpansionTotal.toLocaleString('en-US')} works.` to `seedActionStatus` (the api helper must return the parsed JSON; check `promoteSeedCandidates` in `api.js` returns `response.json()`).

CSS:

```css
.promotion-confirm { margin-top: 8px; padding: 10px 12px; border: 1px solid #f59e0b; border-radius: var(--radius-md); background: #fffbeb; }
.promotion-confirm p { margin: 0 0 8px; }
.promotion-confirm__actions { display: flex; gap: 8px; justify-content: flex-end; }
```

Admin field (OpenAlex group):

```svelte
        <label>
          <span class="muted small">Expansion hard cap (works per promotion)</span>
          <input type="number" min="1" step="100" placeholder="5000" bind:value={appSettingsDraft.expansion_hard_cap} />
          <span class="muted small">{fallbackHint(appSettings.expansion_hard_cap)}</span>
        </label>
```

- [ ] **Step 2: E2e, mocked**

`frontend/tests/expansion-safety.spec.ts`: mock `/api/seed/sources` with one search seed of `candidate_count: 40, state_counts: { in_corpus: 0 }`; mock the promote route to capture its body and return `{ success: true, message: 'ok', expansion_truncated: true, jobs: [], promotion: {} }`. Enable Downstream (checkbox), set depth 1, click the seed's `→` (promote all). Assert `promotion-confirm` is visible with text `Promote 40 items with downstream expansion (depth 1, up to 30 related per item)?`, `up to 1,000 works` (min(40×30=1200, 1000)) and `about 40 OpenAlex requests`; click `Promote anyway`; assert the request body has `maxExpansionTotal: 1000`; assert the status contains `Expansion stopped at the total cap of 1,000 works.` A second test: with expansion off, `→` promotes immediately and no panel appears.

Run: `cd frontend && npx playwright test expansion-safety.spec.ts`
Expected: PASS.

- [ ] **Step 3: Full suites, commit**

```bash
git add frontend/src/App.svelte frontend/src/app.css frontend/src/lib/api.js frontend/src/components/AdminPanel.svelte frontend/tests/expansion-safety.spec.ts
git commit -m "Confirm big promotions with expansion; max expansion total setting; truncation message"
```

---

## Task 6: Docs and PR

- `.env.example`: `# Absolute ceiling for one promotion's expansion (default 5000)` / `# RAG_FEEDER_EXPANSION_HARD_CAP=5000`; `docker-compose.yml`: pass `RAG_FEEDER_EXPANSION_HARD_CAP` to `rag_backend` and `rag_enrich_worker`; README: one paragraph under the promotion modes explaining the confirmation and the two caps.
- Run all three suites, then open the PR `Round 2 phase 4: expansion confirmation and hard cap`.

```bash
git add .env.example docker-compose.yml README.md
git commit -m "Document the expansion confirmation and caps"
```

---

## Self-review notes

- Spec coverage: confirmation thresholds/copy/estimate (Task 5); `maxExpansionTotal` setting + admin ceiling + clamp (Tasks 4, 5); `seed_expand --max-total` (1); `keyword_search --max-total` (2); enrich budget with `expansion_truncated` (3); truncation shown in the UI (5). 8b deliberately absent.
- Type consistency: `maxExpansionTotal` (frontend + request + builders), `--max-total` (both scripts), `maxTotal` (enrich payload), `truncated`/`expansion_truncated` naming matches producers and consumers per task.
- Known limit: in download-everything mode the enrich job runs asynchronously, so its truncation flag lands in the pipeline job result, not in the promote response. The UI shows the synchronous flag only; the job summary endpoint already exposes results if a later phase wants to surface it.
