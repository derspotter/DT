# Jochen Round 2 — Phase 5: Optional GROBID Extraction — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** When an admin points the app at a GROBID instance, bibliography extraction for uploaded PDFs with a text layer tries GROBID first and falls back to the existing LLM chain; with no GROBID configured nothing changes.

**Architecture:** A new Python module `dl_lit/grobid_extractor.py` posts a PDF to GROBID's `processReferences` and turns the TEI reply into the entry dicts `insert_ingest_entry` already accepts. A new CLI `backend/scripts/grobid_extract.py` runs it for one upload and writes the entries. The extract-bibliography route runs that CLI before the get-bib-pages + LLM chain when `RAG_FEEDER_GROBID_URL` is set and the PDF has a text layer; if it yields fewer than `RAG_FEEDER_GROBID_MIN_REFS` entries the LLM chain runs as today. A compose profile ships the CRF-only image, off by default.

**Tech Stack:** Python (`requests`, `defusedxml` (new dependency), `pdfminer.six` already in requirements), Node/Express, docker compose profiles, pytest with a TEI fixture, Jest.

**Spec:** `docs/superpowers/specs/2026-09-03-jochen-round-2-design.md`, section "Phase 5 — Optional GROBID (item 9)". Jay's decision (2026-09-03): not the default, env-gated, never on the dev box, may be cut.

## Global Constraints

- Line anchors verified at `b0c7b43` on 2026-09-06. Independent of Phases 3 and 4.
- Settings: `RAG_FEEDER_GROBID_URL` (admin key `grobid_url`, empty = off), `RAG_FEEDER_GROBID_MIN_REFS` (default 5), `RAG_FEEDER_GROBID_TIMEOUT_SEC` (default 120).
- GROBID endpoint: `POST {url}/api/processReferences` multipart field `input`, form fields `consolidateCitations=0`, `includeRawCitations=1`; liveness `GET {url}/api/isalive`.
- Entries written with `metadata_source_type: 'grobid'`; extract-status signal `mode=grobid` on success, `reason=grobid_fallback:<no_text_layer|too_few|unavailable>` when the LLM chain runs instead.
- Compose service `grobid`, image `lfoppiano/grobid:0.8.1-crf`, `profiles: ["grobid"]`, `mem_limit: 3g`, no published port. README notes the RAM and arm64 caveats.
- Seed-document metadata (title/authors of the upload) stays on the LLM header pass; GROBID only supplies reference entries.
- Commit trailer: `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>` and `Claude-Session: https://claude.ai/code/session_01U3c4gfg7jQvvmPdpLNBN5T`.

---

## File structure

| File | Responsibility |
|---|---|
| `dl_lit_project/dl_lit/grobid_extractor.py` | `has_text_layer(pdf_path)`, `extract_references(pdf_path, base_url, timeout) -> list[dict] | None`, `parse_tei_references(xml_text) -> list[dict]`, `is_alive(base_url)` |
| `backend/scripts/grobid_extract.py` | CLI: `--db-path --input-pdf --ingest-source --corpus-id --min-refs`; prints `{"status": "ok", "count": n}` or `{"status": "fallback", "reason": ...}` |
| `backend/src/app.js` | extraction route branch; admin settings `grobid_url`; `GET /api/admin/grobid/health` proxy |
| `docker-compose.yml`, `.env.example`, `README.md` | profile, env, docs |
| `frontend/src/components/AdminPanel.svelte` | `grobid_url` field + "Test connection" |
| `frontend/src/App.svelte` | Document-items seed subtitle `via GROBID` / `via LLM` |

---

## Task 1: TEI parsing and text-layer check (pure Python)

**Files:**
- Create: `dl_lit_project/dl_lit/grobid_extractor.py`
- Create: `dl_lit_project/tests/fixtures/grobid_refs.tei.xml`, `dl_lit_project/tests/test_grobid_extractor.py`
- Modify: `requirements.txt` (add `defusedxml`) and rebuild the backend image (`docker compose build rag_backend`) so the container has it

**Interfaces:**
- Produces: `parse_tei_references(xml_text: str) -> list[dict]` with keys `title, authors (list[str]), year (int|None), doi, source, volume, issue, pages, publisher, url, raw`; `has_text_layer(pdf_path, pages=3) -> bool` (pdfminer `extract_text` on the first pages, true if ≥ 50 non-whitespace characters).

- [ ] **Step 1: Write the fixture and failing tests**

Fixture, a minimal TEI with two `biblStruct` entries (one journal article with DOI, volume, pages; one monograph with publisher and no DOI) plus one entry with no title, which must be skipped:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<TEI xmlns="http://www.tei-c.org/ns/1.0"><text><back><div type="references"><listBibl>
<biblStruct xml:id="b0"><analytic><title level="a" type="main">The Nature of the Firm</title>
<author><persName><forename>Ronald</forename><surname>Coase</surname></persName></author>
<idno type="DOI">10.1111/j.1468-0335.1937.tb00002.x</idno></analytic>
<monogr><title level="j">Economica</title><imprint><biblScope unit="volume">4</biblScope><biblScope unit="issue">16</biblScope>
<biblScope unit="page" from="386" to="405"/><date type="published" when="1937"/></imprint></monogr>
<note type="raw_reference">Coase, R. (1937). The nature of the firm. Economica, 4(16), 386-405.</note></biblStruct>
<biblStruct xml:id="b1"><monogr><title level="m" type="main">Governing the Commons</title>
<author><persName><forename>Elinor</forename><surname>Ostrom</surname></persName></author>
<imprint><publisher>Cambridge University Press</publisher><date type="published" when="1990-01-01"/></imprint></monogr></biblStruct>
<biblStruct xml:id="b2"><monogr><imprint><date when="2001"/></imprint></monogr></biblStruct>
</listBibl></div></back></text></TEI>
```

Tests:

```python
from pathlib import Path
from dl_lit import grobid_extractor as g

FIXTURE = Path(__file__).parent / 'fixtures' / 'grobid_refs.tei.xml'


def test_parse_tei_references_maps_fields():
    refs = g.parse_tei_references(FIXTURE.read_text(encoding='utf-8'))
    assert len(refs) == 2
    a, b = refs
    assert a == {
        'title': 'The Nature of the Firm', 'authors': ['Ronald Coase'], 'year': 1937,
        'doi': '10.1111/j.1468-0335.1937.tb00002.x', 'source': 'Economica', 'volume': '4', 'issue': '16',
        'pages': '386--405', 'publisher': None, 'url': None,
        'raw': 'Coase, R. (1937). The nature of the firm. Economica, 4(16), 386-405.',
    }
    assert b['title'] == 'Governing the Commons' and b['authors'] == ['Elinor Ostrom'] and b['year'] == 1990
    assert b['publisher'] == 'Cambridge University Press' and b['doi'] is None and b['source'] is None


def test_parse_tei_references_tolerates_garbage():
    assert g.parse_tei_references('<not xml') == []
    assert g.parse_tei_references('') == []


def test_has_text_layer(tmp_path):
    from reportlab.pdfgen import canvas  # if reportlab is absent, build the PDF with the tiny writer in tests/test_get_bib_pages.py
    pdf = tmp_path / 't.pdf'
    c = canvas.Canvas(str(pdf)); c.drawString(72, 720, 'Some searchable text ' * 5); c.save()
    assert g.has_text_layer(pdf) is True
    blank = tmp_path / 'b.pdf'
    c = canvas.Canvas(str(blank)); c.save()
    assert g.has_text_layer(blank) is False
```

If `reportlab` is not installed, reuse the PDF-bytes helper that `frontend/tests/app.spec.ts` builds (port it to Python in the test) rather than adding a dependency.

- [ ] **Step 2: Run and confirm failure**

Run: `cd dl_lit_project && /home/jay/DT/venv/bin/python -m pytest tests/test_grobid_extractor.py -q`
Expected: FAIL (`No module named dl_lit.grobid_extractor`).

- [ ] **Step 3: Implement**

First `echo defusedxml >> requirements.txt` and `/home/jay/DT/venv/bin/pip install defusedxml` (the venv does not have it yet; the container gets it on the next image build).

```python
"""GROBID reference extraction: TEI → the entry dicts insert_ingest_entry expects."""
from __future__ import annotations

from pathlib import Path

import requests
# defusedxml: GROBID is an internal service, but its reply is still parsed XML;
# the stdlib parser is open to entity-expansion attacks, defusedxml is not.
from defusedxml import ElementTree as ET

TEI = '{http://www.tei-c.org/ns/1.0}'


def _text(el):
    return ''.join(el.itertext()).strip() if el is not None else ''


def _year(imprint):
    if imprint is None:
        return None
    for date in imprint.findall(f'{TEI}date'):
        when = date.get('when') or _text(date)
        if when and when[:4].isdigit():
            return int(when[:4])
    return None


def parse_tei_references(xml_text: str) -> list[dict]:
    if not xml_text or not xml_text.strip():
        return []
    try:
        root = ET.fromstring(xml_text)
    except (ET.ParseError, ValueError):  # ValueError: defusedxml's forbidden-construct errors
        return []
    out = []
    for bibl in root.iter(f'{TEI}biblStruct'):
        analytic = bibl.find(f'{TEI}analytic')
        monogr = bibl.find(f'{TEI}monogr')
        title_el = (analytic.find(f'{TEI}title') if analytic is not None else None)
        if title_el is None and monogr is not None:
            title_el = monogr.find(f"{TEI}title[@level='m']") or monogr.find(f'{TEI}title')
        title = _text(title_el)
        if not title:
            continue
        holder = analytic if analytic is not None else monogr
        authors = []
        for pers in holder.findall(f'{TEI}author/{TEI}persName'):
            name = ' '.join(filter(None, [_text(pers.find(f'{TEI}forename')), _text(pers.find(f'{TEI}surname'))]))
            if name:
                authors.append(name)
        doi_el = holder.find(f"{TEI}idno[@type='DOI']")
        imprint = monogr.find(f'{TEI}imprint') if monogr is not None else None
        journal = monogr.find(f"{TEI}title[@level='j']") if (monogr is not None and analytic is not None) else None
        def scope(unit):
            el = imprint.find(f"{TEI}biblScope[@unit='{unit}']") if imprint is not None else None
            if el is None:
                return None
            if unit == 'page' and el.get('from'):
                return f"{el.get('from')}--{el.get('to')}" if el.get('to') else el.get('from')
            return _text(el) or None
        out.append({
            'title': title,
            'authors': authors,
            'year': _year(imprint),
            'doi': _text(doi_el) or None,
            'source': _text(journal) or None,
            'volume': scope('volume'),
            'issue': scope('issue'),
            'pages': scope('page'),
            'publisher': _text(imprint.find(f'{TEI}publisher')) or None if imprint is not None else None,
            'url': None,
            'raw': _text(bibl.find(f"{TEI}note[@type='raw_reference']")) or None,
        })
    return out


def has_text_layer(pdf_path, pages: int = 3, minimum_chars: int = 50) -> bool:
    try:
        from pdfminer.high_level import extract_text
        text = extract_text(str(pdf_path), maxpages=pages) or ''
    except Exception:
        return False
    return len(''.join(text.split())) >= minimum_chars


def is_alive(base_url: str, timeout: float = 5) -> bool:
    try:
        r = requests.get(f"{base_url.rstrip('/')}/api/isalive", timeout=timeout)
        return r.ok
    except requests.RequestException:
        return False


def extract_references(pdf_path, base_url: str, timeout: float = 120) -> list[dict] | None:
    """None means GROBID was unreachable or errored; [] means it answered with no references."""
    try:
        with open(pdf_path, 'rb') as fh:
            r = requests.post(
                f"{base_url.rstrip('/')}/api/processReferences",
                files={'input': (Path(pdf_path).name, fh, 'application/pdf')},
                data={'consolidateCitations': '0', 'includeRawCitations': '1'},
                timeout=timeout,
            )
    except requests.RequestException:
        return None
    if r.status_code == 204:
        return []
    if not r.ok:
        return None
    return parse_tei_references(r.text)
```

- [ ] **Step 4: Run, commit**

```bash
cd dl_lit_project && /home/jay/DT/venv/bin/python -m pytest tests -q
git add requirements.txt dl_lit_project/dl_lit/grobid_extractor.py dl_lit_project/tests/fixtures/grobid_refs.tei.xml dl_lit_project/tests/test_grobid_extractor.py
git commit -m "grobid_extractor: TEI reference parsing, text-layer check, client"
```

---

## Task 2: `grobid_extract.py` CLI writing ingest entries

**Files:**
- Create: `backend/scripts/grobid_extract.py`
- Test: `dl_lit_project/tests/test_grobid_extract_script.py`

**Interfaces:**
- Consumes: `grobid_extractor.extract_references`, `has_text_layer`; `DatabaseManager.insert_ingest_entry(ref, ingest_source)` where `ref` carries `corpus_id, source_pdf, title, authors, year, doi, source, publisher, url` (see `db_manager.py:1491`).
- Produces: stdout JSON `{"status": "ok", "count": n, "mode": "grobid"}` (exit 0) or `{"status": "fallback", "reason": "no_text_layer" | "too_few" | "unavailable", "count": n}` (exit 0). Entries are written only on `ok`; on `too_few` nothing is written so the LLM chain owns the seed.

- [ ] **Step 1: Write the failing test**

```python
import importlib.util, json, sys
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / 'backend' / 'scripts' / 'grobid_extract.py'


def _load(monkeypatch):
    monkeypatch.syspath_prepend(str(SCRIPT.parent))
    spec = importlib.util.spec_from_file_location('grobid_extract_script', SCRIPT)
    mod = importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)
    return mod


def _run(mod, monkeypatch, tmp_path, min_refs='2'):
    pdf = tmp_path / 'u.pdf'; pdf.write_bytes(b'%PDF-1.4\n')
    monkeypatch.setenv('RAG_FEEDER_GROBID_URL', 'http://grobid:8070')
    monkeypatch.setattr(sys, 'argv', ['x', '--db-path', str(tmp_path / 't.db'), '--input-pdf', str(pdf), '--ingest-source', 'u', '--corpus-id', '7', '--min-refs', min_refs])
    mod.main()


def test_writes_entries_when_enough(monkeypatch, tmp_path, capsys):
    mod = _load(monkeypatch)
    monkeypatch.setattr(mod, 'has_text_layer', lambda p: True)
    monkeypatch.setattr(mod, 'extract_references', lambda p, url, timeout: [{'title': 'A', 'authors': ['X'], 'year': 2001}, {'title': 'B', 'authors': [], 'year': None}])
    _run(mod, monkeypatch, tmp_path)
    assert json.loads(capsys.readouterr().out.strip()) == {'status': 'ok', 'count': 2, 'mode': 'grobid'}
    from dl_lit.db_manager import DatabaseManager
    db = DatabaseManager(db_path=tmp_path / 't.db')
    rows = db.conn.execute("SELECT title, corpus_id, ingest_source FROM ingest_entries ORDER BY title").fetchall()
    assert [tuple(r) for r in rows] == [('A', 7, 'u'), ('B', 7, 'u')]
    db.close_connection()


def test_falls_back_when_too_few(monkeypatch, tmp_path, capsys):
    mod = _load(monkeypatch)
    monkeypatch.setattr(mod, 'has_text_layer', lambda p: True)
    monkeypatch.setattr(mod, 'extract_references', lambda p, url, timeout: [{'title': 'only one', 'authors': []}])
    _run(mod, monkeypatch, tmp_path)
    assert json.loads(capsys.readouterr().out.strip()) == {'status': 'fallback', 'reason': 'too_few', 'count': 1}


def test_falls_back_without_text_layer_and_when_unavailable(monkeypatch, tmp_path, capsys):
    mod = _load(monkeypatch)
    monkeypatch.setattr(mod, 'has_text_layer', lambda p: False)
    _run(mod, monkeypatch, tmp_path)
    assert json.loads(capsys.readouterr().out.strip())['reason'] == 'no_text_layer'
    monkeypatch.setattr(mod, 'has_text_layer', lambda p: True)
    monkeypatch.setattr(mod, 'extract_references', lambda p, url, timeout: None)
    _run(mod, monkeypatch, tmp_path)
    assert json.loads(capsys.readouterr().out.strip())['reason'] == 'unavailable'
```

- [ ] **Step 2: Run and confirm failure**

Expected: FAIL (script missing).

- [ ] **Step 3: Implement**

```python
"""Try GROBID for an uploaded PDF's references; report ok or a fallback reason."""
import argparse, json, os, sys
from pathlib import Path
from _bootstrap import ensure_import_paths

ROOT = Path(__file__).resolve().parents[2]
ensure_import_paths(__file__)
if str(ROOT / 'dl_lit_project') not in sys.path:
    sys.path.insert(0, str(ROOT / 'dl_lit_project'))

from dl_lit.db_manager import DatabaseManager
from dl_lit.grobid_extractor import extract_references, has_text_layer


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--db-path', required=True)
    p.add_argument('--input-pdf', required=True)
    p.add_argument('--ingest-source', required=True)
    p.add_argument('--corpus-id', type=int, default=None)
    p.add_argument('--min-refs', type=int, default=int(os.environ.get('RAG_FEEDER_GROBID_MIN_REFS', '5') or 5))
    args = p.parse_args()

    url = (os.environ.get('RAG_FEEDER_GROBID_URL') or '').strip()
    if not url:
        print(json.dumps({'status': 'fallback', 'reason': 'unavailable', 'count': 0})); return
    if not has_text_layer(args.input_pdf):
        print(json.dumps({'status': 'fallback', 'reason': 'no_text_layer', 'count': 0})); return
    timeout = float(os.environ.get('RAG_FEEDER_GROBID_TIMEOUT_SEC', '120') or 120)
    refs = extract_references(args.input_pdf, url, timeout=timeout)
    if refs is None:
        print(json.dumps({'status': 'fallback', 'reason': 'unavailable', 'count': 0})); return
    if len(refs) < max(0, args.min_refs):
        print(json.dumps({'status': 'fallback', 'reason': 'too_few', 'count': len(refs)})); return

    db = DatabaseManager(db_path=Path(args.db_path))
    try:
        for ref in refs:
            db.insert_ingest_entry({
                **ref,
                'corpus_id': args.corpus_id,
                'source_pdf': args.input_pdf,
                'metadata_source_type': 'grobid',
            }, ingest_source=args.ingest_source)
    finally:
        db.close_connection()
    print(json.dumps({'status': 'ok', 'count': len(refs), 'mode': 'grobid'}))


if __name__ == '__main__':
    main()
```

Check `insert_ingest_entry`'s exact keyword name for the source (`ingest_source=`) and whether it commits; call `db.conn.commit()` after the loop if it does not.

- [ ] **Step 4: Run, commit**

```bash
cd dl_lit_project && /home/jay/DT/venv/bin/python -m pytest tests -q
git add backend/scripts/grobid_extract.py dl_lit_project/tests/test_grobid_extract_script.py
git commit -m "grobid_extract.py: write GROBID references as ingest entries or report a fallback reason"
```

---

## Task 3: Extraction route branch, admin setting and health proxy

**Files:**
- Modify: `backend/src/app.js` — script constant beside `GET_BIB_PAGES_SCRIPT` (~line 94); `APP_SETTING_DEFS`; extraction route just before `const getPagesProcess = spawn(PYTHON_EXEC, [GET_BIB_PAGES_SCRIPT, ...` (~line 4622); new `GET /api/admin/grobid/health`
- Test: `backend/tests/grobid-route.test.js` (new, stub-mode)

**Interfaces:**
- Produces: `GROBID_EXTRACT_SCRIPT`; admin setting `grobid_url` (env `RAG_FEEDER_GROBID_URL`) plus `grobid_min_refs` (`RAG_FEEDER_GROBID_MIN_REFS`, positive int); `GET /api/admin/grobid/health` → `{ configured: boolean, alive: boolean, url }` (admin-only middleware as the other admin routes use). Extraction: when `appSettingsEnv().RAG_FEEDER_GROBID_URL || process.env.RAG_FEEDER_GROBID_URL` is non-empty, run `grobid_extract.py --db-path DB_PATH --input-pdf <path> --ingest-source <baseName>` via `runPythonJson` before the get-pages spawn; on `{status: 'ok'}` send `sendExtractionSignal({ status: 'success', mode: 'grobid' })`, run the seed-document metadata step exactly as the LLM path does for the upload's own title/authors (locate where `_upsert_seed_document_metadata` is triggered — the inline APIscraper run with `--header-only` if such a flag exists; otherwise keep the LLM header pass by running the existing chain with an env flag `RAG_FEEDER_SKIP_REFERENCE_EXTRACTION=1` that `APIscraper_v2.py` honours by skipping reference insertion; pick whichever the code supports and state it in the report) and `finishTask()`; on `fallback`, log `reason` and continue into the existing chain with `reason=grobid_fallback:<reason>` added to the eventual done signal.

- [ ] **Step 1: Write the failing test**

```js
import request from 'supertest'
import { createApp } from '../src/app.js'

describe('GROBID admin health', () => {
  let app
  beforeAll(() => { process.env.RAG_FEEDER_STUB = '1'; app = createApp({ broadcast: () => {} }) })
  afterAll(() => { delete process.env.RAG_FEEDER_STUB; delete process.env.RAG_FEEDER_GROBID_URL })

  test('reports not configured when the URL is empty', async () => {
    delete process.env.RAG_FEEDER_GROBID_URL
    const res = await request(app).get('/api/admin/grobid/health')
    expect(res.status).toBe(200)
    expect(res.body).toEqual({ configured: false, alive: false, url: '' })
  })

  test('reports configured but not alive for an unreachable URL', async () => {
    process.env.RAG_FEEDER_GROBID_URL = 'http://127.0.0.1:9'
    const res = await request(app).get('/api/admin/grobid/health')
    expect(res.body).toMatchObject({ configured: true, alive: false, url: 'http://127.0.0.1:9' })
  })
})
```

- [ ] **Step 2: Run and confirm failure**

Expected: 404.

- [ ] **Step 3: Implement**

Health route (use Node's global `fetch` with a 3 s `AbortController`; admin guard = the same middleware `GET /api/admin/settings` uses):

```js
  app.get('/api/admin/grobid/health', requireAuthMiddleware, requireAdmin, async (req, res) => {
    const url = String(appSettingsEnv().RAG_FEEDER_GROBID_URL || process.env.RAG_FEEDER_GROBID_URL || '').trim();
    if (!url) return res.json({ configured: false, alive: false, url: '' });
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 3000);
    try {
      const r = await fetch(`${url.replace(/\/$/, '')}/api/isalive`, { signal: controller.signal });
      return res.json({ configured: true, alive: r.ok, url });
    } catch {
      return res.json({ configured: true, alive: false, url });
    } finally {
      clearTimeout(timer);
    }
  });
```

Extraction branch, inserted at the top of the `enqueueExtractionTask` body before the get-pages spawn:

```js
        const grobidUrl = String(appSettingsEnv().RAG_FEEDER_GROBID_URL || process.env.RAG_FEEDER_GROBID_URL || '').trim();
        let grobidFallbackReason = '';
        if (grobidUrl && inputExists) {
          try {
            const grobid = await runPythonJson(GROBID_EXTRACT_SCRIPT, ['--db-path', DB_PATH, '--input-pdf', inputPdfPath, '--ingest-source', baseName], { dbPath: DB_PATH, corpusId });
            if (grobid?.status === 'ok') {
              send(`[extract-status][corpus=${corpusTag}][base=${baseName}] GROBID extracted ${grobid.count} references`);
              sendExtractionSignal({ status: 'success', mode: 'grobid' });
              finishTask();
              return;
            }
            grobidFallbackReason = `grobid_fallback:${grobid?.reason || 'unknown'}`;
          } catch (error) {
            grobidFallbackReason = 'grobid_fallback:unavailable';
            console.warn(`[/api/extract-bibliography] GROBID step failed, using LLM chain: ${error?.message || error}`);
          }
        }
```

and pass `reason: grobidFallbackReason || reason` into the existing `sendExtractionSignal({ status: 'success', ... })` calls of the LLM chain (append when a reason already exists: `[reason, grobidFallbackReason].filter(Boolean).join(';')`).

Seed-document metadata: verify in `APIscraper_v2.py` whether `_upsert_seed_document_metadata` runs from `process_single_pdf` regardless of reference count; if the GROBID branch returns early, the upload's own title/authors would be missing. Add `--header-only` to `APIscraper_v2.py` that runs only the source-metadata pass, and call it in the GROBID branch before `finishTask()` (`runPythonJson(API_SCRAPER_SCRIPT, ['--db-path', DB_PATH, '--input-pdf', inputPdfPath, '--ingest-source', baseName, '--header-only'], ...)`). Cover `--header-only` with a pytest in `tests/test_apiscraper_v2_args.py` asserting the parser accepts it.

Settings: add `{ key: 'grobid_url', env: 'RAG_FEEDER_GROBID_URL', secret: false }` and `{ key: 'grobid_min_refs', env: 'RAG_FEEDER_GROBID_MIN_REFS', secret: false }` with positive-integer validation for the latter.

- [ ] **Step 4: Run, commit**

```bash
cd backend && npm test
git add backend/src/app.js backend/tests/grobid-route.test.js dl_lit_project/dl_lit/APIscraper_v2.py dl_lit_project/tests/test_apiscraper_v2_args.py
git commit -m "Optional GROBID pass before the LLM extraction chain; admin URL and health check"
```

---

## Task 4: Compose profile, admin UI, seed subtitle, docs

**Files:**
- Modify: `docker-compose.yml` (new `grobid` service; `RAG_FEEDER_GROBID_URL`, `RAG_FEEDER_GROBID_MIN_REFS`, `RAG_FEEDER_GROBID_TIMEOUT_SEC` passthrough on `rag_backend`), `.env.example`, `README.md`
- Modify: `frontend/src/components/AdminPanel.svelte` (new "Extraction" group with URL, min refs, and a "Test connection" button calling `fetchGrobidHealth()`), `frontend/src/lib/api.js` (`fetchGrobidHealth`), `frontend/src/App.svelte` (Document-items seed subtitle: append ` · via GROBID` when the seed's entries carry `metadata_source_type === 'grobid'` — expose `extraction_mode` on pdf sources in `listSeedSources` by reading `MAX(ie.entry_json)`'s `metadata_source_type` or a new `ingest_source_metadata.extraction_mode` column written by `grobid_extract.py`; choose the column: add `extraction_mode TEXT` via `_ensure_column` in `db_manager.py`, set `'grobid'` in `grobid_extract.py` and `'llm'` in `_upsert_seed_document_metadata`)

- [ ] **Step 1: Compose**

```yaml
  grobid:
    image: lfoppiano/grobid:0.8.1-crf
    profiles: ["grobid"]
    mem_limit: 3g
    restart: unless-stopped
    # Internal only: the backend reaches it as http://grobid:8070
```

Backend env: `- RAG_FEEDER_GROBID_URL=${RAG_FEEDER_GROBID_URL:-}` and the two others. `docker compose config --quiet` must exit 0, and `docker compose config --services` must NOT list `grobid` without `--profile grobid`.

- [ ] **Step 2: Docs**

`.env.example`:

```
# Optional GROBID reference extraction (off when empty). With the compose profile:
#   docker compose --profile grobid up -d   and   RAG_FEEDER_GROBID_URL=http://grobid:8070
# RAG_FEEDER_GROBID_URL=
# RAG_FEEDER_GROBID_MIN_REFS=5
# RAG_FEEDER_GROBID_TIMEOUT_SEC=120
```

README section "Optional GROBID extraction": what it does, the fallback rule, the profile command, RAM (~2 GB for the CRF image), the arm64 caveat (official images are amd64; build locally or point at an external instance), and that seed-document metadata still comes from the LLM.

- [ ] **Step 3: Admin UI and subtitle**

Admin group markup follows the existing label pattern; the button calls `fetchGrobidHealth()` and shows `Reachable at {url}` / `Not reachable` / `Not configured`. Seed subtitle: in `listSeedSources` pdf push, add `extraction_mode: row.extraction_mode || null` (select it from `ingest_source_metadata`), and in App.svelte append ` · via GROBID` to the pdf seed subtitle when `source.extraction_mode === 'grobid'`.

- [ ] **Step 4: Verify without running GROBID locally**

Dev box must not run GROBID. Verify: Jest + pytest green; `docker compose config` checks above; admin "Test connection" with an empty URL shows "Not configured", with `http://127.0.0.1:9` shows "Not reachable"; uploading a PDF with the URL unset behaves exactly as before (e2e upload test passes). Live GROBID verification is a production-only step; note it in the PR as unverified.

- [ ] **Step 5: Commit and PR**

```bash
git add docker-compose.yml .env.example README.md frontend/src/components/AdminPanel.svelte frontend/src/lib/api.js frontend/src/App.svelte backend/src/seed.js dl_lit_project/dl_lit/db_manager.py backend/scripts/grobid_extract.py dl_lit_project/dl_lit/APIscraper_v2.py
git commit -m "GROBID compose profile, admin connection test, extraction mode on seeds"
```

PR title `Round 2 phase 5: optional GROBID extraction (off by default)`.

---

## Self-review notes

- Spec coverage: compose profile + image + mem limit (Task 4); settings (Tasks 3, 4); extractor + TEI parsing (Task 1); text-layer check (Task 1); route branch with `mode=grobid` / `reason=grobid_fallback` (Task 3); seed metadata stays on the LLM (Task 3 `--header-only`); UI `via GROBID` + admin test button (Task 4).
- Type consistency: `extract_references` returns `None | list`, the CLI maps `None → unavailable`, `< min → too_few`; the route maps `status` and `reason` verbatim into the extract signal.
- Open point for the implementer: confirm `_upsert_seed_document_metadata`'s trigger path before adding `--header-only`; if the header pass already runs from `get_bib_pages.py`'s source-metadata file, reuse that instead and say so in the report.
