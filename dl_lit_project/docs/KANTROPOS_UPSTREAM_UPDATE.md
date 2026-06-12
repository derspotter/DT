# Kantropos Upstream Update Runbook

## Purpose

Use this runbook when DT shows pending additions for an upstream Kantropos corpus and those additions need to be made available in the live RAG.

This is based on the admin PDF `Verwendung von Kantropos als Admin_neu.pdf` and the current DT implementation.

## Short Workflow

From the DT repo root:

```bash
bash backend/scripts/kantropos_upstream.sh count
```

Create a reviewable draft:

```bash
bash backend/scripts/kantropos_upstream.sh draft
```

Review the printed `draft_dir`. It contains:

- `metadata.pending-additions.bib`: only the new BibTeX entries
- `metadata.bib.new`: full replacement `metadata.bib`
- `files/`: PDFs staged for the upstream corpus
- `manifest.json`: source work ids, source PDF paths, target file names, and BibTeX keys

Validate the draft BibTeX and staged files:

```bash
bash backend/scripts/kantropos_upstream.sh validate <draft_dir>
```

Scan the staged PDFs for extractable text:

```bash
bash backend/scripts/kantropos_upstream.sh scan-text --draft-dir <draft_dir> --write
```

If the scan reports weak PDFs, run OCR against the staged draft before applying:

```bash
RAG_FEEDER_OCR_SERVICE_URL=http://<rechtmaschine-host>:8004 \
  bash backend/scripts/kantropos_upstream.sh ocr <draft_dir> --keep-going
```

Dry-run the apply step:

```bash
bash backend/scripts/kantropos_upstream.sh apply <draft_dir>
```

Apply after review:

```bash
bash backend/scripts/kantropos_upstream.sh apply <draft_dir> --yes
```

Print the Kantropos corpus-updater commands:

```bash
bash backend/scripts/kantropos_upstream.sh commands
```

Then run the printed markdown and embedding commands.

For the full draft-to-RAG path, including OCR for weak PDFs, dry-run first:

```bash
bash backend/scripts/kantropos_upstream.sh rag-flow
```

Apply to the upstream corpus, wait for Kantropos markdown generation, and start incremental embedding:

```bash
bash backend/scripts/kantropos_upstream.sh rag-flow --yes
```

## What "Pending Additions" Means

In DT, pending additions are downloaded works from local DT corpora assigned to a Kantropos target, excluding works already imported from that target's `metadata.bib`.

For `Anthropozän & Nachhaltiges Management`, the target id is:

```text
anthropozan-nachhaltiges-management
```

The target directory is:

```text
/data/projects/kantropos/corpora/Anthropozän & Nachhaltiges Management
```

The upstream metadata file is:

```text
/data/projects/kantropos/corpora/Anthropozän & Nachhaltiges Management/metadata.bib
```

## Important Current Limitation

The DT upstream tab can show pending additions, but it does not yet perform a one-click upstream apply from the browser.

Use the CLI wrapper instead:

```bash
bash backend/scripts/kantropos_upstream.sh draft
bash backend/scripts/kantropos_upstream.sh apply <draft_dir> --yes
bash backend/scripts/kantropos_upstream.sh commands
```

The draft tool copies pending PDFs into a staging directory and renders Kantropos-compatible BibTeX. The apply tool backs up the live `metadata.bib`, copies staged PDFs into the upstream corpus directory, and replaces `metadata.bib` with the reviewed `metadata.bib.new`.

If OCR sidecars are staged in the draft, the apply tool also copies those `.txt` files into the upstream corpus directory. Kantropos copies same-named `.txt` files into `markdown/` before parsing PDFs, so OCR text sidecars are used for scanned PDFs.

The draft and apply commands validate that every manifest entry has a parsed BibTeX `file` field matching Kantropos' expected `<pdf filename>:PDF` format. Apply refuses to run when validation fails.

The scraper lab has a `metadata.bib` draft workflow, but that is for scraper artifacts, not for existing upstream pending additions.

## How This Relates to DT

The upstream tab is a comparison view over two data sources:

- current upstream baseline: rows imported from the target's `metadata.bib`
- pending additions: downloaded DT works assigned to local corpora mapped to the same Kantropos target

For pending additions, DT already knows:

- the canonical work row in `works`
- the local corpus memberships in `corpus_works`
- the upstream assignment in `corpus_kantropos_assignments`
- the downloaded PDF path in `works.file_path`
- structured/enriched metadata in `works.bibtex_entry_json` and columns such as `title`, `authors`, `year`, `doi`, `abstract`

DT does not currently store those pending additions as final upstream BibTeX entries. They must be rendered into Kantropos-compatible BibTeX before being added to the upstream corpus.

## OCR Readiness

Kantropos generates RAG text from markdown files. Its markdown command uses embedded PDF text via PyMuPDF/pymupdf4llm. It does not OCR image-only pages by itself.

Always scan a draft before applying it:

```bash
bash backend/scripts/kantropos_upstream.sh scan-text --draft-dir <draft_dir> --write
```

The default weak-text thresholds are:

- fewer than `500` extracted text characters
- or fewer than `25%` non-empty pages

The scan writes `text-scan.json` into the draft when `--write` is used.

Preferred OCR runtime: Rechtmaschine branch `codex/debian-rag-ocr`. On that branch, the normal endpoint is `service_manager.py` on port `8004`, which lazy-loads `ocr/ocr_service_hibernate.py` on port `9003`. The backend processes PDFs page-by-page with `pypdfium2`, accepts `X-Request-ID`, and returns OCR text plus page/confidence/VRAM metadata.

Run or reach that service, then OCR weak staged PDFs through the service-manager endpoint:

```bash
RAG_FEEDER_OCR_SERVICE_URL=http://<rechtmaschine-host>:8004 \
  bash backend/scripts/kantropos_upstream.sh ocr <draft_dir> --keep-going
```

The OCR client expects this Rechtmaschine contract:

```text
POST /ocr
X-Request-ID: dt-upstream-<work_id>-<timestamp>
multipart file=<pdf>
response: request_id, full_text, page_count, avg_confidence, pages, metadata
```

If you bypass the service manager and call the OCR backend directly, set `RAG_FEEDER_OCR_SERVICE_URL` to port `9003` instead. Prefer port `8004` because it queues requests and handles lazy loading.

For each weak PDF, the command writes a staged text sidecar:

```text
files/<target-pdf-stem>.txt
```

It also records `ocr_text_file` and OCR statistics in `manifest.json`. Re-run `scan-text` after OCR if you want to confirm which PDFs were OCR sidecar-backed before applying.

The wrapper can start the local Rechtmaschine OCR manager automatically when needed:

```bash
bash backend/scripts/kantropos_upstream.sh ocr <draft_dir> --keep-going
```

By default it expects the Rechtmaschine checkout at:

```text
/home/spott/rechtmaschine-debian-rag-ocr
```

It starts `service_manager.py` in OCR-only mode on the host (`127.0.0.1:8004`), discovers the Docker host gateway for `rag_feeder_backend`, and passes the container-reachable OCR URL into the backend command. Override paths/URLs with:

```bash
RAG_FEEDER_OCR_HOME=/path/to/rechtmaschine \
RAG_FEEDER_OCR_HOST_URL=http://127.0.0.1:8004 \
RAG_FEEDER_OCR_SERVICE_URL=http://<docker-host-gateway>:8004 \
  bash backend/scripts/kantropos_upstream.sh ocr <draft_dir> --keep-going
```

To run the whole upstream-to-RAG sequence:

```bash
# Dry-run: draft, scan, OCR weak files, apply dry-run
bash backend/scripts/kantropos_upstream.sh rag-flow

# Real run: draft, scan, OCR weak files, apply, wait for markdown, start INSERT embedding
bash backend/scripts/kantropos_upstream.sh rag-flow --yes
```

`rag-flow --yes` waits for the Kantropos markdown endpoint to finish before starting embeddings with `sync_mode=INSERT`.

## Are the PDFs Already Available?

Yes. Downloaded pending additions have a `works.file_path`, usually under the backend container path:

```text
/usr/src/app/dl_lit_project/data/pdf_library
```

The backend container can also see the Kantropos corpus root:

```text
/data/projects/kantropos/corpora
```

The `draft` command copies directly from `works.file_path` into the selected draft's `files/` directory without downloading through the browser. The `apply` command then copies those staged PDFs into the selected Kantropos corpus directory.

To inspect sample pending PDF paths, create a small draft and inspect its manifest:

```bash
bash backend/scripts/kantropos_upstream.sh draft --limit 10
```

The printed `draft_dir/manifest.json` lists each source PDF path and target file name.

## Is the Metadata Already in the Right Format?

Not exactly.

DT has enough metadata to generate an upstream BibTeX entry, but the stored form is not the final `metadata.bib` text. Pending rows usually have:

- `works.title`
- `works.authors`, often as a JSON array string
- `works.year`
- `works.doi`
- `works.abstract`
- `works.source`
- `works.bibtex_entry_json`, a structured JSON metadata blob
- `works.file_path`, the downloaded PDF path

Kantropos needs a BibTeX entry with a `file` field pointing to the PDF name as it will exist in the Kantropos corpus directory:

```bibtex
file = {filename.pdf:PDF}
```

Therefore the DT metadata needs a render step:

1. choose/generate a stable BibTeX key
2. render title/authors/year/abstract/doi/source fields
3. copy the PDF to the upstream directory
4. set the BibTeX `file` field to the copied PDF basename

Do not assume the generic DT export format is already perfect for Kantropos. In particular, any generated `file` field should be checked to ensure it matches the Kantropos format from the admin PDF.

## Append or Produce a New metadata.bib?

The tool produces a new full `metadata.bib` draft, then replaces the live file only when `apply --yes` is run.

It follows this safer sequence:

1. read the current upstream `metadata.bib`
2. generate `metadata.pending-additions.bib` containing only the new entries
3. generate `metadata.bib.new` containing current metadata plus appended entries
4. copy PDFs into a staging directory or directly into the corpus directory
5. review duplicate keys, file names, and BibTeX syntax
6. back up the live `metadata.bib`
7. atomically replace `metadata.bib` with the reviewed `metadata.bib.new`

This is also the shape a future DT browser button should use: "Create upstream update draft" first, then "Apply reviewed draft" only after inspection.

## Before You Start

Confirm the relevant containers are running:

```bash
docker ps --format '{{.Names}}\t{{.Status}}' | rg 'kantropos|rag_feeder'
```

Expected relevant containers include:

- `rag_feeder_backend`
- `kantropos-corpus-updater`
- `kantropos-rag`
- `kantropos-postgres`
- `kantropos-qdrant`

Confirm the corpus directory exists:

```bash
docker exec rag_feeder_backend sh -lc \
  'ls -la "/data/projects/kantropos/corpora/Anthropozän & Nachhaltiges Management"'
```

## Count Pending Additions

Run this from the DT repo root to count pending additions for the Anthropozän upstream target:

```bash
bash backend/scripts/kantropos_upstream.sh count
```

## Back Up the Upstream Metadata

`apply --yes` automatically backs up the live `metadata.bib` next to the original before replacement.

```bash
bash backend/scripts/kantropos_upstream.sh apply <draft_dir> --yes
```

The command prints the backup path in its JSON output.

## Prepare the Corpus Files

Use the draft command:

```bash
bash backend/scripts/kantropos_upstream.sh draft
```

It performs the preparation step by:

1. locating each pending downloaded PDF in DT
2. copying PDFs into the draft `files/` directory
3. rendering `metadata.pending-additions.bib`
4. rendering the full `metadata.bib.new`

Kantropos expects each PDF to be referenced by a BibTeX `file` field.

Valid examples:

```bibtex
file = {filename.pdf:PDF}
```

or:

```bibtex
file = {{filename.pdf:PDF}}
```

Example full entry:

```bibtex
@article{some_key,
  title = {Some Title},
  year = {2025},
  abstract = {Some abstract},
  author = {First, Author and Second, Author},
  file = {filename.pdf:PDF}
}
```

The PDF notes:

- every corpus lives in its own directory under `/data/projects/kantropos/corpora`
- each corpus contains PDFs used for embedding
- each corpus has one `metadata.bib`
- `title`, `year`, `abstract`, and `author` are optional
- `file` is required for metadata-to-PDF linking
- if a same-named `.txt` file exists next to a PDF, Kantropos uses it instead of parsing the PDF

## Generate Markdown Files

Enter the corpus-updater container:

```bash
docker exec -it kantropos-corpus-updater bash
```

Generate missing markdowns:

```bash
curl -X POST "http://localhost:8001/markdowns/Anthropoz%C3%A4n%20%26%20Nachhaltiges%20Management" &
```

Notes:

- the corpus name must be URL encoded
- existing markdowns are not overwritten
- if a PDF changed and its markdown already exists, delete the corresponding markdown manually first
- markdown generation can take a long time for large corpora

Watch progress from the host:

```bash
docker logs -f kantropos-corpus-updater
```

Completion appears in the logs as a successful `POST /markdowns/<corpus>` response.

## Embed the Updated Corpus

Inside `kantropos-corpus-updater`, run:

```bash
curl -X POST "http://localhost:8001/embeddings/Anthropoz%C3%A4n%20%26%20Nachhaltiges%20Management?sync_mode=INSERT" &
```

The embedding model comes from `/data/projects/kantropos/config.yml`.

To override request parameters explicitly:

```bash
curl -X POST \
  "http://localhost:8001/embeddings/Anthropoz%C3%A4n%20%26%20Nachhaltiges%20Management?sync_mode=INSERT&model_name=jina%2Fjina-embeddings-v2-base-de&embed_batch_size=10" &
```

Operational notes:

- embedding can take days for large corpora
- with `sync_mode=INSERT`, later runs for the same corpus and model add only new documents
- `backend/scripts/kantropos_upstream.sh commands` prints the embedding command with `sync_mode=INSERT` explicitly
- retrieval should not be switched to a new corpus/model combination until its collection exists, because otherwise the RAG may trigger embedding during a user query and block

## Metadata-Only Update

To refresh PostgreSQL metadata used for displayed references:

```bash
curl -X POST "http://localhost:8001/metadata/Anthropoz%C3%A4n%20%26%20Nachhaltiges%20Management"
```

This does not update Qdrant metadata passed to the LLM. If title, year, or author metadata should change in retrieval context, rerun embeddings.

## Restart Containers After Config Changes

If `/data/projects/kantropos/config.yml` changes:

```bash
docker restart kantropos-corpus-updater
docker restart kantropos-rag
```

If `.env` files used by the Kantropos compose stack change:

```bash
docker compose \
  -f /data/projects/kantropos/compose.yml \
  -f /data/projects/kantropos/compose.staging.yml \
  -f /data/projects/kantropos/compose.production.yml \
  -f /data/projects/kantropos/compose.production-uol.yml \
  up -d
```

## Verify After Update

Check the corpus-updater logs:

```bash
docker logs -f kantropos-corpus-updater
```

Check the Kantropos Postgres database if needed:

```bash
docker exec -it kantropos-postgres psql -d kantropos -U kantropos-user
```

Then reload DT's upstream tab. The pending count for the target should drop only after DT has imported the updated upstream `metadata.bib` into its local `works` table with:

- `origin_type = 'bibtex_import'`
- `origin_key = /data/projects/kantropos/corpora/Anthropozän & Nachhaltiges Management/metadata.bib`

If `metadata.bib` was updated on disk but DT still shows the old pending count, the RAG may be updated while DT's local imported-baseline view is stale.
