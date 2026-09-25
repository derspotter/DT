# Kantropos adapters

DT maintains this narrow replacement for upstream `util/markdown_util.py`.
It is not Rechtmaschine code and does not alter OCR, models or dependencies.

Operators should follow the complete
[Corpusbuilder-to-RAG runbook](../../dl_lit_project/docs/KANTROPOS_UPSTREAM_UPDATE.md).
This file covers adapter deployment, not the whole import/embedding sequence.
The adapter is a local patch to Integral Learning's RAG component.

The adapter retries empty/invalid UTF-8 outputs, falls back to the PDF's
existing text layer when layout conversion fails or produces no text, records
invalid surrogate replacements, writes atomically, logs progress/heartbeats,
and propagates per-file failures to the HTTP route. It does not manufacture
text for image-only PDFs: empty fallback or unreadable pages fail closed.
An existing nonempty output is reused, not a claim of semantic completeness.

## Production integration

On the UOL host, `/data/projects/kantropos/compose.production-uol.yml` adds
this read-only volume to `services.corpus-updater.volumes`:

```yaml
- /home/spott/DT/backend/kantropos/markdown_util.py:/corpus-updater/util/markdown_util.py:ro
```

The existing image and all other service configuration stay unchanged.
Use the existing four Compose files, in their existing order, to recreate
**only** `corpus-updater` with `up -d --no-deps --pull never corpus-updater`.
Confirm that no markdown/embedding request is active before recreation.
The mount survives future normal Compose recreations. After updating this
module, recreate that service (single-file bind mounts can retain old inodes).
Recheck compatibility when upgrading the upstream image.

Keep a timestamped copy of the old deployment file and original module before
deploying. To roll back, restore the old deployment file and recreate only
the updater. Existing PDFs and successfully generated text are preserved.

Default concurrency is four CPU processes, capped by available CPUs.
`DT_MARKDOWN_WORKERS` may override it. No GPU/OCR work is performed here.

## Tests and run safety

```sh
python3 -m unittest discover -s dl_lit_project/tests -p 'test_kantropos_markdown.py'
```

Before embedding a resumed DT draft, run `upstream_update.py check-markdown`
in the DT backend. The saved draft and original imported PDFs must still
match. Never bypass the coverage check or start overlapping embedding jobs.
HTTP request success is not a substitute for checking stored new document IDs.

## Draft-scoped incremental embedding

`embed_draft.py` runs as a separate process inside the existing updater container.
The host wrapper streams the saved manifest on stdin and supplies the runner from
the DT checkout. No image rebuild, HTTP route change or service restart is needed.
It uses upstream's configured Ollama model, chunk sizes, document metadata format
and metadata writer. Other model providers fail explicitly rather than switching
models. Preview checks text, metadata and chunking without contacting Ollama or
writing vectors/metadata. The configured Qdrant must support exact `/facet` queries
(supported by the target runtime), otherwise the runner stops. It does not fall back to a full import.
The index check reads distinct IDs without a large `MatchAny` filter and intersects
them with the manifest locally. This avoids a potentially memory-intensive query
on older Qdrant versions. The inventory has a 100,000-ID safety cap and fails closed
if the response reaches it rather than assuming absent IDs are unindexed.

```sh
# Run on the production host, after confirming all earlier imports have ended.
bash backend/scripts/kantropos_upstream.sh embed-draft "$draft_dir"
# Explicitly write only pending IDs in that exact draft.
bash backend/scripts/kantropos_upstream.sh embed-draft "$draft_dir" --yes
```

The wrapper retains import/text preflights and its existing host lock. The old
upstream HTTP endpoint does not share that lock. Never run it concurrently.
Unlike that endpoint, the runner does not enumerate unrelated old PDFs. It skips
already indexed draft IDs, refuses vector/metadata mismatches, checks empty text
and empty chunk inputs, and reports each filename and final coverage. Failed
writes are not automatically retried: a partial vector insert needs investigation.
Completion coverage proves document presence, not historical chunk completeness.

Regression tests:

```sh
python3 -m unittest discover -s dl_lit_project/tests -p 'test_kantropos_embed_draft.py'
python3 -m unittest discover -s dl_lit_project/tests -p 'test_upstream_recovery.py'
```
