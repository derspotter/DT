# WZB Abstracts RAG Import

This workflow prepares the Weizenbaum abstracts folder for the current Kantropos RAG corpus-updater.

The source folder used during development was:

```bash
/home/spott/wzb
```

It contains `metadata_wzb_abstracts.bib` and the `Weizenbaum_Abstracts/` files.

## Why This Extra Step Exists

The Kantropos corpus-updater currently discovers documents by listing `*.pdf` files in the corpus directory. It then embeds the matching text from `markdown/<stem>.txt`.

For WZB, most source files are ODTs, not PDFs. The helper extracts ODT text directly with Python stdlib, creates `<key>.txt` sidecars, and creates small `<key>.pdf` placeholders so the existing updater can discover the documents. The generated `metadata.bib` points to those placeholders with the normal Kantropos format:

```bibtex
file = {{wzb-002.pdf:PDF}}
```

No LibreOffice, Pandoc, or unoconv is required.

## Draft

Create a draft:

```bash
backend/scripts/wzb_rag_prepare.py --source-dir /home/spott/wzb draft --copy-pdfs
```

The command prints a `draft_dir`, for example:

```bash
/tmp/wzb_rag_drafts/wzb_20260526_215834
```

The draft contains:

- `metadata.bib`: rewritten Kantropos-compatible metadata
- `files/*.txt`: extracted text sidecars to embed
- `files/*.pdf`: placeholder PDFs for ODT entries, copied PDFs for PDF entries
- `manifest.json`: source-to-target mapping and extraction status

Validate the draft:

```bash
backend/scripts/wzb_rag_prepare.py validate --draft-dir /tmp/wzb_rag_drafts/wzb_YYYYMMDD_HHMMSS
```

## Apply

Dry-run the copy into a new Kantropos corpus directory:

```bash
backend/scripts/wzb_rag_prepare.py --target-name "Weizenbaum Abstracts" apply --draft-dir /tmp/wzb_rag_drafts/wzb_YYYYMMDD_HHMMSS
```

Apply for real:

```bash
backend/scripts/wzb_rag_prepare.py --target-name "Weizenbaum Abstracts" apply --draft-dir /tmp/wzb_rag_drafts/wzb_YYYYMMDD_HHMMSS --yes
```

If the target already has a `metadata.bib`, the tool writes a timestamped backup before replacing it.

## Generate Markdown And Embed

After applying, run:

```bash
docker exec kantropos-corpus-updater curl -fsS -X POST "http://localhost:8001/markdowns/Weizenbaum%20Abstracts"
docker exec kantropos-corpus-updater curl -fsS -X POST "http://localhost:8001/embeddings/Weizenbaum%20Abstracts?sync_mode=INSERT"
```

The helper can print these commands:

```bash
backend/scripts/wzb_rag_prepare.py --target-name "Weizenbaum Abstracts" commands
```

## Validation Notes

On May 26, 2026, the helper produced:

- 171 BibTeX entries
- 168 ODT-extracted text sidecars
- 3 copied PDF-backed entries
- 0 missing source files after normalized filename matching
- 0 BibTeX parse failures in both `rag_feeder_backend` and `kantropos-corpus-updater`

## Completed Import

On May 26, 2026, the import was applied to:

```bash
/data/projects/kantropos/corpora/Weizenbaum Abstracts
```

Post-import checks showed:

- 171 `metadata` rows for `corpus = 'Weizenbaum Abstracts'`
- 171 markdown sidecars in the corpus `markdown/` directory
- 342 Qdrant points in `Collection_Weizenbaum Abstracts_embedding_jina-jina-embeddings-v2-base-de`
- 3.6M corpus directory size

The Qdrant point count is larger than the document count because documents are split into embedding chunks.

## Revert

If the import must be rolled back, remove the corpus directory, delete the Qdrant collection, and delete the metadata rows:

```bash
rm -rf "/data/projects/kantropos/corpora/Weizenbaum Abstracts"

docker exec kantropos-corpus-updater python -c "from util.database_util import create_client, delete_collection_by_name; create_client(); delete_collection_by_name('Collection_Weizenbaum Abstracts_embedding_jina-jina-embeddings-v2-base-de')"

docker exec kantropos-postgres psql -U kantropos-user -d kantropos -c "DELETE FROM metadata WHERE corpus='Weizenbaum Abstracts';"
```

Only run these commands when intentionally reverting the WZB import.
