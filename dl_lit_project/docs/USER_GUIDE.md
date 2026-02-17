# User Guide

## 1. Access the app

1. Start services:
   - `docker compose up -d`
2. Open frontend:
   - `http://localhost:5175`
3. Log in (default local setup):
   - Username: `admin`
   - Password: `admin`

## 2. Select or create a corpus

- Use the **Corpus** dropdown in the header.
- Click **New corpus** to create an isolated workspace.
- All ingest, search, enrichment, and downloads are scoped to the selected corpus.

## 3. Ingest seed PDFs

1. Go to **Ingest**.
2. Upload one or more PDFs.
3. Run extraction.
4. Review extracted entries and select rows.
5. Enqueue selected rows for enrichment.

## 4. Keyword search (OpenAlex)

1. Go to **Keyword Search**.
2. Choose mode:
   - Free text query
   - Seed JSON (`openalex_id` / DOI list)
3. Configure year filters and expansion depth.
4. Run search and enqueue selected results.

## 5. Pipeline processing

- Use **Start / Continue** in the header.
- The global worker performs:
  1. `raw -> with metadata` enrichment
  2. download processing from metadata stage
- Use **Pause** to stop safely.
- Watch live status in **Pipeline Logs**.

## 6. Inspect outputs

- **Corpus**: browse stage tables and entry details.
- **Downloads**: view queue and downloaded outputs.
- **Graph**: inspect citation network and filters.

## 7. Export data

- Open **Downloads** and use **Corpus Exports**:
  - JSON
  - BibTeX
  - PDF bundle (ZIP)
