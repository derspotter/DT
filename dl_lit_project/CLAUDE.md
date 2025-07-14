# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This is `dl_lit`, an automated PDF literature pipeline for academic research. The system extracts bibliographies from PDFs, enriches metadata via APIs (OpenAlex, Crossref), downloads referenced papers, and manages everything in a SQLite database with a staged processing workflow.

## Environment Setup

**CRITICAL**: All commands must be run from within the activated Python virtual environment:

```bash
source venv/bin/activate  # or your venv path
```

Required environment variables:
- `GOOGLE_API_KEY`: Required for bibliography extraction using Gemini AI

## Core Commands

The main CLI entry point is `python -m dl_lit.cli`. Key commands:

### Database Management
- `python -m dl_lit.cli init-db [--db-path PATH]` - Initialize database schema
- `python -m dl_lit.cli clear-downloaded [--db-path PATH]` - Clear downloaded_references table
- `python -m dl_lit.cli show-sample [--limit N] [--show-all]` - View downloaded entries

### Bibliography Processing Pipeline
1. `python -m dl_lit.cli extract-bib-pages <pdf_file> [--output-dir DIR]` - Extract reference pages from PDF
2. `python -m dl_lit.cli extract-bib-api --input-path <path> --output-dir <dir> [--workers N]` - Parse references to JSON using GenAI
3. `python -m dl_lit.cli enrich-openalex-db [--limit N] [--db-path PATH]` - Enrich metadata via OpenAlex/Crossref APIs  
4. `python -m dl_lit.cli process-downloads [--limit N] [--db-path PATH]` - Queue enriched entries for download
5. `python -m dl_lit.cli download-pdfs [--limit N] [--db-path PATH]` - Download PDF files

### Import/Export
- `python -m dl_lit.cli import-bib <bibtex_file> [--pdf-base-dir DIR] [--db-path PATH]` - Import existing BibTeX library
- `python -m dl_lit.cli export-bibtex <output_file> [--db-path PATH]` - Export to BibTeX format
- `python -m dl_lit.cli add-to-no-metadata --json-file <file> [--source-pdf PATH]` - Import JSON bibliography entries

### Testing
Run tests with pytest:
```bash
pytest tests/
```

Tests include mocked CLI commands for downloader, enricher, and bibliography extraction components.

## Architecture

### Staged Database Workflow
The pipeline uses a 4-stage SQLite database approach to prevent duplicate work:

1. **`no_metadata`** - Raw references extracted by APIscraper_v2 from PDFs
2. **`with_metadata`** - References enriched with OpenAlex/Crossref metadata  
3. **`to_download_references`** - Final download queue
4. **`downloaded_references`** - Successfully processed entries (master table)

Additional tables: `duplicate_references`, `failed_downloads`

### Key Components
- **`cli.py`**: Click-based CLI with all commands
- **`db_manager.py`**: DatabaseManager class handling all SQL operations and schema
- **`get_bib_pages.py`**: GenAI-powered bibliography page extraction from PDFs
- **`APIscraper_v2.py`**: GenAI text parsing to structured JSON, streams to `no_metadata` table
- **`OpenAlexScraper.py`**: Metadata enrichment via OpenAlex/Crossref APIs
- **`new_dl.py`**: BibliographyEnhancer class for PDF downloading
- **`bibtex_formatter.py`**: BibTeX export formatting with JabRef-compatible `file` fields

### Processing Flow
```
PDF → extract-bib-pages → extract-bib-api → no_metadata → 
enrich-openalex-db → with_metadata → process-downloads → 
to_download_references → download-pdfs → downloaded_references
```

### Dependencies
Core libraries: `click`, `sqlite3`, `requests`, `bibtexparser>=2.0.0b8`, `google-generativeai`, `pikepdf`, `PyMuPDF`, `beautifulsoup4`, `tqdm`, `python-dotenv`, `rapidfuzz`, `colorama`, `google-cloud-aiplatform`

### Database Schema
All tables share common columns: `id`, `title`, `authors`, `editors`, `doi`, `normalized_doi`, `openalex_id`, `bibtex_key`, `year`, `journal_conference`, `abstract`, `file_path`, `metadata_source_type`, `bibtex_entry_json`, `date_added`, etc.

Key features:
- DOI normalization for consistent duplicate detection
- UNIQUE constraints on `doi` and `openalex_id` in final tables
- Transaction-based moves between staging tables
- In-memory database support for bulk imports
- **Editor extraction**: Separate `editors` field for book/volume editors to improve matching accuracy

### File Structure
- `dl_lit/` - Main package with all modules
- `data/` - Default location for `literature.db` and downloaded PDFs
- `tests/` - pytest-based unit tests
- `requirements.txt` - Python dependencies
- `plan.md` - Detailed project documentation and roadmap

## Recent Updates

### Editor Extraction Implementation (2025-01-09)

**New Feature**: Complete editor extraction and matching system for improved bibliographic accuracy.

#### Key Improvements:
1. **Enhanced Gemini Extraction**: Updated `APIscraper_v2.py` prompt to:
   - Extract editors as separate field from authors
   - Properly handle series editors (e.g., "Obst/Hintner") vs. actual source titles
   - Clean source title extraction without editor contamination

2. **Database Schema Enhancement**: Added `editors` field to all pipeline tables:
   - `no_metadata`, `with_metadata`, `to_download_references`, `downloaded_references`
   - `failed_downloads`, `duplicate_references`
   - JSON array format for consistency with authors field

3. **Improved Author Matching**: Enhanced `OpenAlexScraper.py` with:
   - Separate matching for authors vs. editors
   - **Smart OR logic**: Match succeeds if EITHER authors OR editors match well (>0.85)
   - German name particle handling ("von", "van", "de", etc.)
   - **Standardized Rate Limiting**: All components now use shared global rate limiter with proper service names

4. **Enhanced Matching Accuracy**: Now successfully matches:
   - Book chapters by their container's editors when chapter isn't indexed separately
   - German academic names with nobility particles
   - Citations with different author/editor role assignments

#### Technical Details:
- **Container Matching**: When specific chapters aren't in OpenAlex, system matches parent book via editors
- **Bibliographic Relationships**: Handles chapter authors ≠ book editors scenarios
- **Quality Threshold**: Maintains 0.85 matching threshold for high precision
- **Progressive Search**: 9-step search strategy from specific to broad matching

#### Example Success Case:
- **Input**: "Das Finanzsystem in Deutschland" by Alexander V., Bohl, M. (editors: von Hagen, J., von Stein, J. H.)
- **Match**: "Geld-, Bank- und Börsenwesen : Handbuch des Finanzsystems" via perfect editor matching
- **Result**: Successful enrichment with OpenAlex metadata and relationship tracking

### Previous Critical Fixes (2025-06-30)

1. **Metadata Access**: Fixed `bib_entry` extraction in `new_dl.py:645` - downloader now properly accesses enriched metadata
2. **Rate Limiting**: Fixed `ServiceRateLimiter.wait_if_needed()` in `utils.py` to return `True` instead of `None`
3. **Author Handling**: Fixed LibGen author surname extraction to handle both dict and string formats
4. **Database Schema**: Added missing `date_processed` column to `failed_downloads` table
5. **LibGen Scraping**: Completely rewrote LibGen search/parsing with:
   - Proper table parsing using `#tablelibgen` ID
   - Correct column indexing (extension in column 7, mirrors in rightmost column)
   - Multi-mirror link extraction from the "Mirrors" column
   - Smart retry logic for ad/redirect pages
   - PDF validation and automatic fallback to real download links

### Pipeline Status:
✅ **Full end-to-end functionality with enhanced matching**:
- JSON bibliography extraction → metadata enrichment → PDF downloads
- All download sources working: DOI, Sci-Hub (all mirrors), LibGen (with smart parsing)
- **NEW**: Editor-based matching for improved bibliographic accuracy
- Proper error handling and fallback mechanisms
- Transaction-safe database operations