"""Gemini-based bibliography extraction from page-level PDFs into database staging.

Accepted inputs:
- a directory of PDFs via `--input-dir`
- a single PDF via `--input-pdf`
- a positional path (file or directory) for backward compatibility.
"""

import argparse
import concurrent.futures
from contextlib import nullcontext
import glob
import json
import os
import re
import shutil
import sys
import tempfile
import time
import unicodedata
from pathlib import Path
from threading import Lock
from typing import List, Optional

from dotenv import load_dotenv
from google import genai
from google.genai import types

try:
    import pikepdf
except Exception:
    pikepdf = None

# Make local package imports work when this module is executed directly.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dl_lit.db_manager import DatabaseManager
from dl_lit.utils import get_global_rate_limiter
try:
    from pydantic import BaseModel, ValidationError
    try:
        from pydantic import ConfigDict
    except Exception:
        ConfigDict = None
except ImportError:
    BaseModel = None
    ValidationError = Exception
    ConfigDict = None

# --- Configuration ---
# Use Gemini 3.1 Flash Lite preview for all extraction steps in this script.
DEFAULT_MODEL_NAME = "gemini-3.1-flash-lite-preview"


def _env_int(name: str, default: int, minimum: int = 1) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return max(minimum, int(default))
    try:
        return max(minimum, int(raw))
    except Exception:
        return max(minimum, int(default))


DEFAULT_EXTRACT_WORKERS = _env_int("RAG_FEEDER_API_EXTRACT_WORKERS", 10)
DEFAULT_PAGE_BATCH_SIZE = _env_int("RAG_FEEDER_PAGE_BATCH_SIZE", 4)
REF_PAGE_FILE_RE = re.compile(r"^(?P<prefix>.+?)_refs_physical_p(?P<page>\d+)\.pdf$", re.IGNORECASE)

# Global client
api_client = None

# --- Rate Limiter --- (Shared global instance)
rate_limiter = get_global_rate_limiter()

# --- Bibliography Prompt ---
BIBLIOGRAPHY_PROMPT = (
    "Extract bibliography entries from the attached PDF page (or short consecutive page batch) and return a JSON object with keys "
    "'source_metadata' and 'items' matching the provided schema. "
    "CRITICAL: You MUST extract the 'authors' for every entry if they exist in the text. "
    "Look closely for names at the beginning of each citation. Never leave the 'authors' list empty if names are visible. "
    "For 'source_metadata', infer metadata for the seed/source document only if clearly identifiable on these pages; otherwise return null. "
    "If no bibliography entries are found, return items as an empty list."
)

INLINE_CITATION_PROMPT = (
    "Extract all cited works from this PDF chunk, including inline citations, footnotes, endnotes, "
    "and per-page references. Return a JSON object with keys 'source_metadata' and 'items' matching the provided schema. "
    "Include each cited work once if identifiable. "
    "CRITICAL: You MUST extract the 'authors' for every cited work. Look closely for names before the year or title. "
    "Also extract metadata for the source/seed document itself into 'source_metadata' (title, authors, year, doi, source, publisher) when identifiable. "
    "If no citations are found, return items as an empty list."
)

SOURCE_METADATA_PROMPT = (
    "Extract metadata of the source/seed document from the attached PDF. "
    "Return a JSON object with keys 'source_metadata' and 'items'. "
    "Populate 'source_metadata' with: title, authors, year, doi, source, publisher. "
    "Use null for unknown fields. "
    "Set 'items' to an empty list."
)

if BaseModel:
    if ConfigDict:
        class BibliographyEntry(BaseModel):
            model_config = ConfigDict(extra="ignore")
            authors: Optional[List[str]] = None
            editors: Optional[List[str]] = None
            year: Optional[int | str] = None
            title: Optional[str] = None
            source: Optional[str] = None
            volume: Optional[int | str] = None
            issue: Optional[int | str] = None
            pages: Optional[str] = None
            publisher: Optional[str] = None
            location: Optional[str] = None
            type: Optional[str] = None
            doi: Optional[str] = None
            url: Optional[str] = None
            isbn: Optional[str] = None
            issn: Optional[str] = None
            abstract: Optional[str] = None
            keywords: Optional[List[str]] = None
            language: Optional[str] = None
            translated_title: Optional[str] = None
            translated_year: Optional[int | str] = None
            translated_language: Optional[str] = None
            translation_relationship: Optional[str] = None

        class SourceDocumentMetadata(BaseModel):
            model_config = ConfigDict(extra="ignore")
            title: Optional[str] = None
            authors: Optional[List[str]] = None
            year: Optional[int | str] = None
            doi: Optional[str] = None
            source: Optional[str] = None
            publisher: Optional[str] = None

        class BibliographyEntryList(BaseModel):
            model_config = ConfigDict(extra="ignore")
            items: List[BibliographyEntry] = []
            source_metadata: Optional[SourceDocumentMetadata] = None

    else:
        class BibliographyEntry(BaseModel):
            class Config:
                extra = "ignore"
            authors: Optional[List[str]] = None
            editors: Optional[List[str]] = None
            year: Optional[int | str] = None
            title: Optional[str] = None
            source: Optional[str] = None
            volume: Optional[int | str] = None
            issue: Optional[int | str] = None
            pages: Optional[str] = None
            publisher: Optional[str] = None
            location: Optional[str] = None
            type: Optional[str] = None
            doi: Optional[str] = None
            url: Optional[str] = None
            isbn: Optional[str] = None
            issn: Optional[str] = None
            abstract: Optional[str] = None
            keywords: Optional[List[str]] = None
            language: Optional[str] = None
            translated_title: Optional[str] = None
            translated_year: Optional[int | str] = None
            translated_language: Optional[str] = None
            translation_relationship: Optional[str] = None

        class SourceDocumentMetadata(BaseModel):
            class Config:
                extra = "ignore"
            title: Optional[str] = None
            authors: Optional[List[str]] = None
            year: Optional[int | str] = None
            doi: Optional[str] = None
            source: Optional[str] = None
            publisher: Optional[str] = None

        class BibliographyEntryList(BaseModel):
            class Config:
                extra = "ignore"
            items: List[BibliographyEntry] = []
            source_metadata: Optional[SourceDocumentMetadata] = None


# --- Helper Functions ---

def load_api_key():
    """Loads the appropriate API key based on the chosen model."""
    load_dotenv() # Load .env for local execution
    key = os.getenv('GEMINI_API_KEY') or os.getenv('GOOGLE_API_KEY')
    if not key:
        print("[ERROR] GEMINI_API_KEY (or GOOGLE_API_KEY) not found.", flush=True)
    return key

def configure_api_client():
    """Configures the API client/model."""
    global api_client
    api_key = load_api_key()
    if not api_key:
        return False

    try:
        api_client = genai.Client(api_key=api_key)
        print(f"[INFO] Successfully configured Google client ({DEFAULT_MODEL_NAME}).", flush=True)
        return True
    except Exception as e:
        print(f"[ERROR] Failed to configure API client: {e}", flush=True)
        api_client = None
        return False

def _wait_rate_limit(service_name: str, units: int = 1) -> None:
    """Best-effort rate limiting without turning throttling issues into hard failures."""
    try:
        if rate_limiter:
            rate_limiter.wait_if_needed(service_name, units=units)
    except Exception:
        return


def _extract_total_tokens(response) -> int | None:
    try:
        usage = getattr(response, "usage_metadata", None)
        if usage is None:
            return None
        for key in ("total_token_count", "total_tokens"):
            value = getattr(usage, key, None)
            if isinstance(value, (int, float)) and value > 0:
                return int(value)
            if isinstance(value, str) and value.strip().isdigit():
                return int(value.strip())
    except Exception:
        return None
    return None

def _resolve_prompt(extract_mode: str) -> str:
    if extract_mode == "inline":
        return INLINE_CITATION_PROMPT
    if extract_mode == "source_meta":
        return SOURCE_METADATA_PROMPT
    return BIBLIOGRAPHY_PROMPT


def _ascii_safe_pdf_name(filename: str) -> str:
    normalized = unicodedata.normalize("NFKD", filename or "")
    ascii_only = normalized.encode("ascii", "ignore").decode("ascii")
    sanitized = "".join(ch if (ch.isalnum() or ch in "._-") else "_" for ch in ascii_only).strip("._")
    if not sanitized:
        sanitized = "upload"
    if not sanitized.lower().endswith(".pdf"):
        sanitized += ".pdf"
    return sanitized


def _prepare_pdf_upload(pdf_path: str) -> tuple[str, str, str | None]:
    source_path = str(pdf_path)
    display_name = Path(source_path).name
    try:
        source_path.encode("ascii")
        display_name.encode("ascii")
        return source_path, display_name, None
    except UnicodeEncodeError:
        safe_dir = tempfile.mkdtemp(prefix="gemini_upload_")
        safe_name = _ascii_safe_pdf_name(display_name)
        safe_path = os.path.join(safe_dir, safe_name)
        shutil.copy2(source_path, safe_path)
        return safe_path, safe_name, safe_dir


def _normalize_source_metadata(value):
    if not isinstance(value, dict):
        return None
    metadata = {}
    title = value.get("title")
    if isinstance(title, str) and title.strip():
        metadata["title"] = title.strip()
    authors = value.get("authors")
    if isinstance(authors, list):
        cleaned = [str(a).strip() for a in authors if str(a).strip()]
        if cleaned:
            metadata["authors"] = cleaned
    year = value.get("year")
    if isinstance(year, (int, float)):
        metadata["year"] = int(year)
    elif isinstance(year, str):
        cleaned_year = year.strip()
        if cleaned_year:
            metadata["year"] = cleaned_year
    doi = value.get("doi")
    if isinstance(doi, str) and doi.strip():
        metadata["doi"] = doi.strip()
    source = value.get("source")
    if isinstance(source, str) and source.strip():
        metadata["source"] = source.strip()
    publisher = value.get("publisher")
    if isinstance(publisher, str) and publisher.strip():
        metadata["publisher"] = publisher.strip()
    return metadata or None


def _upsert_seed_document_metadata(
    db_manager: DatabaseManager,
    ingest_source: str | None,
    corpus_id: int | None,
    source_pdf: str | None,
    source_metadata: dict | None,
) -> None:
    if not ingest_source or not source_metadata:
        return
    conn = db_manager.conn
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ingest_source_metadata (
            corpus_id INTEGER NOT NULL DEFAULT 0,
            ingest_source TEXT NOT NULL,
            source_pdf TEXT,
            title TEXT,
            authors TEXT,
            year INTEGER,
            doi TEXT,
            source TEXT,
            publisher TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (corpus_id, ingest_source)
        )
        """
    )
    corpus_key = int(corpus_id) if corpus_id is not None else 0
    authors_json = None
    if isinstance(source_metadata.get("authors"), list):
        authors_json = json.dumps(source_metadata.get("authors"), ensure_ascii=False)
    year_value = source_metadata.get("year")
    year_int = None
    if isinstance(year_value, int):
        year_int = year_value
    elif isinstance(year_value, str) and year_value.strip().isdigit():
        year_int = int(year_value.strip())
    cur.execute(
        """
        INSERT INTO ingest_source_metadata (
            corpus_id, ingest_source, source_pdf, title, authors, year, doi, source, publisher
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(corpus_id, ingest_source)
        DO UPDATE SET
            source_pdf = excluded.source_pdf,
            title = COALESCE(excluded.title, ingest_source_metadata.title),
            authors = COALESCE(excluded.authors, ingest_source_metadata.authors),
            year = COALESCE(excluded.year, ingest_source_metadata.year),
            doi = COALESCE(excluded.doi, ingest_source_metadata.doi),
            source = COALESCE(excluded.source, ingest_source_metadata.source),
            publisher = COALESCE(excluded.publisher, ingest_source_metadata.publisher),
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            corpus_key,
            str(ingest_source),
            str(source_pdf) if source_pdf else None,
            source_metadata.get("title"),
            authors_json,
            year_int,
            source_metadata.get("doi"),
            source_metadata.get("source"),
            source_metadata.get("publisher"),
        ),
    )
    conn.commit()


def _split_pdf_into_chunks(pdf_path: str, chunk_pages: int) -> tuple[str | None, list[str]]:
    """Create temporary chunk PDFs and return (temp_dir, chunk_paths)."""
    if chunk_pages <= 0:
        return None, [pdf_path]
    if pikepdf is None:
        raise RuntimeError("pikepdf is required for chunked extraction fallback")

    with pikepdf.Pdf.open(pdf_path) as src_pdf:
        total_pages = len(src_pdf.pages)
        if total_pages <= chunk_pages:
            return None, [pdf_path]

    temp_dir = tempfile.mkdtemp(prefix="apiscraper_chunks_")
    chunk_paths: list[str] = []

    with pikepdf.Pdf.open(pdf_path) as src_pdf:
        for start in range(0, len(src_pdf.pages), chunk_pages):
            end = min(start + chunk_pages, len(src_pdf.pages))
            chunk_pdf = pikepdf.Pdf.new()
            for idx in range(start, end):
                chunk_pdf.pages.append(src_pdf.pages[idx])
            chunk_path = os.path.join(temp_dir, f"{Path(pdf_path).stem}_chunk_{start+1:05d}_{end:05d}.pdf")
            chunk_pdf.save(chunk_path)
            chunk_paths.append(chunk_path)

    return temp_dir, chunk_paths


def _build_page_batches(
    pdf_files: list[str],
    page_batch_size: int,
) -> tuple[list[tuple[str, str]], str | None]:
    """Build logical extraction units.

    Returns list of tuples: (pdf_path_for_api_call, source_pdf_override_for_db)
    and optional temp directory for generated batch PDFs.
    """
    if page_batch_size <= 1 or pikepdf is None:
        return [(path, str(path)) for path in pdf_files], None

    ref_groups: dict[str, list[tuple[int, str]]] = {}
    passthrough: list[str] = []
    for path in pdf_files:
        base = os.path.basename(path)
        match = REF_PAGE_FILE_RE.match(base)
        if not match:
            passthrough.append(path)
            continue
        prefix = match.group("prefix")
        page = int(match.group("page"))
        ref_groups.setdefault(prefix, []).append((page, path))

    if not ref_groups:
        return [(path, str(path)) for path in pdf_files], None

    temp_batch_dir = tempfile.mkdtemp(prefix="apiscraper_page_batches_")
    work_items: list[tuple[str, str]] = [(path, str(path)) for path in passthrough]
    created_batch_count = 0

    def _flush_group(prefix: str, group: list[tuple[int, str]]) -> None:
        nonlocal created_batch_count
        if not group:
            return
        if len(group) == 1:
            single_path = group[0][1]
            work_items.append((single_path, str(single_path)))
            return

        start_page = group[0][0]
        end_page = group[-1][0]
        out_name = f"{prefix}_refs_batch_p{start_page}-{end_page}.pdf"
        out_path = os.path.join(temp_batch_dir, out_name)
        merged = pikepdf.Pdf.new()
        for _, src_path in group:
            with pikepdf.Pdf.open(src_path) as src_pdf:
                for pg in src_pdf.pages:
                    merged.pages.append(pg)
        merged.save(out_path)
        created_batch_count += 1
        # Keep first source page path for DB traceability.
        work_items.append((out_path, str(group[0][1])))

    try:
        for prefix in sorted(ref_groups.keys()):
            entries = sorted(ref_groups[prefix], key=lambda item: item[0])
            current_group: list[tuple[int, str]] = []
            prev_page: int | None = None
            for page, src_path in entries:
                if not current_group:
                    current_group = [(page, src_path)]
                    prev_page = page
                    continue
                if page == (prev_page + 1) and len(current_group) < page_batch_size:
                    current_group.append((page, src_path))
                    prev_page = page
                    continue
                _flush_group(prefix, current_group)
                current_group = [(page, src_path)]
                prev_page = page
            _flush_group(prefix, current_group)
    except Exception as exc:
        print(f"[WARNING] Failed to build page batches ({exc}); falling back to single-page calls.", flush=True)
        shutil.rmtree(temp_batch_dir, ignore_errors=True)
        return [(path, str(path)) for path in pdf_files], None

    if created_batch_count == 0:
        shutil.rmtree(temp_batch_dir, ignore_errors=True)
        return [(path, str(path)) for path in pdf_files], None

    work_items = sorted(work_items, key=lambda item: item[0])
    return work_items, temp_batch_dir


def upload_pdf_and_extract_bibliography(
    pdf_path,
    timeout_seconds: int = 120,
    extract_mode: str = "page",
    uploaded_file_name: str | None = None,
):
    """Uploads a PDF file directly to the API and attempts to extract bibliography entries."""
    print(f"[INFO] Uploading PDF directly for bibliography extraction: {pdf_path}", flush=True)
    start_time = time.time()
    bibliography_data = []
    source_metadata = None
    uploaded_file = None
    temp_upload_dir = None

    if api_client is None:
        if not configure_api_client():
            print("[ERROR] API client not configured; aborting extraction.", flush=True)
            return {"items": [], "source_metadata": None}
    
    try:
        if uploaded_file_name:
            uploaded_file = api_client.files.get(name=uploaded_file_name)
            print(f"[DEBUG] Reusing uploaded PDF as name: {uploaded_file.name}", flush=True)
        else:
            upload_path, upload_display_name, temp_upload_dir = _prepare_pdf_upload(str(pdf_path))
            # Upload the PDF file via Files API.
            uploaded_file = api_client.files.upload(
                file=upload_path,
                config={
                    "mime_type": "application/pdf",
                    "display_name": upload_display_name,
                },
            )
            print(f"[DEBUG] Uploaded PDF as name: {uploaded_file.name}", flush=True)
        
        # Prepare the prompt for bibliography extraction
        prompt = _resolve_prompt(extract_mode)
        _wait_rate_limit("gemini")
        _wait_rate_limit("gemini_daily")
        
        def _call_model():
            config = types.GenerateContentConfig(
                temperature=1.0,
                response_mime_type="application/json",
            )
            if BaseModel:
                config.response_schema = BibliographyEntryList
            return api_client.models.generate_content(
                model=DEFAULT_MODEL_NAME,
                contents=[prompt, uploaded_file],
                config=config,
            )

        # Send request to API with uploaded file (timeout guard)
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        future = executor.submit(_call_model)
        try:
            response = future.result(timeout=timeout_seconds)
        except concurrent.futures.TimeoutError as e:
            future.cancel()
            executor.shutdown(wait=False, cancel_futures=True)
            raise TimeoutError(f"Gemini request timed out after {timeout_seconds}s") from e
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

        # Track token usage against internal token/min throttling.
        token_count = _extract_total_tokens(response)
        if token_count:
            _wait_rate_limit("gemini_tokens", units=token_count)

        response_text = response.text
        print(f"[DEBUG] Raw API response content: {response_text[:200]}...", flush=True)
        
        try:
            parsed_data = json.loads(response_text)
            if isinstance(parsed_data, dict):
                source_metadata = _normalize_source_metadata(parsed_data.get("source_metadata"))
                bibliography_data = parsed_data.get("items") or []
            else:
                bibliography_data = parsed_data
            if isinstance(bibliography_data, list):
                print(f"[DEBUG] Parsed JSON successfully as list.", flush=True)
            else:
                print(f"[WARNING] API response is not a list, attempting extraction.", flush=True)
                extracted_data = extract_json_from_text(response_text)
                if extracted_data is not None:
                    bibliography_data = extracted_data
                else:
                    print(f"[WARNING] Extraction failed.", flush=True)
                    bibliography_data = []
        except json.JSONDecodeError as jde:
            print(f"[ERROR] JSON decode error: {jde}", flush=True)
            print(f"[ERROR] Response content: {response_text[:500]}...", flush=True)
            extracted_data = extract_json_from_text(response_text)
            if extracted_data is not None:
                bibliography_data = extracted_data
            else:
                bibliography_data = []

        if isinstance(bibliography_data, list):
            bibliography_data = validate_bibliography_entries(bibliography_data)
    except Exception as e:
        print(f"[ERROR] API error while processing {pdf_path}: {e}", flush=True)
        bibliography_data = []
        source_metadata = None
    finally:
        if temp_upload_dir and os.path.exists(temp_upload_dir):
            shutil.rmtree(temp_upload_dir, ignore_errors=True)
        # Attempt to delete the uploaded file
        if uploaded_file and not uploaded_file_name:
            try:
                api_client.files.delete(name=uploaded_file.name)
                print(f"[DEBUG] Deleted uploaded file: {uploaded_file.name}", flush=True)
            except Exception as delete_error:
                print(f"[WARNING] Failed to delete uploaded file {uploaded_file.name if uploaded_file else 'unknown'}: {delete_error}", flush=True)
                print(f"[INFO] Continuing with processing despite deletion failure.", flush=True)
        end_time = time.time()
        print(f"[TIMER] Bibliography extraction for {pdf_path} took {end_time - start_time:.2f} seconds.", flush=True)
        return {
            "items": bibliography_data if isinstance(bibliography_data, list) else [],
            "source_metadata": source_metadata,
        }

def extract_json_from_text(text):
    """Attempts to extract valid JSON array from text, even if surrounded by non-JSON content."""
    text = text.strip()
    start_idx = text.find('[')
    end_idx = text.rfind(']')
    if start_idx >= 0 and end_idx > start_idx:
        potential_json = text[start_idx:end_idx+1]
        try:
            data = json.loads(potential_json)
            if isinstance(data, list):
                print(f"[DEBUG] Extracted valid JSON array from response.", flush=True)
                return data
        except json.JSONDecodeError:
            pass
    return None

def validate_bibliography_entries(entries):
    """Validate/normalize model output against the bibliography schema."""
    if os.getenv("DISABLE_BIBLIO_PYDANTIC", "").lower() in {"1", "true", "yes"}:
        print("[INFO] Pydantic validation disabled via DISABLE_BIBLIO_PYDANTIC.", flush=True)
        return entries
    if not BaseModel:
        print("[WARNING] pydantic not available; skipping bibliography validation.", flush=True)
        return entries

    validated = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        try:
            if hasattr(BibliographyEntry, "model_validate"):
                model = BibliographyEntry.model_validate(entry)
                data = model.model_dump(exclude_none=True)
            else:
                model = BibliographyEntry.parse_obj(entry)
                data = model.dict(exclude_none=True)

            # Normalize numeric fields to strings for storage consistency
            for key in ("volume", "issue", "pages"):
                if key in data and data[key] is not None and not isinstance(data[key], str):
                    data[key] = str(data[key])
            if "year" in data and isinstance(data["year"], int):
                data["year"] = data["year"]

            validated.append(data)
        except ValidationError as e:
            print(f"[WARNING] Skipping invalid entry: {e}", flush=True)
            validated.append(entry)
    return validated

# --- Main Processing Logic ---

def process_single_pdf(
    pdf_path,
    db_manager: DatabaseManager,
    summary_dict,
    summary_lock,
    db_write_lock=None,
    ingest_source: str | None = None,
    corpus_id: int | None = None,
    extract_mode: str = "page",
    source_pdf_override: str | None = None,
    uploaded_file_name: str | None = None,
):
    """Processes a single PDF file: upload directly, call API, save result."""
    filename = os.path.basename(pdf_path)
    print(f"\n[INFO] Processing {filename}...", flush=True)
    file_success = False
    failure_reason = "Unknown"

    try:
        # Get Bibliography via API by uploading PDF directly
        stage_start_time = time.time()
        extraction_result = upload_pdf_and_extract_bibliography(
            pdf_path,
            extract_mode=extract_mode,
            uploaded_file_name=uploaded_file_name,
        )
        bibliography_data = extraction_result.get("items") if isinstance(extraction_result, dict) else extraction_result
        source_metadata = extraction_result.get("source_metadata") if isinstance(extraction_result, dict) else None
        source_pdf_value = str(source_pdf_override or pdf_path)
        api_duration = time.time() - stage_start_time
        print(f"[TIMER] Bibliography extraction for {filename} took {api_duration:.2f} seconds.", flush=True)

        write_guard = db_write_lock if db_write_lock is not None else nullcontext()
        with write_guard:
            _upsert_seed_document_metadata(
                db_manager=db_manager,
                ingest_source=ingest_source,
                corpus_id=corpus_id,
                source_pdf=source_pdf_value,
                source_metadata=source_metadata,
            )

            if extract_mode == "source_meta":
                file_success = bool(source_metadata)
                if not file_success:
                    failure_reason = "Source metadata not found by API"
            elif bibliography_data is None:
                failure_reason = "API call failed or returned None"
                raise ValueError(failure_reason)
            elif not bibliography_data:
                print(f"[WARNING] No bibliography found by API for {filename}", flush=True)
                # Consider this a success in terms of processing, but no data found
                failure_reason = "No bibliography content found by API" # Not strictly a failure, but no output
                file_success = True # Treat as success because process completed
                # Don't save an empty file, just report no data found
            else:
                # Record entries for UI display and insert into no_metadata stage
                successes = 0
                for entry in bibliography_data:
                    minimal_ref = {
                        "title": entry.get("title"),
                        "authors": entry.get("authors"),
                        "editors": entry.get("editors"),
                        "year": entry.get("year"),
                        "doi": entry.get("doi"),
                        "source": entry.get("source"),  # journal/conference/book title
                        "volume": entry.get("volume"),
                        "issue": entry.get("issue"),
                        "pages": entry.get("pages"),
                        "publisher": entry.get("publisher"),
                        "type": entry.get("type"),
                        "url": entry.get("url"),
                        "isbn": entry.get("isbn"),
                        "issn": entry.get("issn"),
                        "abstract": entry.get("abstract"),
                        "keywords": entry.get("keywords"),
                        "source_pdf": source_pdf_value,
                        "translated_title": entry.get("translated_title"),
                        "translated_year": entry.get("translated_year"),
                        "translated_language": entry.get("translated_language"),
                        "translation_relationship": entry.get("translation_relationship"),
                        "ingest_source": ingest_source,
                        "corpus_id": corpus_id,
                    }
                    db_manager.insert_ingest_entry(minimal_ref, ingest_source=ingest_source)
                    row_id, err = db_manager.insert_no_metadata(minimal_ref)
                    if err:
                        print(f"[WARNING] Skipped entry due to: {err}", flush=True)
                    else:
                        successes += 1
                print(f"[SUCCESS] Inserted {successes}/{len(bibliography_data)} entries from {filename} into database.", flush=True)
                file_success = True if successes else False
                if not file_success:
                    failure_reason = "All inserts skipped (duplicates or errors)"


    except Exception as e:
        # Catch errors during API call steps
        if failure_reason == "Unknown": # If not already set
            failure_reason = f"Processing error: {e}"
        print(f"[ERROR] Failed processing {filename}: {failure_reason}", flush=True)
        file_success = False

    # Update shared summary dictionary safely
    with summary_lock:
        summary_dict["processed_files"] += 1
        if file_success:
            summary_dict["successful_files"] += 1
        else:
            summary_dict["failed_files"] += 1
            summary_dict["failures"].append({"file": filename, "reason": failure_reason})


def process_single_pdf_chunked(
    pdf_path,
    db_manager: DatabaseManager,
    summary_dict,
    summary_lock,
    db_write_lock=None,
    ingest_source: str | None = None,
    corpus_id: int | None = None,
    extract_mode: str = "inline",
    chunk_pages: int = 50,
    uploaded_file_name: str | None = None,
):
    """Fallback extraction for inline/per-page citations by chunking larger PDFs."""
    temp_chunk_dir = None
    try:
        temp_chunk_dir, chunk_paths = _split_pdf_into_chunks(str(pdf_path), int(chunk_pages))
    except Exception as exc:
        print(f"[ERROR] Failed to create PDF chunks for {pdf_path}: {exc}", flush=True)
        with summary_lock:
            summary_dict["processed_files"] += 1
            summary_dict["failed_files"] += 1
            summary_dict["failures"].append({"file": os.path.basename(str(pdf_path)), "reason": str(exc)})
        return

    try:
        print(
            f"[INFO] Chunked fallback extraction for {pdf_path}: {len(chunk_paths)} chunk(s), mode={extract_mode}, chunk_pages={chunk_pages}",
            flush=True,
        )
        for chunk_path in chunk_paths:
            process_single_pdf(
                chunk_path,
                db_manager,
                summary_dict,
                summary_lock,
                db_write_lock,
                ingest_source=ingest_source,
                corpus_id=corpus_id,
                extract_mode=extract_mode,
                source_pdf_override=str(pdf_path),
                uploaded_file_name=None if chunk_path != str(pdf_path) else uploaded_file_name,
            )
    finally:
        if temp_chunk_dir and os.path.exists(temp_chunk_dir):
            shutil.rmtree(temp_chunk_dir, ignore_errors=True)

def process_directory_v2(
    input_dir,
    db_manager: DatabaseManager,
    max_workers=DEFAULT_EXTRACT_WORKERS,
    ingest_source: str | None = None,
    corpus_id: int | None = None,
    extract_mode: str = "page",
    chunk_pages: int = 0,
    page_batch_size: int = DEFAULT_PAGE_BATCH_SIZE,
):
    """Processes all PDFs in the input directory concurrently, saving to the specified output dir."""
    print(f"[INFO] Processing directory: {input_dir}", flush=True)


    # Find all PDFs recursively using glob
    pdf_files = sorted(glob.glob(os.path.join(input_dir, "**/*.pdf"), recursive=True))
    print(f"[INFO] Found {len(pdf_files)} PDF files in {input_dir} and its subdirectories.", flush=True)
    if not pdf_files:
        print("[WARNING] No PDF files found to process. Exiting.", flush=True)
        return

    print(f"[INFO] Starting concurrent processing with up to {max_workers} workers.", flush=True)

    # Initialize summary for tracking results
    results_summary = {
        'processed_files': 0,
        'successful_files': 0,
        'failed_files': 0,
        'failures': []
    }
    summary_lock = Lock()
    db_write_lock = Lock()
    temp_page_batch_dir = None
    work_items: list[tuple[str, str]] = [(path, str(path)) for path in pdf_files]

    if chunk_pages and chunk_pages > 0:
        print(f"[INFO] Chunked mode enabled for directory processing (chunk_pages={chunk_pages}).", flush=True)
        for pdf_path in pdf_files:
            process_single_pdf_chunked(
                pdf_path,
                db_manager,
                results_summary,
                summary_lock,
                db_write_lock,
                ingest_source=ingest_source,
                corpus_id=corpus_id,
                extract_mode=extract_mode,
                chunk_pages=chunk_pages,
            )
        print("\n--- Processing Summary ---")
        print(f"Total files found: {len(pdf_files)}")
        print(f"Files processed: {results_summary['processed_files']}")
        print(f"Successful extractions: {results_summary['successful_files']}")
        print(f"Failed extractions: {results_summary['failed_files']}")
        if results_summary['failures']:
            print("Failures:")
            for failure in results_summary['failures']:
                print(f"  - {failure['file']}: {failure['reason']}")
        print("-------------------------")
        return

    if extract_mode == "page" and page_batch_size > 1:
        work_items, temp_page_batch_dir = _build_page_batches(pdf_files, page_batch_size)
        if temp_page_batch_dir:
            print(
                f"[INFO] Page batching enabled: {len(pdf_files)} page PDFs grouped into {len(work_items)} API calls (batch_size={page_batch_size}).",
                flush=True,
            )

    try:
        # Use ThreadPoolExecutor for I/O-bound tasks
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit tasks for each PDF
            futures = [
                executor.submit(
                    process_single_pdf,
                    work_path,
                    db_manager,
                    results_summary,
                    summary_lock,
                    db_write_lock,
                    ingest_source,
                    corpus_id,
                    extract_mode,
                    source_pdf_override,
                )
                for work_path, source_pdf_override in work_items]

            # Wait for all tasks to complete and handle potential exceptions
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()  # Retrieve result or re-raise exception from the thread
                except Exception as exc:
                    print(f'[ERROR] An error occurred in a worker thread: {exc}', flush=True)
                    # Error is already logged within process_single_pdf and summary updated
    except KeyboardInterrupt:
        print("[INFO] Received interrupt (Ctrl+C). Shutting down gracefully...", flush=True)
        executor.shutdown(wait=False, cancel_futures=True)  # Properly shutdown the executor and cancel pending tasks
        print("[INFO] All tasks stopped. Exiting.", flush=True)
        # Print partial summary before exiting
        print("\n--- Partial Processing Summary ---")
        print(f"Total files found: {len(work_items)}")
        print(f"Files processed: {results_summary['processed_files']}")
        print(f"Successful extractions: {results_summary['successful_files']}")
        print(f"Failed extractions: {results_summary['failed_files']}")
        if results_summary['failures']:
            print("Failures:")
            for failure in results_summary['failures']:
                print(f"  - {failure['file']}: {failure['reason']}")
        print("-------------------------")
        return
    finally:
        if temp_page_batch_dir and os.path.exists(temp_page_batch_dir):
            shutil.rmtree(temp_page_batch_dir, ignore_errors=True)

    # Print summary
    print("\n--- Processing Summary ---")
    print(f"Total files found: {len(work_items)}")
    print(f"Files processed: {results_summary['processed_files']}")
    print(f"Successful extractions: {results_summary['successful_files']}")
    print(f"Failed extractions: {results_summary['failed_files']}")
    if results_summary['failures']:
        print("Failures:")
        for failure in results_summary['failures']:
            print(f"  - {failure['file']}: {failure['reason']}")
    print("-------------------------")

def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="V2: Extract bibliographies from PDFs using Gemini.")
    parser.add_argument("input", nargs="?", help="Path to a PDF file or a directory containing PDFs.")
    parser.add_argument("--input-pdf", dest="input_pdf", help="Path to a single input PDF file.")
    parser.add_argument("--input-dir", dest="input_dir", help="Path to a directory containing input PDF files.")
    parser.add_argument("--workers", type=int, default=DEFAULT_EXTRACT_WORKERS, help="Maximum number of concurrent worker threads.")
    parser.add_argument("--db-path", required=True, help="Path to the SQLite database file.")
    parser.add_argument("--ingest-source", default=None, help="Identifier for this ingest run (e.g., base filename).")
    parser.add_argument("--corpus-id", type=int, default=None, help="Corpus ID to associate with inserted entries.")
    parser.add_argument(
        "--extract-mode",
        choices=("page", "inline", "source_meta"),
        default="page",
        help="Extraction strategy: page=reference page PDFs, inline=all citations in full/chunked PDF, source_meta=seed document metadata only.",
    )
    parser.add_argument(
        "--chunk-pages",
        type=int,
        default=0,
        help="If > 0, split each PDF into chunks with this many pages before extraction.",
    )
    parser.add_argument(
        "--page-batch-size",
        type=int,
        default=DEFAULT_PAGE_BATCH_SIZE,
        help="For page-mode directory runs: merge up to N consecutive extracted reference pages per API call.",
    )
    parser.add_argument(
        "--uploaded-file-name",
        default=None,
        help="Reuse an already uploaded Gemini file (files/<id>) for single-PDF extraction.",
    )
    return parser


def _resolve_input_path(args: argparse.Namespace) -> str | None:
    return args.input_pdf or args.input_dir or args.input


def main(argv: list[str] | None = None) -> int:
    start_total_time = time.time()

    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    if not configure_api_client():
        print("[FATAL] Exiting due to API configuration failure.", flush=True)
        return 1

    input_path = _resolve_input_path(args)
    if not input_path:
        print("[FATAL] No input provided. Use --input-dir, --input-pdf, or positional input.", flush=True)
        return 2

    db_path_resolved = Path(args.db_path).expanduser().resolve()
    print(f"[INFO] Using database at: {db_path_resolved}", flush=True)

    db_manager = None
    try:
        db_manager = DatabaseManager(db_path=db_path_resolved)
        if os.path.isdir(input_path):
            print(f"[INFO] Processing directory: {input_path}", flush=True)
            process_directory_v2(
                input_path,
                db_manager,
                args.workers,
                args.ingest_source,
                args.corpus_id,
                args.extract_mode,
                args.chunk_pages,
                args.page_batch_size,
            )
        elif os.path.isfile(input_path) and str(input_path).lower().endswith(".pdf"):
            print(f"[INFO] Processing single PDF file: {input_path}", flush=True)
            summary = {"processed_files": 0, "successful_files": 0, "failed_files": 0, "failures": []}
            if args.chunk_pages and args.chunk_pages > 0:
                process_single_pdf_chunked(
                    input_path,
                    db_manager,
                    summary,
                    Lock(),
                    Lock(),
                    args.ingest_source,
                    args.corpus_id,
                    args.extract_mode,
                    args.chunk_pages,
                    args.uploaded_file_name,
                )
            else:
                process_single_pdf(
                    input_path,
                    db_manager,
                    summary,
                    Lock(),
                    Lock(),
                    args.ingest_source,
                    args.corpus_id,
                    args.extract_mode,
                    None,
                    args.uploaded_file_name,
                )
            print(f"[INFO] Single file processing complete. Summary: {summary}", flush=True)
        else:
            print(f"[ERROR] Input {input_path} is neither a directory nor a PDF file.", flush=True)
            return 2
    finally:
        if db_manager:
            db_manager.close_connection()

    print(f"\n[INFO] Total processing time: {time.time() - start_total_time:.2f} seconds.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
