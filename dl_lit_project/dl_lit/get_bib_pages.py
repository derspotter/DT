"""
Gemini-assisted extraction of bibliography / reference-section pages from PDFs.

Used in two ways:
1. As a library function via the package CLI (`python -m dl_lit.cli extract-bib-pages ...`).
2. As a script invoked by the web backend (`python get_bib_pages.py --input-pdf ... --output-dir ...`).

Output layout (important for the backend):
  <output_dir>/<pdf_base_name>/<pdf_base_name>_refs_physical_p{N}.pdf

Where N is the 1-based *physical* page index in the original PDF.
"""

from __future__ import annotations

import argparse
import concurrent.futures
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

from dotenv import load_dotenv

try:
    import pikepdf
except Exception as exc:  # pragma: no cover
    raise RuntimeError("pikepdf is required for PDF page extraction") from exc

try:
    from google import genai
    from google.genai import types
except Exception as exc:  # pragma: no cover
    raise RuntimeError("google-genai is required (pip install google-genai)") from exc


# Make `dl_lit.*` imports work when this file is executed directly.
DL_LIT_PROJECT_DIR = Path(__file__).resolve().parent.parent
if str(DL_LIT_PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(DL_LIT_PROJECT_DIR))

try:
    from dl_lit.utils import get_global_rate_limiter
except Exception:  # pragma: no cover
    get_global_rate_limiter = None


MODEL_NAME = os.getenv("RAG_FEEDER_GEMINI_MODEL", "gemini-3-flash-preview")
MAX_PAGES_FOR_API = 50
CHUNK_SIZE = 50


def _env_int(name: str, default: int, minimum: int = 1) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return max(minimum, int(default))
    try:
        return max(minimum, int(raw))
    except Exception:
        return max(minimum, int(default))


GET_BIB_CHUNK_WORKERS = _env_int("RAG_FEEDER_GET_BIB_CHUNK_WORKERS", 6)


load_dotenv()
api_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
api_client = None
if api_key:
    try:
        api_client = genai.Client(api_key=api_key)
    except Exception:
        api_client = None


SECTION_JSON_RE = re.compile(r"```json\s*([\s\S]*?)\s*```", re.IGNORECASE)
SOURCE_METADATA_FILENAME = "source_metadata.json"


def _clean_json_response(text: str) -> str:
    text = (text or "").strip()
    if not text:
        return ""
    match = SECTION_JSON_RE.search(text)
    if match:
        return match.group(1).strip()
    if text.startswith("```json"):
        text = text[7:]
    elif text.startswith("```"):
        text = text[3:]
    if text.endswith("```"):
        text = text[:-3]
    return text.strip()


def _normalize_sections(sections: list[dict], total_pages: int) -> list[dict]:
    normalized: list[dict] = []
    for sec in sections or []:
        try:
            start_page = int(sec.get("start_page"))
            end_page = int(sec.get("end_page"))
        except Exception:
            continue
        if start_page <= 0 or end_page <= 0:
            continue
        if start_page > end_page:
            continue
        if start_page > total_pages:
            continue
        if end_page > total_pages:
            end_page = total_pages
        normalized.append({"start_page": start_page, "end_page": end_page})
    return normalized


def _normalize_source_metadata(value: dict | None) -> dict | None:
    if not isinstance(value, dict):
        return None

    normalized: dict = {}
    title = value.get("title")
    if isinstance(title, str) and title.strip():
        normalized["title"] = title.strip()

    authors = value.get("authors")
    if isinstance(authors, list):
        cleaned = [str(a).strip() for a in authors if str(a).strip()]
        if cleaned:
            normalized["authors"] = cleaned

    year = value.get("year")
    if isinstance(year, (int, float)):
        normalized["year"] = int(year)
    elif isinstance(year, str):
        y = year.strip()
        if y:
            normalized["year"] = y

    doi = value.get("doi")
    if isinstance(doi, str) and doi.strip():
        normalized["doi"] = doi.strip()

    source = value.get("source")
    if isinstance(source, str) and source.strip():
        normalized["source"] = source.strip()

    publisher = value.get("publisher")
    if isinstance(publisher, str) and publisher.strip():
        normalized["publisher"] = publisher.strip()

    return normalized or None


def _parse_section_and_source_payload(json_text: str) -> tuple[list[dict], dict | None]:
    if not json_text:
        return [], None
    try:
        parsed = json.loads(json_text)
    except Exception:
        return [], None

    # Backward compatibility: plain array response.
    if isinstance(parsed, list):
        return parsed, None

    if not isinstance(parsed, dict):
        return [], None

    sections = parsed.get("reference_sections")
    if not isinstance(sections, list):
        sections = parsed.get("sections")
    if not isinstance(sections, list):
        sections = []

    source_metadata = _normalize_source_metadata(parsed.get("source_metadata"))
    return sections, source_metadata


def _write_source_metadata_file(output_dir: str, source_metadata: dict | None) -> None:
    payload = {"source_metadata": source_metadata}
    out_path = os.path.join(output_dir, SOURCE_METADATA_FILENAME)
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)


def _rate_limiter_wait(kind: str, units: int = 1) -> None:
    if not get_global_rate_limiter:
        return
    rl = get_global_rate_limiter()
    try:
        rl.wait_if_needed(kind, units=units)
    except Exception:
        # Rate limiter is best-effort; do not fail extraction because of it.
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


def _find_reference_section_pages(
    uploaded_pdf,
    total_physical_pages: int,
    timeout_seconds: int = 180,
    include_source_metadata: bool = True,
) -> tuple[list[dict], dict | None]:
    if api_client is None:
        raise RuntimeError("Gemini client not configured (set GEMINI_API_KEY or GOOGLE_API_KEY).")

    prompt = f"""
Identify ALL bibliography/reference sections in this PDF and extract source document metadata.

Use 1-based PHYSICAL page indices (first PDF page = 1) and ignore any printed page numbers.
Return ONLY valid JSON in this exact shape:
{{
  "reference_sections": [{{"start_page": 12, "end_page": 15}}],
  "source_metadata": {{
    "title": "...",
    "authors": ["..."],
    "year": 2021,
    "doi": "...",
    "source": "...",
    "publisher": "..."
  }}
}}

Rules:
- start_page = page where the references section begins (title or first entry). Be inclusive.
- end_page = page where the last reference entry ends (do not include unrelated pages after).
- Include every distinct bibliography/reference section, especially after chapters.
- For source_metadata, infer from the attached PDF. If unavailable, use null per field.

Document has {total_physical_pages} physical pages.
""".strip()
    if not include_source_metadata:
        prompt = f"""
Identify ALL bibliography/reference sections in this PDF.

Use 1-based PHYSICAL page indices (first PDF page = 1) and ignore any printed page numbers.
Return ONLY valid JSON in this exact shape:
{{
  "reference_sections": [{{"start_page": 12, "end_page": 15}}]
}}

Rules:
- start_page = page where the references section begins (title or first entry). Be inclusive.
- end_page = page where the last reference entry ends (do not include unrelated pages after).
- Include every distinct bibliography/reference section, especially after chapters.

Document has {total_physical_pages} physical pages.
""".strip()

    _rate_limiter_wait("gemini")
    _rate_limiter_wait("gemini_daily")

    def _call():
        return api_client.models.generate_content(
            model=MODEL_NAME,
            contents=[prompt, uploaded_pdf],
            config=types.GenerateContentConfig(
                temperature=1.0,
                response_mime_type="application/json",
            ),
        )

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(_call)
        try:
            resp = future.result(timeout=timeout_seconds)
        except concurrent.futures.TimeoutError as exc:
            future.cancel()
            raise TimeoutError(f"Gemini request timed out after {timeout_seconds}s") from exc

    token_count = _extract_total_tokens(resp)
    if token_count:
        _rate_limiter_wait("gemini_tokens", units=token_count)

    json_text = _clean_json_response(getattr(resp, "text", "") or "")
    return _parse_section_and_source_payload(json_text)


def _upload_pdf(path: str):
    if api_client is None:
        raise RuntimeError("Gemini client not configured (set GEMINI_API_KEY or GOOGLE_API_KEY).")
    source_path = str(path)
    display_name = Path(source_path).name
    temp_upload_dir = None
    try:
        source_path.encode("ascii")
        display_name.encode("ascii")
        upload_path = source_path
    except UnicodeEncodeError:
        normalized = unicodedata.normalize("NFKD", display_name)
        ascii_only = normalized.encode("ascii", "ignore").decode("ascii")
        safe_name = "".join(ch if (ch.isalnum() or ch in "._-") else "_" for ch in ascii_only).strip("._")
        if not safe_name:
            safe_name = "upload"
        if not safe_name.lower().endswith(".pdf"):
            safe_name += ".pdf"
        temp_upload_dir = tempfile.mkdtemp(prefix="gemini_upload_")
        upload_path = os.path.join(temp_upload_dir, safe_name)
        shutil.copy2(source_path, upload_path)
        display_name = safe_name

    try:
        return api_client.files.upload(
            file=upload_path,
            config={"mime_type": "application/pdf", "display_name": display_name},
        )
    finally:
        if temp_upload_dir and os.path.exists(temp_upload_dir):
            shutil.rmtree(temp_upload_dir, ignore_errors=True)


def _get_uploaded(name: str):
    if api_client is None:
        raise RuntimeError("Gemini client not configured (set GEMINI_API_KEY or GOOGLE_API_KEY).")
    return api_client.files.get(name=name)


def _delete_uploaded(uploaded) -> None:
    if api_client is None or not uploaded:
        return
    try:
        api_client.files.delete(name=uploaded.name)
    except Exception:
        return


def _extract_chunk_sections(
    chunk_idx: int,
    total_chunks: int,
    start: int,
    end: int,
    chunk_path: str,
    include_source_metadata: bool,
):
    uploaded = _upload_pdf(chunk_path)
    try:
        sections, chunk_source_metadata = _find_reference_section_pages(
            uploaded,
            total_physical_pages=end - start,
            include_source_metadata=include_source_metadata,
        )
    finally:
        _delete_uploaded(uploaded)

    print(
        f"[INFO] Chunk {chunk_idx}/{total_chunks} returned {len(sections)} section range(s).",
        flush=True,
    )
    return {
        "chunk_idx": chunk_idx,
        "start": start,
        "end": end,
        "sections": sections,
        "source_metadata": chunk_source_metadata if include_source_metadata else None,
    }


def extract_reference_sections(pdf_path: str, output_dir: str, uploaded_file_name: str | None = None) -> list[str]:
    """Extract bibliography/reference pages from a PDF into per-page PDF files.

    Returns a list of generated file paths (best-effort; may be empty).
    """
    if api_client is None:
        raise RuntimeError("Gemini client not configured (set GEMINI_API_KEY or GOOGLE_API_KEY).")

    output_dir_abs = os.path.abspath(os.path.expanduser(output_dir))
    base_name = os.path.splitext(os.path.basename(pdf_path))[0]
    if os.path.basename(output_dir_abs) == base_name:
        specific_output_dir = output_dir_abs
    else:
        specific_output_dir = os.path.join(output_dir_abs, base_name)
    os.makedirs(specific_output_dir, exist_ok=True)

    password_error = getattr(pikepdf, "PasswordError", Exception)
    if not isinstance(password_error, type) or not issubclass(password_error, BaseException):
        password_error = Exception

    files_to_clean_up = []
    temp_chunk_dir = None

    try:
        with pikepdf.Pdf.open(pdf_path) as pdf_doc:
            total_physical_pages = len(pdf_doc.pages)

        all_sections: list[dict] = []
        source_metadata: dict | None = None

        if total_physical_pages > MAX_PAGES_FOR_API:
            total_chunks = (total_physical_pages + CHUNK_SIZE - 1) // CHUNK_SIZE
            print(
                f"[INFO] Large PDF mode: {total_physical_pages} pages split into {total_chunks} chunk(s) of up to {CHUNK_SIZE}.",
                flush=True,
            )
            temp_chunk_dir = tempfile.mkdtemp(prefix="bib_chunks_")
            chunk_specs: list[tuple[int, int, int, str]] = []
            with pikepdf.Pdf.open(pdf_path) as src_pdf:
                for chunk_idx, start in enumerate(range(0, total_physical_pages, CHUNK_SIZE), start=1):
                    end = min(start + CHUNK_SIZE, total_physical_pages)
                    chunk_path = os.path.join(temp_chunk_dir, f"{base_name}_chunk_{start+1}-{end}.pdf")
                    chunk_pdf = pikepdf.Pdf.new()
                    for idx in range(start, end):
                        chunk_pdf.pages.append(src_pdf.pages[idx])
                    chunk_pdf.save(chunk_path)
                    chunk_specs.append((chunk_idx, start, end, chunk_path))

            first_chunk_idx, first_start, first_end, first_chunk_path = chunk_specs[0]
            print(
                f"[INFO] Analyzing chunk {first_chunk_idx}/{total_chunks}: pages {first_start + 1}-{first_end} (source metadata enabled).",
                flush=True,
            )
            first_result = _extract_chunk_sections(
                chunk_idx=first_chunk_idx,
                total_chunks=total_chunks,
                start=first_start,
                end=first_end,
                chunk_path=first_chunk_path,
                include_source_metadata=True,
            )
            source_metadata = first_result.get("source_metadata")
            if source_metadata:
                print("[INFO] Source metadata extracted from first chunk (pages 1-50).", flush=True)
            else:
                print("[INFO] No source metadata inferred from first chunk (pages 1-50).", flush=True)

            for sec in first_result.get("sections") or []:
                try:
                    sec_start = int(sec.get("start_page"))
                    sec_end = int(sec.get("end_page"))
                except Exception:
                    continue
                all_sections.append(
                    {
                        "start_page": sec_start + first_start,
                        "end_page": sec_end + first_start,
                    }
                )

            remaining_specs = chunk_specs[1:]
            if remaining_specs:
                remaining_workers = min(GET_BIB_CHUNK_WORKERS, len(remaining_specs))
                print(
                    f"[INFO] Analyzing remaining {len(remaining_specs)} chunk(s) in parallel (workers={remaining_workers}).",
                    flush=True,
                )
                with concurrent.futures.ThreadPoolExecutor(max_workers=remaining_workers) as executor:
                    future_map = {}
                    for chunk_idx, start, end, chunk_path in remaining_specs:
                        print(
                            f"[INFO] Queued chunk {chunk_idx}/{total_chunks}: pages {start + 1}-{end}.",
                            flush=True,
                        )
                        future = executor.submit(
                            _extract_chunk_sections,
                            chunk_idx,
                            total_chunks,
                            start,
                            end,
                            chunk_path,
                            False,
                        )
                        future_map[future] = (chunk_idx, start, end)

                    for future in concurrent.futures.as_completed(future_map):
                        chunk_idx, start, _ = future_map[future]
                        result = future.result()
                        for sec in result.get("sections") or []:
                            try:
                                sec_start = int(sec.get("start_page"))
                                sec_end = int(sec.get("end_page"))
                            except Exception:
                                continue
                            all_sections.append(
                                {
                                    "start_page": sec_start + start,
                                    "end_page": sec_end + start,
                                }
                            )
            else:
                print("[INFO] No additional chunks after first chunk.", flush=True)
        else:
            print(
                f"[INFO] Single-call mode: analyzing full PDF ({total_physical_pages} pages) for sections + source metadata.",
                flush=True,
            )
            if uploaded_file_name:
                uploaded = _get_uploaded(uploaded_file_name)
                print(f"[INFO] Reusing uploaded Gemini file: {uploaded_file_name}", flush=True)
            else:
                uploaded = _upload_pdf(pdf_path)
                files_to_clean_up.append(uploaded)
            # Only auto-clean uploads we created in this process.
            # Reused uploads are cleaned up by the caller/orchestrator.
            all_sections, source_metadata = _find_reference_section_pages(uploaded, total_physical_pages=total_physical_pages)
            print(f"[INFO] Section detection returned {len(all_sections)} section range(s).", flush=True)
            if source_metadata:
                print("[INFO] Source metadata extracted from section-detection call.", flush=True)
            else:
                print("[INFO] No source metadata inferred from section-detection call.", flush=True)

        # Persist seed/source metadata from the same Gemini call as section detection.
        _write_source_metadata_file(specific_output_dir, source_metadata)

        normalized_sections = _normalize_sections(all_sections, total_physical_pages)
        if not normalized_sections:
            print("[INFO] No valid reference sections after normalization.", flush=True)
            return []

        generated_files: list[str] = []
        seen_pages: set[int] = set()

        with pikepdf.Pdf.open(pdf_path) as pdf_doc:
            for section in normalized_sections:
                for page_1_based in range(section["start_page"], section["end_page"] + 1):
                    if page_1_based in seen_pages:
                        continue
                    seen_pages.add(page_1_based)
                    page_idx = page_1_based - 1
                    if page_idx < 0 or page_idx >= len(pdf_doc.pages):
                        continue

                    out_name = f"{base_name}_refs_physical_p{page_1_based}.pdf"
                    out_path = os.path.join(specific_output_dir, out_name)
                    new_pdf = pikepdf.Pdf.new()
                    new_pdf.pages.append(pdf_doc.pages[page_idx])
                    new_pdf.save(out_path)
                    generated_files.append(out_path)

        return generated_files
    except (FileNotFoundError, password_error) as exc:
        raise RuntimeError(f"Failed to open PDF: {exc}") from exc
    finally:
        for uploaded in files_to_clean_up:
            _delete_uploaded(uploaded)
        if temp_chunk_dir and os.path.exists(temp_chunk_dir):
            shutil.rmtree(temp_chunk_dir, ignore_errors=True)


def _process_pdf(
    pdf_path: str,
    output_dir: str,
    move_to_done: bool = False,
    uploaded_file_name: str | None = None,
) -> list[str]:
    files = extract_reference_sections(pdf_path, output_dir, uploaded_file_name=uploaded_file_name)
    if move_to_done and files:
        done_bibs_dir = os.path.join(os.path.abspath(os.path.expanduser(output_dir)), "done_bibs")
        os.makedirs(done_bibs_dir, exist_ok=True)
        try:
            shutil.move(pdf_path, os.path.join(done_bibs_dir, os.path.basename(pdf_path)))
        except Exception:
            pass
    return files


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Extract bibliography pages from PDFs.")
    p.add_argument("input", nargs="?", help="Path to a PDF file or a directory containing PDFs")
    p.add_argument("--input-pdf", dest="input_pdf", help="Path to a single PDF file")
    p.add_argument("--input-dir", dest="input_dir", help="Path to a directory containing PDFs")
    p.add_argument(
        "--uploaded-file-name",
        dest="uploaded_file_name",
        help="Reuse an already uploaded Gemini file (files/<id>) for single-PDF extraction.",
    )
    p.add_argument("--output-dir", default="bib_output", help="Base output directory for extracted pages")
    p.add_argument(
        "--move-to-done",
        action="store_true",
        help="Move processed PDFs into OUTPUT_DIR/done_bibs/ after extraction.",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    if api_client is None:
        print("Error: GEMINI_API_KEY (or GOOGLE_API_KEY) environment variable not set or invalid.", flush=True)
        return 2

    output_dir = os.path.abspath(os.path.expanduser(args.output_dir))
    os.makedirs(output_dir, exist_ok=True)

    input_source = args.input_pdf or args.input_dir or args.input
    if not input_source:
        print("Error: No input provided. Use --input-pdf, --input-dir, or the positional input.", flush=True)
        return 2

    input_path = os.path.abspath(os.path.expanduser(input_source))
    if os.path.isdir(input_path):
        pdf_files = sorted(set(glob.glob(os.path.join(input_path, "*.pdf"))))
    else:
        pdf_files = [input_path] if input_path.lower().endswith(".pdf") else []

    if not pdf_files:
        print(f"Error: No PDF files found at {input_path}", flush=True)
        return 2

    print(f"Extracting bibliography pages for {len(pdf_files)} PDF(s)...", flush=True)
    start_time = time.time()
    total_pages = 0

    for idx, pdf in enumerate(pdf_files, 1):
        try:
            files = _process_pdf(
                pdf,
                output_dir,
                move_to_done=bool(args.move_to_done),
                uploaded_file_name=args.uploaded_file_name if len(pdf_files) == 1 else None,
            )
            total_pages += len(files)
            print(
                f"Completed {idx}/{len(pdf_files)}: {os.path.basename(pdf)} - {len(files)} page(s) extracted.",
                flush=True,
            )
        except Exception as exc:
            print(f"Error processing {pdf}: {exc}", flush=True)

    elapsed = time.time() - start_time
    print(f"Done. Extracted {total_pages} page(s) in {elapsed:.2f}s.", flush=True)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
