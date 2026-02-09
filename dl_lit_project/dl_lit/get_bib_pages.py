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


load_dotenv()
api_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
api_client = None
if api_key:
    try:
        api_client = genai.Client(api_key=api_key)
    except Exception:
        api_client = None


SECTION_JSON_RE = re.compile(r"```json\s*([\s\S]*?)\s*```", re.IGNORECASE)


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


def _rate_limiter_wait(kind: str) -> None:
    if not get_global_rate_limiter:
        return
    rl = get_global_rate_limiter()
    try:
        rl.wait_if_needed(kind)
    except Exception:
        # Rate limiter is best-effort; do not fail extraction because of it.
        return


def _find_reference_section_pages(uploaded_pdf, total_physical_pages: int, timeout_seconds: int = 180) -> list[dict]:
    if api_client is None:
        raise RuntimeError("Gemini client not configured (set GEMINI_API_KEY or GOOGLE_API_KEY).")

    prompt = f"""
Identify ALL bibliography/reference sections in this PDF.

Use 1-based PHYSICAL page indices (first PDF page = 1) and ignore any printed page numbers.
Return only a JSON array of objects with start_page and end_page.

Rules:
- start_page = page where the references section begins (title or first entry). Be inclusive.
- end_page = page where the last reference entry ends (do not include unrelated pages after).
- Include every distinct bibliography/reference section, especially after chapters.

Document has {total_physical_pages} physical pages.

Format:
[
  {{"start_page": 12, "end_page": 15}}
]
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

    json_text = _clean_json_response(getattr(resp, "text", "") or "")
    if not json_text:
        return []
    try:
        parsed = json.loads(json_text)
        return parsed if isinstance(parsed, list) else []
    except Exception:
        return []


def _upload_pdf(path: str):
    if api_client is None:
        raise RuntimeError("Gemini client not configured (set GEMINI_API_KEY or GOOGLE_API_KEY).")
    return api_client.files.upload(file=path)


def _delete_uploaded(uploaded) -> None:
    if api_client is None or not uploaded:
        return
    try:
        api_client.files.delete(name=uploaded.name)
    except Exception:
        return


def extract_reference_sections(pdf_path: str, output_dir: str) -> list[str]:
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

        if total_physical_pages > MAX_PAGES_FOR_API:
            temp_chunk_dir = tempfile.mkdtemp(prefix="bib_chunks_")
            for start in range(0, total_physical_pages, CHUNK_SIZE):
                end = min(start + CHUNK_SIZE, total_physical_pages)
                chunk_path = os.path.join(temp_chunk_dir, f"{base_name}_chunk_{start+1}-{end}.pdf")

                with pikepdf.Pdf.open(pdf_path) as src_pdf:
                    chunk_pdf = pikepdf.Pdf.new()
                    for idx in range(start, end):
                        chunk_pdf.pages.append(src_pdf.pages[idx])
                    chunk_pdf.save(chunk_path)

                uploaded = _upload_pdf(chunk_path)
                files_to_clean_up.append(uploaded)

                sections = _find_reference_section_pages(uploaded, total_physical_pages=end - start)
                for sec in sections:
                    try:
                        sec_start = int(sec.get("start_page"))
                        sec_end = int(sec.get("end_page"))
                    except Exception:
                        continue
                    # Shift chunk-local 1-based indices to doc-global 1-based indices.
                    all_sections.append(
                        {
                            "start_page": sec_start + start,
                            "end_page": sec_end + start,
                        }
                    )
        else:
            uploaded = _upload_pdf(pdf_path)
            files_to_clean_up.append(uploaded)
            all_sections = _find_reference_section_pages(uploaded, total_physical_pages=total_physical_pages)

        normalized_sections = _normalize_sections(all_sections, total_physical_pages)
        if not normalized_sections:
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


def _process_pdf(pdf_path: str, output_dir: str, move_to_done: bool = False) -> list[str]:
    files = extract_reference_sections(pdf_path, output_dir)
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
            files = _process_pdf(pdf, output_dir, move_to_done=bool(args.move_to_done))
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

