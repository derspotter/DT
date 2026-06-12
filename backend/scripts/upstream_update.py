#!/usr/bin/env python3
"""Draft and apply Kantropos upstream corpus updates from DT pending additions."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sqlite3
import sys
import unicodedata
from datetime import datetime
from pathlib import Path
from urllib.parse import quote


DEFAULT_TARGET_NAME = "Anthropozän & Nachhaltiges Management"
DEFAULT_CONTAINER_DB_PATH = "/usr/src/app/dl_lit_project/data/literature.db"
DEFAULT_OUTPUT_DIR = "/usr/src/app/dl_lit_project/data/upstream_update_drafts"
DEFAULT_CORPORA_ROOT = "/data/projects/kantropos/corpora"
DEFAULT_MIN_TEXT_CHARS = 500
DEFAULT_MIN_TEXT_PAGE_RATIO = 0.25
DEFAULT_OCR_SERVICE_URL = "http://127.0.0.1:8004"


def slugify(value: str) -> str:
    text = unicodedata.normalize("NFKD", value or "")
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^a-zA-Z0-9]+", "-", text.lower()).strip("-")
    return text


def timestamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def parse_kantropos_targets(corpora_root: Path) -> list[dict]:
    raw = os.environ.get("RAG_FEEDER_KANTROPOS_CORPORA", "").strip()
    values: list[object] = [DEFAULT_TARGET_NAME]
    if raw:
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list) and parsed:
                values = parsed
        except json.JSONDecodeError:
            values = [part.strip() for part in re.split(r"\r?\n|,", raw) if part.strip()]

    targets: list[dict] = []
    for entry in values:
        if isinstance(entry, str):
            name = entry.strip()
            if not name:
                continue
            targets.append({
                "id": slugify(name),
                "name": name,
                "path": str(corpora_root / name),
            })
            continue
        if isinstance(entry, dict):
            name = str(entry.get("name") or "").strip()
            if not name:
                continue
            targets.append({
                "id": str(entry.get("id") or slugify(name)).strip(),
                "name": name,
                "path": str(entry.get("path") or corpora_root / name),
            })
    return targets


def resolve_target(args: argparse.Namespace) -> dict:
    corpora_root = Path(args.corpora_root or os.environ.get("RAG_FEEDER_KANTROPOS_CORPORA_ROOT") or DEFAULT_CORPORA_ROOT)
    targets = parse_kantropos_targets(corpora_root)
    if args.target_path:
        name = args.target_name or Path(args.target_path).name
        return {
            "id": args.target_id or slugify(name),
            "name": name,
            "path": str(Path(args.target_path)),
        }
    if args.target_id:
        for target in targets:
            if target["id"] == args.target_id:
                return target
    if args.target_name:
        for target in targets:
            if target["name"] == args.target_name:
                return target
        return {
            "id": args.target_id or slugify(args.target_name),
            "name": args.target_name,
            "path": str(corpora_root / args.target_name),
        }
    if args.target_id:
        raise SystemExit(f"Unknown target id: {args.target_id}")
    for target in targets:
        if target["name"] == DEFAULT_TARGET_NAME:
            return target
    return targets[0]


def connect_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def read_existing_bib(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def parse_existing_bib_keys(text: str) -> set[str]:
    return set(re.findall(r"@\w+\s*\{\s*([^,\s]+)", text or ""))


def normalize_doi(value: object) -> str:
    if not value:
        return ""
    text = str(value).strip().lower()
    text = re.sub(r"^(https?://(dx\.)?doi\.org/)", "", text)
    text = re.sub(r"^doi:\s*", "", text)
    return text


def parse_authors(value: object) -> list[str]:
    if not value:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value).strip()
    if text.startswith("[") and text.endswith("]"):
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return parse_authors(parsed)
        except json.JSONDecodeError:
            pass
    if " and " in text:
        return [part.strip() for part in text.split(" and ") if part.strip()]
    if "," in text and not re.search(r"\w+,\s+\w+\s+and\s+", text):
        return [part.strip() for part in text.split(",") if part.strip()]
    return [text] if text else []


def bibtex_escape(value: object) -> str:
    text = "" if value is None else str(value)
    text = re.sub(r"\s+", " ", text).strip()
    return (
        text
        .replace("\\", "\\textbackslash{}")
        .replace("{", "\\{")
        .replace("}", "\\}")
    )


def simple_key_part(value: object, fallback: str = "ref") -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^a-zA-Z0-9]+", "_", text.lower()).strip("_")
    return text[:48] or fallback


def entry_type(value: object) -> str:
    raw = simple_key_part(value or "misc", "misc").replace("_", "-")
    mapping = {
        "journal-article": "article",
        "article": "article",
        "book": "book",
        "book-chapter": "incollection",
        "chapter": "incollection",
        "proceedings-article": "inproceedings",
        "conference-paper": "inproceedings",
        "report": "techreport",
        "working-paper": "techreport",
        "thesis": "phdthesis",
    }
    return mapping.get(raw, re.sub(r"[^a-z0-9]", "", raw) or "misc")


def make_bibtex_key(row: sqlite3.Row, metadata: dict, used_keys: set[str]) -> str:
    explicit = str(metadata.get("ID") or metadata.get("id") or "").strip()
    candidates = []
    if explicit:
        candidates.append(simple_key_part(explicit, "ref"))
    doi = normalize_doi(row["doi"] or metadata.get("doi"))
    if doi:
        candidates.append(f"doi_{simple_key_part(doi, 'doi')}")
    authors = parse_authors(row["authors"] or metadata.get("authors") or metadata.get("author"))
    lead_author = authors[0].split()[-1] if authors else "dt"
    year = row["year"] or metadata.get("year") or "nd"
    candidates.append(f"{simple_key_part(lead_author, 'dt')}_{simple_key_part(year, 'nd')}_{simple_key_part(row['title'], 'work')}")
    candidates.append(f"dt_{row['id']}")

    for candidate in candidates:
        base = candidate[:80]
        if base not in used_keys:
            used_keys.add(base)
            return base
        for idx in range(2, 1000):
            next_key = f"{base}_{idx}"
            if next_key not in used_keys:
                used_keys.add(next_key)
                return next_key
    raise RuntimeError(f"Could not generate unique BibTeX key for work {row['id']}")


def safe_pdf_name(row: sqlite3.Row, used_names: set[str]) -> str:
    source_name = Path(str(row["file_path"] or "")).name or f"work_{row['id']}.pdf"
    stem = re.sub(r"[^A-Za-z0-9._ -]+", "_", Path(source_name).stem).strip(" ._") or f"work_{row['id']}"
    stem = re.sub(r"\s+", " ", stem)
    suffix = Path(source_name).suffix or ".pdf"
    base = f"dt_{row['id']}_{stem}"[:140].rstrip(" ._")
    candidate = f"{base}{suffix}"
    counter = 2
    while candidate in used_names:
        candidate = f"{base}_{counter}{suffix}"
        counter += 1
    used_names.add(candidate)
    return candidate


def parse_metadata(raw: object) -> dict:
    if not raw:
        return {}
    try:
        parsed = json.loads(str(raw))
        return parsed if isinstance(parsed, dict) else {}
    except json.JSONDecodeError:
        return {}


def row_title(row: sqlite3.Row, metadata: dict) -> str:
    return str(row["title"] or metadata.get("title") or "").strip()


def row_authors(row: sqlite3.Row, metadata: dict) -> list[str]:
    return parse_authors(row["authors"] or metadata.get("author") or metadata.get("authors"))


def render_bibtex_entry(row: sqlite3.Row, pdf_name: str, used_keys: set[str]) -> tuple[str, str]:
    metadata = parse_metadata(row["bibtex_entry_json"])
    key = make_bibtex_key(row, metadata, used_keys)
    fields: dict[str, object] = {}

    title = row_title(row, metadata)
    if title:
        fields["title"] = title
    authors = row_authors(row, metadata)
    if authors:
        fields["author"] = " and ".join(authors)
    year = row["year"] or metadata.get("year")
    if year:
        fields["year"] = year
    abstract = row["abstract"] or metadata.get("abstract")
    if abstract:
        fields["abstract"] = abstract
    doi = normalize_doi(row["doi"] or metadata.get("doi"))
    if doi:
        fields["doi"] = doi
    source = row["source"] or metadata.get("source") or metadata.get("journal") or metadata.get("container")
    if source:
        fields["journal"] = source
    url = row["url"] or metadata.get("url") or metadata.get("open_access_url")
    if url:
        fields["url"] = url
    openalex_id = row["openalex_id"] or metadata.get("openalex_id")
    if openalex_id:
        fields["openalexid"] = openalex_id
    fields["file"] = f"{pdf_name}:PDF"

    lines = [f"@{entry_type(row['type'] or metadata.get('ENTRYTYPE') or metadata.get('type'))}{{{key},"]
    ordered_names = ["title", "author", "year", "abstract", "doi", "journal", "url", "openalexid", "file"]
    rendered = [(name, fields[name]) for name in ordered_names if fields.get(name)]
    for idx, (name, value) in enumerate(rendered):
        comma = "," if idx < len(rendered) - 1 else ""
        lines.append(f"  {name} = {{{bibtex_escape(value)}}}{comma}")
    lines.append("}")
    return key, "\n".join(lines)


def scan_pdf_text(path: Path) -> dict:
    try:
        import fitz  # type: ignore
    except Exception as exc:
        raise SystemExit(
            "PyMuPDF is required for text scanning. Run this command with "
            "the backend virtualenv Python or install dl_lit_project/requirements.txt."
        ) from exc

    with fitz.open(path) as doc:
        pages = len(doc)
        text_chars = 0
        nonempty_pages = 0
        for page in doc:
            text = page.get_text("text") or ""
            page_chars = len(text.strip())
            text_chars += page_chars
            if page_chars >= 20:
                nonempty_pages += 1
    return {
        "pages": pages,
        "text_chars": text_chars,
        "nonempty_pages": nonempty_pages,
    }


def text_scan_reason(scan: dict, min_chars: int, min_page_ratio: float) -> str:
    text_chars = int(scan.get("text_chars") or 0)
    pages = int(scan.get("pages") or 0)
    nonempty_pages = int(scan.get("nonempty_pages") or 0)
    if text_chars == 0 or nonempty_pages == 0:
        return "empty_text"
    if text_chars < min_chars or nonempty_pages / max(pages, 1) < min_page_ratio:
        return "low_text"
    return "ok"


def scan_item_text(item: dict, draft_dir: Path, min_chars: int, min_page_ratio: float) -> dict:
    staged_file = str(item.get("staged_file") or "")
    source_file = str(item.get("source_file_path") or item.get("source_file") or "")
    pdf_path = draft_dir / staged_file if staged_file else Path(source_file)
    result = {
        "work_id": item.get("work_id"),
        "title": item.get("title"),
        "year": item.get("year"),
        "target_file": item.get("target_file"),
        "source_file_path": source_file,
        "staged_file": staged_file,
    }
    if not pdf_path.exists():
        result["reason"] = "missing"
        return result
    try:
        scan = scan_pdf_text(pdf_path)
        result.update(scan)
        result["reason"] = text_scan_reason(scan, min_chars, min_page_ratio)
    except Exception as exc:
        result["reason"] = "error"
        result["error"] = str(exc)
    return result


def summarize_text_scan(results: list[dict]) -> dict:
    summary = {
        "total": len(results),
        "ok": 0,
        "low_text": 0,
        "empty_text": 0,
        "missing": 0,
        "errors": 0,
        "problematic": [],
    }
    text_char_counts = []
    for result in results:
        reason = result.get("reason")
        if reason == "ok":
            summary["ok"] += 1
        elif reason == "low_text":
            summary["low_text"] += 1
            summary["problematic"].append(result)
        elif reason == "empty_text":
            summary["empty_text"] += 1
            summary["problematic"].append(result)
        elif reason == "missing":
            summary["missing"] += 1
            summary["problematic"].append(result)
        else:
            summary["errors"] += 1
            summary["problematic"].append(result)
        if isinstance(result.get("text_chars"), int):
            text_char_counts.append(result["text_chars"])

    if text_char_counts:
        sorted_counts = sorted(text_char_counts)
        middle = len(sorted_counts) // 2
        if len(sorted_counts) % 2:
            median = sorted_counts[middle]
        else:
            median = int((sorted_counts[middle - 1] + sorted_counts[middle]) / 2)
        summary["text_chars"] = {
            "min": sorted_counts[0],
            "median": median,
            "max": sorted_counts[-1],
        }
    summary["problematic_count"] = len(summary["problematic"])
    return summary


def pending_rows(conn: sqlite3.Connection, target_id: str, metadata_bib: Path) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT DISTINCT w.*
          FROM works w
          JOIN corpus_works cw ON cw.work_id = w.id
          JOIN corpus_kantropos_assignments a ON a.corpus_id = cw.corpus_id
         WHERE a.target_id = ?
           AND w.download_status = 'downloaded'
           AND w.id NOT IN (
             SELECT id FROM works
              WHERE origin_type = 'bibtex_import'
                AND origin_key = ?
           )
         ORDER BY COALESCE(w.year, 0) DESC, w.id DESC
        """,
        (target_id, str(metadata_bib)),
    ).fetchall()


def pending_counts(conn: sqlite3.Connection, target_id: str, metadata_bib: Path) -> dict:
    rows = conn.execute(
        """
        SELECT c.id, c.name, COUNT(DISTINCT w.id) AS pending
          FROM works w
          JOIN corpus_works cw ON cw.work_id = w.id
          JOIN corpus_kantropos_assignments a ON a.corpus_id = cw.corpus_id
          JOIN corpora c ON c.id = cw.corpus_id
         WHERE a.target_id = ?
           AND w.download_status = 'downloaded'
           AND w.id NOT IN (
             SELECT id FROM works
              WHERE origin_type = 'bibtex_import'
                AND origin_key = ?
           )
         GROUP BY c.id, c.name
         ORDER BY pending DESC, LOWER(c.name)
        """,
        (target_id, str(metadata_bib)),
    ).fetchall()
    total = sum(int(row["pending"]) for row in rows)
    return {
        "total": total,
        "corpora": [dict(row) for row in rows],
    }


def append_bibtex(existing: str, additions: str) -> str:
    base = existing or ""
    extra = additions.strip()
    if not extra:
        return base
    separator = "\n\n" if base and not base.endswith("\n\n") else ""
    return f"{base}{separator}{extra}\n"


def validate_draft_dir(draft_dir: Path) -> dict:
    manifest_path = draft_dir / "manifest.json"
    pending_bib_path = draft_dir / "metadata.pending-additions.bib"
    metadata_new_path = draft_dir / "metadata.bib.new"
    files_dir = draft_dir / "files"
    errors = []
    warnings = []

    if not manifest_path.exists():
        errors.append(f"missing manifest: {manifest_path}")
        return {"valid": False, "errors": errors, "warnings": warnings}
    if not pending_bib_path.exists():
        errors.append(f"missing pending BibTeX: {pending_bib_path}")
    if not metadata_new_path.exists():
        errors.append(f"missing full metadata draft: {metadata_new_path}")
    if not files_dir.exists():
        errors.append(f"missing files directory: {files_dir}")
    if errors:
        return {"valid": False, "errors": errors, "warnings": warnings}

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    pending_bib = pending_bib_path.read_text(encoding="utf-8")
    items = manifest.get("items") or []
    parsed_entries = []
    parser_name = "regex"
    try:
        import bibtexparser  # type: ignore

        library = bibtexparser.parse_string(pending_bib)
        parser_name = "bibtexparser"
        for key, entry in library.entries_dict.items():
            raw_file = entry.get("file")
            if not raw_file:
                continue
            value = raw_file.value if hasattr(raw_file, "value") else str(raw_file)
            if value.startswith("{") and value.endswith("}"):
                value = value[1:-1].strip()
            parsed_entries.append({"key": key, "file": value})
    except Exception as exc:
        warnings.append(f"bibtexparser unavailable or failed ({exc}); using regex validation")
        for match in re.finditer(r"@\w+\s*\{\s*([^,\s]+).*?file\s*=\s*\{([^{}]+)\}", pending_bib, re.DOTALL):
            parsed_entries.append({"key": match.group(1), "file": match.group(2).strip()})

    files_by_value = {entry["file"]: entry["key"] for entry in parsed_entries if entry.get("file")}
    expected_files = []
    for item in items:
        target_file = str(item.get("target_file") or "").strip()
        staged_file = str(item.get("staged_file") or "").strip()
        bibtex_key = str(item.get("bibtex_key") or "").strip()
        if not target_file:
            errors.append(f"work {item.get('work_id')} has no target_file")
            continue
        expected = f"{target_file}:PDF"
        expected_files.append(expected)
        if expected not in files_by_value:
            errors.append(f"missing BibTeX file field for {target_file}: expected {expected}")
        elif bibtex_key and files_by_value[expected] != bibtex_key:
            errors.append(f"BibTeX key mismatch for {target_file}: manifest={bibtex_key}, parsed={files_by_value[expected]}")
        if staged_file and not (draft_dir / staged_file).exists():
            errors.append(f"missing staged file for {target_file}: {staged_file}")

    duplicate_files = sorted({value for value in expected_files if expected_files.count(value) > 1})
    if duplicate_files:
        errors.append(f"duplicate target file fields: {', '.join(duplicate_files)}")

    if len(parsed_entries) != len(items):
        warnings.append(f"parsed entry count {len(parsed_entries)} differs from manifest item count {len(items)}")

    return {
        "valid": not errors,
        "parser": parser_name,
        "entry_count": len(parsed_entries),
        "manifest_item_count": len(items),
        "errors": errors,
        "warnings": warnings,
    }


def command_count(args: argparse.Namespace) -> None:
    target = resolve_target(args)
    metadata_bib = Path(target["path"]) / "metadata.bib"
    with connect_db(args.db_path) as conn:
        counts = pending_counts(conn, target["id"], metadata_bib)
    print(json.dumps({
        "target": target,
        "metadata_bib": str(metadata_bib),
        "pending": counts,
    }, ensure_ascii=False, indent=2))


def command_draft(args: argparse.Namespace) -> None:
    target = resolve_target(args)
    target_dir = Path(target["path"])
    metadata_bib = target_dir / "metadata.bib"
    existing_bib = read_existing_bib(metadata_bib)
    used_keys = parse_existing_bib_keys(existing_bib)
    used_file_names = {path.name for path in target_dir.glob("*") if path.is_file()} if target_dir.exists() else set()

    out_root = Path(args.output_dir or DEFAULT_OUTPUT_DIR)
    draft_dir = out_root / f"{target['id']}-{timestamp()}"
    files_dir = draft_dir / "files"
    files_dir.mkdir(parents=True, exist_ok=False)

    manifest_items = []
    skipped = []
    entries = []
    with connect_db(args.db_path) as conn:
        rows = pending_rows(conn, target["id"], metadata_bib)
        counts = pending_counts(conn, target["id"], metadata_bib)

    limit = args.limit if args.limit and args.limit > 0 else None
    selected_rows = rows[:limit] if limit else rows
    for row in selected_rows:
        source_path = Path(str(row["file_path"] or ""))
        if not source_path.exists() or not source_path.is_file():
            skipped.append({
                "work_id": row["id"],
                "title": row["title"],
                "reason": "missing_pdf",
                "file_path": str(source_path),
            })
            continue
        pdf_name = safe_pdf_name(row, used_file_names)
        staged_path = files_dir / pdf_name
        shutil.copy2(source_path, staged_path)
        bibtex_key, entry = render_bibtex_entry(row, pdf_name, used_keys)
        entries.append(entry)
        manifest_items.append({
            "work_id": row["id"],
            "title": row["title"],
            "year": row["year"],
            "doi": normalize_doi(row["doi"]),
            "source_file_path": str(source_path),
            "staged_file": str(staged_path.relative_to(draft_dir)),
            "target_file": pdf_name,
            "bibtex_key": bibtex_key,
        })

    additions_bib = "\n\n".join(entries) + ("\n" if entries else "")
    (draft_dir / "metadata.current.bib").write_text(existing_bib, encoding="utf-8")
    (draft_dir / "metadata.pending-additions.bib").write_text(additions_bib, encoding="utf-8")
    (draft_dir / "metadata.bib.new").write_text(append_bibtex(existing_bib, additions_bib), encoding="utf-8")
    manifest = {
        "created_at": datetime.now().isoformat(),
        "target": target,
        "metadata_bib": str(metadata_bib),
        "total_pending_at_draft_time": counts["total"],
        "selected_count": len(selected_rows),
        "staged_count": len(manifest_items),
        "skipped_count": len(skipped),
        "items": manifest_items,
        "skipped": skipped,
    }
    (draft_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    (draft_dir / "README.txt").write_text(
        "\n".join([
            "Review this draft before applying it.",
            "",
            "Important files:",
            "- metadata.pending-additions.bib: new entries only",
            "- metadata.bib.new: full replacement metadata.bib",
            "- files/: PDFs to copy into the target corpus directory",
            "- manifest.json: source and target mapping",
            "",
            "Apply after review:",
            f"python3 /usr/src/app/scripts/upstream_update.py apply --draft-dir {draft_dir} --yes",
            "",
            "Then run:",
            f"python3 /usr/src/app/scripts/upstream_update.py commands --target-id {target['id']}",
            "",
        ]),
        encoding="utf-8",
    )

    validation = validate_draft_dir(draft_dir)
    print(json.dumps({
        "draft_dir": str(draft_dir),
        "target": target,
        "staged_count": len(manifest_items),
        "skipped_count": len(skipped),
        "metadata_new": str(draft_dir / "metadata.bib.new"),
        "pending_bib": str(draft_dir / "metadata.pending-additions.bib"),
        "manifest": str(draft_dir / "manifest.json"),
        "validation": validation,
    }, ensure_ascii=False, indent=2))
    if not validation["valid"]:
        raise SystemExit(1)


def load_draft_manifest(draft_dir: Path) -> dict:
    manifest_path = draft_dir / "manifest.json"
    if not manifest_path.exists():
        raise SystemExit(f"Missing manifest: {manifest_path}")
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def scan_draft_items(args: argparse.Namespace) -> tuple[Path, list[dict], dict]:
    draft_dir = Path(args.draft_dir)
    manifest = load_draft_manifest(draft_dir)
    results = [
        scan_item_text(item, draft_dir, args.min_text_chars, args.min_text_page_ratio)
        for item in manifest.get("items", [])
    ]
    summary = summarize_text_scan(results)
    return draft_dir, results, summary


def command_scan_text(args: argparse.Namespace) -> None:
    if args.draft_dir:
        draft_dir, results, summary = scan_draft_items(args)
        output = {
            "draft_dir": str(draft_dir),
            "thresholds": {
                "min_text_chars": args.min_text_chars,
                "min_text_page_ratio": args.min_text_page_ratio,
            },
            "summary": summary,
        }
        if args.write:
            scan_path = draft_dir / "text-scan.json"
            scan_path.write_text(json.dumps({
                **output,
                "items": results,
            }, ensure_ascii=False, indent=2), encoding="utf-8")
            output["scan_path"] = str(scan_path)
        if args.include_items:
            output["items"] = results
        print(json.dumps(output, ensure_ascii=False, indent=2))
        if args.fail_on_weak and summary["problematic_count"]:
            raise SystemExit(1)
        return

    target = resolve_target(args)
    metadata_bib = Path(target["path"]) / "metadata.bib"
    with connect_db(args.db_path) as conn:
        rows = pending_rows(conn, target["id"], metadata_bib)
    limit = args.limit if args.limit and args.limit > 0 else None
    selected_rows = rows[:limit] if limit else rows
    results = []
    for row in selected_rows:
        source_path = Path(str(row["file_path"] or ""))
        item = {
            "work_id": row["id"],
            "title": row["title"],
            "year": row["year"],
            "source_file_path": str(source_path),
        }
        results.append(scan_item_text(item, Path("/"), args.min_text_chars, args.min_text_page_ratio))
    summary = summarize_text_scan(results)
    output = {
        "target": target,
        "thresholds": {
            "min_text_chars": args.min_text_chars,
            "min_text_page_ratio": args.min_text_page_ratio,
        },
        "summary": summary,
    }
    if args.include_items:
        output["items"] = results
    print(json.dumps(output, ensure_ascii=False, indent=2))
    if args.fail_on_weak and summary["problematic_count"]:
        raise SystemExit(1)


def ocr_pdf(ocr_url: str, pdf_path: Path, timeout: int, request_id: str) -> dict:
    try:
        import requests  # type: ignore
    except Exception as exc:
        raise SystemExit("The OCR client requires the requests package.") from exc

    with pdf_path.open("rb") as fh:
        response = requests.post(
            f"{ocr_url.rstrip('/')}/ocr",
            headers={"X-Request-ID": request_id},
            files={"file": (pdf_path.name, fh, "application/pdf")},
            timeout=timeout,
        )
    if response.status_code != 200:
        raise RuntimeError(f"OCR failed ({response.status_code}): {response.text[:1000]}")
    return response.json()


def command_ocr(args: argparse.Namespace) -> None:
    draft_dir = Path(args.draft_dir)
    manifest_path = draft_dir / "manifest.json"
    manifest = load_draft_manifest(draft_dir)
    scan_args = argparse.Namespace(
        draft_dir=args.draft_dir,
        min_text_chars=args.min_text_chars,
        min_text_page_ratio=args.min_text_page_ratio,
    )
    _, scan_results, scan_summary = scan_draft_items(scan_args)
    scan_by_target = {item.get("target_file"): item for item in scan_results}
    ocr_url = args.ocr_url or os.environ.get("RAG_FEEDER_OCR_SERVICE_URL") or os.environ.get("OCR_SERVICE_URL") or DEFAULT_OCR_SERVICE_URL

    selected = []
    for item in manifest.get("items", []):
        scan = scan_by_target.get(item.get("target_file")) or {}
        if args.all or scan.get("reason") in {"empty_text", "low_text"}:
            selected.append((item, scan))

    completed = []
    skipped = []
    failed = []
    for item, scan in selected:
        staged_pdf = draft_dir / item["staged_file"]
        text_name = f"{Path(item['target_file']).stem}.txt"
        text_rel = str(Path("files") / text_name)
        text_path = draft_dir / text_rel
        if text_path.exists() and not args.overwrite:
            skipped.append({
                "work_id": item.get("work_id"),
                "target_file": item.get("target_file"),
                "ocr_text_file": text_rel,
                "reason": "text_sidecar_exists",
            })
            item["ocr_text_file"] = text_rel
            continue
        try:
            request_id = f"dt-upstream-{item.get('work_id')}-{timestamp()}"
            result = ocr_pdf(ocr_url, staged_pdf, args.timeout, request_id)
            full_text = str(result.get("full_text") or result.get("text") or "").strip()
            if not full_text:
                raise RuntimeError("OCR response did not contain full_text/text")
            text_path.write_text(full_text + "\n", encoding="utf-8")
            item["ocr_text_file"] = text_rel
            item["ocr"] = {
                "created_at": datetime.now().isoformat(),
                "service_url": ocr_url,
                "request_id": result.get("request_id") or request_id,
                "source_reason": scan.get("reason"),
                "page_count": result.get("page_count"),
                "avg_confidence": result.get("avg_confidence", result.get("confidence")),
                "text_chars": len(full_text),
                "metadata": result.get("metadata"),
            }
            completed.append({
                "work_id": item.get("work_id"),
                "target_file": item.get("target_file"),
                "ocr_text_file": text_rel,
                "request_id": result.get("request_id") or request_id,
                "text_chars": len(full_text),
                "page_count": result.get("page_count"),
                "avg_confidence": result.get("avg_confidence", result.get("confidence")),
            })
        except Exception as exc:
            failed.append({
                "work_id": item.get("work_id"),
                "target_file": item.get("target_file"),
                "error": str(exc),
            })
            if not args.keep_going:
                manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
                raise

    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    output = {
        "draft_dir": str(draft_dir),
        "ocr_url": ocr_url,
        "input_scan_summary": scan_summary,
        "selected_count": len(selected),
        "completed_count": len(completed),
        "skipped_count": len(skipped),
        "failed_count": len(failed),
        "completed": completed,
        "skipped": skipped,
        "failed": failed,
    }
    print(json.dumps(output, ensure_ascii=False, indent=2))
    if failed:
        raise SystemExit(1)


def command_apply(args: argparse.Namespace) -> None:
    draft_dir = Path(args.draft_dir)
    validation = validate_draft_dir(draft_dir)
    if not validation["valid"]:
        print(json.dumps({"validation": validation}, ensure_ascii=False, indent=2), file=sys.stderr)
        raise SystemExit("Draft validation failed; refusing to apply.")
    manifest_path = draft_dir / "manifest.json"
    metadata_new = draft_dir / "metadata.bib.new"
    files_dir = draft_dir / "files"
    if not manifest_path.exists():
        raise SystemExit(f"Missing manifest: {manifest_path}")
    if not metadata_new.exists():
        raise SystemExit(f"Missing metadata draft: {metadata_new}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    target_dir = Path(args.target_path or manifest["target"]["path"])
    metadata_bib = target_dir / "metadata.bib"

    actions = []
    for item in manifest.get("items", []):
        staged = draft_dir / item["staged_file"]
        dest = target_dir / item["target_file"]
        if not staged.exists():
            raise SystemExit(f"Missing staged file: {staged}")
        if dest.exists() and not args.overwrite_files:
            if dest.stat().st_size == staged.stat().st_size:
                actions.append(("skip_existing_file", staged, dest))
            else:
                raise SystemExit(f"Target file already exists with different size: {dest}")
        else:
            actions.append(("copy_file", staged, dest))
        ocr_text_file = str(item.get("ocr_text_file") or "").strip()
        if ocr_text_file:
            staged_text = draft_dir / ocr_text_file
            dest_text = target_dir / Path(ocr_text_file).name
            if not staged_text.exists():
                raise SystemExit(f"Missing OCR text sidecar: {staged_text}")
            if dest_text.exists() and not args.overwrite_files:
                if dest_text.read_text(encoding="utf-8") == staged_text.read_text(encoding="utf-8"):
                    actions.append(("skip_existing_text", staged_text, dest_text))
                    continue
                raise SystemExit(f"Target OCR text sidecar already exists with different content: {dest_text}")
            actions.append(("copy_text", staged_text, dest_text))

    backup_path = metadata_bib.with_name(f"{metadata_bib.name}.{timestamp()}.bak")
    if not args.yes:
        print(json.dumps({
            "dry_run": True,
            "target_dir": str(target_dir),
            "metadata_bib": str(metadata_bib),
            "metadata_backup": str(backup_path),
            "file_actions": len(actions),
            "pdf_copy_actions": sum(1 for action, _, _ in actions if action == "copy_file"),
            "ocr_text_copy_actions": sum(1 for action, _, _ in actions if action == "copy_text"),
            "message": "Rerun with --yes to apply.",
        }, ensure_ascii=False, indent=2))
        return

    target_dir.mkdir(parents=True, exist_ok=True)
    if metadata_bib.exists():
        shutil.copy2(metadata_bib, backup_path)
    else:
        backup_path = None
    for action, staged, dest in actions:
        if action == "skip_existing_file":
            continue
        shutil.copy2(staged, dest)
    shutil.copy2(metadata_new, metadata_bib)
    print(json.dumps({
        "applied": True,
        "target_dir": str(target_dir),
        "metadata_bib": str(metadata_bib),
        "metadata_backup": str(backup_path) if backup_path else "",
        "copied_files": sum(1 for action, _, _ in actions if action == "copy_file"),
        "copied_ocr_text_files": sum(1 for action, _, _ in actions if action == "copy_text"),
        "skipped_existing_files": sum(1 for action, _, _ in actions if action == "skip_existing_file"),
        "skipped_existing_text_files": sum(1 for action, _, _ in actions if action == "skip_existing_text"),
    }, ensure_ascii=False, indent=2))


def command_commands(args: argparse.Namespace) -> None:
    target = resolve_target(args)
    encoded = quote(target["name"], safe="")
    print("\n".join([
        "# Generate missing markdowns",
        "docker exec -it kantropos-corpus-updater bash",
        f'curl -X POST "http://localhost:8001/markdowns/{encoded}" &',
        "",
        "# Watch progress",
        "docker logs -f kantropos-corpus-updater",
        "",
        "# Embed new corpus files after markdown generation finishes",
        "docker exec -it kantropos-corpus-updater bash",
        f'curl -X POST "http://localhost:8001/embeddings/{encoded}?sync_mode=INSERT" &',
        "",
        "# Optional metadata-only refresh for displayed references",
        f'curl -X POST "http://localhost:8001/metadata/{encoded}"',
    ]))


def command_validate(args: argparse.Namespace) -> None:
    validation = validate_draft_dir(Path(args.draft_dir))
    print(json.dumps({"draft_dir": args.draft_dir, "validation": validation}, ensure_ascii=False, indent=2))
    if not validation["valid"]:
        raise SystemExit(1)


def add_target_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--target-id", default="", help="Kantropos target id. Defaults to Anthropozän target when configured.")
    parser.add_argument("--target-name", default="", help="Kantropos target name.")
    parser.add_argument("--target-path", default="", help="Explicit target directory path.")
    parser.add_argument("--corpora-root", default="", help="Kantropos corpora root.")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", default=os.environ.get("RAG_FEEDER_DB_PATH") or DEFAULT_CONTAINER_DB_PATH)
    sub = parser.add_subparsers(dest="command", required=True)

    count_parser = sub.add_parser("count", help="Count pending additions for a target.")
    add_target_args(count_parser)
    count_parser.set_defaults(func=command_count)

    draft_parser = sub.add_parser("draft", help="Create a reviewable upstream update draft.")
    add_target_args(draft_parser)
    draft_parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    draft_parser.add_argument("--limit", type=int, default=0, help="Draft only the first N pending additions.")
    draft_parser.set_defaults(func=command_draft)

    scan_parser = sub.add_parser("scan-text", help="Scan pending or drafted PDFs for extractable text.")
    add_target_args(scan_parser)
    scan_parser.add_argument("--draft-dir", default="", help="Scan staged PDFs in a draft instead of live pending rows.")
    scan_parser.add_argument("--limit", type=int, default=0, help="Scan only the first N pending additions.")
    scan_parser.add_argument("--min-text-chars", type=int, default=DEFAULT_MIN_TEXT_CHARS)
    scan_parser.add_argument("--min-text-page-ratio", type=float, default=DEFAULT_MIN_TEXT_PAGE_RATIO)
    scan_parser.add_argument("--include-items", action="store_true", help="Include per-item scan results.")
    scan_parser.add_argument("--write", action="store_true", help="Write text-scan.json into the draft directory.")
    scan_parser.add_argument("--fail-on-weak", action="store_true", help="Exit non-zero if any PDF is weak, missing, or failed.")
    scan_parser.set_defaults(func=command_scan_text)

    ocr_parser = sub.add_parser("ocr", help="OCR weak PDFs in a draft and stage text sidecars.")
    ocr_parser.add_argument("--draft-dir", required=True)
    ocr_parser.add_argument("--ocr-url", default="", help="OCR service URL. Defaults to RAG_FEEDER_OCR_SERVICE_URL, OCR_SERVICE_URL, then localhost:8004.")
    ocr_parser.add_argument("--min-text-chars", type=int, default=DEFAULT_MIN_TEXT_CHARS)
    ocr_parser.add_argument("--min-text-page-ratio", type=float, default=DEFAULT_MIN_TEXT_PAGE_RATIO)
    ocr_parser.add_argument("--timeout", type=int, default=600)
    ocr_parser.add_argument("--all", action="store_true", help="OCR all draft PDFs instead of only weak PDFs.")
    ocr_parser.add_argument("--overwrite", action="store_true", help="Overwrite existing staged OCR text sidecars.")
    ocr_parser.add_argument("--keep-going", action="store_true", help="Continue OCR after per-file failures.")
    ocr_parser.set_defaults(func=command_ocr)

    apply_parser = sub.add_parser("apply", help="Apply a reviewed draft to the upstream corpus directory.")
    apply_parser.add_argument("--draft-dir", required=True)
    apply_parser.add_argument("--target-path", default="", help="Override target path from manifest.")
    apply_parser.add_argument("--overwrite-files", action="store_true")
    apply_parser.add_argument("--yes", action="store_true", help="Actually apply. Without this, prints a dry-run summary.")
    apply_parser.set_defaults(func=command_apply)

    validate_parser = sub.add_parser("validate", help="Validate a generated upstream update draft.")
    validate_parser.add_argument("--draft-dir", required=True)
    validate_parser.set_defaults(func=command_validate)

    commands_parser = sub.add_parser("commands", help="Print corpus-updater commands for the target.")
    add_target_args(commands_parser)
    commands_parser.set_defaults(func=command_commands)

    args = parser.parse_args(argv)
    args.func(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
