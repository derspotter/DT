import argparse
import json
import os
import re
import sqlite3
import zipfile
from datetime import datetime
from pathlib import Path


TABLES = [
    ("downloaded_references", "downloaded_references"),
    ("with_metadata", "with_metadata"),
    ("no_metadata", "no_metadata"),
    ("to_download_references", "to_download_references"),  # legacy fallback
]

TABLE_PRIORITY = {
    "downloaded_references": 4,
    "to_download_references": 3,
    "with_metadata": 2,
    "no_metadata": 1,
}
VALID_STATUSES = set(TABLE_PRIORITY.keys())


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
        (table_name,),
    ).fetchone()
    return bool(row)


def normalize_doi(value):
    if not value:
        return None
    text = str(value).strip().lower()
    text = re.sub(r"^(https?://(dx\.)?doi\.org/)", "", text)
    text = re.sub(r"^doi:\s*", "", text)
    return text or None


def normalize_text(value):
    if value is None:
        return ""
    text = str(value).lower()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def parse_authors(value):
    if not value:
        return ""
    if isinstance(value, list):
        return ", ".join([str(v).strip() for v in value if str(v).strip()])
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            try:
                parsed = json.loads(stripped)
                if isinstance(parsed, list):
                    return parse_authors(parsed)
            except json.JSONDecodeError:
                pass
        return stripped
    return str(value)


def build_source(row_dict):
    return (
        row_dict.get("ingest_source")
        or row_dict.get("source_pdf")
        or row_dict.get("metadata_source_type")
        or row_dict.get("journal_conference")
        or row_dict.get("source")
        or ""
    )


def pick_key(item):
    if item.get("openalex_id"):
        return f"openalex:{item['openalex_id']}"
    if item.get("normalized_doi"):
        return f"doi:{item['normalized_doi']}"
    title_key = normalize_text(item.get("title"))
    author_key = normalize_text(item.get("authors"))
    year_key = item.get("year") or ""
    if title_key:
        return f"tay:{title_key}|{author_key}|{year_key}"
    return f"{item['status']}:{item['id']}"


def should_replace(existing, candidate):
    existing_p = TABLE_PRIORITY.get(existing["status"], 0)
    candidate_p = TABLE_PRIORITY.get(candidate["status"], 0)
    if candidate_p != existing_p:
        return candidate_p > existing_p
    if candidate.get("file_path") and not existing.get("file_path"):
        return True
    return False


def load_items(conn: sqlite3.Connection, corpus_id: int | None):
    items_by_key = {}
    for table_name, status in TABLES:
        if not table_exists(conn, table_name):
            continue
        if corpus_id is None:
            rows = conn.execute(f"SELECT * FROM {table_name}").fetchall()
        else:
            if not table_exists(conn, "corpus_items"):
                rows = []
            else:
                rows = conn.execute(
                    f"""SELECT t.* FROM {table_name} t
                        JOIN corpus_items ci
                          ON ci.table_name = ? AND ci.row_id = t.id
                       WHERE ci.corpus_id = ?""",
                    (table_name, corpus_id),
                ).fetchall()
        for row in rows:
            data = dict(row)
            item_status = status
            if table_name == 'with_metadata':
                state = (data.get('download_state') or '').strip().lower()
                if state in ('queued', 'in_progress'):
                    item_status = 'to_download_references'
                elif state == 'failed':
                    item_status = 'failed_downloads'
                else:
                    item_status = 'with_metadata'
            item = {
                "id": data.get("id"),
                "status": item_status,
                "title": data.get("title") or "",
                "authors": parse_authors(data.get("authors")),
                "year": data.get("year"),
                "doi": data.get("doi"),
                "normalized_doi": normalize_doi(data.get("doi") or data.get("normalized_doi")),
                "openalex_id": data.get("openalex_id"),
                "source": build_source(data),
                "entry_type": data.get("entry_type") or data.get("type") or "misc",
                "bibtex_key": data.get("bibtex_key"),
                "bibtex_entry_json": data.get("bibtex_entry_json"),
                "file_path": data.get("file_path"),
            }
            key = pick_key(item)
            existing = items_by_key.get(key)
            if existing is None or should_replace(existing, item):
                items_by_key[key] = item
    items = list(items_by_key.values())

    def _year(value):
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    def _id(value):
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    items.sort(key=lambda row: (_year(row.get("year")), _id(row.get("id"))), reverse=True)
    return items


def escape_bibtex(value):
    if value is None:
        return ""
    text = str(value).replace("\n", " ").strip()
    text = text.replace("{", "\\{").replace("}", "\\}")
    return text


def to_bibtex_entry(item):
    data = {}
    raw_json = item.get("bibtex_entry_json")
    if raw_json:
        try:
            parsed = json.loads(raw_json)
            if isinstance(parsed, dict):
                data = parsed
        except json.JSONDecodeError:
            data = {}

    entry_type = str(data.get("ENTRYTYPE") or item.get("entry_type") or "misc").lower()
    key = data.get("ID") or item.get("bibtex_key") or f"ref_{item.get('status')}_{item.get('id')}"

    fields = {}
    for k, v in data.items():
        if k in ("ENTRYTYPE", "ID") or v is None:
            continue
        fields[str(k).lower()] = str(v)

    if not fields.get("title") and item.get("title"):
        fields["title"] = item["title"]
    if not fields.get("author") and item.get("authors"):
        fields["author"] = item["authors"]
    if not fields.get("year") and item.get("year"):
        fields["year"] = str(item["year"])
    if not fields.get("doi") and item.get("doi"):
        fields["doi"] = item["doi"]
    if not fields.get("journal") and item.get("source"):
        fields["journal"] = item["source"]
    if item.get("file_path"):
        fields["file"] = f":{Path(item['file_path']).name}:PDF"

    lines = [f"@{entry_type}{{{key},"]
    keys = sorted(fields.keys())
    for idx, k in enumerate(keys):
        v = fields[k]
        suffix = "," if idx < len(keys) - 1 else ""
        lines.append(f"  {k} = {{{escape_bibtex(v)}}}{suffix}")
    lines.append("}")
    return "\n".join(lines)


def render_json_payload(items):
    return {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "total": len(items),
        "items": items,
    }


def write_json_export(items, out_path: Path):
    payload = render_json_payload(items)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return {"exported_count": len(items)}


def render_bibtex_text(items):
    entries = [to_bibtex_entry(item) for item in items if item.get("title")]
    return "\n\n".join(entries) + ("\n" if entries else "")


def write_bibtex_export(items, out_path: Path):
    out_path.write_text(render_bibtex_text(items), encoding="utf-8")
    entries = [item for item in items if item.get("title")]
    return {"exported_count": len(entries)}


def collect_pdf_files(items):
    existing_files = []
    missing = 0
    for item in items:
        fp = item.get("file_path")
        if not fp:
            continue
        p = Path(fp)
        if p.exists() and p.is_file():
            existing_files.append((item, p))
        else:
            missing += 1
    return existing_files, missing


def write_pdf_zip_export(items, out_path: Path):
    existing_files, missing = collect_pdf_files(items)
    used_names = set()
    added = 0
    with zipfile.ZipFile(out_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        manifest = []
        for item, p in existing_files:
            base_name = p.name
            if base_name in used_names:
                base_name = f"{item.get('id')}_{base_name}"
            used_names.add(base_name)
            zf.write(p, arcname=base_name)
            manifest.append(
                {
                    "id": item.get("id"),
                    "title": item.get("title"),
                    "status": item.get("status"),
                    "file_name": base_name,
                }
            )
            added += 1
        zf.writestr(
            "manifest.json",
            json.dumps(
                {
                    "generated_at": datetime.utcnow().isoformat() + "Z",
                    "files": manifest,
                },
                indent=2,
            ),
        )
    return {"exported_count": added, "missing_files": missing}


def write_bundle_zip_export(items, out_path: Path, filters):
    existing_files, missing = collect_pdf_files(items)
    used_names = set()
    added = 0
    with zipfile.ZipFile(out_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(
            "references.json",
            json.dumps(render_json_payload(items), ensure_ascii=False, indent=2),
        )
        zf.writestr("references.bib", render_bibtex_text(items))

        file_manifest = []
        for item, p in existing_files:
            base_name = p.name
            if base_name in used_names:
                base_name = f"{item.get('id')}_{base_name}"
            used_names.add(base_name)
            arc_name = f"pdfs/{base_name}"
            zf.write(p, arcname=arc_name)
            file_manifest.append(
                {
                    "id": item.get("id"),
                    "title": item.get("title"),
                    "status": item.get("status"),
                    "file_name": arc_name,
                }
            )
            added += 1

        zf.writestr(
            "manifest.json",
            json.dumps(
                {
                    "generated_at": datetime.utcnow().isoformat() + "Z",
                    "filters": filters,
                    "totals": {
                        "items": len(items),
                        "pdfs_added": added,
                        "pdfs_missing": missing,
                    },
                    "pdf_files": file_manifest,
                },
                ensure_ascii=False,
                indent=2,
            ),
        )
    return {"exported_count": len(items), "pdfs_added": added, "missing_files": missing}


def parse_status_filter(raw_status):
    if not raw_status:
        return set()
    return {
        str(part).strip().lower()
        for part in str(raw_status).split(",")
        if str(part).strip()
    }


def parse_year(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def apply_filters(items, statuses, year_from, year_to, source_substring):
    source_substring = (source_substring or "").strip().lower()
    filtered = []
    for item in items:
        if statuses and item.get("status") not in statuses:
            continue

        year = parse_year(item.get("year"))
        if year_from is not None and (year is None or year < year_from):
            continue
        if year_to is not None and (year is None or year > year_to):
            continue

        source = str(item.get("source") or "").lower()
        if source_substring and source_substring not in source:
            continue

        filtered.append(item)
    return filtered


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--format", required=True, choices=["json", "bibtex", "pdfs_zip", "bundle_zip"])
    parser.add_argument("--corpus-id", type=int, default=None)
    parser.add_argument("--status", default="")
    parser.add_argument("--year-from", type=int, default=None)
    parser.add_argument("--year-to", type=int, default=None)
    parser.add_argument("--source", default="")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    corpus_suffix = f"corpus-{args.corpus_id}" if args.corpus_id is not None else "corpus-all"
    ext = {"json": "json", "bibtex": "bib", "pdfs_zip": "zip", "bundle_zip": "zip"}[args.format]
    filename = f"references-{corpus_suffix}-{timestamp}.{ext}"
    output_path = output_dir / filename
    requested_statuses = parse_status_filter(args.status)
    selected_statuses = set(s for s in requested_statuses if s in VALID_STATUSES)
    applied_filters = {
        "status": sorted(list(selected_statuses)),
        "year_from": args.year_from,
        "year_to": args.year_to,
        "source": args.source or "",
    }

    if os.environ.get("RAG_FEEDER_STUB") == "1":
        if args.format == "json":
            output_path.write_text(
                json.dumps(
                    {
                        "generated_at": datetime.utcnow().isoformat() + "Z",
                        "total": 0,
                        "items": [],
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )
            stats = {"exported_count": 0}
        elif args.format == "bibtex":
            output_path.write_text("", encoding="utf-8")
            stats = {"exported_count": 0}
        elif args.format == "pdfs_zip":
            with zipfile.ZipFile(output_path, "w", compression=zipfile.ZIP_DEFLATED):
                pass
            stats = {"exported_count": 0, "missing_files": 0}
        else:
            with zipfile.ZipFile(output_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                zf.writestr("references.json", json.dumps(render_json_payload([]), indent=2))
                zf.writestr("references.bib", "")
                zf.writestr(
                    "manifest.json",
                    json.dumps(
                        {
                            "generated_at": datetime.utcnow().isoformat() + "Z",
                            "filters": applied_filters,
                            "totals": {"items": 0, "pdfs_added": 0, "pdfs_missing": 0},
                            "pdf_files": [],
                        },
                        indent=2,
                    ),
                )
            stats = {"exported_count": 0, "pdfs_added": 0, "missing_files": 0}
        print(
            json.dumps(
                {
                    "format": args.format,
                    "filename": filename,
                    "output_path": str(output_path),
                    **stats,
                    "applied_filters": applied_filters,
                    "source": "stub",
                }
            )
        )
        return

    conn = sqlite3.connect(args.db_path)
    conn.row_factory = sqlite3.Row
    try:
        items = load_items(conn, args.corpus_id)
    finally:
        conn.close()

    items = apply_filters(items, selected_statuses, args.year_from, args.year_to, args.source)

    if args.format == "json":
        stats = write_json_export(items, output_path)
    elif args.format == "bibtex":
        stats = write_bibtex_export(items, output_path)
    elif args.format == "pdfs_zip":
        stats = write_pdf_zip_export(items, output_path)
    else:
        stats = write_bundle_zip_export(items, output_path, applied_filters)

    print(
        json.dumps(
            {
                "format": args.format,
                "filename": filename,
                "output_path": str(output_path),
                **stats,
                "applied_filters": applied_filters,
                "source": "db",
            }
        )
    )


if __name__ == "__main__":
    main()
