import argparse
import json
import os
import re
import sqlite3
import zipfile
from datetime import datetime
from pathlib import Path


VALID_STATUSES = {
    "downloaded",
    "matched",
    "raw",
    "queued_download",
    "failed_enrichment",
    "failed_download",
    "enriching",
}


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


def work_status(row_dict):
    metadata = str(row_dict.get("metadata_status") or "pending").strip().lower()
    download = str(row_dict.get("download_status") or "not_requested").strip().lower()
    if download == "downloaded":
        return "downloaded"
    if download in {"queued", "in_progress"}:
        return "queued_download"
    if download == "failed":
        return "failed_download"
    if metadata == "in_progress":
        return "enriching"
    if metadata == "failed":
        return "failed_enrichment"
    if metadata == "matched":
        return "matched"
    return "raw"


def build_source(row_dict):
    return (
        row_dict.get("origin_key")
        or row_dict.get("source_pdf")
        or row_dict.get("metadata_source")
        or row_dict.get("source")
        or row_dict.get("publisher")
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
    return f"works:{item['id']}"


def load_items(conn: sqlite3.Connection, corpus_id: int | None):
    if corpus_id is None:
        rows = conn.execute("SELECT * FROM works").fetchall()
    else:
        rows = conn.execute(
            """
            SELECT w.*
            FROM works w
            JOIN corpus_works cw ON cw.work_id = w.id
            WHERE cw.corpus_id = ?
            """,
            (corpus_id,),
        ).fetchall()

    items_by_key = {}
    for row in rows:
        data = dict(row)
        item = {
            "id": data.get("id"),
            "status": work_status(data),
            "title": data.get("title") or "",
            "authors": parse_authors(data.get("authors")),
            "year": data.get("year"),
            "doi": data.get("doi"),
            "normalized_doi": normalize_doi(data.get("doi") or data.get("normalized_doi")),
            "openalex_id": data.get("openalex_id"),
            "source": build_source(data),
            "entry_type": data.get("type") or "misc",
            "bibtex_key": None,
            "bibtex_entry_json": data.get("bibtex_entry_json"),
            "file_path": data.get("file_path"),
        }
        items_by_key[pick_key(item)] = item

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
    return {"exported_count": len([item for item in items if item.get("title")])}


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


def write_pdf_zip(items, out_path: Path):
    files, missing = collect_pdf_files(items)
    with zipfile.ZipFile(out_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for item, pdf_path in files:
            zf.write(pdf_path, arcname=pdf_path.name)
    return {"exported_count": len(files), "missing_files": missing}


def write_bundle_zip(items, out_path: Path):
    files, missing = collect_pdf_files(items)
    json_payload = json.dumps(render_json_payload(items), ensure_ascii=False, indent=2)
    bibtex_text = render_bibtex_text(items)
    with zipfile.ZipFile(out_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("metadata.json", json_payload)
        zf.writestr("references.bib", bibtex_text)
        for item, pdf_path in files:
            zf.write(pdf_path, arcname=f"pdfs/{pdf_path.name}")
    return {"exported_count": len(items), "pdf_count": len(files), "missing_files": missing}


def filter_items(items, *, status=None, year_from=None, year_to=None, source=None):
    filtered = items
    if status:
        filtered = [item for item in filtered if item.get("status") == status]
    if year_from is not None:
        filtered = [item for item in filtered if item.get("year") is not None and int(item.get("year")) >= year_from]
    if year_to is not None:
        filtered = [item for item in filtered if item.get("year") is not None and int(item.get("year")) <= year_to]
    if source:
        needle = source.strip().lower()
        filtered = [item for item in filtered if needle in str(item.get("source") or "").lower()]
    return filtered


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--format", required=True, choices=["json", "bibtex", "pdfs_zip", "bundle_zip"])
    parser.add_argument("--status", default="", help="Optional status filter.")
    parser.add_argument("--year-from", type=int, default=None)
    parser.add_argument("--year-to", type=int, default=None)
    parser.add_argument("--source", default="")
    parser.add_argument("--corpus-id", type=int, default=None)
    args = parser.parse_args()

    if args.status and args.status not in VALID_STATUSES:
        raise SystemExit(f"Invalid status: {args.status}")

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(args.db_path)
    conn.row_factory = sqlite3.Row
    items = load_items(conn, args.corpus_id)
    conn.close()

    items = filter_items(
        items,
        status=args.status or None,
        year_from=args.year_from,
        year_to=args.year_to,
        source=args.source or None,
    )

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    suffix_map = {
        "json": ".json",
        "bibtex": ".bib",
        "pdfs_zip": ".zip",
        "bundle_zip": ".zip",
    }
    filename = f"export_{timestamp}{suffix_map[args.format]}"
    out_path = out_dir / filename

    if args.format == "json":
        stats = write_json_export(items, out_path)
    elif args.format == "bibtex":
        stats = write_bibtex_export(items, out_path)
    elif args.format == "pdfs_zip":
        stats = write_pdf_zip(items, out_path)
    else:
        stats = write_bundle_zip(items, out_path)

    print(
        json.dumps(
            {
                "output_path": str(out_path),
                "filename": filename,
                "format": args.format,
                "count": len(items),
                "stats": stats,
            }
        )
    )


if __name__ == "__main__":
    main()
