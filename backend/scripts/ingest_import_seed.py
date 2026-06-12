"""Import a BibTeX or JSON seed file into the corpus (web-upload ingest path).

Routes uploaded `.bib`/`.json` seed documents into the canonical `works` table
as pending works and links each new work to the current corpus. Inserts go
directly to the on-disk DB via DatabaseManager.create_pending_work (which
returns the new row id), so this is safe to run while the enrich/download
workers are writing — unlike the in-memory rewrite used by the `import-bib`
CLI command.

Prints a JSON summary: {added, skipped, corpus_linked, total, errors}.
"""

import argparse
import json
import sys
from pathlib import Path

from _bootstrap import ensure_import_paths

ensure_import_paths(__file__)
ROOT = Path(__file__).resolve().parents[2]
DL_LIT_PROJECT = ROOT / "dl_lit_project"
if str(DL_LIT_PROJECT) not in sys.path:
    sys.path.insert(0, str(DL_LIT_PROJECT))

from dl_lit.db_manager import DatabaseManager  # noqa: E402


def _field(entry, *names):
    """Read a field value from a bibtexparser v2 Entry, trying common casings."""
    fields = getattr(entry, "fields_dict", {}) or {}
    for name in names:
        field = fields.get(name) or fields.get(name.upper()) or fields.get(name.capitalize())
        if field is not None:
            value = getattr(field, "value", field)
            if value:
                return str(value).replace("\n", " ").strip().strip("{}").strip()
    return None


def load_bibtex_entries(path):
    import bibtexparser

    with open(path, "r", encoding="utf-8") as handle:
        library = bibtexparser.parse_string(handle.read())
    entries = []
    for entry in library.entries:
        title = _field(entry, "title")
        if not title:
            continue
        entries.append(
            {
                "title": title,
                "authors": _field(entry, "author"),
                "editors": _field(entry, "editor"),
                "year": _field(entry, "year"),
                "doi": _field(entry, "doi"),
                "openalex_id": _field(entry, "openalex", "openalexid"),
                "type": getattr(entry, "entry_type", None),
            }
        )
    return entries


def load_json_entries(path):
    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)
    if isinstance(data, dict):
        for key in ("entries", "references", "works", "results"):
            if isinstance(data.get(key), list):
                return data[key]
        return [data]
    return data if isinstance(data, list) else []


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", required=True)
    parser.add_argument("--file", required=True)
    parser.add_argument("--kind", choices=["bib", "json"], required=True)
    parser.add_argument("--corpus-id", type=int, default=None)
    parser.add_argument("--source-label", default=None)
    args = parser.parse_args()

    try:
        entries = (
            load_bibtex_entries(args.file)
            if args.kind == "bib"
            else load_json_entries(args.file)
        )
    except Exception as exc:
        print(json.dumps({"added": 0, "skipped": 0, "corpus_linked": 0, "total": 0,
                          "errors": [f"Failed to parse {args.kind}: {exc}"]}))
        return

    db = DatabaseManager(args.db_path)
    added = skipped = linked = 0
    errors = []
    try:
        for raw in entries:
            if not isinstance(raw, dict):
                skipped += 1
                errors.append(f"Skipped non-object entry: {type(raw).__name__}")
                continue
            entry = dict(raw)
            if args.source_label and not entry.get("source"):
                entry["source"] = args.source_label
            try:
                row_id, err = db.create_pending_work(entry)
            except Exception as exc:
                skipped += 1
                errors.append(str(exc))
                continue
            if row_id:
                added += 1
                if args.corpus_id is not None:
                    db.add_corpus_item(args.corpus_id, "works", int(row_id))
                    linked += 1
            else:
                skipped += 1
                if err and "Duplicate" not in err and "Title is required" not in err:
                    errors.append(err)
        try:
            db.conn.commit()
        except Exception:
            pass
    finally:
        try:
            db.close_connection()
        except Exception:
            pass

    print(json.dumps({"added": added, "skipped": skipped, "corpus_linked": linked,
                      "total": len(entries), "errors": errors[:20]}))


if __name__ == "__main__":
    main()
