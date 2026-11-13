"""Import a BibTeX or JSON seed file into the corpus (web-upload ingest path).

Routes uploaded `.bib`/`.json` seed documents into the canonical `works` table
as pending works and links each work — new or already present — to the
current corpus (create_pending_work links duplicates too). Inserts go
directly to the on-disk DB via DatabaseManager.create_pending_work (which
returns the new row id), so this is safe to run while the enrich/download
workers are writing — unlike the in-memory rewrite used by the `import-bib`
CLI command.

Entries are written in transactions of INGEST_COMMIT_CHUNK entries (so no write lock
is held for the whole file), each entry inside its own savepoint: an entry that
raises leaves no rows behind and counts as skipped. If a chunk fails as a whole (BEGIN
or COMMIT fails, or SQLite rolls back the transaction on a full disk or an I/O error),
the import stops there: earlier chunks stay committed, and a rerun is safe because
duplicates merge.

Every outcome prints one JSON summary line and exits 0:
{added, skipped, corpus_linked, total, processed, stopped, error, errors}. `added`
counts new works; `corpus_linked` counts entries (new or duplicate) linked to
--corpus-id; `skipped` counts entries that could not be imported (non-object
entries, missing titles, errors); `total` counts the parsed entries. The counts and
`processed` cover only committed chunks. `stopped` is true when a parse failure, a
database setup failure or a failed chunk ended the run early, with
`error: "<ExceptionType>: <message>"`; otherwise `stopped` is false, `processed`
equals `total` and `error` is null. `errors` holds per-entry messages.
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

from dl_lit.db_manager import INGEST_COMMIT_CHUNK, DatabaseManager  # noqa: E402


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


def _print_summary(*, added=0, skipped=0, corpus_linked=0, total=0, processed=0, error=None, errors=()):
    """Print the one summary shape every outcome uses; `stopped` is set exactly when there is an error."""
    print(json.dumps({
        "added": added,
        "skipped": skipped,
        "corpus_linked": corpus_linked,
        "total": total,
        "processed": processed,
        "stopped": error is not None,
        "error": error,
        "errors": list(errors)[:20],
    }))


def _describe(exc):
    return f"{type(exc).__name__}: {exc}"


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", required=True)
    parser.add_argument("--file", required=True)
    parser.add_argument("--kind", choices=["bib", "json"], required=True)
    parser.add_argument("--corpus-id", type=int, default=None)
    parser.add_argument("--source-label", default=None)
    args = parser.parse_args(argv)

    try:
        entries = (
            load_bibtex_entries(args.file)
            if args.kind == "bib"
            else load_json_entries(args.file)
        )
    except Exception as exc:
        _print_summary(error=f"{type(exc).__name__}: Failed to parse {args.kind}: {exc}")
        return

    try:
        db = DatabaseManager(args.db_path)
    except Exception as exc:
        _print_summary(total=len(entries), error=_describe(exc))
        return

    added = skipped = linked = processed = 0
    errors = []
    error = None
    try:
        for chunk_start in range(0, len(entries), INGEST_COMMIT_CHUNK):
            chunk = entries[chunk_start:chunk_start + INGEST_COMMIT_CHUNK]
            # Chunk-local counts join the totals only once the chunk's transaction committed.
            c_added = c_skipped = c_linked = 0
            c_errors = []
            try:
                with db.transaction():
                    for raw in chunk:
                        if not isinstance(raw, dict):
                            c_skipped += 1
                            c_errors.append(f"Skipped non-object entry: {type(raw).__name__}")
                            continue
                        entry = dict(raw)
                        if args.source_label and not entry.get("source"):
                            entry["source"] = args.source_label
                        if args.corpus_id is not None:
                            entry["corpus_id"] = args.corpus_id
                        try:
                            with db.savepoint():
                                row_id, err = db.create_pending_work(entry, commit=False)
                        except Exception as exc:
                            if not db.conn.in_transaction:
                                raise  # SQLite dropped the transaction: the whole chunk is lost
                            c_skipped += 1
                            c_errors.append(str(exc))
                            continue
                        if row_id:
                            c_added += 1
                            if args.corpus_id is not None:
                                c_linked += 1
                        elif err and "Duplicate" in err:
                            # create_pending_work linked the existing work to the corpus.
                            if args.corpus_id is not None:
                                c_linked += 1
                        else:
                            c_skipped += 1
                            if err and "Title is required" not in err:
                                c_errors.append(err)
            except Exception as exc:
                # BEGIN, COMMIT or a lost transaction: this chunk is gone, earlier ones stay.
                error = _describe(exc)
                break
            added += c_added
            skipped += c_skipped
            linked += c_linked
            processed += len(chunk)
            errors.extend(c_errors)
    finally:
        try:
            db.close_connection()
        except Exception:
            pass

    _print_summary(added=added, skipped=skipped, corpus_linked=linked, total=len(entries),
                   processed=processed, error=error, errors=errors)

if __name__ == "__main__":
    main()
