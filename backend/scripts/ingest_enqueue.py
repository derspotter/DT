import argparse
import json
import sys
from pathlib import Path


def _ensure_import_paths() -> None:
    # Prefer PYTHONPATH, but defensively add repo roots for docker/local runs.
    here = Path(__file__).resolve()
    cursor = here.parent
    while True:
        candidate = cursor / "dl_lit_project"
        if candidate.exists():
            sys.path.insert(0, str(candidate))
            sys.path.insert(0, str(candidate.parent))
            return
        if cursor.parent == cursor:
            return
        cursor = cursor.parent


_ensure_import_paths()

from dl_lit.db_manager import DatabaseManager  # noqa: E402


def _parse_ids(value: str) -> list[int]:
    if not value:
        return []
    ids: list[int] = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            ids.append(int(part))
        except ValueError:
            continue
    seen: set[int] = set()
    out: list[int] = []
    for i in ids:
        if i in seen:
            continue
        seen.add(i)
        out.append(i)
    return out


def _mark_selected(db: DatabaseManager, no_meta_id: int, selected: int = 1) -> None:
    cur = db.conn.cursor()
    cur.execute(
        "UPDATE no_metadata SET selected_for_enrichment = ? WHERE id = ?",
        (1 if selected else 0, no_meta_id),
    )
    db.conn.commit()


def main():
    parser = argparse.ArgumentParser(
        description="Mark selected ingest_entries as selected_for_enrichment in no_metadata (no stage skipping)."
    )
    parser.add_argument("--db-path", required=True, help="Path to SQLite DB.")
    parser.add_argument("--entry-ids", required=True, help="Comma-separated ingest_entries ids.")
    parser.add_argument("--corpus-id", type=int, default=None, help="Corpus ID to scope the operation.")
    args = parser.parse_args()

    entry_ids = _parse_ids(args.entry_ids)
    if not entry_ids:
        print(json.dumps({"requested": 0, "marked": 0, "errors": ["No valid entry ids provided."], "source": "db"}))
        return

    db = DatabaseManager(db_path=args.db_path)
    cur = db.conn.cursor()

    placeholders = ",".join(["?"] * len(entry_ids))
    params: list[object] = list(entry_ids)
    where = f"id IN ({placeholders})"
    if args.corpus_id is not None:
        where += " AND corpus_id = ?"
        params.append(args.corpus_id)

    # Prefer entry_json for lossless re-staging if we need to insert missing rows into no_metadata.
    cur.execute(
        f"""
        SELECT id, entry_json, title, authors, year, doi, source, publisher, url, source_pdf, ingest_source
        FROM ingest_entries
        WHERE {where}
        """,
        params,
    )
    rows = cur.fetchall()

    found_ids = {row[0] for row in rows}
    missing_ingest = [eid for eid in entry_ids if eid not in found_ids]

    marked = 0
    staged = 0
    already_processed = 0
    duplicates = 0
    errors: list[str] = []
    results: list[dict] = []

    for (
        ingest_id,
        entry_json,
        title,
        authors,
        year,
        doi,
        source,
        publisher,
        url,
        source_pdf,
        ingest_source,
    ) in rows:
        try:
            entry = json.loads(entry_json) if entry_json else {}
        except Exception:
            entry = {}

        # Fall back to structured columns if entry_json is absent.
        entry.setdefault("title", title)
        entry.setdefault("authors", json.loads(authors) if isinstance(authors, str) and authors.startswith("[") else authors)
        entry.setdefault("year", year)
        entry.setdefault("doi", doi)
        entry.setdefault("source", source)
        entry.setdefault("publisher", publisher)
        entry.setdefault("url", url)
        entry.setdefault("source_pdf", source_pdf)
        entry.setdefault("ingest_source", ingest_source)
        if args.corpus_id is not None:
            entry["corpus_id"] = args.corpus_id

        tbl, eid, field = db.check_if_exists(
            doi=entry.get("doi"),
            openalex_id=entry.get("openalex_id"),
            title=entry.get("title"),
            authors=entry.get("authors"),
            year=entry.get("year"),
        )

        if tbl == "no_metadata" and eid is not None:
            _mark_selected(db, int(eid), selected=1)
            marked += 1
            results.append({"ingest_entry_id": ingest_id, "action": "marked", "no_metadata_id": int(eid)})
            continue

        if tbl in ("with_metadata", "to_download_references", "downloaded_references") and eid is not None:
            # Do not skip stages: these are already past raw. We just report.
            already_processed += 1
            results.append(
                {
                    "ingest_entry_id": ingest_id,
                    "action": "already_processed",
                    "existing_table": tbl,
                    "existing_id": int(eid),
                    "matched_on": field,
                }
            )
            continue

        # Not staged yet: insert into no_metadata, then mark selected.
        no_meta_id, err = db.insert_no_metadata(entry, resolve_potential_duplicates=False)
        if no_meta_id:
            staged += 1
            _mark_selected(db, int(no_meta_id), selected=1)
            marked += 1
            results.append({"ingest_entry_id": ingest_id, "action": "staged_and_marked", "no_metadata_id": int(no_meta_id)})
        else:
            duplicates += 1
            results.append({"ingest_entry_id": ingest_id, "action": "duplicate_or_skipped", "message": err})
            if err and "Duplicate already exists" not in err and "Title is required" not in err:
                errors.append(err)

    for eid in missing_ingest:
        errors.append(f"Ingest entry not found (or not in corpus): {eid}")

    payload = {
        "requested": len(entry_ids),
        "found": len(rows),
        "marked": marked,
        "staged": staged,
        "already_processed": already_processed,
        "duplicates": duplicates,
        "errors": errors,
        "results": results,
        "source": "db",
    }
    print(json.dumps(payload))


if __name__ == "__main__":
    main()
