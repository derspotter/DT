import argparse
import json
import sys
from pathlib import Path

def _ensure_import_paths() -> None:
    # When invoked from the Node backend, we prefer PYTHONPATH, but in case it is
    # misconfigured we defensively add the repo's `dl_lit_project` parent.
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
    # Stable de-dupe while preserving order.
    seen: set[int] = set()
    out: list[int] = []
    for i in ids:
        if i in seen:
            continue
        seen.add(i)
        out.append(i)
    return out


def _coerce_json_list(value):
    if value is None:
        return None
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return parsed
        except Exception:
            return [value]
    return [str(value)]


def _enqueue_no_metadata(db: DatabaseManager, no_meta_id: int, *, corpus_id: int | None):
    row = db.get_entry_by_id("no_metadata", no_meta_id)
    if not row:
        return ("error", None, f"Row not found in no_metadata: {no_meta_id}", None)

    parsed_data = {
        "title": row.get("title"),
        "authors": _coerce_json_list(row.get("authors")),
        "year": row.get("year"),
        "doi": row.get("doi"),
        "openalex_id": row.get("openalex_id"),
        "abstract": row.get("abstract"),
        "keywords": _coerce_json_list(row.get("keywords")),
        "source": row.get("source"),
        "volume": row.get("volume"),
        "issue": row.get("issue"),
        "pages": row.get("pages"),
        "publisher": row.get("publisher"),
        "url": row.get("url"),
        "source_work_id": row.get("source_work_id"),
        "relationship_type": row.get("relationship_type"),
        "entry_type": row.get("type"),
        "ingest_source": row.get("ingest_source"),
        "run_id": row.get("run_id"),
        "corpus_id": corpus_id,
    }

    status, item_id, message, existing_table = db.add_entry_to_download_queue(
        parsed_data,
        exclude_id=no_meta_id,
        exclude_table="no_metadata",
        corpus_id=corpus_id,
    )

    if status in ("added_to_queue", "duplicate") and item_id is not None:
        # Best-effort: if we managed to enqueue (or find an existing queued item),
        # remove the raw row so it doesn't keep showing up as "Raw".
        try:
            if status == "added_to_queue":
                db.update_work_alias_owner("no_metadata", no_meta_id, "to_download_references", item_id)
                db.reassign_corpus_items("no_metadata", no_meta_id, "to_download_references", item_id)
            else:
                db.delete_work_aliases("no_metadata", no_meta_id)
                if existing_table:
                    db.reassign_corpus_items("no_metadata", no_meta_id, existing_table, item_id)
            cur = db.conn.cursor()
            cur.execute("DELETE FROM no_metadata WHERE id = ?", (no_meta_id,))
            db.conn.commit()
        except Exception:
            # Keep going; the queue operation already succeeded.
            pass

    return (status, item_id, message, existing_table)


def main():
    parser = argparse.ArgumentParser(description="Enqueue selected ingest_entries rows for download processing.")
    parser.add_argument("--db-path", required=True, help="Path to SQLite DB.")
    parser.add_argument("--entry-ids", required=True, help="Comma-separated ingest_entries ids.")
    parser.add_argument("--corpus-id", type=int, default=None, help="Corpus ID to scope the operation.")
    args = parser.parse_args()

    entry_ids = _parse_ids(args.entry_ids)
    if not entry_ids:
        print(json.dumps({"requested": 0, "queued": 0, "duplicates": 0, "errors": ["No valid entry ids provided."]}))
        return

    db = DatabaseManager(db_path=args.db_path)
    cur = db.conn.cursor()

    placeholders = ",".join(["?"] * len(entry_ids))
    params: list[object] = list(entry_ids)
    where = f"id IN ({placeholders})"
    if args.corpus_id is not None:
        where += " AND corpus_id = ?"
        params.append(args.corpus_id)

    cur.execute(f"SELECT id, entry_json FROM ingest_entries WHERE {where}", params)
    rows = cur.fetchall()

    found_ids = {row[0] for row in rows}
    missing = [eid for eid in entry_ids if eid not in found_ids]

    queued = 0
    duplicates = 0
    errors: list[str] = []
    results: list[dict] = []

    for ingest_id, entry_json in rows:
        try:
            entry = json.loads(entry_json) if entry_json else {}
        except Exception:
            entry = {}
        if args.corpus_id is not None:
            entry["corpus_id"] = args.corpus_id

        # Prefer state transitions over "duplicate" logs when the work already exists in `no_metadata`.
        tbl, eid, _field = db.check_if_exists(
            doi=entry.get("doi"),
            openalex_id=entry.get("openalex_id"),
            title=entry.get("title"),
            authors=entry.get("authors"),
            year=entry.get("year"),
        )

        if tbl == "no_metadata" and eid is not None:
            status, item_id, message, existing_table = _enqueue_no_metadata(db, int(eid), corpus_id=args.corpus_id)
        elif tbl == "with_metadata" and eid is not None:
            item_id, err = db.enqueue_for_download(int(eid))
            if err:
                status, message, existing_table = ("duplicate", None, "with_metadata")
                item_id = int(eid)
            else:
                status, message, existing_table = ("added_to_queue", "Moved with_metadata to download queue.", None)
        else:
            status, item_id, message, existing_table = db.add_entry_to_download_queue(entry, corpus_id=args.corpus_id)
        results.append(
            {
                "ingest_entry_id": ingest_id,
                "status": status,
                "queue_id": item_id,
                "message": message,
                "existing_table": existing_table,
            }
        )
        if status == "added_to_queue":
            queued += 1
        elif status == "duplicate":
            duplicates += 1
        else:
            errors.append(message or f"Failed to enqueue ingest_entry {ingest_id}")

    for eid in missing:
        errors.append(f"Ingest entry not found (or not in corpus): {eid}")

    payload = {
        "requested": len(entry_ids),
        "found": len(rows),
        "queued": queued,
        "duplicates": duplicates,
        "errors": errors,
        "results": results,
        "source": "db",
    }
    print(json.dumps(payload))


if __name__ == "__main__":
    main()
