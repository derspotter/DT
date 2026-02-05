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
