import argparse
import json
from _bootstrap import ensure_import_paths

ensure_import_paths(__file__)

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
    if selected:
        db.queue_for_enrichment(int(no_meta_id))
        return
    cur = db.conn.cursor()
    cur.execute(
        """UPDATE works
              SET metadata_status = 'pending',
                  metadata_claimed_by = NULL,
                  metadata_claimed_at = NULL,
                  metadata_lease_expires_at = NULL
            WHERE id = ?""",
        (no_meta_id,),
    )
    db.conn.commit()


def main():
    parser = argparse.ArgumentParser(
        description="Stage selected ingest_entries as pending works for enrichment."
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

    # Prefer entry_json for lossless re-staging if we need to insert missing works.
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
            editors=entry.get("editors"),
            year=entry.get("year"),
        )

        if tbl == "works" and eid is not None:
            work_row = db.get_entry_by_id("works", int(eid)) or {}
            if args.corpus_id is not None:
                db.add_corpus_item(args.corpus_id, "works", int(eid))
            metadata_status = str(work_row.get("metadata_status") or "pending").strip().lower()
            download_status = str(work_row.get("download_status") or "not_requested").strip().lower()
            if download_status == "downloaded":
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

            if metadata_status in {"pending", "failed"} and download_status in {"not_requested", ""}:
                _mark_selected(db, int(eid), selected=1)
                marked += 1
                results.append({"ingest_entry_id": ingest_id, "action": "marked", "work_id": int(eid)})
                continue

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

        # Not staged yet: insert into canonical works, then mark selected.
        work_id, err = db.create_pending_work(entry, resolve_potential_duplicates=False)
        if work_id:
            staged += 1
            _mark_selected(db, int(work_id), selected=1)
            marked += 1
            results.append({"ingest_entry_id": ingest_id, "action": "staged_and_marked", "work_id": int(work_id)})
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
