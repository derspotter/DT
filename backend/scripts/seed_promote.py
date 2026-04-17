import argparse
import json
from pathlib import Path

from _bootstrap import ensure_import_paths

ensure_import_paths(__file__)

from dl_lit.db_manager import DatabaseManager  # noqa: E402


def _ensure_seed_membership_schema(db: DatabaseManager) -> None:
    return None


def _parse_candidates(value: str | None = None, file_path: str | None = None):
    if file_path:
        try:
            value = Path(file_path).read_text(encoding="utf-8")
        except Exception:
            return []
    if not value:
        return []
    try:
        parsed = json.loads(value)
    except Exception:
        return []
    return parsed if isinstance(parsed, list) else []


def _parse_name_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []
        if raw.startswith("[") and raw.endswith("]"):
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, list):
                    return [str(item).strip() for item in parsed if str(item).strip()]
            except Exception:
                pass
        return [raw]
    return [str(value).strip()] if str(value).strip() else []


def _mark_selected(db: DatabaseManager, no_meta_id: int, selected: int = 1) -> None:
    if selected:
        db.queue_for_enrichment(int(no_meta_id))
        return
    db.reset_enrich_claim(int(no_meta_id), state="pending", error=None, selected=0)


def _candidate_ref(candidate: dict, corpus_id: int) -> dict:
    authors = _parse_name_list(candidate.get("authors"))
    ref = {
        "title": candidate.get("title"),
        "authors": authors,
        "year": candidate.get("year"),
        "doi": candidate.get("doi"),
        "openalex_id": candidate.get("openalex_id"),
        "source": candidate.get("source"),
        "publisher": candidate.get("publisher"),
        "url": candidate.get("url"),
        "type": candidate.get("type"),
        "abstract": candidate.get("abstract"),
        "keywords": candidate.get("keywords"),
        "source_pdf": candidate.get("source_pdf"),
        "ingest_source": candidate.get("ingest_source"),
        "run_id": candidate.get("run_id"),
        "corpus_id": corpus_id,
    }
    return ref


def _mark_candidate_in_corpus(db: DatabaseManager, corpus_id: int, candidate: dict) -> None:
    return None


def _add_existing_to_corpus(db: DatabaseManager, corpus_id: int, table_name: str, row_id: int) -> None:
    if corpus_id and table_name and row_id:
        try:
            db.add_corpus_item(corpus_id, table_name, row_id)
        except Exception:
            pass


def _merge_candidate_into_existing(db: DatabaseManager, table_name: str, row_id: int, ref: dict, matched_on: str | None) -> None:
    try:
        if table_name != "works":
            return
        existing = db.get_entry_by_id("works", int(row_id)) or {}
        incoming = db._build_raw_work_payload(ref)  # noqa: SLF001
        merged = db._apply_work_merge_priority(existing, incoming)  # noqa: SLF001
        db._update_work_record(int(row_id), merged)  # noqa: SLF001
        db.conn.commit()
    except Exception:
        pass


def _handle_existing(db: DatabaseManager, corpus_id: int, table_name: str, row_id: int, *, enqueue_download: bool, ref: dict | None = None, matched_on: str | None = None):
    _add_existing_to_corpus(db, corpus_id, table_name, row_id)
    if ref:
        _merge_candidate_into_existing(db, table_name, row_id, ref, matched_on)
    row_id = int(row_id)
    if table_name == "works":
        work = db.get_entry_by_id("works", row_id) or {}
        metadata_status = str(work.get("metadata_status") or "pending").strip().lower()
        download_status = str(work.get("download_status") or "not_requested").strip().lower()
        if download_status == "downloaded":
            return {
                "action": "already_downloaded",
                "work_id": row_id,
                "downloaded_id": row_id,
            }, {"pending_work_id": None, "download_queued": False}
        if download_status in {"queued", "in_progress"}:
            return {
                "action": "already_queued_download",
                "work_id": row_id,
                "queue_id": row_id,
            }, {"pending_work_id": None, "download_queued": True}
        if download_status == "failed":
            return {
                "action": "failed_download_existing",
                "work_id": row_id,
                "failed_id": row_id,
            }, {"pending_work_id": None, "download_queued": False}
        if metadata_status == "failed":
            return {
                "action": "failed_enrichment_existing",
                "work_id": row_id,
                "failed_id": row_id,
            }, {"pending_work_id": None, "download_queued": False}
        if metadata_status == "matched":
            queued_download = False
            queue_id = None
            queue_error = None
            if enqueue_download:
                queue_id, queue_error = db.enqueue_for_download(row_id)
                queued_download = bool(queue_id)
            return {
                "action": "already_enriched",
                "work_id": row_id,
                "matched_work_id": row_id,
                "download_queued": queued_download,
                "queue_id": int(queue_id) if queue_id else None,
                "queue_error": queue_error,
            }, {"pending_work_id": None, "download_queued": queued_download}
        _mark_selected(db, row_id, selected=1)
        return {
            "action": "marked_existing_raw",
            "work_id": row_id,
            "pending_work_id": row_id,
        }, {"pending_work_id": row_id, "download_queued": False}

    return {
        "action": "already_present",
        "existing_table": table_name,
        "existing_id": row_id,
    }, {"pending_work_id": None, "download_queued": False}


def main():
    parser = argparse.ArgumentParser(description="Promote seed candidates into the corpus pipeline.")
    parser.add_argument("--db-path", required=True, help="Path to SQLite DB.")
    parser.add_argument("--corpus-id", type=int, required=True, help="Corpus ID.")
    parser.add_argument("--candidates-json", default=None, help="JSON list of normalized seed candidates.")
    parser.add_argument("--candidates-file", default=None, help="Path to JSON file with normalized seed candidates.")
    parser.add_argument("--enqueue-download", action="store_true", help="Queue existing matched works for download.")
    args = parser.parse_args()

    candidates = _parse_candidates(args.candidates_json, args.candidates_file)
    if not candidates:
        print(json.dumps({"requested": 0, "promoted": 0, "errors": ["No valid candidates provided."], "source": "db"}))
        return

    db = DatabaseManager(db_path=args.db_path)
    _ensure_seed_membership_schema(db)
    pending_work_ids: list[int] = []
    requested = 0
    staged_raw = 0
    marked_existing_raw = 0
    already_enriched = 0
    already_downloaded = 0
    already_queued_download = 0
    failed_enrichment_existing = 0
    failed_download_existing = 0
    queued_existing_download = 0
    errors: list[str] = []
    results: list[dict] = []

    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        requested += 1
        ref = _candidate_ref(candidate, args.corpus_id)
        tbl, eid, field = db.check_if_exists(
            doi=ref.get("doi"),
            openalex_id=ref.get("openalex_id"),
            title=ref.get("title"),
            authors=ref.get("authors"),
            editors=ref.get("editors"),
            year=ref.get("year"),
        )

        if tbl and eid is not None:
            result, meta = _handle_existing(db, args.corpus_id, tbl, int(eid), enqueue_download=args.enqueue_download, ref=ref, matched_on=field)
            _mark_candidate_in_corpus(db, args.corpus_id, candidate)
            result.update(
                {
                    "candidate_key": candidate.get("candidate_key"),
                    "matched_on": field,
                    "existing_table": tbl,
                }
            )
            results.append(result)
            pending_work_id = meta.get("pending_work_id")
            if pending_work_id:
                marked_existing_raw += 1
                if pending_work_id not in pending_work_ids:
                    pending_work_ids.append(int(pending_work_id))
            if result["action"] == "already_enriched":
                already_enriched += 1
                if meta.get("download_queued"):
                    queued_existing_download += 1
            elif result["action"] == "already_downloaded":
                already_downloaded += 1
            elif result["action"] == "already_queued_download":
                already_queued_download += 1
            elif result["action"] == "failed_enrichment_existing":
                failed_enrichment_existing += 1
            elif result["action"] == "failed_download_existing":
                failed_download_existing += 1
            continue

        work_id, err = db.create_pending_work(ref, resolve_potential_duplicates=False)
        if work_id:
            work_id = int(work_id)
            _mark_selected(db, work_id, selected=1)
            _mark_candidate_in_corpus(db, args.corpus_id, candidate)
            staged_raw += 1
            if work_id not in pending_work_ids:
                pending_work_ids.append(work_id)
            results.append(
                {
                    "candidate_key": candidate.get("candidate_key"),
                    "action": "staged_raw",
                    "work_id": work_id,
                    "pending_work_id": work_id,
                }
            )
            continue

        # create_pending_work may have merged into an existing record; check again.
        tbl, eid, field = db.check_if_exists(
            doi=ref.get("doi"),
            openalex_id=ref.get("openalex_id"),
            title=ref.get("title"),
            authors=ref.get("authors"),
            editors=ref.get("editors"),
            year=ref.get("year"),
        )
        if tbl and eid is not None:
            result, meta = _handle_existing(db, args.corpus_id, tbl, int(eid), enqueue_download=args.enqueue_download, ref=ref, matched_on=field)
            _mark_candidate_in_corpus(db, args.corpus_id, candidate)
            result.update(
                {
                    "candidate_key": candidate.get("candidate_key"),
                    "matched_on": field,
                    "existing_table": tbl,
                    "merge_note": err,
                }
            )
            results.append(result)
            pending_work_id = meta.get("pending_work_id")
            if pending_work_id:
                marked_existing_raw += 1
                if pending_work_id not in pending_work_ids:
                    pending_work_ids.append(int(pending_work_id))
            if result["action"] == "already_enriched":
                already_enriched += 1
                if meta.get("download_queued"):
                    queued_existing_download += 1
            elif result["action"] == "already_downloaded":
                already_downloaded += 1
            elif result["action"] == "already_queued_download":
                already_queued_download += 1
            elif result["action"] == "failed_enrichment_existing":
                failed_enrichment_existing += 1
            elif result["action"] == "failed_download_existing":
                failed_download_existing += 1
            continue

        results.append(
            {
                "candidate_key": candidate.get("candidate_key"),
                "action": "error",
                "error": err or "Failed to stage candidate.",
            }
        )
        if err:
            errors.append(err)

    payload = {
        "requested": requested,
        "staged_raw": staged_raw,
        "marked_existing_raw": marked_existing_raw,
        "already_enriched": already_enriched,
        "already_downloaded": already_downloaded,
        "already_queued_download": already_queued_download,
        "failed_enrichment_existing": failed_enrichment_existing,
        "failed_download_existing": failed_download_existing,
        "queued_existing_download": queued_existing_download,
        "pending_work_ids": pending_work_ids,
        "results": results,
        "errors": errors,
        "source": "db",
    }
    print(json.dumps(payload, ensure_ascii=False))


if __name__ == "__main__":
    main()
