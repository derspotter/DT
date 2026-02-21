import argparse
import json
import os
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from _bootstrap import ensure_import_paths

ensure_import_paths(__file__)

from dl_lit.db_manager import DatabaseManager  # noqa: E402
from dl_lit.OpenAlexScraper import (  # noqa: E402
    OpenAlexCrossrefSearcher,
    process_single_reference,
)
from dl_lit.utils import get_global_rate_limiter  # noqa: E402
from reference_expansion import (
    DEFAULT_MAX_RELATED,
    DEFAULT_RELATED_DEPTH,
)


_thread_local = threading.local()


def _get_thread_searcher(mailto: str, rate_limiter):
    searcher = getattr(_thread_local, "searcher", None)
    if searcher is None:
        searcher = OpenAlexCrossrefSearcher(mailto=mailto, rate_limiter=rate_limiter)
        _thread_local.searcher = searcher
    return searcher


def _rows_to_process(db: DatabaseManager, *, corpus_id: int | None, limit: int) -> list[dict]:
    cur = db.conn.cursor()
    if corpus_id is not None:
        cur.execute(
            """
            SELECT nm.id, nm.title, nm.authors, nm.year, nm.doi, nm.source, nm.volume, nm.issue, nm.pages,
                   nm.publisher, nm.type, nm.url, nm.isbn, nm.issn, nm.abstract, nm.keywords
            FROM no_metadata nm
            JOIN corpus_items ci ON ci.table_name = 'no_metadata' AND ci.row_id = nm.id
            WHERE ci.corpus_id = ? AND nm.selected_for_enrichment = 1
            ORDER BY nm.id
            LIMIT ?
            """,
            (corpus_id, limit),
        )
    else:
        cur.execute(
            """
            SELECT id, title, authors, year, doi, source, volume, issue, pages,
                   publisher, type, url, isbn, issn, abstract, keywords
            FROM no_metadata
            WHERE selected_for_enrichment = 1
            ORDER BY id
            LIMIT ?
            """,
            (limit,),
        )

    cols = [d[0] for d in cur.description]
    rows = []
    for r in cur.fetchall():
        row = dict(zip(cols, r))
        # Normalize json-ish fields to python lists where possible.
        for key in ("authors", "keywords"):
            val = row.get(key)
            if isinstance(val, str):
                try:
                    parsed = json.loads(val)
                    row[key] = parsed
                except Exception:
                    pass
        rows.append(row)
    return rows


def _unmark(db: DatabaseManager, no_meta_id: int) -> None:
    cur = db.conn.cursor()
    cur.execute("UPDATE no_metadata SET selected_for_enrichment = 0 WHERE id = ?", (no_meta_id,))
    db.conn.commit()


def _build_ref_for_scraper(entry: dict) -> dict:
    return {
        "title": entry.get("title"),
        "authors": entry.get("authors") or [],
        "doi": entry.get("doi"),
        "year": entry.get("year"),
        "container-title": entry.get("source"),
        "volume": entry.get("volume"),
        "issue": entry.get("issue"),
        "pages": entry.get("pages"),
        "abstract": entry.get("abstract"),
        "keywords": entry.get("keywords"),
    }


def _enrich_entry(entry: dict, args, rate_limiter):
    searcher = _get_thread_searcher(args.mailto, rate_limiter)
    attempts = max(1, int(args.api_retries) + 1)
    last_diag = {}
    attempts_used = 0
    for attempt in range(attempts):
        attempts_used = attempt + 1
        enriched, diag = process_single_reference(
            _build_ref_for_scraper(entry),
            searcher,
            rate_limiter,
            fetch_references=args.fetch_references,
            fetch_citations=args.fetch_citations,
            max_citations=args.max_citations,
            related_depth=args.related_depth,
            max_related_per_reference=args.max_related,
            return_diagnostics=True,
        )
        last_diag = diag or {}
        if enriched:
            return int(entry["id"]), enriched, {"status": "matched", "attempts": attempt + 1, "diagnostics": last_diag}
        if (diag or {}).get("status") != "api_error":
            break
        if attempt < attempts - 1:
            delay = float(args.api_retry_base_delay) * (2 ** attempt) + random.uniform(0, 0.15)
            time.sleep(max(0.0, delay))
    status = (last_diag or {}).get("status") or "no_match"
    return int(entry["id"]), None, {"status": status, "attempts": attempts_used, "diagnostics": last_diag}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Enrich + enqueue marked no_metadata rows (selected_for_enrichment=1) without skipping stages."
    )
    parser.add_argument("--db-path", required=True, help="Path to SQLite DB.")
    parser.add_argument("--limit", type=int, default=10, help="Max number of marked rows to process.")
    parser.add_argument("--corpus-id", type=int, default=None, help="Corpus ID to scope the operation.")
    parser.add_argument("--mailto", default="spott@wzb.eu", help="Email for API politeness.")
    parser.add_argument(
        "--fetch-references",
        action="store_true",
        default=False,
        help="Fetch referenced works from OpenAlex (can be API intensive).",
    )
    parser.add_argument(
        "--no-fetch-references",
        action="store_false",
        dest="fetch_references",
        help="Disable referenced works fetching.",
    )
    parser.add_argument(
        "--fetch-citations",
        action="store_true",
        default=False,
        help="Fetch citing works from OpenAlex (API intensive).",
    )
    parser.add_argument("--max-citations", type=int, default=100, help="Max citing works per paper.")
    parser.add_argument("--related-depth", type=int, default=DEFAULT_RELATED_DEPTH, help="Reference expansion depth.")
    parser.add_argument("--max-related", type=int, default=DEFAULT_MAX_RELATED, help="Max nested references per work.")
    parser.add_argument(
        "--api-retries",
        type=int,
        default=max(0, int(os.getenv("RAG_FEEDER_ENRICH_API_RETRIES", "2"))),
        help="Retry attempts for transient API errors (added on top of initial attempt).",
    )
    parser.add_argument(
        "--api-retry-base-delay",
        type=float,
        default=float(os.getenv("RAG_FEEDER_ENRICH_API_RETRY_BASE_DELAY", "0.5")),
        help="Base delay in seconds for exponential backoff on API-error retries.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=max(1, int(os.getenv("RAG_FEEDER_ENRICH_WORKERS", "6"))),
        help="Parallel enrichment workers (API-bound).",
    )
    args = parser.parse_args()

    if args.limit <= 0:
        print(json.dumps({"processed": 0, "promoted": 0, "queued": 0, "failed": 0, "errors": ["limit must be > 0"]}))
        return

    db = DatabaseManager(args.db_path)
    rate_limiter = get_global_rate_limiter()

    to_process = _rows_to_process(db, corpus_id=args.corpus_id, limit=args.limit)
    if not to_process:
        print(json.dumps({"processed": 0, "promoted": 0, "queued": 0, "failed": 0, "source": "db"}))
        db.close_connection()
        return

    processed = 0
    promoted = 0
    queued = 0
    failed = 0
    errors: list[str] = []
    results: list[dict] = []

    workers = max(1, min(32, int(args.workers or 1)))
    try:
        futures = {}
        with ThreadPoolExecutor(max_workers=workers) as executor:
            for entry in to_process:
                future = executor.submit(_enrich_entry, entry, args, rate_limiter)
                futures[future] = entry

            for future in as_completed(futures):
                entry = futures[future]
                no_meta_id = int(entry["id"])
                title = (entry.get("title") or "").strip()
                print(f"[PROCESS] no_metadata:{no_meta_id} {title[:80]}", flush=True)
                processed += 1

                try:
                    _, enriched, enrich_meta = future.result()
                except Exception as exc:
                    enriched = None
                    enrich_meta = {"status": "api_error", "attempts": 1, "diagnostics": {"api_errors": [{"error": str(exc)}]}}
                    errors.append(f"no_metadata:{no_meta_id} enrichment exception: {exc}")

                if not enriched:
                    failed += 1
                    failure_kind = (enrich_meta or {}).get("status") or "no_match"
                    attempts_used = int((enrich_meta or {}).get("attempts") or 1)
                    api_errors = ((enrich_meta or {}).get("diagnostics") or {}).get("api_errors") or []
                    api_error_text = "; ".join(str(item.get("error") or "") for item in api_errors if isinstance(item, dict))
                    api_error_text = api_error_text[:400]
                    if failure_kind == "api_error":
                        reason = f"Metadata API error after retries ({attempts_used} attempts): {api_error_text or 'unknown API error'}"
                    else:
                        reason = "Metadata fetch failed (no match found in OpenAlex/Crossref)"
                    moved_id, err = db.move_no_meta_entry_to_failed(no_meta_id, reason)
                    if not moved_id:
                        errors.append(err or f"Failed to move no_metadata:{no_meta_id} to failed_enrichments")
                    results.append(
                        {
                            "no_metadata_id": no_meta_id,
                            "action": "failed_enrichment",
                            "failure_kind": failure_kind,
                            "attempts": attempts_used,
                        }
                    )
                    continue

                wid, err = db.promote_to_with_metadata(
                    no_meta_id,
                    enriched,
                    expand_related=args.related_depth > 1 and args.fetch_references,
                    max_related_per_source=args.max_related,
                )
                if not wid:
                    failed += 1
                    errors.append(err or f"Promotion failed for no_metadata:{no_meta_id}")
                    results.append({"no_metadata_id": no_meta_id, "action": "promotion_failed", "error": err})
                    # Ensure we don't keep re-processing a stuck "marked" item forever.
                    _unmark(db, no_meta_id)
                    continue

                promoted += 1

                qid, qerr = db.enqueue_for_download(int(wid))
                if qid:
                    queued += 1
                    results.append(
                        {"no_metadata_id": no_meta_id, "with_metadata_id": int(wid), "queue_id": int(qid), "action": "queued"}
                    )
                else:
                    # enqueue_for_download already handles dupes; either way, the with_metadata row is removed.
                    results.append(
                        {"no_metadata_id": no_meta_id, "with_metadata_id": int(wid), "action": "enqueue_skipped", "error": qerr}
                    )

    except Exception as exc:
        errors.append(str(exc))
    finally:
        db.close_connection()

    print(
        json.dumps(
            {
                "processed": processed,
                "promoted": promoted,
                "queued": queued,
                "failed": failed,
                "errors": errors,
                "results": results,
                "workers": workers,
                "source": "db",
            }
        )
    )


if __name__ == "__main__":
    main()
