import argparse
import json
from _bootstrap import ensure_import_paths

ensure_import_paths(__file__)

from dl_lit.db_manager import DatabaseManager  # noqa: E402
from dl_lit.OpenAlexScraper import (  # noqa: E402
    OpenAlexCrossrefSearcher,
    process_single_reference,
)
from dl_lit.utils import get_global_rate_limiter  # noqa: E402


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
        default=True,
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
    parser.add_argument("--related-depth", type=int, default=1, help="Reference expansion depth.")
    parser.add_argument("--max-related", type=int, default=40, help="Max nested references per work.")
    args = parser.parse_args()

    if args.limit <= 0:
        print(json.dumps({"processed": 0, "promoted": 0, "queued": 0, "failed": 0, "errors": ["limit must be > 0"]}))
        return

    db = DatabaseManager(args.db_path)
    rl = get_global_rate_limiter()
    searcher = OpenAlexCrossrefSearcher(mailto=args.mailto)

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

    try:
        for entry in to_process:
            processed += 1
            no_meta_id = int(entry["id"])
            title = (entry.get("title") or "").strip()
            print(f"[PROCESS] no_metadata:{no_meta_id} {title[:80]}", flush=True)

            ref_for_scraper = {
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

            enriched = process_single_reference(
                ref_for_scraper,
                searcher,
                rl,
                fetch_references=args.fetch_references,
                fetch_citations=args.fetch_citations,
                max_citations=args.max_citations,
                related_depth=args.related_depth,
                max_related_per_reference=args.max_related,
            )

            if not enriched:
                failed += 1
                # Keep the pipeline invariant: failures leave the raw stage via failed_enrichments.
                reason = "Metadata fetch failed (no match found in OpenAlex/Crossref)"
                moved_id, err = db.move_no_meta_entry_to_failed(no_meta_id, reason)
                if not moved_id:
                    errors.append(err or f"Failed to move no_metadata:{no_meta_id} to failed_enrichments")
                results.append({"no_metadata_id": no_meta_id, "action": "failed_enrichment"})
                continue

            wid, err = db.promote_to_with_metadata(
                no_meta_id,
                enriched,
                expand_related=args.related_depth > 1,
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
                "source": "db",
            }
        )
    )


if __name__ == "__main__":
    main()
