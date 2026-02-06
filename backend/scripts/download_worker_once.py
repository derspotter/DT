import argparse
import copy
import hashlib
import json
import os
import sys
from pathlib import Path


def _ensure_import_paths() -> None:
    # Prefer PYTHONPATH (set by backend), but defensively add repo roots for docker/local runs.
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
from dl_lit.new_dl import BibliographyEnhancer  # noqa: E402
from dl_lit.utils import get_global_rate_limiter  # noqa: E402


def _parse_authors(value) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        # Sometimes we store list[str] already.
        return [str(v).strip() for v in value if str(v).strip()]
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                return [str(v).strip() for v in parsed if str(v).strip()]
        except Exception:
            pass
        return [raw]
    return [str(value)]


def _fetch_queue_rows(db: DatabaseManager, *, corpus_id: int | None, limit: int) -> list[dict]:
    cur = db.conn.cursor()
    cur.row_factory = None
    if corpus_id is not None:
        cur.execute(
            """
            SELECT t.id, t.title, t.authors, t.year, t.doi, t.entry_type
            FROM to_download_references t
            JOIN corpus_items ci
              ON ci.table_name = 'to_download_references'
             AND ci.row_id = t.id
            WHERE ci.corpus_id = ?
            ORDER BY t.date_added ASC
            LIMIT ?
            """,
            (corpus_id, limit),
        )
    else:
        cur.execute(
            """
            SELECT id, title, authors, year, doi, entry_type
            FROM to_download_references
            ORDER BY date_added ASC
            LIMIT ?
            """,
            (limit,),
        )
    rows = cur.fetchall()
    out = []
    for r in rows:
        out.append(
            {
                "id": r[0],
                "title": r[1],
                "authors": r[2],
                "year": r[3],
                "doi": r[4],
                "entry_type": r[5],
            }
        )
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Process a batch of queued download items (one-shot).")
    parser.add_argument("--db-path", required=True, help="Path to SQLite DB.")
    parser.add_argument("--limit", type=int, default=3, help="Number of queue entries to process.")
    parser.add_argument("--corpus-id", type=int, default=None, help="Corpus ID to scope the operation.")
    parser.add_argument(
        "--download-dir",
        default="",
        help="Directory to store downloaded PDFs. Defaults to dl_lit_project/data/pdf_library.",
    )
    parser.add_argument("--mailto", default="spott@wzb.eu", help="Email for API politeness.")
    args = parser.parse_args()

    limit = int(args.limit or 0)
    if limit <= 0:
        print(json.dumps({"processed": 0, "downloaded": 0, "failed": 0, "skipped": 0, "errors": ["limit must be > 0"]}))
        return

    db = DatabaseManager(db_path=args.db_path)
    rl = get_global_rate_limiter()

    # Single shared storage across corpora unless overridden.
    download_dir = args.download_dir.strip()
    if not download_dir:
        # Find repo root from this script and use dl_lit_project/data/pdf_library.
        here = Path(__file__).resolve()
        cursor = here.parent
        repo_root = None
        while True:
            if (cursor / "dl_lit_project").exists():
                repo_root = cursor
                break
            if cursor.parent == cursor:
                break
            cursor = cursor.parent
        repo_root = repo_root or Path.cwd()
        download_dir = str(repo_root / "dl_lit_project" / "data" / "pdf_library")

    download_dir_path = Path(download_dir)
    download_dir_path.mkdir(parents=True, exist_ok=True)

    enhancer = BibliographyEnhancer(db_manager=db, rate_limiter=rl, email=args.mailto, output_folder=download_dir_path)

    rows = _fetch_queue_rows(db, corpus_id=args.corpus_id, limit=limit)
    if not rows:
        print(json.dumps({"processed": 0, "downloaded": 0, "failed": 0, "skipped": 0, "source": "db"}))
        db.close_connection()
        return

    processed = 0
    downloaded = 0
    failed = 0
    skipped = 0
    errors: list[str] = []
    results: list[dict] = []

    try:
        for row in rows:
            processed += 1
            qid = int(row["id"])
            title = (row.get("title") or "").strip()
            print(f"[DOWNLOAD] qid={qid} {title[:80]}", flush=True)

            authors = _parse_authors(row.get("authors"))

            # Drop queue entry if already in downloaded_references.
            dup_table, dup_id, dup_field = db.check_if_exists(
                doi=row.get("doi"),
                openalex_id=row.get("openalex_id"),
                title=row.get("title"),
                authors=authors,
                year=row.get("year"),
                exclude_id=qid,
                exclude_table="to_download_references",
            )
            if dup_table == "downloaded_references" and dup_id is not None:
                ok, msg = db.drop_queue_entry_as_duplicate(qid, dup_table, dup_id, dup_field)
                skipped += 1
                results.append({"queue_id": qid, "action": "dropped_duplicate", "message": msg})
                print(f"[DOWNLOAD] duplicate -> {msg}", flush=True)
                continue

            ref = {
                "title": row.get("title"),
                "authors": authors,
                "doi": row.get("doi"),
                "type": row.get("entry_type") or "",
                "year": row.get("year"),
            }

            try:
                result = enhancer.enhance_bibliography(copy.deepcopy(ref), download_dir_path)
            except Exception as exc:
                result = None
                errors.append(f"enhance_bibliography failed for qid={qid}: {exc}")

            if result and result.get("downloaded_file"):
                fp = str(result["downloaded_file"])
                checksum = ""
                try:
                    with open(fp, "rb") as fh:
                        checksum = hashlib.sha256(fh.read()).hexdigest()
                except Exception:
                    pass
                db.move_entry_to_downloaded(qid, result, fp, checksum, result.get("download_source", "unknown"))
                downloaded += 1
                results.append({"queue_id": qid, "action": "downloaded", "file_path": fp, "source": result.get("download_source")})
                print(f"[DOWNLOAD] ok source={result.get('download_source', '?')}", flush=True)
            else:
                db.move_queue_entry_to_failed(qid, "download_failed")
                failed += 1
                results.append({"queue_id": qid, "action": "failed"})
                print("[DOWNLOAD] failed", flush=True)
    finally:
        db.close_connection()

    print(
        json.dumps(
            {
                "processed": processed,
                "downloaded": downloaded,
                "failed": failed,
                "skipped": skipped,
                "errors": errors,
                "results": results,
                "source": "db",
            }
        )
    )


if __name__ == "__main__":
    main()

