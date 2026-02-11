import argparse
import copy
import hashlib
import json
import os
import socket
import time
from pathlib import Path
from _bootstrap import ensure_import_paths

PROJECT_DIR = ensure_import_paths(__file__)

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


def _default_claimed_by() -> str:
    return f"backend:{socket.gethostname()}:{os.getpid()}:{int(time.time())}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Process a batch of queued download items (one-shot).")
    parser.add_argument("--db-path", required=True, help="Path to SQLite DB.")
    parser.add_argument("--limit", type=int, default=3, help="Number of queue entries to process.")
    parser.add_argument("--corpus-id", type=int, default=None, help="Corpus ID to scope the operation.")
    parser.add_argument("--lease-seconds", type=int, default=15 * 60, help="Claim lease duration to avoid churn on crash/stop.")
    parser.add_argument("--claimed-by", default="", help="Identifier for the worker claiming rows.")
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
        project_dir = PROJECT_DIR or (Path.cwd() / "dl_lit_project")
        download_dir = str(project_dir / "data" / "pdf_library")

    download_dir_path = Path(download_dir)
    download_dir_path.mkdir(parents=True, exist_ok=True)

    enhancer = BibliographyEnhancer(db_manager=db, rate_limiter=rl, email=args.mailto, output_folder=download_dir_path)

    claimed_by = (args.claimed_by or "").strip() or _default_claimed_by()
    rows = db.claim_download_batch(
        limit=limit,
        corpus_id=args.corpus_id,
        claimed_by=claimed_by,
        lease_seconds=int(args.lease_seconds),
    )
    if not rows:
        print(json.dumps({"processed": 0, "downloaded": 0, "failed": 0, "skipped": 0, "source": "db", "claimed_by": claimed_by}))
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
                "claimed_by": claimed_by,
            }
        )
    )


if __name__ == "__main__":
    main()
