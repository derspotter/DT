import os
import sys

# Ensure imports work regardless of where the script is executed from
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../dl_lit_project')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
sys.path.append('/usr/src/app/dl_lit_project')
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))) # backend/scripts
sys.path.append('/usr/src/app/scripts')

import sqlite3
import json
import time
import argparse
import random
import socket
import hashlib
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from dl_lit.db_manager import DatabaseManager
from dl_lit.utils import get_global_rate_limiter
from dl_lit.OpenAlexScraper import OpenAlexCrossrefSearcher, process_single_reference
from dl_lit.new_dl import BibliographyEnhancer

# Import constants
from reference_expansion import (
    DEFAULT_MAX_RELATED,
    DEFAULT_RELATED_DEPTH,
    DEFAULT_RELATED_DEPTH_UPSTREAM,
    DEFAULT_INCLUDE_UPSTREAM,
)

def log(msg: str):
    timestamp = datetime.now().isoformat()
    formatted_msg = f"[{timestamp}] [DAEMON] {msg}"
    print(formatted_msg, flush=True)
    
    # Also append to the same pipeline log file that Node.js uses, so the UI can read it
    log_dir = os.environ.get("RAG_FEEDER_LOG_DIR", "/usr/src/app/logs")
    log_file = os.path.join(log_dir, "backend-pipeline.log")
    try:
        os.makedirs(log_dir, exist_ok=True)
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(formatted_msg + "\n")
    except Exception as e:
        print(f"[DAEMON ERROR] Failed to write to log file: {e}", file=sys.stderr)


def parse_duplicate_merge_marker(message: str | None) -> dict | None:
    """Parse DUPLICATE_MERGED marker emitted by db_manager promotion path."""
    if not message or "DUPLICATE_MERGED|" not in message:
        return None
    marker_text = message.split("DUPLICATE_MERGED|", 1)[1]
    marker_text = marker_text.rstrip("]").strip()
    data: dict[str, str] = {}
    for part in marker_text.split("|"):
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        data[key.strip()] = value.strip()
    return data or None

class PipelineDaemon:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.db = DatabaseManager(db_path)
        self.rate_limiter = get_global_rate_limiter()
        self.mailto = os.getenv("RAG_FEEDER_MAILTO", "spott@wzb.eu")
        self.searcher = OpenAlexCrossrefSearcher(mailto=self.mailto, rate_limiter=self.rate_limiter)
        
        project_dir = Path.cwd() / "dl_lit_project"
        self.download_dir = project_dir / "data" / "pdf_library"
        self.download_dir.mkdir(parents=True, exist_ok=True)
        
        self.enhancer = BibliographyEnhancer(
            db_manager=self.db, 
            rate_limiter=self.rate_limiter, 
            email=self.mailto, 
            output_folder=self.download_dir
        )
        self.running = True
        self.worker_id = f"daemon:{socket.gethostname()}:{os.getpid()}"
        self._requeue_running_jobs_on_startup()
        log(f"Initialized daemon worker {self.worker_id} at {db_path}")

    def _requeue_running_jobs_on_startup(self):
        """Requeue previously running jobs after daemon restarts."""
        cur = self.db.conn.cursor()
        cur.execute(
            """
            UPDATE pipeline_jobs
            SET status = 'pending', started_at = NULL
            WHERE status = 'running'
            """
        )
        requeued = int(cur.rowcount or 0)
        self.db.conn.commit()
        if requeued:
            log(f"Requeued {requeued} previously running pipeline job(s) on startup.")

    def fetch_next_job(self):
        """Fetch the next pending job from the queue with interactive-priority ordering."""
        cur = self.db.conn.cursor()
        cur.execute(
            """
            SELECT id, corpus_id, job_type, parameters_json
            FROM pipeline_jobs
            WHERE status = 'pending'
            ORDER BY
                CASE
                    WHEN job_type = 'enrich' AND corpus_id IS NOT NULL THEN 0
                    WHEN job_type = 'enrich' THEN 1
                    WHEN job_type = 'pipeline_tick' THEN 2
                    WHEN job_type = 'download' AND corpus_id IS NOT NULL THEN 3
                    WHEN job_type = 'download' THEN 4
                    ELSE 5
                END ASC,
                CASE
                    WHEN job_type = 'enrich' THEN created_at
                    ELSE NULL
                END DESC,
                created_at ASC
            LIMIT 1
            """
        )
        row = cur.fetchone()
        if not row:
            return None
            
        job_id, corpus_id, job_type, params_json = row
        
        cur.execute(
            "UPDATE pipeline_jobs SET status = 'running', started_at = CURRENT_TIMESTAMP WHERE id = ?", 
            (job_id,)
        )
        self.db.conn.commit()
        
        return {
            "id": job_id,
            "corpus_id": corpus_id,
            "job_type": job_type,
            "params": json.loads(params_json) if params_json else {}
        }

    def mark_job_completed(self, job_id: int, result: dict):
        cur = self.db.conn.cursor()
        cur.execute(
            "UPDATE pipeline_jobs SET status = 'completed', finished_at = CURRENT_TIMESTAMP, result_json = ? WHERE id = ? AND status = 'running'",
            (json.dumps(result), job_id)
        )
        self.db.conn.commit()
        return int(cur.rowcount or 0) > 0

    def mark_job_failed(self, job_id: int, error_msg: str):
        cur = self.db.conn.cursor()
        cur.execute(
            "UPDATE pipeline_jobs SET status = 'failed', finished_at = CURRENT_TIMESTAMP, result_json = ? WHERE id = ? AND status = 'running'",
            (json.dumps({"error": error_msg}), job_id)
        )
        self.db.conn.commit()
        return int(cur.rowcount or 0) > 0

    # --- MARKING LOGIC ---
    def do_mark(self, corpus_id: int, limit: int):
        cur = self.db.conn.cursor()
        if corpus_id is None:
            cur.execute("SELECT COUNT(*) FROM no_metadata WHERE COALESCE(selected_for_enrichment, 0) = 0")
            available = int(cur.fetchone()[0])
            cur.execute("SELECT id FROM no_metadata WHERE COALESCE(selected_for_enrichment, 0) = 0 ORDER BY id LIMIT ?", (limit,))
        else:
            cur.execute(
                """
                SELECT COUNT(*) FROM no_metadata nm
                JOIN corpus_items ci ON ci.table_name = 'no_metadata' AND ci.row_id = nm.id
                WHERE ci.corpus_id = ? AND COALESCE(nm.selected_for_enrichment, 0) = 0
                """, (corpus_id,)
            )
            available = int(cur.fetchone()[0])
            cur.execute(
                """
                SELECT nm.id FROM no_metadata nm
                JOIN corpus_items ci ON ci.table_name = 'no_metadata' AND ci.row_id = nm.id
                WHERE ci.corpus_id = ? AND COALESCE(nm.selected_for_enrichment, 0) = 0
                ORDER BY nm.id LIMIT ?
                """, (corpus_id, limit)
            )
        
        ids = [int(row[0]) for row in cur.fetchall()]
        if ids:
            placeholders = ",".join("?" for _ in ids)
            cur.execute(f"UPDATE no_metadata SET selected_for_enrichment = 1 WHERE id IN ({placeholders})", ids)
            self.db.conn.commit()
            
        return {"requested": limit, "marked": len(ids), "available": available, "ids": ids}

    # --- ENRICHMENT LOGIC ---
    def _enrich_single(self, entry, fetch_refs, fetch_cites, max_cites, rel_down, rel_up, max_rel):
        attempts = 3
        last_diag = {}
        for attempt in range(attempts):
            ref = {
                "title": entry.get("title"), "authors": entry.get("authors") or [],
                "doi": entry.get("doi"), "year": entry.get("year"),
                "container-title": entry.get("source"), "volume": entry.get("volume"),
                "issue": entry.get("issue"), "pages": entry.get("pages"),
                "abstract": entry.get("abstract"), "keywords": entry.get("keywords"),
            }
            enriched, diag = process_single_reference(
                ref, self.searcher, self.rate_limiter,
                fetch_references=fetch_refs, fetch_citations=fetch_cites,
                max_citations=max_cites, related_depth=rel_down,
                related_depth_upstream=rel_up, max_related_per_reference=max_rel, return_diagnostics=True
            )
            last_diag = diag or {}
            if enriched:
                return int(entry["id"]), enriched, {"status": "matched", "attempts": attempt + 1, "diagnostics": last_diag}
            if (diag or {}).get("status") != "api_error":
                break
            time.sleep(0.5 * (2 ** attempt) + random.uniform(0, 0.15))
            
        status = (last_diag or {}).get("status") or "no_match"
        return int(entry["id"]), None, {"status": status, "attempts": attempt + 1, "diagnostics": last_diag}

    def do_enrich(self, corpus_id: int, limit: int, workers: int, expansion: dict):
        cur = self.db.conn.cursor()
        if corpus_id is not None:
            cur.execute("""
                SELECT nm.id, nm.title, nm.authors, nm.year, nm.doi, nm.source, nm.volume, nm.issue, nm.pages,
                       nm.publisher, nm.type, nm.url, nm.isbn, nm.issn, nm.abstract, nm.keywords
                FROM no_metadata nm
                JOIN corpus_items ci ON ci.table_name = 'no_metadata' AND ci.row_id = nm.id
                WHERE ci.corpus_id = ? AND nm.selected_for_enrichment = 1
                ORDER BY nm.id LIMIT ?
            """, (corpus_id, limit))
        else:
            cur.execute("""
                SELECT id, title, authors, year, doi, source, volume, issue, pages,
                       publisher, type, url, isbn, issn, abstract, keywords
                FROM no_metadata WHERE selected_for_enrichment = 1 ORDER BY id LIMIT ?
            """, (limit,))
            
        cols = [d[0] for d in cur.description]
        to_process = []
        for r in cur.fetchall():
            row = dict(zip(cols, r))
            for key in ("authors", "keywords"):
                if isinstance(row.get(key), str):
                    try:
                        row[key] = json.loads(row[key])
                    except: pass
            to_process.append(row)

        if not to_process:
            return {
                "processed": 0,
                "promoted": 0,
                "queued": 0,
                "failed": 0,
                "duplicates_merged": 0,
                "results": [],
            }

        processed, promoted, queued, failed, duplicates_merged = 0, 0, 0, 0, 0
        results, errors = [], []
        
        fetch_refs = expansion.get("includeDownstream", True)
        fetch_cites = expansion.get("includeUpstream", False)
        max_cites = expansion.get("maxCitations", 100)
        rel_down = expansion.get("relatedDepthDownstream", 1)
        rel_up = expansion.get("relatedDepthUpstream", 1)
        max_rel = expansion.get("maxRelated", 30)

        with ThreadPoolExecutor(max_workers=min(workers, 32)) as executor:
            futures = {executor.submit(self._enrich_single, e, fetch_refs, fetch_cites, max_cites, rel_down, rel_up, max_rel): e for e in to_process}
            for future in as_completed(futures):
                entry = futures[future]
                no_meta_id = int(entry["id"])
                processed += 1
                try:
                    _, enriched, enrich_meta = future.result()
                except Exception as exc:
                    enriched, enrich_meta = None, {"status": "api_error"}
                    errors.append(str(exc))

                if not enriched:
                    failed += 1
                    reason = "Metadata fetch failed"
                    moved_id, err = self.db.move_no_meta_entry_to_failed(no_meta_id, reason)
                    results.append({"no_metadata_id": no_meta_id, "action": "failed_enrichment"})
                    continue

                wid, err = self.db.promote_to_with_metadata(
                    no_meta_id, enriched, expand_related=(rel_down > 1 and fetch_refs), max_related_per_source=max_rel
                )
                if not wid:
                    dup = parse_duplicate_merge_marker(err)
                    if dup:
                        duplicates_merged += 1
                        canonical_table = dup.get("table") or ""
                        canonical_id_raw = dup.get("id") or ""
                        queue_id_raw = dup.get("queue_id") or ""
                        queued_flag = (dup.get("queued") or "") == "1"
                        if canonical_table == "with_metadata" and queued_flag:
                            queued += 1
                        results.append(
                            {
                                "no_metadata_id": no_meta_id,
                                "action": "duplicate_merged",
                                "canonical_table": canonical_table,
                                "canonical_id": int(canonical_id_raw) if canonical_id_raw.isdigit() else None,
                                "queue_id": int(queue_id_raw) if queue_id_raw.isdigit() else None,
                                "queued": queued_flag,
                            }
                        )
                    else:
                        failed += 1
                        errors.append(err or f"Promotion failed for no_metadata:{no_meta_id}")
                        results.append({"no_metadata_id": no_meta_id, "action": "promotion_failed", "error": err})
                    # no_metadata row may already be merged away; this is a no-op in that case.
                    cur.execute("UPDATE no_metadata SET selected_for_enrichment = 0 WHERE id = ?", (no_meta_id,))
                    self.db.conn.commit()
                    continue

                promoted += 1
                qid, qerr = self.db.enqueue_for_download(int(wid))
                if qid:
                    queued += 1
                    results.append({"no_metadata_id": no_meta_id, "with_metadata_id": int(wid), "queue_id": int(qid), "action": "queued"})
                else:
                    results.append({"no_metadata_id": no_meta_id, "with_metadata_id": int(wid), "action": "enqueue_skipped"})

        return {
            "processed": processed,
            "promoted": promoted,
            "queued": queued,
            "failed": failed,
            "duplicates_merged": duplicates_merged,
            "errors": errors,
            "results": results,
        }

    def _download_single(self, row):
        qid = int(row["id"])
        authors_raw = row.get("authors")
        authors = []
        if isinstance(authors_raw, str):
            try: authors = json.loads(authors_raw)
            except: authors = [authors_raw]
        elif isinstance(authors_raw, list):
            authors = authors_raw

        dup_table, dup_id, dup_field = self.db.check_if_exists(
            doi=row.get("doi"), openalex_id=row.get("openalex_id"), title=row.get("title"),
            authors=authors, editors=row.get("editors"), year=row.get("year"), exclude_id=qid, exclude_table="with_metadata"
        )
        if dup_table == "downloaded_references" and dup_id is not None:
            ok, msg = self.db.drop_queue_entry_as_duplicate(qid, dup_table, dup_id, dup_field)
            return "skipped", {"queue_id": qid, "action": "dropped_duplicate"}

        ref = {
            "title": row.get("title"),
            "authors": authors,
            "doi": row.get("doi"),
            "type": row.get("type") or row.get("entry_type") or "",
            "year": row.get("year"),
            "open_access_url": row.get("url_source") or row.get("url"),
            "openalex_json": row.get("openalex_json"),
        }
        
        try:
            result = self.enhancer.enhance_bibliography(ref, self.download_dir)
        except Exception as exc:
            return "error", str(exc)

        if result and result.get("downloaded_file"):
            fp = str(result["downloaded_file"])
            checksum = ""
            try:
                with open(fp, "rb") as fh: checksum = hashlib.sha256(fh.read()).hexdigest()
            except: pass
            
            downloaded_id, move_error = self.db.move_entry_to_downloaded(qid, result, fp, checksum, result.get("download_source", "unknown"))
            if move_error or not downloaded_id:
                self.db.move_queue_entry_to_failed(qid, "move_to_downloaded_failed")
                return "failed", {"queue_id": qid, "action": "failed"}
                
            return "downloaded", {"queue_id": qid, "action": "downloaded", "downloaded_id": int(downloaded_id)}
        else:
            self.db.move_queue_entry_to_failed(qid, "download_failed")
            return "failed", {"queue_id": qid, "action": "failed"}

    # --- DOWNLOAD LOGIC ---
    def do_download(self, corpus_id: int, limit: int):
        # Prevent one very large queued job (e.g. batchSize=3000) from monopolizing
        # the single daemon loop for a long period.
        per_job_max = int(os.getenv("RAG_FEEDER_DOWNLOAD_JOB_MAX", "150") or "150")
        per_job_max = max(1, min(per_job_max, 5000))
        effective_limit = min(max(1, int(limit or 1)), per_job_max)
        if effective_limit != limit:
            log(f"Clamped download job batch from {limit} to {effective_limit} (RAG_FEEDER_DOWNLOAD_JOB_MAX={per_job_max})")

        rows = self.db.claim_download_batch(limit=effective_limit, corpus_id=corpus_id, claimed_by=self.worker_id, lease_seconds=900)
        if not rows:
            return {"processed": 0, "downloaded": 0, "failed": 0, "skipped": 0, "results": []}

        processed, downloaded, failed, skipped = 0, 0, 0, 0
        results, errors = [], []

        # Download multiple files concurrently. Be careful not to overwhelm the disk or rate limiters.
        # Max out at min(limit, configured cap) parallel downloads.
        # Keep env configurable so operators can tune throughput without code changes.
        max_workers_cfg = int(os.getenv("RAG_FEEDER_DOWNLOAD_WORKERS", "16") or "16")
        max_workers_cfg = max(1, min(max_workers_cfg, 128))
        max_download_workers = min(len(rows), max_workers_cfg)
        
        with ThreadPoolExecutor(max_workers=max_download_workers) as executor:
            futures = {executor.submit(self._download_single, row): row for row in rows}
            for future in as_completed(futures):
                processed += 1
                try:
                    status, data = future.result()
                    if status == "skipped":
                        skipped += 1
                        results.append(data)
                    elif status == "downloaded":
                        downloaded += 1
                        results.append(data)
                    elif status == "failed":
                        failed += 1
                        results.append(data)
                    elif status == "error":
                        failed += 1
                        errors.append(data)
                except Exception as exc:
                    failed += 1
                    errors.append(str(exc))

        return {"processed": processed, "downloaded": downloaded, "failed": failed, "skipped": skipped, "errors": errors, "results": results}

    def run(self):
        log("Pipeline Daemon starting. Waiting for jobs...")
        while self.running:
            try:
                job = self.fetch_next_job()
                if not job:
                    time.sleep(5)
                    continue
                    
                log(f"Picked up job {job['id']} of type '{job['job_type']}'")
                corpus_id = job.get("corpus_id")
                params = job.get("params", {})
                result = None
                
                try:
                    if job["job_type"] == "enrich":
                        limit = params.get("limit", 10)
                        workers = params.get("workers", 6)
                        expansion = params.get("expansion", {})
                        result = self.do_enrich(corpus_id, limit, workers, expansion)
                        
                    elif job["job_type"] == "download":
                        limit = params.get("batchSize", 3)
                        result = self.do_download(corpus_id, limit)
                        
                    elif job["job_type"] == "pipeline_tick":
                        cfg = params.get("config", {})
                        limit = cfg.get("promoteBatchSize", 10)
                        workers = cfg.get("promoteWorkers", 6)
                        dl_batch = cfg.get("downloadBatchSize", 3)
                        
                        m_res = self.do_mark(corpus_id, limit)
                        e_res = self.do_enrich(corpus_id, limit, workers, {})
                        d_res = self.do_download(corpus_id, dl_batch)
                        
                        result = {"marked": m_res, "enriched": e_res, "downloaded": d_res}
                        
                    else:
                        raise ValueError(f"Unknown job type: {job['job_type']}")
                        
                    updated = self.mark_job_completed(job["id"], result or {})
                    if updated:
                        log(f"Successfully completed job {job['id']}")
                    else:
                        log(f"Job {job['id']} finished execution but was already cancelled; completion not recorded.")
                    
                except Exception as e:
                    import traceback
                    err_msg = traceback.format_exc()
                    log(f"Job {job['id']} failed: {e}")
                    updated = self.mark_job_failed(job["id"], err_msg)
                    if not updated:
                        log(f"Job {job['id']} failure not recorded because it was already cancelled.")
                    
            except KeyboardInterrupt:
                log("Received shutdown signal. Exiting gracefully...")
                self.running = False
            except Exception as e:
                log(f"Critical error in main loop: {e}")
                time.sleep(10)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DT Pipeline Daemon")
    parser.add_argument("--db-path", required=True, help="Path to SQLite DB")
    args = parser.parse_args()
    
    daemon = PipelineDaemon(args.db_path)
    daemon.run()
