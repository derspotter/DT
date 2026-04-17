import os
import sys
import threading


class _LockedTextStream:
    def __init__(self, stream, lock):
        self._stream = stream
        self._lock = lock
        self._local = threading.local()

    def _get_buffer(self):
        return getattr(self._local, "buffer", "")

    def _set_buffer(self, value):
        self._local.buffer = value

    def write(self, data):
        if not data:
            return 0
        buffer = self._get_buffer() + str(data)
        written = len(data)
        while "\n" in buffer:
            line, buffer = buffer.split("\n", 1)
            with self._lock:
                self._stream.write(line + "\n")
                self._stream.flush()
        self._set_buffer(buffer)
        return written

    def writelines(self, lines):
        total = 0
        for line in lines:
            total += self.write(line)
        return total

    def flush(self):
        buffer = self._get_buffer()
        with self._lock:
            if buffer:
                self._stream.write(buffer)
                self._set_buffer("")
            return self._stream.flush()

    def isatty(self):
        return self._stream.isatty()

    @property
    def encoding(self):
        return getattr(self._stream, "encoding", None)

    def __getattr__(self, name):
        return getattr(self._stream, name)


_STREAM_LOCK = threading.RLock()
sys.stdout = _LockedTextStream(sys.stdout, _STREAM_LOCK)
sys.stderr = _LockedTextStream(sys.stderr, _STREAM_LOCK)

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
from collections import Counter, defaultdict
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
    def __init__(self, db_path: str, role: str = "all"):
        self.db_path = db_path
        self.role = str(role or "all").strip().lower()
        self.db = DatabaseManager(db_path)
        self.heartbeat_conn = sqlite3.connect(db_path, timeout=3.0, check_same_thread=False)
        self.heartbeat_conn.execute("PRAGMA journal_mode=WAL")
        self.heartbeat_conn.execute("PRAGMA busy_timeout = 3000")
        self.heartbeat_lock = threading.RLock()
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
        self.db_lock = threading.RLock()
        self.running = True
        self.daemon_name = {
            "enrich": "enrich",
            "download": "download",
        }.get(self.role, "pipeline")
        self.heartbeat_interval_seconds = max(1, int(os.getenv("RAG_FEEDER_DAEMON_HEARTBEAT_SECONDS", "3") or "3"))
        self._last_heartbeat_at = 0.0
        self.worker_id = f"daemon:{socket.gethostname()}:{os.getpid()}"
        self._ensure_heartbeat_table()
        if self.role == "all":
            self._requeue_running_jobs_on_startup()
        self._heartbeat("starting", {"worker_id": self.worker_id}, force=True)
        log(f"Initialized {self.role} daemon worker {self.worker_id} at {db_path}")

    def _ensure_heartbeat_table(self):
        with self.heartbeat_lock:
            cur = self.heartbeat_conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS daemon_heartbeat (
                    daemon_name TEXT PRIMARY KEY,
                    worker_id TEXT,
                    status TEXT,
                    details_json TEXT,
                    last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            self.heartbeat_conn.commit()

    def _heartbeat(self, status: str, details: dict | None = None, force: bool = False):
        now = time.time()
        if not force and (now - self._last_heartbeat_at) < self.heartbeat_interval_seconds:
            return
        with self.heartbeat_lock:
            cur = self.heartbeat_conn.cursor()
            cur.execute(
                """
                INSERT INTO daemon_heartbeat (daemon_name, worker_id, status, details_json, last_seen_at, updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT(daemon_name) DO UPDATE SET
                    worker_id = excluded.worker_id,
                    status = excluded.status,
                    details_json = excluded.details_json,
                    last_seen_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    self.daemon_name,
                    self.worker_id,
                    str(status or "idle"),
                    json.dumps(details or {}),
                ),
            )
            self.heartbeat_conn.commit()
        self._last_heartbeat_at = now

    def _start_job_heartbeat(self, job: dict):
        stop_event = threading.Event()
        details = {
            "state": "running_job",
            "job_id": int(job["id"]),
            "job_type": str(job["job_type"]),
            "corpus_id": job.get("corpus_id"),
        }

        def beat():
            while not stop_event.wait(self.heartbeat_interval_seconds):
                self._heartbeat("running", details, force=True)

        thread = threading.Thread(target=beat, name=f"job-heartbeat-{job['id']}", daemon=True)
        thread.start()
        return stop_event, thread

    def _start_direct_batch_heartbeat(self, details: dict):
        stop_event = threading.Event()
        payload = {
            "state": "db_batch",
            "mode": "db-driven",
            "role": self.role,
            **(details or {}),
        }

        def beat():
            while not stop_event.wait(self.heartbeat_interval_seconds):
                self._heartbeat("running", payload, force=True)

        thread = threading.Thread(target=beat, name=f"direct-heartbeat-{self.role}", daemon=True)
        thread.start()
        return stop_event, thread

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
        role_filter = ""
        if self.role == "enrich":
            role_filter = "AND job_type IN ('enrich', 'pipeline_tick')"
        elif self.role == "download":
            role_filter = "AND job_type = 'download'"
        cur.execute(
            f"""
            SELECT id, corpus_id, job_type, parameters_json
            FROM pipeline_jobs
            WHERE status = 'pending'
              {role_filter}
            ORDER BY
                CASE
                    WHEN job_type = 'download' AND corpus_id IS NOT NULL THEN 0
                    WHEN job_type = 'enrich' AND corpus_id IS NOT NULL THEN 1
                    WHEN job_type = 'download' THEN 2
                    WHEN job_type = 'enrich' THEN 3
                    WHEN job_type = 'pipeline_tick' AND corpus_id IS NOT NULL THEN 4
                    WHEN job_type = 'pipeline_tick' THEN 5
                    ELSE 6
                END ASC,
                created_at ASC,
                id ASC
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

    def _has_pending_interactive_jobs(self) -> bool:
        cur = self.db.conn.cursor()
        cur.execute(
            """
            SELECT 1
            FROM pipeline_jobs
            WHERE status = 'pending'
              AND corpus_id IS NOT NULL
              AND job_type IN ('enrich', 'download')
            LIMIT 1
            """
        )
        return cur.fetchone() is not None

    def _has_pending_corpus_download_jobs(self) -> bool:
        cur = self.db.conn.cursor()
        cur.execute(
            """
            SELECT 1
            FROM pipeline_jobs
            WHERE status = 'pending'
              AND corpus_id IS NOT NULL
              AND job_type = 'download'
            LIMIT 1
            """
        )
        return cur.fetchone() is not None

    def mark_job_completed(self, job_id: int, result: dict):
        cur = self.db.conn.cursor()
        cur.execute(
            "UPDATE pipeline_jobs SET status = 'completed', finished_at = CURRENT_TIMESTAMP, result_json = ? WHERE id = ? AND status = 'running'",
            (json.dumps(result), job_id)
        )
        self.db.conn.commit()
        return int(cur.rowcount or 0) > 0

    def enqueue_job(self, corpus_id: int | None, job_type: str, params: dict) -> int:
        cur = self.db.conn.cursor()
        cur.execute(
            "INSERT INTO pipeline_jobs (corpus_id, job_type, status, parameters_json) VALUES (?, ?, 'pending', ?)",
            (corpus_id, job_type, json.dumps(params or {})),
        )
        self.db.conn.commit()
        return int(cur.lastrowid)

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
            cur.execute(
                """
                SELECT COUNT(*)
                FROM works
                WHERE COALESCE(metadata_status, 'pending') IN ('pending', 'in_progress')
                  AND COALESCE(download_status, 'not_requested') = 'not_requested'
                """
            )
            available = int(cur.fetchone()[0])
        else:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM works w
                JOIN corpus_works cw ON cw.work_id = w.id
                WHERE cw.corpus_id = ?
                  AND COALESCE(w.metadata_status, 'pending') IN ('pending', 'in_progress')
                  AND COALESCE(w.download_status, 'not_requested') = 'not_requested'
                """, (corpus_id,)
            )
            available = int(cur.fetchone()[0])

        # Canonical works no longer require a separate raw-marking step.
        return {"requested": limit, "marked": 0, "available": available, "ids": []}

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

    def _attempt_raw_download(self, entry: dict) -> tuple[bool, dict]:
        pending_work_id = int(entry["id"])
        ref = {
            "title": entry.get("title"),
            "authors": entry.get("authors") or [],
            "doi": entry.get("doi"),
            "type": entry.get("type") or "",
            "year": entry.get("year"),
            "open_access_url": entry.get("url"),
            "url": entry.get("url"),
            "openalex_json": None,
        }
        try:
            result = self.enhancer.enhance_bibliography(ref, self.download_dir)
        except Exception as exc:
            return False, {
                "pending_work_id": pending_work_id,
                "action": "raw_download_failed",
                "failure_category": "enhancer_exception",
                "failure_detail": str(exc),
            }

        download_result = result if isinstance(result, dict) else ref
        if not download_result or not download_result.get("downloaded_file"):
            return False, {
                "pending_work_id": pending_work_id,
                "action": "raw_download_failed",
                "failure_category": download_result.get("download_failure_category") if isinstance(download_result, dict) else "download_failed",
                "failure_detail": download_result.get("download_failure_detail") if isinstance(download_result, dict) else "download_failed",
            }

        fp = str(download_result["downloaded_file"])
        checksum = ""
        try:
            with open(fp, "rb") as fh:
                checksum = hashlib.sha256(fh.read()).hexdigest()
        except Exception:
            checksum = ""

        with self.db_lock:
            downloaded_id, move_error = self.db.mark_raw_work_downloaded(
                pending_work_id,
                download_result,
                fp,
                checksum,
                download_result.get("download_source", "unknown"),
            )

        if move_error or not downloaded_id:
            return False, {
                "pending_work_id": pending_work_id,
                "action": "raw_download_failed",
                "failure_category": "move_to_downloaded_failed",
                "failure_detail": move_error or "move_to_downloaded_failed",
            }

        return True, {
            "pending_work_id": pending_work_id,
            "action": "downloaded_after_enrich_failure",
            "downloaded_id": int(downloaded_id),
            "source": download_result.get("download_source", "unknown"),
            "route": download_result.get("download_route"),
        }

    def do_enrich(self, corpus_id: int, limit: int, workers: int, expansion: dict, pending_work_ids: list[int] | None = None):
        target_ids = [int(value) for value in (pending_work_ids or []) if str(value).strip().isdigit() and int(value) > 0]
        to_process = self.db.claim_enrich_batch(
            limit=max(1, int(limit or 1)),
            corpus_id=corpus_id,
            claimed_by=self.worker_id,
            lease_seconds=15 * 60,
            target_ids=target_ids,
        )
        for row in to_process:
            for key in ("authors", "keywords"):
                if isinstance(row.get(key), str):
                    try:
                        row[key] = json.loads(row[key])
                    except Exception:
                        pass

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
        
        rel_down = max(0, int(expansion.get("relatedDepthDownstream", 0) or 0))
        rel_up = max(0, int(expansion.get("relatedDepthUpstream", 0) or 0))
        fetch_refs = bool(expansion.get("includeDownstream", False)) and rel_down > 0
        fetch_cites = bool(expansion.get("includeUpstream", False)) and rel_up > 0
        max_cites = expansion.get("maxCitations", 100)
        max_rel = expansion.get("maxRelated", 30)

        wave_size_cfg = int(os.getenv("RAG_FEEDER_ENRICH_WAVE_SIZE", "2") or "2")
        wave_size = max(1, min(len(to_process), max(1, min(workers, 32)), wave_size_cfg))
        remaining_entries = list(to_process)
        deferred_reason = None
        remaining_ids = []

        while remaining_entries:
            batch_entries = remaining_entries[:wave_size]
            remaining_entries = remaining_entries[wave_size:]

            with ThreadPoolExecutor(max_workers=min(len(batch_entries), workers, 32)) as executor:
                futures = {
                    executor.submit(self._enrich_single, e, fetch_refs, fetch_cites, max_cites, rel_down, rel_up, max_rel): e
                    for e in batch_entries
                }
                for future in as_completed(futures):
                    entry = futures[future]
                    pending_work_id = int(entry["id"])
                    processed += 1
                    try:
                        _, enriched, enrich_meta = future.result()
                    except Exception as exc:
                        enriched, enrich_meta = None, {"status": "api_error"}
                        errors.append(str(exc))

                    if not enriched:
                        raw_downloaded, raw_download_result = self._attempt_raw_download(entry)
                        if raw_downloaded:
                            results.append(raw_download_result)
                            continue
                        failed += 1
                        reason = "Metadata fetch failed"
                        moved_id, err = self.db.mark_metadata_failed(pending_work_id, reason)
                        result_payload = {"pending_work_id": pending_work_id, "action": "failed_enrichment"}
                        if raw_download_result:
                            result_payload["raw_download"] = raw_download_result
                        results.append(result_payload)
                        continue

                    wid, err = self.db.apply_enriched_metadata(
                        pending_work_id, enriched, expand_related=(rel_down > 1 and fetch_refs), max_related_per_source=max_rel
                    )
                    if not wid:
                        dup = parse_duplicate_merge_marker(err)
                        if dup:
                            duplicates_merged += 1
                            canonical_table = dup.get("table") or ""
                            canonical_id_raw = dup.get("id") or ""
                            queue_id_raw = dup.get("queue_id") or ""
                            queued_flag = (dup.get("queued") or "") == "1"
                            if canonical_table == "works" and queued_flag:
                                queued += 1
                            results.append(
                                {
                                    "pending_work_id": pending_work_id,
                                    "action": "duplicate_merged",
                                    "canonical_table": canonical_table,
                                    "canonical_id": int(canonical_id_raw) if canonical_id_raw.isdigit() else None,
                                    "queue_id": int(queue_id_raw) if queue_id_raw.isdigit() else None,
                                    "queued": queued_flag,
                                }
                            )
                        else:
                            failed += 1
                            errors.append(err or f"Promotion failed for pending work {pending_work_id}")
                            self.db.reset_enrich_claim(pending_work_id, state="pending", error=err or "promotion_failed", selected=0)
                            results.append({"pending_work_id": pending_work_id, "action": "promotion_failed", "error": err})
                        if dup:
                            self.db.delete_work(pending_work_id)
                        continue

                    promoted += 1
                    qid, qerr = self.db.enqueue_for_download(int(wid))
                    if qid:
                        queued += 1
                        results.append({"pending_work_id": pending_work_id, "work_id": int(wid), "queue_id": int(qid), "action": "queued"})
                    else:
                        results.append({"pending_work_id": pending_work_id, "work_id": int(wid), "action": "enqueue_skipped"})

            if remaining_entries and self._has_pending_corpus_download_jobs():
                deferred_reason = "pending_download"
                remaining_ids = [int(entry["id"]) for entry in remaining_entries]
                log(
                    f"Yielding enrich batch after {processed} item(s); {len(remaining_ids)} pending work row(s) remain and a corpus download job is pending."
                )
                break
            if corpus_id is None and remaining_entries and self._has_pending_interactive_jobs():
                deferred_reason = "interactive_backlog"
                remaining_ids = [int(entry["id"]) for entry in remaining_entries]
                log(
                    f"Yielding global enrich batch after {processed} item(s); {len(remaining_ids)} pending work row(s) remain and an interactive corpus job is waiting."
                )
                break

        return {
            "processed": processed,
            "promoted": promoted,
            "queued": queued,
            "failed": failed,
            "duplicates_merged": duplicates_merged,
            "targeted_ids": target_ids,
            "deferred": bool(remaining_ids),
            "deferred_reason": deferred_reason,
            "remaining_pending_work_ids": remaining_ids,
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

        # DatabaseManager uses a shared sqlite connection. Keep DB mutations and lookups serialized
        # while still allowing network/download work to run in parallel.
        with self.db_lock:
            dup_table, dup_id, dup_field = self.db.check_if_exists(
                doi=row.get("doi"), openalex_id=row.get("openalex_id"), title=row.get("title"),
                authors=authors, editors=row.get("editors"), year=row.get("year"), exclude_id=qid, exclude_table="works"
            )
        if dup_table == "works" and dup_id is not None:
            existing = self.db.get_entry_by_id("works", int(dup_id)) or {}
            if str(existing.get("download_status") or "").strip().lower() != "downloaded":
                dup_table = None
                dup_id = None
        if dup_table == "works" and dup_id is not None:
            with self.db_lock:
                ok, msg = self.db.merge_duplicate_queued_work(qid, dup_table, dup_id, dup_field)
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
            return "error", {
                "queue_id": qid,
                "action": "failed",
                "failure_category": "enhancer_exception",
                "failure_detail": str(exc),
                "attempted_sources": [],
                "_trace": list(ref.get("download_trace") or []),
            }

        download_result = result if isinstance(result, dict) else ref
        trace = list(download_result.get("download_trace") or [])
        attempted_sources = []
        for event in trace:
            source_name = event.get("source")
            if source_name and source_name not in attempted_sources:
                attempted_sources.append(source_name)

        if result and result.get("downloaded_file"):
            fp = str(result["downloaded_file"])
            checksum = ""
            try:
                with open(fp, "rb") as fh: checksum = hashlib.sha256(fh.read()).hexdigest()
            except: pass
            
            with self.db_lock:
                downloaded_id, move_error = self.db.move_entry_to_downloaded(qid, result, fp, checksum, result.get("download_source", "unknown"))
            if move_error or not downloaded_id:
                with self.db_lock:
                    self.db.mark_download_failed(qid, "move_to_downloaded_failed")
                return "failed", {
                    "queue_id": qid,
                    "action": "failed",
                    "failure_category": "move_to_downloaded_failed",
                    "failure_detail": move_error or "move_to_downloaded_failed",
                    "attempted_sources": attempted_sources,
                    "_trace": trace,
                }
                
            return "downloaded", {
                "queue_id": qid,
                "action": "downloaded",
                "downloaded_id": int(downloaded_id),
                "source": result.get("download_source", "unknown"),
                "route": result.get("download_route"),
                "_trace": trace,
            }
        else:
            failure_category = download_result.get("download_failure_category") or "download_failed"
            failure_detail = download_result.get("download_failure_detail") or download_result.get("download_reason") or "download_failed"
            with self.db_lock:
                self.db.mark_download_failed(qid, failure_category)
            return "failed", {
                "queue_id": qid,
                "action": "failed",
                "failure_category": failure_category,
                "failure_detail": failure_detail,
                "attempted_sources": attempted_sources,
                "_trace": trace,
            }

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
        source_stats = defaultdict(lambda: {"attempts": 0, "successes": 0, "failures": 0, "duration_ms": 0})
        failure_reasons = Counter()

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
                    trace = data.pop("_trace", []) if isinstance(data, dict) else []
                    for event in trace:
                        if event.get("phase") != "download":
                            continue
                        source_name = event.get("source") or "unknown"
                        source_stats[source_name]["attempts"] += 1
                        if event.get("outcome") == "success":
                            source_stats[source_name]["successes"] += 1
                        elif event.get("outcome") == "failure":
                            source_stats[source_name]["failures"] += 1
                        if event.get("duration_ms") is not None:
                            source_stats[source_name]["duration_ms"] += int(event.get("duration_ms") or 0)
                    if status == "skipped":
                        skipped += 1
                        results.append(data)
                    elif status == "downloaded":
                        downloaded += 1
                        results.append(data)
                    elif status == "failed":
                        failed += 1
                        failure_reasons[data.get("failure_category") or "unknown"] += 1
                        results.append(data)
                    elif status == "error":
                        failed += 1
                        failure_reasons[data.get("failure_category") or "unknown"] += 1
                        errors.append(data.get("failure_detail") or data.get("failure_category") or "download_error")
                        results.append({k: v for k, v in data.items() if k != "failure_detail"})
                except Exception as exc:
                    failed += 1
                    failure_reasons["worker_exception"] += 1
                    errors.append(str(exc))

        source_stats_out = {}
        for source_name, metrics in source_stats.items():
            attempts = int(metrics["attempts"])
            successes = int(metrics["successes"])
            failures_count = int(metrics["failures"])
            duration_ms = int(metrics["duration_ms"])
            source_stats_out[source_name] = {
                "attempts": attempts,
                "successes": successes,
                "failures": failures_count,
                "avg_duration_ms": round(duration_ms / attempts, 1) if attempts else 0.0,
            }

        return {
            "processed": processed,
            "downloaded": downloaded,
            "failed": failed,
            "skipped": skipped,
            "errors": errors,
            "failure_reasons": dict(failure_reasons),
            "source_stats": source_stats_out,
            "results": results,
        }

    def _run_direct_enrich_cycle(self):
        default_limit = max(1, int(os.getenv("RAG_FEEDER_ENRICH_BATCH_SIZE", "10") or "10"))
        default_workers = max(1, int(os.getenv("RAG_FEEDER_ENRICH_WORKERS", "6") or "6"))
        result = self.do_enrich(None, default_limit, default_workers, {})
        return result if (result or {}).get("processed", 0) > 0 else None

    def _run_direct_download_cycle(self):
        default_limit = max(1, int(os.getenv("RAG_FEEDER_DOWNLOAD_BATCH_SIZE", "50") or "50"))
        result = self.do_download(None, default_limit)
        return result if (result or {}).get("processed", 0) > 0 else None

    def _process_job(self, job: dict) -> None:
        log(f"Picked up job {job['id']} of type '{job['job_type']}'")
        self._heartbeat(
            "running",
            {
                "state": "running_job",
                "job_id": int(job["id"]),
                "job_type": str(job["job_type"]),
                "corpus_id": job.get("corpus_id"),
            },
            force=True,
        )
        heartbeat_stop, heartbeat_thread = self._start_job_heartbeat(job)
        corpus_id = job.get("corpus_id")
        params = job.get("params", {})
        result = None

        try:
            if job["job_type"] == "enrich":
                limit = params.get("limit", 10)
                workers = params.get("workers", 6)
                expansion = params.get("expansion", {})
                pending_work_ids = params.get("pending_work_ids") or []
                result = self.do_enrich(corpus_id, limit, workers, expansion, pending_work_ids=pending_work_ids)
                remaining_ids = [int(value) for value in (result or {}).get("remaining_pending_work_ids") or [] if str(value).isdigit()]
                if remaining_ids:
                    continuation_params = dict(params)
                    continuation_params["pending_work_ids"] = remaining_ids
                    continuation_params["limit"] = len(remaining_ids)
                    continuation_id = self.enqueue_job(corpus_id, "enrich", continuation_params)
                    result["continuation_job_id"] = continuation_id

            elif job["job_type"] == "download":
                limit = params.get("batchSize", 3)
                result = self.do_download(corpus_id, limit)

            elif job["job_type"] == "pipeline_tick":
                cfg = params.get("config", {})
                limit = cfg.get("promoteBatchSize", 10)
                workers = cfg.get("promoteWorkers", 6)

                if corpus_id is None and self._has_pending_interactive_jobs():
                    result = {
                        "deferred": True,
                        "reason": "interactive_backlog_before_mark",
                    }
                else:
                    m_res = self.do_mark(corpus_id, limit)
                    if corpus_id is None and self._has_pending_interactive_jobs():
                        result = {
                            "marked": m_res,
                            "deferred": True,
                            "reason": "interactive_backlog_after_mark",
                        }
                    else:
                        e_res = self.do_enrich(corpus_id, limit, workers, {})
                        if corpus_id is None and self._has_pending_interactive_jobs():
                            result = {
                                "marked": m_res,
                                "enriched": e_res,
                                "deferred": True,
                                "reason": "interactive_backlog_after_enrich",
                            }
                        else:
                            result = {"marked": m_res, "enriched": e_res}

            else:
                raise ValueError(f"Unknown job type: {job['job_type']}")

            updated = self.mark_job_completed(job["id"], result or {})
            if updated:
                log(f"Successfully completed job {job['id']}")
            else:
                log(f"Job {job['id']} finished execution but was already cancelled; completion not recorded.")
            self._heartbeat(
                "idle",
                {
                    "state": "job_completed",
                    "job_id": int(job["id"]),
                    "job_type": str(job["job_type"]),
                    "corpus_id": corpus_id,
                },
                force=True,
            )

        except Exception as e:
            import traceback
            err_msg = traceback.format_exc()
            log(f"Job {job['id']} failed: {e}")
            updated = self.mark_job_failed(job["id"], err_msg)
            if not updated:
                log(f"Job {job['id']} failure not recorded because it was already cancelled.")
            self._heartbeat(
                "error",
                {
                    "state": "job_failed",
                    "job_id": int(job["id"]),
                    "job_type": str(job["job_type"]),
                    "corpus_id": corpus_id,
                    "error": str(e),
                },
                force=True,
            )
        finally:
            heartbeat_stop.set()
            heartbeat_thread.join(timeout=1)

    def run(self):
        log(f"{self.role} daemon starting. Waiting for work...")
        if self.role in {"enrich", "download"}:
            while self.running:
                try:
                    self._heartbeat("idle", {"state": "polling", "mode": "db-driven", "role": self.role})
                    job = self.fetch_next_job()
                    if job:
                        self._process_job(job)
                        continue
                    direct_result = None
                    heartbeat_stop = None
                    heartbeat_thread = None
                    if self.role == "enrich":
                        heartbeat_stop, heartbeat_thread = self._start_direct_batch_heartbeat({"phase": "enrich"})
                        direct_result = self._run_direct_enrich_cycle()
                    elif self.role == "download":
                        heartbeat_stop, heartbeat_thread = self._start_direct_batch_heartbeat({"phase": "download"})
                        direct_result = self._run_direct_download_cycle()
                    if heartbeat_stop:
                        heartbeat_stop.set()
                    if heartbeat_thread:
                        heartbeat_thread.join(timeout=1)
                    if direct_result:
                        self._heartbeat(
                            "running",
                            {
                                "state": "processed_batch",
                                "mode": "db-driven",
                                "role": self.role,
                                "result": {
                                    "processed": int(direct_result.get("processed", 0) or 0),
                                    "promoted": int(direct_result.get("promoted", 0) or 0),
                                    "queued": int(direct_result.get("queued", 0) or 0),
                                    "downloaded": int(direct_result.get("downloaded", 0) or 0),
                                    "failed": int(direct_result.get("failed", 0) or 0),
                                },
                            },
                            force=True,
                        )
                        continue
                    time.sleep(5)
                except KeyboardInterrupt:
                    log("Received shutdown signal. Exiting gracefully...")
                    self.running = False
                    self._heartbeat("stopped", {"state": "keyboard_interrupt"}, force=True)
                except Exception as e:
                    log(f"Critical error in main loop: {e}")
                    self._heartbeat("error", {"state": "loop_exception", "error": str(e)}, force=True)
                    time.sleep(10)
            return

        while self.running:
            try:
                self._heartbeat("idle", {"state": "polling"})
                job = self.fetch_next_job()
                if not job:
                    direct_result = None
                    if self.role == "enrich":
                        direct_result = self._run_direct_enrich_cycle()
                    elif self.role == "download":
                        direct_result = self._run_direct_download_cycle()
                    if not direct_result:
                        time.sleep(5)
                    continue
                self._process_job(job)
                    
            except KeyboardInterrupt:
                log("Received shutdown signal. Exiting gracefully...")
                self.running = False
                self._heartbeat("stopped", {"state": "keyboard_interrupt"}, force=True)
            except Exception as e:
                log(f"Critical error in main loop: {e}")
                self._heartbeat("error", {"state": "loop_exception", "error": str(e)}, force=True)
                time.sleep(10)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DT Pipeline Daemon")
    parser.add_argument("--db-path", required=True, help="Path to SQLite DB")
    parser.add_argument("--role", choices=["all", "enrich", "download"], default="all", help="Worker role")
    args = parser.parse_args()
    
    daemon = PipelineDaemon(args.db_path, role=args.role)
    daemon.run()
