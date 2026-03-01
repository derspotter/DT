import os
import sys

# Ensure imports work regardless of where the script is executed from
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../dl_lit_project')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

import sqlite3
import json
import time
import argparse
from datetime import datetime

from dl_lit.db_manager import DatabaseManager
from dl_lit.utils import get_global_rate_limiter

def log(msg: str):
    print(f"[{datetime.now().isoformat()}] [DAEMON] {msg}", flush=True)

class PipelineDaemon:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.db = DatabaseManager(db_path)
        self.running = True
        log(f"Initialized DatabaseManager at {db_path}")

    def fetch_next_job(self):
        """Fetch the oldest pending job from the queue."""
        cur = self.db.conn.cursor()
        cur.execute(
            """
            SELECT id, corpus_id, job_type, parameters_json
            FROM pipeline_jobs
            WHERE status = 'pending'
            ORDER BY created_at ASC
            LIMIT 1
            """
        )
        row = cur.fetchone()
        if not row:
            return None
            
        job_id, corpus_id, job_type, params_json = row
        
        # Mark as running
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
            "UPDATE pipeline_jobs SET status = 'completed', finished_at = CURRENT_TIMESTAMP, result_json = ? WHERE id = ?",
            (json.dumps(result), job_id)
        )
        self.db.conn.commit()

    def mark_job_failed(self, job_id: int, error_msg: str):
        cur = self.db.conn.cursor()
        cur.execute(
            "UPDATE pipeline_jobs SET status = 'failed', finished_at = CURRENT_TIMESTAMP, result_json = ? WHERE id = ?",
            (json.dumps({"error": error_msg}), job_id)
        )
        self.db.conn.commit()

    def handle_enrich_job(self, job):
        log(f"Handling enrich job {job['id']} for corpus {job['corpus_id']}")
        # TO BE IMPLEMENTED: Port logic from ingest_process_marked.py
        time.sleep(2) # Simulate work
        return {"processed": 0, "promoted": 0}

    def handle_download_job(self, job):
        log(f"Handling download job {job['id']} for corpus {job['corpus_id']}")
        # TO BE IMPLEMENTED: Port logic from download_worker_once.py
        time.sleep(2) # Simulate work
        return {"downloaded": 0, "failed": 0}

    def run(self):
        log("Pipeline Daemon starting. Waiting for jobs...")
        while self.running:
            try:
                job = self.fetch_next_job()
                if not job:
                    time.sleep(5)  # Poll every 5 seconds if idle
                    continue
                    
                log(f"Picked up job {job['id']} of type '{job['job_type']}'")
                
                try:
                    result = None
                    if job["job_type"] == "enrich":
                        result = self.handle_enrich_job(job)
                    elif job["job_type"] == "download":
                        result = self.handle_download_job(job)
                    else:
                        raise ValueError(f"Unknown job type: {job['job_type']}")
                        
                    self.mark_job_completed(job["id"], result or {})
                    log(f"Successfully completed job {job['id']}")
                    
                except Exception as e:
                    import traceback
                    err_msg = traceback.format_exc()
                    log(f"Job {job['id']} failed: {e}")
                    self.mark_job_failed(job["id"], err_msg)
                    
            except KeyboardInterrupt:
                log("Received shutdown signal. Exiting gracefully...")
                self.running = False
            except Exception as e:
                log(f"Critical error in main loop: {e}")
                time.sleep(10) # Backoff on critical failure


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DT Pipeline Daemon")
    parser.add_argument("--db-path", required=True, help="Path to SQLite DB")
    args = parser.parse_args()
    
    daemon = PipelineDaemon(args.db_path)
    daemon.run()
