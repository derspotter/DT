import inspect

from dl_lit.db_manager import DatabaseManager


def test_enrich_claim_select_uses_the_download_status_index(tmp_path):
    db = DatabaseManager(str(tmp_path / "lit.db"))
    cur = db.conn.cursor()
    cur.execute(
        """EXPLAIN QUERY PLAN SELECT id FROM works nm
        WHERE (metadata_status = 'pending' OR (metadata_status = 'in_progress' AND metadata_lease_expires_at IS NOT NULL AND metadata_lease_expires_at <= ?))
          AND nm.download_status = 'not_requested' ORDER BY id LIMIT 10""",
        (0,),
    )
    plan = " ".join(str(r[-1]) for r in cur.fetchall())
    assert any(name in plan for name in ("idx_works_download_status", "idx_works_metadata_status", "idx_works_state"))


def test_claim_sql_has_no_coalesce_on_status_columns():
    src = inspect.getsource(DatabaseManager.claim_enrich_batch) + inspect.getsource(
        DatabaseManager.fetch_matched_work_batch
    )
    assert "COALESCE(metadata_status" not in src
    assert "COALESCE(nm.download_status" not in src
    assert "COALESCE(download_status" not in src


def test_worker_job_probes_use_the_status_index(tmp_path):
    db = DatabaseManager(str(tmp_path / "lit.db"))
    statements = [
        ("SELECT id, corpus_id, job_type, parameters_json FROM pipeline_jobs WHERE status = 'pending' "
         "AND job_type = 'download' ORDER BY CASE "
         "WHEN job_type = 'download' AND corpus_id IS NOT NULL THEN 0 WHEN job_type = 'enrich' AND corpus_id IS NOT NULL THEN 1 "
         "WHEN job_type = 'download' THEN 2 WHEN job_type = 'enrich' THEN 3 WHEN job_type = 'pipeline_tick' AND corpus_id IS NOT NULL THEN 4 "
         "WHEN job_type = 'pipeline_tick' THEN 5 ELSE 6 END ASC, created_at ASC, id ASC LIMIT 1", ()),
        ("SELECT 1 FROM pipeline_jobs WHERE status = 'pending' AND corpus_id IS NOT NULL "
         "AND job_type IN ('enrich', 'download') LIMIT 1", ()),
        ("UPDATE pipeline_jobs SET status='pending' WHERE status='running' AND worker_id=?", ("w1",)),
    ]
    for sql, params in statements:
        rows = db.conn.execute("EXPLAIN QUERY PLAN " + sql, params).fetchall()
        plan = " ".join(str(r[-1]) for r in rows)
        assert "idx_pipeline_jobs_status_finished" in plan, (sql, plan)
