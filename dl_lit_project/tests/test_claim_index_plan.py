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
