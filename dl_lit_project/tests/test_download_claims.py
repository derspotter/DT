import time

from dl_lit.db_manager import DatabaseManager


def _seed_queue(db: DatabaseManager, *, corpus_id: int, n: int) -> list[int]:
    cur = db.conn.cursor()
    ids: list[int] = []
    for i in range(n):
        cur.execute(
            "INSERT INTO with_metadata (title, authors, year, doi, normalized_doi, entry_type, download_state) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (f"Paper {i}", '["A. Author"]', 2000 + i, f"10.0000/test{i}", f"10.0000/test{i}".upper(), "article", "queued"),
        )
        qid = int(cur.lastrowid)
        ids.append(qid)
        cur.execute(
            "INSERT OR IGNORE INTO corpus_items (corpus_id, table_name, row_id) VALUES (?, 'with_metadata', ?)",
            (int(corpus_id), qid),
        )
    db.conn.commit()
    return ids


def test_claim_batch_scoped_to_corpus(tmp_path):
    db_path = tmp_path / "claims.db"
    db = DatabaseManager(db_path=str(db_path))
    try:
        ids = _seed_queue(db, corpus_id=1, n=3)

        batch1 = db.claim_download_batch(limit=2, corpus_id=1, claimed_by="w1", lease_seconds=60)
        assert [b["id"] for b in batch1] == ids[:2]

        batch2 = db.claim_download_batch(limit=5, corpus_id=1, claimed_by="w2", lease_seconds=60)
        assert [b["id"] for b in batch2] == ids[2:]

        batch3 = db.claim_download_batch(limit=5, corpus_id=1, claimed_by="w3", lease_seconds=60)
        assert batch3 == []

        # Rows should be marked in_progress for claimed items.
        cur = db.conn.cursor()
        cur.execute(
            "SELECT id, download_state, download_claimed_by, download_lease_expires_at FROM with_metadata ORDER BY id"
        )
        rows = cur.fetchall()
        assert len(rows) == 3
        for rid, state, claimed_by, lease_expires_at in rows:
            assert rid in ids
            assert state == "in_progress"
            assert claimed_by in ("w1", "w2")
            assert int(lease_expires_at) >= int(time.time())
    finally:
        db.close_connection()


def test_claim_does_not_churn_before_lease_expires(tmp_path):
    db_path = tmp_path / "claims2.db"
    db = DatabaseManager(db_path=str(db_path))
    try:
        ids = _seed_queue(db, corpus_id=1, n=1)
        batch1 = db.claim_download_batch(limit=1, corpus_id=1, claimed_by="w1", lease_seconds=3600)
        assert [b["id"] for b in batch1] == ids

        # Immediately trying again should return nothing until lease expires.
        batch2 = db.claim_download_batch(limit=1, corpus_id=1, claimed_by="w2", lease_seconds=3600)
        assert batch2 == []
    finally:
        db.close_connection()


def test_claim_reclaims_after_lease_expires(tmp_path):
    db_path = tmp_path / "claims3.db"
    db = DatabaseManager(db_path=str(db_path))
    try:
        ids = _seed_queue(db, corpus_id=1, n=1)
        batch1 = db.claim_download_batch(limit=1, corpus_id=1, claimed_by="w1", lease_seconds=1)
        assert [b["id"] for b in batch1] == ids

        # Force the lease into the past to simulate an interrupted worker.
        cur = db.conn.cursor()
        cur.execute(
            "UPDATE with_metadata SET download_lease_expires_at = ? WHERE id = ?",
            (int(time.time()) - 10, int(ids[0])),
        )
        db.conn.commit()

        batch2 = db.claim_download_batch(limit=1, corpus_id=1, claimed_by="w2", lease_seconds=60)
        assert [b["id"] for b in batch2] == ids
    finally:
        db.close_connection()
