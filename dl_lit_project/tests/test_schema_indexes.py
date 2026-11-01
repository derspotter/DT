import importlib.util
import sqlite3
from pathlib import Path

from dl_lit.db_manager import DatabaseManager

SEED_PROMOTE = Path(__file__).resolve().parents[2] / "backend" / "scripts" / "seed_promote.py"

NEW = {"idx_works_state", "idx_works_year_id", "idx_corpus_works_added", "idx_search_results_work"}
DROPPED = {
    "idx_corpus_works_lookup",
    "idx_corpus_works_corpus",
    "idx_work_aliases_norm_title",
    "idx_work_aliases_owner",
    "idx_search_results_run",
}


def test_schema_ensure_creates_new_and_drops_duplicate_indexes_idempotently(tmp_path):
    path = str(tmp_path / "lit.db")
    c = sqlite3.connect(path)
    c.execute(
        "CREATE TABLE corpus_works (corpus_id INTEGER, work_id INTEGER, added_at TIMESTAMP, "
        "PRIMARY KEY (corpus_id, work_id))"
    )
    c.execute("CREATE INDEX idx_corpus_works_lookup ON corpus_works(corpus_id, work_id)")
    c.execute("CREATE INDEX idx_corpus_works_corpus ON corpus_works(corpus_id)")
    c.commit()
    c.close()
    for _ in range(2):
        DatabaseManager(path).conn.close()
    conn = sqlite3.connect(path)
    names = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='index'")}
    conn.close()
    assert NEW <= names
    assert not (DROPPED & names)


def test_seed_promote_bootstrap_does_not_recreate_dropped_lookup_index(monkeypatch, tmp_path):
    monkeypatch.syspath_prepend(str(SEED_PROMOTE.parent))
    spec = importlib.util.spec_from_file_location("seed_promote_script", SEED_PROMOTE)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    db = DatabaseManager(str(tmp_path / "lit.db"))
    try:
        mod._ensure_seed_membership_schema(db)
        names = {r[0] for r in db.conn.execute("SELECT name FROM sqlite_master WHERE type='index'")}
    finally:
        db.conn.close()
    assert "idx_seed_candidates_in_corpus_lookup" not in names
    assert "idx_seed_candidates_in_corpus_work" in names


def test_old_search_results_table_gets_work_id_before_its_index(tmp_path):
    path = str(tmp_path / "lit.db")
    c = sqlite3.connect(path)
    c.execute("CREATE TABLE search_runs (id INTEGER PRIMARY KEY AUTOINCREMENT, query TEXT NOT NULL)")
    c.execute("CREATE TABLE search_results (id INTEGER PRIMARY KEY AUTOINCREMENT, search_run_id INTEGER NOT NULL, openalex_id TEXT)")
    c.commit()
    c.close()
    DatabaseManager(path).conn.close()
    conn = sqlite3.connect(path)
    cols = [r[1] for r in conn.execute("PRAGMA table_info(search_results)")]
    names = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='index'")}
    conn.close()
    assert "work_id" in cols
    assert "idx_search_results_work" in names


def test_busy_timeout_matches_the_connect_timeout(tmp_path):
    # A 5 s busy_timeout crashed the enrich worker at boot while the download worker
    # built the new indexes (production deploy of #74, 2026-10-30).
    db = DatabaseManager(str(tmp_path / "lit.db"))
    try:
        assert db.conn.execute("PRAGMA busy_timeout").fetchone()[0] == 30000
    finally:
        db.conn.close()
