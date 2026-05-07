import json
import sqlite3
import time

from dl_lit.db_manager import DatabaseManager


def test_apply_enriched_metadata_dedupes_existing_matched_work(tmp_path):
    db = DatabaseManager(tmp_path / "test.db")

    title = "Duplicate Title"
    authors = ["Alice A.", "Bob B."]
    year = 2020
    doi = "10.1234/dup"

    existing_id, err = db.create_pending_work({"title": title, "authors": authors, "year": year, "doi": doi})
    assert existing_id is not None and err is None
    db._update_work_record(
        int(existing_id),
        {
            **(db.get_entry_by_id("works", int(existing_id)) or {}),
            "metadata_status": "matched",
            "download_status": "not_requested",
        },
    )
    db.conn.commit()

    work_id, err = db.create_pending_work({"title": f"{title} draft", "authors": authors, "year": year})
    assert work_id is not None
    assert err is None

    new_id, message = db.apply_enriched_metadata(int(work_id), {"title": title, "doi": doi})
    assert new_id is None
    assert message and "DUPLICATE_MERGED" in message

    cur = db.conn.cursor()
    cur.execute("SELECT COUNT(*) FROM works WHERE title = ?", (title,))
    assert cur.fetchone()[0] == 1

    db.close_connection()


def test_add_pending_works_from_json_skips_title_author_year_dupe(tmp_path):
    db = DatabaseManager(tmp_path / "test.db")

    title = "Same Work"
    authors = ["Jane Doe", "John Smith"]
    year = 1999

    row_id, err = db.create_pending_work({"title": title, "authors": authors, "year": year})
    assert row_id is not None and err is None

    json_payload = [{"title": title, "authors": authors, "year": year, "doi": None}]
    json_path = tmp_path / "entries.json"
    json_path.write_text(json.dumps(json_payload), encoding="utf-8")

    added, skipped, errors = db.add_pending_works_from_json(json_path)

    assert added == 0
    assert skipped == 1
    assert errors == []

    db.close_connection()


def test_alias_title_year_match_with_tolerance(tmp_path):
    db = DatabaseManager(tmp_path / "test.db")

    title = "Original Work"
    authors = ["Alice Author"]
    year = 2020

    row_id, err = db.create_pending_work({"title": title, "authors": authors, "year": year})
    assert row_id is not None and err is None

    alias_title = "Translated Work"
    db.add_work_alias(
        work_table="works",
        work_id=row_id,
        alias_title=alias_title,
        alias_year=2019,
        alias_language="de",
        relationship_type="translation",
    )

    table, eid, field = db.check_if_exists(
        doi=None,
        openalex_id=None,
        title=alias_title,
        authors=authors,
        year=2020,
    )

    assert table == "works"
    assert eid == row_id
    assert field == "alias_title"

    db.close_connection()


def test_check_if_exists_uses_editors_when_authors_missing(tmp_path):
    db = DatabaseManager(tmp_path / "test.db")

    ref = {
        "title": "Vom Klassenkampf Zum Co-Management?",
        "authors": [],
        "editors": ["Klitzke, U.", "Betz, H.", "Moreke, M."],
        "year": 2000,
    }
    row_id, err = db.create_pending_work(ref)
    assert row_id is not None and err is None

    table, eid, field = db.check_if_exists(
        doi=None,
        openalex_id=None,
        title=ref["title"],
        authors=[],
        editors=["Klitzke, U.", "Betz, H.", "Moreke, M."],
        year=2000,
    )
    assert table == "works"
    assert eid == row_id
    assert field == "title_authors_year"
    db.close_connection()


def test_create_pending_work_normalizes_editors_when_authors_missing(tmp_path):
    db = DatabaseManager(tmp_path / "test.db")
    row_id, err = db.create_pending_work(
        {
            "title": "Editors Only Work",
            "authors": [],
            "editors": ["Yamamura, K.", "Streeck, W."],
            "year": 2003,
        }
    )
    assert row_id is not None and err is None

    cur = db.conn.cursor()
    cur.execute("SELECT normalized_authors FROM works WHERE id = ?", (row_id,))
    normalized = cur.fetchone()[0]
    assert normalized == db._normalize_authors_value(["Yamamura, K.", "Streeck, W."])
    db.close_connection()


def test_apply_enriched_metadata_serializes_openalex_runtime_fields(tmp_path):
    db = DatabaseManager(tmp_path / "test.db")
    row_id, err = db.create_pending_work({"title": "Schema Drift Work", "authors": ["Alice"], "year": 2024})
    assert row_id is not None and err is None

    updated_id, message = db.apply_enriched_metadata(
        int(row_id),
        {
            "title": "Schema Drift Work",
            "authors": ["Alice"],
            "year": 2024,
            "doi": "10.1234/schema-drift",
            "open_access_url": "https://example.test/open.pdf",
            "referenced_work_ids": ["https://openalex.org/W1"],
            "cited_by_api_url": "https://api.openalex.org/works?filter=cites:W1",
            "referenced_works": [{"id": "https://openalex.org/W2"}],
            "citing_works": [{"id": "https://openalex.org/W3"}],
        },
    )

    assert updated_id == row_id
    assert message is None
    row = db.get_entry_by_id("works", int(row_id))
    assert row["metadata_status"] == "matched"
    assert row["doi"] == "10.1234/schema-drift"
    assert row["open_access_url"] == "https://example.test/open.pdf"
    assert json.loads(row["referenced_work_ids"]) == ["https://openalex.org/W1"]
    assert row["cited_by_api_url"] == "https://api.openalex.org/works?filter=cites:W1"
    db.close_connection()


def test_raw_download_fallback_clears_enrichment_claim(tmp_path):
    db = DatabaseManager(tmp_path / "test.db")
    row_id, err = db.create_pending_work({"title": "Raw Download Work", "authors": ["EC"], "year": 2012})
    assert row_id is not None and err is None

    claimed = db.claim_enrich_batch(limit=1, corpus_id=None, claimed_by="worker-a", lease_seconds=60)
    assert [row["id"] for row in claimed] == [row_id]

    downloaded_id, message = db.mark_raw_work_downloaded(
        int(row_id),
        {
            "title": "Raw Download Work",
            "authors": ["EC"],
            "year": 2012,
            "downloaded_file": str(tmp_path / "raw.pdf"),
        },
        str(tmp_path / "raw.pdf"),
        "checksum",
        "OpenAlex OA URL",
    )

    assert downloaded_id == row_id
    assert message is None
    row = db.get_entry_by_id("works", int(row_id))
    assert row["download_status"] == "downloaded"
    assert row["metadata_status"] == "failed"
    assert row["metadata_source"] == "raw_download_fallback"
    assert row["metadata_claimed_by"] is None
    assert row["metadata_claimed_at"] is None
    assert row["metadata_lease_expires_at"] is None
    assert "raw download fallback succeeded" in row["metadata_error"]

    db.close_connection()


def test_claim_enrich_batch_skips_downloaded_rows(tmp_path):
    db = DatabaseManager(tmp_path / "test.db")
    row_id, err = db.create_pending_work({"title": "Already Downloaded Work", "authors": ["EC"], "year": 2012})
    assert row_id is not None and err is None
    db._update_work_record(
        int(row_id),
        {
            **(db.get_entry_by_id("works", int(row_id)) or {}),
            "metadata_status": "in_progress",
            "metadata_lease_expires_at": int(time.time()) - 10,
            "download_status": "downloaded",
        },
    )
    db.conn.commit()

    claimed = db.claim_enrich_batch(limit=1, corpus_id=None, claimed_by="worker-a", lease_seconds=60)
    assert claimed == []

    db.close_connection()


def test_database_manager_adds_runtime_columns_to_existing_works_table(tmp_path):
    db_path = tmp_path / "old.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE works (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            openalex_id TEXT,
            normalized_doi TEXT,
            normalized_title TEXT,
            normalized_authors TEXT,
            year INTEGER,
            metadata_status TEXT NOT NULL DEFAULT 'pending',
            metadata_lease_expires_at INTEGER,
            download_status TEXT NOT NULL DEFAULT 'not_requested',
            download_lease_expires_at INTEGER,
            origin_type TEXT,
            origin_key TEXT
        )
        """
    )
    conn.commit()
    conn.close()

    db = DatabaseManager(db_path)
    columns = db._table_columns("works")

    assert "open_access_url" in columns
    assert "referenced_work_ids" in columns
    assert "cited_by_api_url" in columns
    db.close_connection()
