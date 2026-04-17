import json

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
