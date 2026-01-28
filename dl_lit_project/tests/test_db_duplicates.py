import json

from dl_lit.db_manager import DatabaseManager


def test_promote_to_with_metadata_dedupes_existing_with_metadata(tmp_path):
    db_path = tmp_path / "test.db"
    db = DatabaseManager(db_path)

    title = "Duplicate Title"
    authors = ["Alice A.", "Bob B."]
    year = 2020
    doi = "10.1234/dup"

    normalized_title = db._normalize_text(title)
    normalized_authors = db._normalize_text(json.dumps(authors))
    normalized_doi = db._normalize_doi(doi)

    cur = db.conn.cursor()
    cur.execute(
        """
        INSERT INTO with_metadata (
            title, authors, year, doi, normalized_doi, normalized_title, normalized_authors
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (title, json.dumps(authors), year, doi, normalized_doi, normalized_title, normalized_authors),
    )
    cur.execute(
        """
        INSERT INTO no_metadata (
            title, authors, year, doi, normalized_doi, normalized_title, normalized_authors
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (title, json.dumps(authors), year, doi, normalized_doi, normalized_title, normalized_authors),
    )
    no_meta_id = cur.lastrowid
    db.conn.commit()

    new_id, message = db.promote_to_with_metadata(no_meta_id, {"title": title})

    assert new_id is None
    assert message and "with_metadata" in message

    cur.execute("SELECT COUNT(*) FROM no_metadata")
    assert cur.fetchone()[0] == 0

    cur.execute("SELECT COUNT(*) FROM with_metadata")
    assert cur.fetchone()[0] == 1

    db.close_connection()


def test_add_entries_to_no_metadata_from_json_skips_title_author_year_dupe(tmp_path):
    db_path = tmp_path / "test.db"
    db = DatabaseManager(db_path)

    title = "Same Work"
    authors = ["Jane Doe", "John Smith"]
    year = 1999

    authors_str = json.dumps(authors)
    normalized_title = db._normalize_text(title)
    normalized_authors = db._normalize_text(authors_str)

    cur = db.conn.cursor()
    cur.execute(
        """
        INSERT INTO no_metadata (
            title, authors, year, normalized_title, normalized_authors
        ) VALUES (?, ?, ?, ?, ?)
        """,
        (title, authors_str, year, normalized_title, normalized_authors),
    )
    db.conn.commit()

    json_payload = [
        {
            "title": title,
            "authors": authors,
            "year": year,
            "doi": None,
        }
    ]
    json_path = tmp_path / "entries.json"
    json_path.write_text(json.dumps(json_payload), encoding="utf-8")

    added, skipped, errors = db.add_entries_to_no_metadata_from_json(json_path)

    assert added == 0
    assert skipped == 1
    assert errors == []

    db.close_connection()


def test_alias_title_year_match_with_tolerance(tmp_path):
    db_path = tmp_path / "test.db"
    db = DatabaseManager(db_path)

    title = "Original Work"
    authors = ["Alice Author"]
    year = 2020

    ref = {"title": title, "authors": authors, "year": year}
    row_id, err = db.insert_no_metadata(ref)
    assert row_id is not None and err is None

    alias_title = "Translated Work"
    db.add_work_alias(
        work_table="no_metadata",
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

    assert table == "no_metadata"
    assert eid == row_id
    assert field == "alias_title_year"

    db.close_connection()


def test_merge_log_records_high_confidence_duplicate(tmp_path):
    db_path = tmp_path / "test.db"
    db = DatabaseManager(db_path)

    ref = {"title": "Merge Target", "authors": ["Ada Lovelace"], "year": 1843}
    row_id, err = db.insert_no_metadata(ref)
    assert row_id is not None and err is None

    dup_ref = {"title": "Merge Target", "authors": ["Ada Lovelace"], "year": 1843}
    dup_id, dup_err = db.insert_no_metadata(dup_ref)
    assert dup_id is None
    assert dup_err and "Duplicate already exists" in dup_err

    cur = db.conn.cursor()
    cur.execute("SELECT COUNT(*) FROM merge_log WHERE action = 'merged' AND match_field = 'title_authors_year'")
    assert cur.fetchone()[0] >= 1

    db.close_connection()
