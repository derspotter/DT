import sqlite3

from dl_lit.db_manager import DatabaseManager


def test_backfill_citation_edges_inserts_edges():
    db = DatabaseManager(db_path=":memory:")
    cursor = db.conn.cursor()

    cursor.execute(
        """
        INSERT INTO with_metadata (title, openalex_id, doi, normalized_doi)
        VALUES (?, ?, ?, ?)
        """,
        ("Source Work", "W100", "10.1000/source", "10.1000/SOURCE"),
    )
    source_id = cursor.lastrowid

    cursor.execute(
        """
        INSERT INTO with_metadata (title, openalex_id, doi, normalized_doi, source_work_id, relationship_type)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        ("Target Work", "W200", "10.1000/target", "10.1000/TARGET", source_id, "references"),
    )
    db.conn.commit()

    summary = db.backfill_citation_edges()
    assert summary["edges_inserted"] == 1

    cursor.execute("SELECT source_id, target_id, relationship_type FROM citation_edges")
    row = cursor.fetchone()
    assert row == ("W100", "W200", "references")

    db.close_connection()
