import json

from dl_lit.db_manager import DatabaseManager
from dl_lit.expand_openalex import expand_openalex_links


def test_expand_openalex_links_adds_reference_edges(monkeypatch):
    db = DatabaseManager(":memory:")
    cursor = db.conn.cursor()
    cursor.execute(
        "INSERT INTO with_metadata (title, openalex_id) VALUES (?, ?)",
        ("Seed Work", "W1"),
    )
    db.conn.commit()

    def fake_fetch_openalex_work(openalex_id, *, searcher, rate_limiter, mailto):
        return {"id": f"https://openalex.org/{openalex_id}", "referenced_works": ["W2", "W3"]}

    def fake_fetch_referenced_work_details(ref_ids, rate_limiter, mailto):
        return [
            {
                "openalex_id": "W2",
                "title": "Ref Two",
                "authors": ["Author A"],
                "year": 2000,
                "doi": None,
                "type": "article",
            },
            {
                "openalex_id": "W3",
                "title": "Ref Three",
                "authors": ["Author B"],
                "year": 2001,
                "doi": None,
                "type": "article",
            },
        ]

    monkeypatch.setattr(
        "dl_lit.expand_openalex.fetch_openalex_work",
        fake_fetch_openalex_work,
    )
    monkeypatch.setattr(
        "dl_lit.expand_openalex.fetch_referenced_work_details",
        fake_fetch_referenced_work_details,
    )
    monkeypatch.setattr(
        "dl_lit.expand_openalex.fetch_citing_work_ids",
        lambda *args, **kwargs: [],
    )

    stats = expand_openalex_links(
        db_manager=db,
        mailto="test@example.com",
        limit=5,
        fetch_references=True,
        fetch_citations=False,
        update_openalex_json=True,
    )

    assert stats.added_refs == 2
    assert stats.errors == 0

    cursor.execute("SELECT COUNT(*) FROM with_metadata")
    assert cursor.fetchone()[0] == 3

    cursor.execute("SELECT COUNT(*) FROM citation_edges")
    assert cursor.fetchone()[0] == 2

    cursor.execute("SELECT openalex_json FROM with_metadata WHERE openalex_id = 'W1'")
    openalex_json = cursor.fetchone()[0]
    assert json.loads(openalex_json)["id"] == "https://openalex.org/W1"
