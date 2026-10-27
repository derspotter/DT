from dl_lit.db_manager import DatabaseManager


def _work(db, title, corpus_ids, **cols):
    """Insert a works row (+ corpus_works rows) and return its id."""
    data = {"title": title, "normalized_title": db._normalize_text(title), **cols}
    if data.get("doi"):
        data["normalized_doi"] = db._normalize_doi(data["doi"])
    names = ", ".join(data)
    marks = ", ".join("?" for _ in data)
    work_id = db.conn.execute(f"INSERT INTO works ({names}) VALUES ({marks})", tuple(data.values())).lastrowid
    for corpus_id in corpus_ids:
        db.conn.execute("INSERT INTO corpus_works (corpus_id, work_id) VALUES (?, ?)", (corpus_id, work_id))
    db.conn.commit()
    return work_id


def _corpora(db, work_id):
    return {row[0] for row in db.conn.execute("SELECT corpus_id FROM corpus_works WHERE work_id = ?", (work_id,))}


def _assert_absorbed(db, canonical, dup):
    assert _corpora(db, canonical) == {1, 2}
    assert _corpora(db, dup) == set()
    assert db.conn.execute("SELECT COUNT(*) FROM works WHERE id = ?", (dup,)).fetchone()[0] == 0
    assert db.conn.execute(
        "SELECT canonical_id, action FROM merge_log WHERE duplicate_id = ?", (dup,)
    ).fetchall() == [(canonical, "absorbed")]
    assert not db.conn.in_transaction


def test_enrichment_merge_keeps_the_duplicates_corpus_membership(tmp_path):
    db = DatabaseManager(str(tmp_path / "lit.db"))
    canonical = _work(db, "Canon", [2], doi="10.1/c")
    dup = _work(db, "Canon draft", [1], metadata_status="in_progress")

    wid, err = db.apply_enriched_metadata(dup, {"doi": "10.1/c", "title": "Canon", "year": 2020})

    assert wid is None and "DUPLICATE_MERGED" in err
    _assert_absorbed(db, canonical, dup)


def test_download_merge_and_queue_merge_keep_membership(tmp_path):
    db = DatabaseManager(str(tmp_path / "lit.db"))
    canonical = _work(db, "Canon", [2], doi="10.1/c", year=2020)
    dup = _work(db, "Canon", [1], year=2020)  # normalized title + year match (DOIs are unique-indexed)

    downloaded_id, err = db.mark_raw_work_downloaded(dup, {"title": "Canon"}, "/tmp/x.pdf", "abc", "test")

    assert (downloaded_id, err) == (canonical, None)
    _assert_absorbed(db, canonical, dup)

    queued = _work(db, "Canon queued", [1, 3], download_status="queued")
    ok, _ = db.merge_duplicate_queued_work(queued, "works", canonical, "normalized_doi")

    assert ok
    assert _corpora(db, canonical) == {1, 2, 3}
    assert db.conn.execute("SELECT COUNT(*) FROM works WHERE id = ?", (queued,)).fetchone()[0] == 0
    assert db.conn.execute(
        "SELECT match_field, action FROM merge_log WHERE duplicate_id = ?", (queued,)
    ).fetchall() == [("normalized_doi", "absorbed")]
    assert not db.conn.in_transaction


def test_absorb_moves_aliases_and_tolerates_duplicates(tmp_path):
    db = DatabaseManager(str(tmp_path / "lit.db"))
    canonical = _work(db, "Canon", [2])
    dup = _work(db, "Canon draft", [1, 2])
    for work_id in (canonical, dup):
        db.add_work_alias("works", work_id, "Kanon")
    db.add_work_alias("works", dup, "Canone")

    db._absorb_duplicate(canonical, dup, match_field="title_year", notes="n", updates={"year": 2020})
    db.conn.commit()

    aliases = db.conn.execute(
        "SELECT work_id, normalized_alias_title FROM work_aliases ORDER BY normalized_alias_title"
    ).fetchall()
    assert aliases == [(canonical, "canone"), (canonical, "kanon")]
    assert _corpora(db, canonical) == {1, 2}
    assert db.conn.execute(
        "SELECT match_field, notes, updates_json FROM merge_log WHERE duplicate_id = ?", (dup,)
    ).fetchall() == [("title_year", "n", '{"year": 2020}')]


def test_merge_records_uses_absorb(tmp_path):
    db = DatabaseManager(str(tmp_path / "lit.db"))
    canonical = _work(db, "Canon", [2])
    dup = _work(db, "Canon draft", [1], doi="10.1/c")

    ok, err = db._merge_records("works", canonical, "works", dup, "title_year")

    assert (ok, err) == (True, None)
    assert db.conn.execute("SELECT doi FROM works WHERE id = ?", (canonical,)).fetchone() == ("10.1/c",)
    _assert_absorbed(db, canonical, dup)
