"""Phase-4 hygiene (spec D8 + the Task 7 controller additions).

The abstract survives a download, ``_table_columns`` asks SQLite once per table, the API key
never reaches stdout, ``worker.log()`` keeps one append handle (the one daemon loop is pinned in
``test_pipeline_daemon.py``), the Crossref mapping lives in ``identifiers`` and a resolved-identifier conflict inserts a new row.
"""

import builtins
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from dl_lit import identifiers
from dl_lit.db_manager import DatabaseManager
from tests._worker_import import load_worker

CROSSREF_FIXTURE = json.loads((Path(__file__).parent / "fixtures" / "crossref_cases.json").read_text())


# --- move_entry_to_downloaded keeps what the downloader's ref does not carry -----

def test_download_keeps_the_existing_abstract_and_clears_the_claim(tmp_path):
    db = DatabaseManager(str(tmp_path / "lit.db"))
    work_id = db.conn.execute(
        "INSERT INTO works (title, abstract, openalex_id, doi, normalized_doi, metadata_status, download_claimed_by)"
        " VALUES (?, ?, ?, ?, ?, 'matched', 'worker-a')",
        ("A Title", "The existing abstract.", "W123", "10.1/x", "10.1/X"),
    ).lastrowid
    db.conn.commit()

    # The worker's ref (enhancer output) has no abstract_inverted_index, and its "id" is not an OpenAlex id.
    ref = {"id": work_id, "title": "A Title", "doi": None, "download_source": "mock"}
    assert db.move_entry_to_downloaded(work_id, ref, "/tmp/a.pdf", "abc", "mock") == (work_id, None)

    row = db.conn.execute(
        "SELECT abstract, openalex_id, doi, normalized_doi, download_status, file_checksum, download_claimed_by"
        " FROM works WHERE id = ?",
        (work_id,),
    ).fetchone()
    assert row == ("The existing abstract.", "W123", "10.1/x", "10.1/X", "downloaded", "abc", None)


def test_download_still_writes_an_abstract_the_ref_carries(tmp_path):
    db = DatabaseManager(str(tmp_path / "lit.db"))
    work_id = db.conn.execute("INSERT INTO works (title, abstract) VALUES ('T', 'old')").lastrowid
    db.conn.commit()
    db.move_entry_to_downloaded(work_id, {"abstract_inverted_index": {"new": [0], "text": [1]}}, "/f.pdf", "c", "s")
    assert db.conn.execute("SELECT abstract FROM works WHERE id = ?", (work_id,)).fetchone() == ("new text",)


# --- _table_columns cache ---------------------------------------------------------

class _CountingConn:
    """Wrap a sqlite3 connection and count PRAGMA table_info statements per table."""

    def __init__(self, conn):
        self._conn = conn
        self.pragmas = []

    def cursor(self):
        outer = self
        real = self._conn.cursor()

        class _Cursor:
            def execute(self, sql, *args):
                if sql.strip().upper().startswith("PRAGMA TABLE_INFO"):
                    outer.pragmas.append(sql)
                return real.execute(sql, *args)

            def __getattr__(self, name):
                return getattr(real, name)

        return _Cursor()

    def execute(self, sql, *args):
        if sql.strip().upper().startswith("PRAGMA TABLE_INFO"):
            self.pragmas.append(sql)
        return self._conn.execute(sql, *args)

    def __getattr__(self, name):
        return getattr(self._conn, name)


def test_table_columns_asks_sqlite_once_per_table(tmp_path):
    db = DatabaseManager(str(tmp_path / "lit.db"))
    spy = _CountingConn(db.conn)
    db.conn = spy
    for i in range(10):
        db.create_pending_work({"title": f"Distinct work number {i}", "authors": [f"Author {i}"], "year": 2000 + i})
    assert db.conn.execute("SELECT COUNT(*) FROM works").fetchone()[0] == 10
    assert spy.pragmas == ["PRAGMA table_info(works)"]


def test_ensure_column_invalidates_the_cache(tmp_path):
    db = DatabaseManager(str(tmp_path / "lit.db"))
    assert "hygiene_probe" not in db._table_columns("works")
    db._ensure_column("works", "hygiene_probe", "TEXT")
    assert "hygiene_probe" in db._table_columns("works")


def test_a_missing_table_is_not_cached_as_empty(tmp_path):
    db = DatabaseManager(str(tmp_path / "lit.db"))
    assert db._table_columns("later_table") == set()
    db.conn.execute("CREATE TABLE later_table (a TEXT)")
    assert db._table_columns("later_table") == {"a"}


# --- the API key never reaches stdout ----------------------------------------------

class _NoRateLimit:
    def wait_if_needed(self, *args, **kwargs):
        pass


def test_fallback_search_never_prints_the_api_key(monkeypatch, capsys):
    from dl_lit import OpenAlexScraper

    monkeypatch.setenv("OPENALEX_API_KEY", "sekret-key-123")
    searcher = OpenAlexScraper.OpenAlexCrossrefSearcher(rate_limiter=_NoRateLimit())
    assert searcher.api_key == "sekret-key-123"
    seen = {}

    def fake_request(**kwargs):
        seen.update(kwargs)
        return {"results": [], "meta": {}}

    monkeypatch.setattr(OpenAlexScraper, "openalex_request_json", fake_request)
    capsys.readouterr()
    for step in (1, 2, 3, 7, 9):
        searcher.search("Some title", 2020, "Some container", None, None, step)
    out = capsys.readouterr().out
    assert seen["params"]["api_key"] == "sekret-key-123"  # the request still carries the key
    assert "Params:" in out
    assert "api_key=" not in out
    assert "sekret-key-123" not in out


def test_citing_works_error_prints_the_url_without_the_api_key(monkeypatch, capsys):
    import requests

    from dl_lit import OpenAlexScraper

    def failing_request(**kwargs):
        raise requests.exceptions.HTTPError(f"500 Server Error for url: {kwargs['url']}")

    monkeypatch.setattr(OpenAlexScraper, "openalex_request_json", failing_request)
    capsys.readouterr()
    result = OpenAlexScraper.fetch_citing_work_ids(
        "https://api.openalex.org/works?filter=cites:W1", _NoRateLimit(), api_key="sekret-key-123"
    )
    out = capsys.readouterr().out
    assert result == []
    assert "Error fetching citing works" in out
    assert "api_key" not in out
    assert "sekret-key-123" not in out


REDACTION_CASES = [
    ("https://x/works?api_key=abc&page=2", "https://x/works?page=2"),  # first
    ("https://x/works?page=2&api_key=abc", "https://x/works?page=2"),  # last
    ("https://x/works?a=1&api_key=abc&b=2 tail", "https://x/works?a=1&b=2 tail"),  # middle
    ("https://x/works?api_key=a&api_key=b", "https://x/works"),  # adjacent, nothing else
    ("https://x/works?api_key=a&api_key=b&page=2", "https://x/works?page=2"),  # adjacent first
    ("https://x/works?a=1&api_key=a&api_key=b&b=2", "https://x/works?a=1&b=2"),  # adjacent middle
    ("https://x/works?a=1&api_key=a&api_key=b", "https://x/works?a=1"),  # adjacent last
    ("https://x/works?api_key=a#frag", "https://x/works#frag"),
    ("url: https://x/w?api_key=k next https://y/w?p=1&api_key=k", "url: https://x/w next https://y/w?p=1"),
    ("https://x/works?page=2&b=3", "https://x/works?page=2&b=3"),  # no key: unchanged
    ("no key here?", "no key here?"),
]


@pytest.mark.parametrize("text, expected", REDACTION_CASES)
def test_redact_api_key_strips_every_occurrence(text, expected):
    from dl_lit.OpenAlexScraper import _redact_api_key

    assert _redact_api_key(text) == expected


ADJACENT = "https://api.openalex.org/works?api_key=sekret-a&api_key=sekret-b&filter=x"


def test_printed_citing_url_drops_adjacent_keys(monkeypatch, capsys):
    import requests

    from dl_lit import OpenAlexScraper

    def failing_request(**kwargs):
        raise requests.exceptions.HTTPError(f"500 Server Error for url: {ADJACENT}")

    monkeypatch.setattr(OpenAlexScraper, "openalex_request_json", failing_request)
    capsys.readouterr()
    OpenAlexScraper.fetch_citing_work_ids(ADJACENT, _NoRateLimit(), api_key="sekret-c")
    out = capsys.readouterr().out
    assert "api_key" not in out and "sekret" not in out
    assert "https://api.openalex.org/works?filter=x" in out


@pytest.mark.parametrize("step", [0, 1, 7])
def test_returned_search_error_drops_adjacent_keys(monkeypatch, step):
    import requests

    from dl_lit import OpenAlexScraper

    def failing_request(**kwargs):
        raise requests.exceptions.HTTPError(f"500 Server Error for url: {ADJACENT}")

    monkeypatch.setattr(OpenAlexScraper, "openalex_request_json", failing_request)
    searcher = OpenAlexScraper.OpenAlexCrossrefSearcher(rate_limiter=_NoRateLimit())
    result = searcher.search("Some title", 2020, None, None, None, step, doi="10.1/x")
    assert result["success"] is False
    assert "api_key" not in result["error"] and "sekret" not in result["error"]
    assert "https://api.openalex.org/works?filter=x" in result["error"]


def _candidate(title, year=2020):
    return {
        "id": f"https://openalex.org/W{abs(hash(title)) % 10000}",
        "display_name": title,
        "publication_year": year,
        "authorships": [{"author": {"display_name": "Jane Doe"}}],
        "type": "article",
    }


@pytest.mark.parametrize("flag, expect_debug", [(None, False), ("1", True)])
def test_match_debug_prints_only_behind_the_flag(monkeypatch, capsys, flag, expect_debug):
    from dl_lit import OpenAlexScraper

    if flag is None:
        monkeypatch.delenv("RAG_FEEDER_MATCH_DEBUG", raising=False)
    else:
        monkeypatch.setenv("RAG_FEEDER_MATCH_DEBUG", flag)

    searcher = OpenAlexScraper.OpenAlexCrossrefSearcher(rate_limiter=_NoRateLimit())
    searcher.search = lambda title, year, container, abstract, keywords, step, rate_limiter=None, doi=None: {
        "step": step,
        "results": [_candidate("A study of things")],
        "success": True,
    }
    searcher.search_semantic_scholar_match = lambda title: {"step": "s2_match", "results": [], "success": True}

    capsys.readouterr()
    OpenAlexScraper.process_single_reference(
        {"title": "A study of things", "authors": ["Jane Doe"], "year": 2020},
        searcher,
        _NoRateLimit(),
        fetch_references=False,
        fetch_citations=False,
    )
    out = capsys.readouterr().out
    assert ("DEBUG: Evaluating potential matches" in out) is expect_debug


# --- worker.log(): one append handle -------------------------------------------------

@pytest.fixture
def worker(monkeypatch):
    mod = load_worker("dt_worker_hygiene", monkeypatch)
    yield mod
    mod._close_log_handle()


def test_log_writes_three_lines_with_one_open(worker, monkeypatch, tmp_path):
    monkeypatch.setenv("RAG_FEEDER_LOG_DIR", str(tmp_path))
    log_file = str(tmp_path / "backend-pipeline.log")
    opens = []
    real_open = builtins.open

    def spy_open(file, *args, **kwargs):
        if str(file) == log_file:
            opens.append(file)
        return real_open(file, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", spy_open)
    for i in range(3):
        worker.log(f"line {i}")
    assert len(opens) == 1
    lines = Path(log_file).read_text().splitlines()
    assert [line.split("[DAEMON] ", 1)[1] for line in lines] == ["line 0", "line 1", "line 2"]


def test_log_follows_a_changed_log_dir_and_a_rotated_file(worker, monkeypatch, tmp_path):
    first, second = tmp_path / "a", tmp_path / "b"
    monkeypatch.setenv("RAG_FEEDER_LOG_DIR", str(first))
    worker.log("one")
    monkeypatch.setenv("RAG_FEEDER_LOG_DIR", str(second))
    worker.log("two")
    assert "one" in (first / "backend-pipeline.log").read_text()
    assert "two" in (second / "backend-pipeline.log").read_text()
    # backend/src/index.js rotates by renaming the file: the next line lands in a new file.
    os.rename(second / "backend-pipeline.log", second / "backend-pipeline.log.1")
    worker.log("three")
    assert "three" in (second / "backend-pipeline.log").read_text()
    assert "three" not in (second / "backend-pipeline.log.1").read_text()


def test_log_reopens_after_a_write_error(worker, monkeypatch, tmp_path):
    monkeypatch.setenv("RAG_FEEDER_LOG_DIR", str(tmp_path))
    worker.log("one")
    real = worker._LOG_STATE["handle"]

    class Broken:
        """Still names the log file's inode, but every write fails (e.g. a lost NFS handle)."""

        def write(self, _):
            raise OSError("stale handle")

        def fileno(self):
            return real.fileno()

        def close(self):
            pass

    worker._LOG_STATE["handle"] = Broken()
    worker.log("two")
    real.close()
    assert [line.split("[DAEMON] ", 1)[1] for line in (tmp_path / "backend-pipeline.log").read_text().splitlines()] == [
        "one",
        "two",
    ]


# --- Crossref -> OpenAlex-like mapping ---------------------------------------------------

def test_crossref_mapping_matches_the_step_8_copy_exactly():
    for item, recorded in zip(CROSSREF_FIXTURE["items"], CROSSREF_FIXTURE["search_step8"]):
        if "results" in recorded:  # the old copy raised on this item; so does the shared one
            with pytest.raises(IndexError):
                identifiers.crossref_item_to_openalex_like(item)
            continue
        assert identifiers.crossref_item_to_openalex_like(item) == recorded


def test_crossref_mapping_is_a_superset_of_the_search_crossref_copy():
    for item, recorded in zip(CROSSREF_FIXTURE["items"], CROSSREF_FIXTURE["search_crossref"]):
        if recorded is None:
            with pytest.raises(IndexError):
                identifiers.crossref_item_to_openalex_like(item)
            continue
        mapped = identifiers.crossref_item_to_openalex_like(item)
        same_keys = {key: mapped[key] for key in recorded}
        if "published-print" in item:
            assert same_keys == recorded
        else:
            # The old copy read published-print only; the shared one falls back to
            # published-online / published (``issued`` is read by neither).
            assert {**same_keys, "publication_year": None} == recorded


class _FakeCrossrefSession:
    def __init__(self, items):
        self.items = items

    def get(self, *args, **kwargs):
        items = self.items
        return SimpleNamespace(raise_for_status=lambda: None, json=lambda: {"message": {"items": items}})


def test_null_referenced_by_count_is_kept_at_both_call_sites():
    """search_crossref never read the count (item kept); step 8 raised TypeError on None and lost
    the whole batch. A null count now maps like 0: the item stays, cited_by_api_url is None."""
    from dl_lit import OpenAlexScraper

    item = {"DOI": "10.1/null", "title": ["Null count"], "is-referenced-by-count": None,
            "published-print": {"date-parts": [[2001]]}}
    other = {"DOI": "10.1/ok", "title": ["Counted"], "is-referenced-by-count": 2}
    searcher = OpenAlexScraper.OpenAlexCrossrefSearcher(rate_limiter=_NoRateLimit())
    searcher.crossref_session = _FakeCrossrefSession([item, other])
    via_crossref = searcher.search_crossref("t", ["a"], 2000)
    via_step8 = searcher.search("t", 2000, None, None, None, 8)
    assert [r["doi"] for r in via_crossref] == ["10.1/null", "10.1/ok"]
    assert via_step8["success"] is True and via_step8["results"] == via_crossref
    assert via_crossref[0]["cited_by_api_url"] is None
    assert via_crossref[1]["cited_by_api_url"] == "https://api.crossref.org/works/10.1/ok/references"


def test_both_crossref_call_sites_use_the_shared_mapping(monkeypatch):
    from dl_lit import OpenAlexScraper

    items = [item for item, rec in zip(CROSSREF_FIXTURE["items"], CROSSREF_FIXTURE["search_crossref"]) if rec]
    searcher = OpenAlexScraper.OpenAlexCrossrefSearcher(rate_limiter=_NoRateLimit())
    searcher.crossref_session = _FakeCrossrefSession(items)
    expected = [identifiers.crossref_item_to_openalex_like(item) for item in items]
    assert searcher.search_crossref("t", ["a"], 2000) == expected
    assert searcher.search("t", 2000, None, None, None, 8)["results"] == expected


# --- a resolved-identifier conflict inserts instead of merging ------------------------------

class _ResolvingSearcher:
    """Resolve each record to the DOI keyed by its authors (the two records share title and year)."""

    def __init__(self, dois):
        self.dois = dois

    def resolve(self, ref):
        authors = ref.get("authors")
        if isinstance(authors, str):
            authors = json.loads(authors)
        return {"doi": self.dois[authors[0]], "openalex_id": None}


def test_resolved_identifier_conflict_keeps_both_records(tmp_path, monkeypatch):
    db = DatabaseManager(str(tmp_path / "lit.db"))
    searcher = _ResolvingSearcher({"Alice Smith": "10.1/first", "Bob Jones": "10.1/second"})
    monkeypatch.setattr(db, "_resolve_identifiers_for_ref", lambda ref, s, rl: s.resolve(ref))
    existing_id, err = db.create_pending_work(
        {"title": "Migration and the European Union", "authors": ["Alice Smith"], "year": 2020, "corpus_id": 1}
    )
    assert err is None

    new_id, err = db.create_pending_work(
        {"title": "Migration and the European Union", "authors": ["Bob Jones"], "year": 2020, "corpus_id": 2},
        searcher=searcher,
        resolve_potential_duplicates=True,
    )

    assert err is None and new_id not in (None, existing_id)
    rows = db.conn.execute("SELECT id, authors FROM works ORDER BY id").fetchall()
    assert rows == [(existing_id, '["Alice Smith"]'), (new_id, '["Bob Jones"]')]
    log = db.conn.execute("SELECT canonical_id, action, notes FROM merge_log").fetchall()
    assert log == [(existing_id, "conflict", "resolved_ids_conflict")]
    corpora = db.conn.execute("SELECT corpus_id, work_id FROM corpus_works ORDER BY corpus_id").fetchall()
    assert corpora == [(1, existing_id), (2, new_id)]


def test_worker_import_leaves_the_process_streams_alone():
    import sys

    before = sys.stdout, sys.stderr
    mod = load_worker("dt_worker_streams")
    assert (sys.stdout, sys.stderr) == before
    assert not isinstance(sys.stdout, mod._LockedTextStream)


# --- final wave I1: an identifier conflict survives enrichment ------------------------------

def _conflicting_pair(tmp_path, monkeypatch):
    db = DatabaseManager(str(tmp_path / "lit.db"))
    searcher = _ResolvingSearcher({"Alice Smith": "10.1/first", "Bob Jones": "10.1/second"})
    monkeypatch.setattr(db, "_resolve_identifiers_for_ref", lambda ref, s, rl: s.resolve(ref))
    first_id, _ = db.create_pending_work(
        {"title": "Migration and the European Union", "authors": ["Alice Smith"], "year": 2020, "corpus_id": 1}
    )
    second_id, err = db.create_pending_work(
        {"title": "Migration and the European Union", "authors": ["Bob Jones"], "year": 2020, "corpus_id": 2},
        searcher=searcher,
        resolve_potential_duplicates=True,
    )
    assert err is None
    return db, first_id, second_id


def test_resolved_conflict_stores_the_resolved_identifiers_on_the_new_record(tmp_path, monkeypatch):
    db, _, second_id = _conflicting_pair(tmp_path, monkeypatch)
    row = db.get_entry_by_id("works", second_id)
    assert (row["doi"], row["normalized_doi"]) == ("10.1/second", db._normalize_doi("10.1/second"))


def test_enrichment_does_not_absorb_a_record_with_a_different_doi(tmp_path, monkeypatch):
    db, first_id, second_id = _conflicting_pair(tmp_path, monkeypatch)

    result = db.apply_enriched_metadata(
        first_id,
        {"title": "Migration and the European Union", "authors": ["Alice Smith"], "year": 2020, "doi": "10.1/first"},
    )

    assert result == (first_id, None)
    rows = db.conn.execute("SELECT id, authors, normalized_doi FROM works ORDER BY id").fetchall()
    norm = db._normalize_doi
    assert rows == [(first_id, '["Alice Smith"]', norm("10.1/first")), (second_id, '["Bob Jones"]', norm("10.1/second"))]
    corpora = db.conn.execute("SELECT corpus_id, work_id FROM corpus_works ORDER BY corpus_id").fetchall()
    assert corpora == [(1, first_id), (2, second_id)]


def test_enrichment_does_not_absorb_a_record_with_a_different_openalex_id(tmp_path):
    db = DatabaseManager(str(tmp_path / "lit.db"))
    a = db._insert_work_record({"title": "Same Title", "normalized_title": db._normalize_text("Same Title"), "year": 2020})
    b = db._insert_work_record(
        {"title": "Same Title", "normalized_title": db._normalize_text("Same Title"), "year": 2020, "openalex_id": "W2"}
    )
    db.conn.commit()
    assert db.apply_enriched_metadata(a, {"title": "Same Title", "year": 2020, "openalex_id": "W1"}) == (a, None)
    assert db.conn.execute("SELECT COUNT(*) FROM works").fetchone()[0] == 2
    # comparing an existing row (exclude_id), an alias of the other record is no match either;
    # an incoming record (no exclude_id) has no guard
    db.add_work_alias("works", b, "Alias Title", alias_year=2020)
    assert db._find_existing_work(title="Alias Title", year=2020, openalex_id="W9", exclude_id=a) == (None, None)
    assert db._find_existing_work(title="Alias Title", year=2020, exclude_id=a) == (b, "alias_title")
    assert db._find_existing_work(title="Alias Title", year=2020, openalex_id="W9") == (b, "alias_title")


@pytest.mark.parametrize("candidate_doi", [None, "10.1/shared"])
def test_title_year_match_without_conflicting_identifiers_still_merges(tmp_path, candidate_doi):
    db = DatabaseManager(str(tmp_path / "lit.db"))
    norm = db._normalize_text("Same Title")
    a = db._insert_work_record({"title": "Same Title", "normalized_title": norm, "year": 2020})
    extra = {"doi": candidate_doi, "normalized_doi": candidate_doi} if candidate_doi else {}
    b = db._insert_work_record({"title": "Same Title", "normalized_title": norm, "year": 2020, **extra})
    db.conn.commit()

    work_id, err = db.apply_enriched_metadata(a, {"title": "Same Title", "year": 2020})

    assert work_id is None and f"(ID {b}) on title_year" in err
    assert [r[0] for r in db.conn.execute("SELECT id FROM works")] == [b]


# --- final wave I4: request exceptions never carry the api key ------------------------------

class _FiveHundredSession:
    def get(self, url, headers=None, params=None, timeout=None):
        import requests

        resp = requests.Response()
        resp.status_code = 500
        resp.reason = "Server Error"
        resp.url = requests.Request("GET", url, params=params).prepare().url
        return resp


class _DownSession:
    def __init__(self, exc_type):
        self.exc_type = exc_type

    def get(self, url, headers=None, params=None, timeout=None):
        import requests

        full = requests.Request("GET", url, params=params).prepare().url
        raise self.exc_type(f"HTTPSConnectionPool: Max retries exceeded with url: {full}")


class _FreeLimiter:
    def remaining_block_seconds(self, *args):
        return 0.0

    def wait_if_needed(self, *args, **kwargs):
        return True

    def impose_block(self, *args, **kwargs):
        pass


@pytest.fixture
def keyed_env(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENALEX_API_KEY", "sekret-key-987")
    monkeypatch.setenv("RAG_FEEDER_OPENALEX_QUOTA_PATH", str(tmp_path / "quota.json"))
    from dl_lit import utils

    monkeypatch.setattr(utils.time, "sleep", lambda s: None)
    return utils


def _assert_redacted(exc):
    assert "sekret-key-987" not in str(exc) and "api_key" not in str(exc)
    assert "sekret-key-987" not in repr(exc) and "api_key" not in repr(exc)


def test_redact_openalex_key_is_shared():
    from dl_lit import OpenAlexScraper, utils

    assert utils.redact_openalex_key("https://x/w?api_key=k&p=1") == "https://x/w?p=1"
    assert OpenAlexScraper._redact_api_key is utils.redact_openalex_key


def test_openalex_request_json_http_error_is_redacted(keyed_env):
    import requests

    with pytest.raises(requests.exceptions.HTTPError) as info:
        keyed_env.openalex_request_json(
            url="https://api.openalex.org/works", params={"filter": "doi:x"},
            session=_FiveHundredSession(), rate_limiter=_FreeLimiter(), retries=1,
        )
    _assert_redacted(info.value)
    assert "https://api.openalex.org/works?filter=doi%3Ax" in str(info.value)
    assert info.value.response is not None and info.value.response.status_code == 500


@pytest.mark.parametrize("exc_name", ["ConnectionError", "Timeout"])
def test_openalex_request_json_transport_error_is_redacted(keyed_env, exc_name):
    import requests

    exc_type = getattr(requests.exceptions, exc_name)
    with pytest.raises(exc_type) as info:
        keyed_env.openalex_request_json(
            url="https://api.openalex.org/works", params={"filter": "doi:x"},
            session=_DownSession(exc_type), rate_limiter=_FreeLimiter(), retries=2,
        )
    _assert_redacted(info.value)


def test_keyword_search_script_openalex_get_is_redacted(keyed_env, monkeypatch):
    import requests

    mod = _load_keyword_search_script(monkeypatch, "keyword_search_script_i4")
    monkeypatch.setattr(mod, "get_openalex_session", lambda: _FiveHundredSession())
    monkeypatch.setattr(mod, "get_global_rate_limiter", lambda: _FreeLimiter())

    with pytest.raises(requests.exceptions.HTTPError) as info:
        mod._openalex_get({"filter": "doi:x"}, retries=1)
    _assert_redacted(info.value)
    assert info.value.response.status_code == 500


def _load_keyword_search_script(monkeypatch, name):
    import importlib.util

    scripts = Path(__file__).resolve().parents[2] / "backend" / "scripts"
    monkeypatch.syspath_prepend(str(scripts))
    spec = importlib.util.spec_from_file_location(name, scripts / "keyword_search.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_keyword_search_script_error_path_redacts(monkeypatch, tmp_path, capsys):
    import sys

    mod = _load_keyword_search_script(monkeypatch, "keyword_search_script_i4_main")
    leaked = "boom for url: https://api.openalex.org/works?api_key=sekret-key-987&page=2"

    def boom(**kwargs):
        raise RuntimeError(leaked)

    monkeypatch.setattr(mod, "search_openalex", boom)
    monkeypatch.setattr(sys, "argv", ["x", "--db-path", str(tmp_path / "t.db"), "--query", "q"])
    with pytest.raises(SystemExit) as info:
        mod.run_cli()
    assert info.value.code == 1
    err = capsys.readouterr().err
    assert "RuntimeError" in err and "https://api.openalex.org/works?page=2" in err
    assert "sekret-key-987" not in err and "api_key" not in err
    db = DatabaseManager(str(tmp_path / "t.db"))
    stored = db.conn.execute("SELECT status, error FROM search_runs").fetchone()
    assert stored[0] == "failed" and "sekret" not in stored[1] and "api_key" not in stored[1]


# --- final wave round 2: I1b (the existing record keeps its resolved id), I1c (owned ids match) --

def test_resolved_conflict_stores_the_existing_records_identifier(tmp_path, monkeypatch):
    db, first_id, _ = _conflicting_pair(tmp_path, monkeypatch)
    row = db.get_entry_by_id("works", first_id)
    assert (row["doi"], row["normalized_doi"]) == ("10.1/first", db._normalize_doi("10.1/first"))


def test_enriching_the_new_record_without_a_doi_keeps_both(tmp_path, monkeypatch):
    db, first_id, second_id = _conflicting_pair(tmp_path, monkeypatch)

    result = db.apply_enriched_metadata(
        second_id, {"title": "Migration and the European Union", "authors": ["Bob Jones"], "year": 2020}
    )

    assert result == (second_id, None)
    rows = db.conn.execute("SELECT id, authors FROM works ORDER BY id").fetchall()
    assert rows == [(first_id, '["Alice Smith"]'), (second_id, '["Bob Jones"]')]
    corpora = db.conn.execute("SELECT corpus_id, work_id FROM corpus_works ORDER BY corpus_id").fetchall()
    assert corpora == [(1, first_id), (2, second_id)]


def test_existing_records_identifier_owned_elsewhere_is_not_stored(tmp_path, monkeypatch):
    db = DatabaseManager(str(tmp_path / "lit.db"))
    searcher = _ResolvingSearcher({"Carol King": "10.1/owned", "Bob Jones": "10.1/new"})
    monkeypatch.setattr(db, "_resolve_identifiers_for_ref", lambda ref, s, rl: s.resolve(ref))
    owner, _ = db.create_pending_work({"title": "Another Work", "doi": "10.1/owned", "year": 2001})
    c_id, _ = db.create_pending_work({"title": "Shared Title", "authors": ["Carol King"], "year": 2020})
    b_id, err = db.create_pending_work(
        {"title": "Shared Title", "authors": ["Bob Jones"], "year": 2020},
        searcher=searcher, resolve_potential_duplicates=True,
    )
    assert err is None and b_id not in (owner, c_id)
    assert db.get_entry_by_id("works", c_id)["normalized_doi"] in (None, "")
    assert db.get_entry_by_id("works", owner)["normalized_doi"] == db._normalize_doi("10.1/owned")


def test_a_resolved_identifier_owned_by_another_row_links_to_that_row(tmp_path, monkeypatch):
    db = DatabaseManager(str(tmp_path / "lit.db"))
    searcher = _ResolvingSearcher({"Bob Jones": "10.1/x", "Carol King": "10.1/c"})
    monkeypatch.setattr(db, "_resolve_identifiers_for_ref", lambda ref, s, rl: s.resolve(ref))
    a_id, _ = db.create_pending_work({"title": "Original Work", "doi": "10.1/x", "year": 2020, "corpus_id": 1})
    c_id, _ = db.create_pending_work({"title": "Shared Title", "authors": ["Carol King"], "year": 2020, "corpus_id": 1})

    row_id, err = db.create_pending_work(
        {"title": "Shared Title", "authors": ["Bob Jones"], "year": 2020, "publisher": "Pub B", "corpus_id": 2},
        searcher=searcher,
        resolve_potential_duplicates=True,
    )

    assert (row_id, err) == (None, f"Duplicate already exists in works (ID {a_id}) on normalized_doi")
    assert [r[0] for r in db.conn.execute("SELECT id FROM works ORDER BY id")] == [a_id, c_id]
    assert db.get_entry_by_id("works", a_id)["publisher"] == "Pub B"
    assert db.get_entry_by_id("works", a_id)["title"] == "Original Work"
    corpora = db.conn.execute("SELECT corpus_id, work_id FROM corpus_works ORDER BY corpus_id, work_id").fetchall()
    assert corpora == [(1, a_id), (1, c_id), (2, a_id)]
    assert db.get_entry_by_id("works", c_id)["normalized_doi"] == db._normalize_doi("10.1/c")
