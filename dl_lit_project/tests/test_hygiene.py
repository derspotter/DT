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
