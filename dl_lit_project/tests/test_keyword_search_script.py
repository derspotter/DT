import importlib.util
import json
import os
import signal
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / 'backend' / 'scripts' / 'keyword_search.py'
SCRIPTS_DIR = SCRIPT.parent


def _load(monkeypatch):
    monkeypatch.syspath_prepend(str(SCRIPTS_DIR))
    spec = importlib.util.spec_from_file_location('keyword_search_script', SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _events(capsys):
    out = capsys.readouterr().out.strip().splitlines()
    return [json.loads(line) for line in out if line.startswith('{')]


def test_persists_each_page_and_emits_progress(monkeypatch, tmp_path, capsys):
    mod = _load(monkeypatch)
    pages = [
        [{"id": "https://openalex.org/W1", "display_name": "one"}, {"id": "https://openalex.org/W2", "display_name": "two"}],
        [{"id": "https://openalex.org/W3", "display_name": "three"}],
    ]

    def fake_search(**kwargs):
        on_page = kwargs["on_page"]
        for page in pages:
            on_page(page, {"count": 3})
        return [i for p in pages for i in p]

    monkeypatch.setattr(mod, "search_openalex", fake_search)
    monkeypatch.setattr(sys, "argv", ["x", "--db-path", str(tmp_path / "t.db"), "--query", "q", "--corpus-id", "1"])
    mod.main()
    events = _events(capsys)
    assert events[0]["event"] == "run_created"
    run_id = events[0]["runId"]
    progress = [e for e in events if e.get("event") == "progress"]
    assert [(p["fetched"], p["expected"]) for p in progress] == [(2, 3), (3, 3)]
    final = events[-1]
    assert final["runId"] == run_id and final["fetched_count"] == 3 and final["truncated_results"] is False

    from dl_lit.db_manager import DatabaseManager
    db = DatabaseManager(db_path=tmp_path / "t.db")
    row = db.conn.execute("SELECT status, fetched_count, expected_count FROM search_runs WHERE id = ?", (run_id,)).fetchone()
    assert tuple(row) == ("done", 3, 3)
    assert db.count_search_results(run_id) == 3
    filters = json.loads(db.conn.execute("SELECT filters_json FROM search_runs WHERE id = ?", (run_id,)).fetchone()[0])
    assert filters["related_sort"] == mod.DEFAULT_RELATED_SORT
    db.close_connection()


def test_failure_after_run_creation_marks_failed(monkeypatch, tmp_path, capsys):
    mod = _load(monkeypatch)

    def boom(**kwargs):
        kwargs["on_page"]([{"id": "https://openalex.org/W1", "display_name": "one"}], {"count": 9})
        raise RuntimeError("openalex down")

    monkeypatch.setattr(mod, "search_openalex", boom)
    monkeypatch.setattr(sys, "argv", ["x", "--db-path", str(tmp_path / "t.db"), "--query", "q", "--corpus-id", "1"])
    try:
        mod.main()
    except RuntimeError:
        pass
    events = _events(capsys)
    run_id = events[0]["runId"]
    from dl_lit.db_manager import DatabaseManager
    db = DatabaseManager(db_path=tmp_path / "t.db")
    row = db.conn.execute("SELECT status, error, fetched_count FROM search_runs WHERE id = ?", (run_id,)).fetchone()
    assert row[0] == "failed" and "openalex down" in row[1] and row[2] == 1
    db.close_connection()


def test_count_only(monkeypatch, tmp_path, capsys):
    mod = _load(monkeypatch)
    monkeypatch.setattr(mod, "count_openalex", lambda **kwargs: 342118)
    monkeypatch.setattr(sys, "argv", ["x", "--db-path", str(tmp_path / "t.db"), "--query", "q", "--count-only"])
    mod.main()
    assert _events(capsys)[-1] == {"count": 342118}


def test_sigterm_marks_cancelled_and_exits_zero(monkeypatch, tmp_path, capsys):
    mod = _load(monkeypatch)

    def fake_search(**kwargs):
        on_page = kwargs["on_page"]
        on_page([{"id": "https://openalex.org/W1", "display_name": "one"}], {"count": 5})
        os.kill(os.getpid(), signal.SIGTERM)
        # Unreachable once the handler exits, but harmless if the signal is
        # somehow delivered later than expected.
        return []

    monkeypatch.setattr(mod, "search_openalex", fake_search)
    monkeypatch.setattr(sys, "argv", ["x", "--db-path", str(tmp_path / "t.db"), "--query", "q", "--corpus-id", "1"])

    # main() installs a SIGTERM handler and exits before restoring it; leaving it
    # installed would make a later SIGTERM in this process run the handler again.
    previous = signal.getsignal(signal.SIGTERM)
    try:
        with pytest.raises(SystemExit) as excinfo:
            mod.main()
        assert excinfo.value.code == 0
    finally:
        signal.signal(signal.SIGTERM, previous if callable(previous) or previous in (signal.SIG_DFL, signal.SIG_IGN) else signal.SIG_DFL)

    events = _events(capsys)
    run_id = events[0]["runId"]
    from dl_lit.db_manager import DatabaseManager
    db = DatabaseManager(db_path=tmp_path / "t.db")
    row = db.conn.execute("SELECT status, fetched_count FROM search_runs WHERE id = ?", (run_id,)).fetchone()
    assert tuple(row) == ("cancelled", 1)
    db.close_connection()


def test_truncation_when_inline_limit_exceeded(monkeypatch, tmp_path, capsys):
    mod = _load(monkeypatch)
    items = [
        {"id": "https://openalex.org/W1", "display_name": "one"},
        {"id": "https://openalex.org/W2", "display_name": "two"},
    ]

    def fake_search(**kwargs):
        kwargs["on_page"](items, {"count": 2})
        return items

    monkeypatch.setattr(mod, "search_openalex", fake_search)
    monkeypatch.setattr(sys, "argv", [
        "x", "--db-path", str(tmp_path / "t.db"), "--query", "q", "--corpus-id", "1",
        "--inline-results-limit", "0",
    ])
    mod.main()
    final = _events(capsys)[-1]
    assert final["truncated_results"] is True
    assert final["results"] == []
    assert final["fetched_count"] == 2


def test_unbounded_run_without_expansion_keeps_no_items(monkeypatch, tmp_path, capsys):
    """A plain query run must not retain the fetched works, yet still persist and report them."""
    mod = _load(monkeypatch)
    pages = [
        [{"id": "https://openalex.org/W1", "display_name": "one", "publication_year": 2001}],
        [{"id": "https://openalex.org/W2", "display_name": "two", "publication_year": 2002}],
        [{"id": "https://openalex.org/W3", "display_name": "three", "publication_year": 2003}],
    ]
    seen = {}

    def fake_search(**kwargs):
        seen["accumulate"] = kwargs.get("accumulate")
        for page in pages:
            kwargs["on_page"](page, {"count": 3})
        return [] if kwargs.get("accumulate") is False else [i for p in pages for i in p]

    monkeypatch.setattr(mod, "search_openalex", fake_search)
    monkeypatch.setattr(sys, "argv", ["x", "--db-path", str(tmp_path / "t.db"), "--query", "q", "--corpus-id", "1"])
    mod.main()

    assert seen["accumulate"] is False
    events = _events(capsys)
    run_id = events[0]["runId"]
    final = events[-1]
    assert final["fetched_count"] == 3
    assert final["truncated_results"] is False
    # Rendered from the persisted rows, not from a retained in-memory list.
    assert [r["title"] for r in final["results"]] == ["one", "two", "three"]
    assert [r["year"] for r in final["results"]] == [2001, 2002, 2003]

    from dl_lit.db_manager import DatabaseManager
    db = DatabaseManager(db_path=tmp_path / "t.db")
    assert db.count_search_results(run_id) == 3
    db.close_connection()


def test_enqueue_and_expansion_still_accumulate(monkeypatch, tmp_path, capsys):
    """The two consumers of the items (enqueue, expansion) still get the full list."""
    mod = _load(monkeypatch)
    items = [{"id": "https://openalex.org/W1", "display_name": "one"}]
    calls = []

    def fake_search(**kwargs):
        calls.append(kwargs.get("accumulate"))
        kwargs["on_page"](items, {"count": 1})
        return items

    monkeypatch.setattr(mod, "search_openalex", fake_search)
    monkeypatch.setattr(mod, "expand_references_recursive",
                        lambda base_items, **kwargs: (list(base_items),
                                                      {"added": 0, "processed": 1,
                                                       "downstream_added": 0, "upstream_added": 0}))

    monkeypatch.setattr(sys, "argv", [
        "x", "--db-path", str(tmp_path / "enq.db"), "--query", "q", "--corpus-id", "1", "--enqueue",
    ])
    mod.main()

    monkeypatch.setattr(sys, "argv", [
        "x", "--db-path", str(tmp_path / "exp.db"), "--query", "q", "--corpus-id", "1",
        "--include-downstream", "--related-depth", "1",
    ])
    mod.main()

    assert calls == [True, True]
    capsys.readouterr()


def test_search_openalex_accumulate_false_returns_nothing(monkeypatch):
    """The library paginator hands every page to the callback but keeps none of it."""
    from dl_lit import keyword_search as ks

    responses = [
        {"results": [{"id": "W1"}, {"id": "W2"}], "meta": {"count": 5, "next_cursor": "c2"}},
        {"results": [{"id": "W3"}, {"id": "W4"}], "meta": {"count": 5, "next_cursor": "c3"}},
        {"results": [{"id": "W5"}], "meta": {"count": 5, "next_cursor": None}},
    ]
    calls = iter(responses)
    monkeypatch.setattr(ks, "_openalex_request", lambda *a, **k: next(calls))

    pages = []
    out = ks.search_openalex("q", max_results=None, on_page=lambda items, meta: pages.append([i["id"] for i in items]),
                             accumulate=False)
    assert out == []
    assert pages == [["W1", "W2"], ["W3", "W4"], ["W5"]]

    calls = iter(responses)
    kept = ks.search_openalex("q", max_results=None, on_page=lambda items, meta: None, accumulate=True)
    assert [i["id"] for i in kept] == ["W1", "W2", "W3", "W4", "W5"]


def test_search_openalex_accumulate_false_honours_max_results(monkeypatch):
    from dl_lit import keyword_search as ks

    calls = iter([
        {"results": [{"id": "W1"}, {"id": "W2"}, {"id": "W3"}], "meta": {"count": 9, "next_cursor": "c2"}},
    ])
    monkeypatch.setattr(ks, "_openalex_request", lambda *a, **k: next(calls))
    pages = []
    out = ks.search_openalex("q", max_results=2, on_page=lambda items, meta: pages.append(len(items)),
                             accumulate=False)
    assert out == []
    assert pages == [2]


def test_search_openalex_accumulate_false_requires_callback():
    from dl_lit import keyword_search as ks
    import pytest as _pytest

    with _pytest.raises(ValueError):
        ks.search_openalex("q", accumulate=False)
