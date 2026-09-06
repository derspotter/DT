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

    with pytest.raises(SystemExit) as excinfo:
        mod.main()
    assert excinfo.value.code == 0

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
