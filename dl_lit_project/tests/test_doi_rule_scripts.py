"""export_bundle, upstream_update and backfill_openalex_topics use the DB's DOI rule (PY-14)."""

import importlib.util
import json
import sqlite3
import sys
from pathlib import Path

import pytest

from dl_lit.db_manager import DatabaseManager
from dl_lit.identifiers import normalize_doi

SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "backend" / "scripts"


def _script(name):
    if str(SCRIPTS_DIR) not in sys.path:
        sys.path.insert(0, str(SCRIPTS_DIR))
    spec = importlib.util.spec_from_file_location(f"{name}_doi_rule_test", SCRIPTS_DIR / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def export_bundle():
    return _script("export_bundle")


@pytest.fixture
def upstream_update():
    return _script("upstream_update")


@pytest.fixture
def backfill():
    return _script("backfill_openalex_topics")


def _make_db(tmp_path):
    path = tmp_path / "works.db"
    manager = DatabaseManager(path)
    manager.close_connection()
    return path


def _insert(conn, **fields):
    fields.setdefault("title", "A title")
    cols = ", ".join(fields)
    marks = ", ".join("?" for _ in fields)
    cur = conn.execute(f"INSERT INTO works ({cols}) VALUES ({marks})", tuple(fields.values()))
    conn.commit()
    return cur.lastrowid


def _connect(path):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


# --- all three scripts -------------------------------------------------------


def test_scripts_use_the_db_doi_rule():
    for name in ("export_bundle", "upstream_update", "backfill_openalex_topics"):
        mod = _script(name)
        assert mod.normalize_doi is normalize_doi
        assert not hasattr(mod, "_normalize_doi")


# --- export_bundle -----------------------------------------------------------


def test_export_uses_the_stored_db_key(tmp_path, export_bundle):
    conn = _connect(_make_db(tmp_path))
    doi = "https://doi.org/10.1000/Ab.C"
    assert normalize_doi(doi) != "10.1000/STORED"
    _insert(conn, doi=doi, normalized_doi="10.1000/STORED")
    item = export_bundle.load_items(conn, None)[0]
    assert item["normalized_doi"] == "10.1000/STORED" and item["doi"] == "https://doi.org/10.1000/Ab.C"


def test_export_falls_back_to_the_rule_when_the_key_is_empty(tmp_path, export_bundle):
    conn = _connect(_make_db(tmp_path))
    _insert(conn, doi="doi:10.5555/xy", normalized_doi=None)
    item = export_bundle.load_items(conn, None)[0]
    assert item["normalized_doi"] == "10.5555/XY"


def test_export_dedupes_on_the_db_key(tmp_path, export_bundle):
    conn = _connect(_make_db(tmp_path))
    _insert(conn, title="One", doi="10.1000/ABC")
    _insert(conn, title="Two", doi="Smith 2020, https://doi.org/10.1000/abc")
    assert len(export_bundle.load_items(conn, None)) == 1


def test_export_still_prefers_the_openalex_key(tmp_path, export_bundle):
    conn = _connect(_make_db(tmp_path))
    _insert(conn, title="One", doi="10.1000/ABC", openalex_id="W1")
    _insert(conn, title="Two", doi="10.1000/ABC", openalex_id="W2")
    assert len(export_bundle.load_items(conn, None)) == 2


def test_export_bibtex_keeps_the_doi_of_bibtex_entry_json(export_bundle):
    item = {
        "id": 1,
        "status": "matched",
        "title": "T",
        "authors": "",
        "year": None,
        "doi": "10.1000/other",
        "normalized_doi": "10.1000/OTHER",
        "source": "",
        "entry_type": "misc",
        "bibtex_key": None,
        "bibtex_entry_json": json.dumps({"doi": "10.1000/FROM-BIB"}),
        "file_path": None,
    }
    entry = export_bundle.to_bibtex_entry(item)
    assert "10.1000/FROM-BIB" in entry and "10.1000/other" not in entry


# --- upstream_update ---------------------------------------------------------


def test_upstream_bibtex_doi_is_the_lowercase_rule(upstream_update):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE w (id INTEGER, title TEXT, authors TEXT, year INTEGER, abstract TEXT, doi TEXT,"
        " source TEXT, url TEXT, openalex_id TEXT, type TEXT, bibtex_entry_json TEXT, file_path TEXT)"
    )
    conn.execute(
        "INSERT INTO w (id, title, doi) VALUES (1, 'A title', 'Smith 2020, doi 10.1000/ABC, p. 3')"
    )
    row = conn.execute("SELECT * FROM w").fetchone()
    key, entry = upstream_update.render_bibtex_entry(row, "x.pdf", set())
    assert "doi = {10.1000/abc}" in entry and key == "doi_10_1000_abc"
    assert upstream_update.display_doi("https://doi.org/10.1000/ABC") == "10.1000/abc"
    assert upstream_update.display_doi(None) == ""


# --- backfill_openalex_topics ------------------------------------------------


def test_backfill_validates_the_whole_doi_before_taking_the_key(backfill):
    assert backfill.bare_doi("https://doi.org/10.1000/ABC") == "10.1000/abc"
    for raw in ("10.1000/ABC+123", "10.1000/ABC|xyz", "10.bogus|xyz", "10.1/abc", "Smith 2020, doi 10.1000/ABC", None):
        assert backfill.bare_doi(raw) is None, raw
    assert backfill.bare_doi(" doi:10.1000/ABC. ") == "10.1000/abc."
    assert backfill.bare_doi("10.1000/K") is None
    assert backfill.doi_filter(["10.1000/ABC"]) == "doi:10.1000/abc"


def test_backfill_helpers_carry_the_specified_annotations(backfill):
    import typing

    assert typing.get_type_hints(backfill.bare_doi) == {"return": typing.Optional[str]}
    assert typing.get_type_hints(backfill.doi_filter) == {"tokens": list[str], "return": str}
    assert typing.get_type_hints(backfill.doi_rows) == {
        "scope_clause": str,
        "limit_clause": str,
        "return": tuple[list[tuple[int, str, str]], int],
    }
    assert typing.get_type_hints(backfill.write_results) == {"guard": str, "scope_clause": str, "return": list[int]}
    assert typing.get_type_hints(backfill.run_pass) == {"return": tuple[int, int, int]}
    assert typing.get_type_hints(backfill)["WRITE_GUARDS"] == dict[str, str]


def _backfill_db(tmp_path, backfill):
    path = _make_db(tmp_path)
    conn = _connect(path)
    backfill.ensure_columns(conn)
    return path, conn


def _topic(name):
    return {
        "display_name": name,
        "subfield": {"display_name": f"{name}-sub"},
        "field": {"display_name": f"{name}-field"},
        "domain": {"display_name": f"{name}-domain"},
    }


def _fake_openalex(monkeypatch, backfill, results_for):
    filters = []

    def fake_request(endpoint, params, mailto):
        filters.append(params["filter"])
        return {"results": results_for(params["filter"])}

    monkeypatch.setattr(backfill, "get_openalex_mailto", lambda: "")
    monkeypatch.setattr(backfill, "openalex_request_json", fake_request)
    return filters


def _run(monkeypatch, backfill, db_path, *extra):
    monkeypatch.setattr(sys, "argv", ["backfill", "--db-path", str(db_path), *extra])
    backfill.main()


def _topic_row(db_path, work_id):
    conn = _connect(db_path)
    row = conn.execute(
        "SELECT primary_topic, primary_field, topics_fetched FROM works WHERE id = ?", (work_id,)
    ).fetchone()
    conn.close()
    return tuple(row)


def test_backfill_pass2_keeps_punctuated_and_plain_dois_apart(tmp_path, monkeypatch, capsys, backfill):
    db_path, conn = _backfill_db(tmp_path, backfill)
    for doi in ("10.1000/ABC", "10.1000/ABC.", "10.1000/XYZ;", "10.1000/K", "10.1000/K"):
        _insert(conn, doi=doi, metadata_status="matched")
    conn.close()

    def results(_filter):
        return [
            {"id": "https://openalex.org/W1", "doi": "https://doi.org/10.1000/abc", "primary_topic": _topic("T1"), "topics": []},
            {"id": "https://openalex.org/W2", "doi": "https://doi.org/10.1000/abc.", "primary_topic": _topic("T2"), "topics": []},
            {"id": "https://openalex.org/W3", "doi": "https://doi.org/10.1000/K", "primary_topic": _topic("T3"), "topics": []},
        ]

    filters = _fake_openalex(monkeypatch, backfill, results)
    _run(monkeypatch, backfill, db_path)

    assert filters == ["doi:10.1000/abc|10.1000/abc.|10.1000/xyz;|10.1000/k"]
    assert _topic_row(db_path, 1) == ("T1", "T1-field", 1)
    assert _topic_row(db_path, 2) == ("T2", "T2-field", 1)
    assert _topic_row(db_path, 3) == (None, None, None)
    assert _topic_row(db_path, 4) == (None, None, None)
    assert _topic_row(db_path, 5) == (None, None, None)
    assert "1 skipped (not a whole DOI)" in capsys.readouterr().err


def test_backfill_pass2_sends_only_whole_dois_and_updates_the_row(tmp_path, monkeypatch, capsys, backfill):
    db_path, conn = _backfill_db(tmp_path, backfill)
    _insert(conn, doi="https://doi.org/10.1000/ABC", metadata_status="matched")
    _insert(conn, doi="10.bogus|xyz", metadata_status="matched")
    _insert(conn, doi="10.1/abc", metadata_status="matched")
    _insert(conn, doi="10.1000/DEF", metadata_status="matched", topics_fetched=1)
    conn.close()

    filters = _fake_openalex(
        monkeypatch,
        backfill,
        lambda _f: [
            {"id": "https://openalex.org/W1", "doi": "https://doi.org/10.1000/abc", "primary_topic": _topic("T1"), "topics": []}
        ],
    )
    _run(monkeypatch, backfill, db_path)

    assert filters == ["doi:10.1000/abc"]
    assert _topic_row(db_path, 1) == ("T1", "T1-field", 1)
    assert _topic_row(db_path, 2) == (None, None, None)
    assert _topic_row(db_path, 3) == (None, None, None)
    assert _topic_row(db_path, 4) == (None, None, 1)
    assert "2 skipped (not a whole DOI)" in capsys.readouterr().err


def test_backfill_pass2_never_updates_a_row_whose_doi_only_starts_like_a_valid_one(tmp_path, monkeypatch, backfill):
    db_path, conn = _backfill_db(tmp_path, backfill)
    _insert(conn, doi="10.1000/ABC+123", metadata_status="matched")
    conn.close()

    filters = _fake_openalex(
        monkeypatch,
        backfill,
        lambda _f: [
            {"id": "https://openalex.org/W1", "doi": "https://doi.org/10.1000/abc", "primary_topic": _topic("T1"), "topics": []}
        ],
    )
    _run(monkeypatch, backfill, db_path)

    assert filters == []
    assert _topic_row(db_path, 1) == (None, None, None)


def test_backfill_pass2_keeps_scope_and_limit(tmp_path, monkeypatch, backfill):
    db_path, conn = _backfill_db(tmp_path, backfill)
    _insert(conn, doi="10.1000/ONE", metadata_status="matched")
    _insert(conn, doi="10.1000/TWO", metadata_status="matched")
    _insert(conn, doi="10.1000/THREE", metadata_status="pending")
    conn.close()

    filters = _fake_openalex(monkeypatch, backfill, lambda _f: [])
    _run(monkeypatch, backfill, db_path, "--limit", "1")
    assert filters == ["doi:10.1000/one"]

    filters.clear()
    _run(monkeypatch, backfill, db_path)
    assert filters == ["doi:10.1000/one|10.1000/two"]

    filters.clear()
    _run(monkeypatch, backfill, db_path, "--scope", "all")
    assert len(filters) == 1 and "10.1000/three" in filters[0].split(":", 1)[1].split("|")



# --- the backfill writes only rows still in the state it read (PY-34) ---------


def _work(openalex, doi, topic):
    return {"id": f"https://openalex.org/{openalex}", "doi": f"https://doi.org/{doi}", "primary_topic": _topic(topic), "topics": []}


def _fake_openalex_changing(monkeypatch, backfill, db_path, change, results_for):
    """Like ``_fake_openalex``, but the first request first runs ``change`` through a second connection."""
    pending = [change]

    def results(filter_):
        if pending:
            other = sqlite3.connect(db_path)
            other.execute(pending.pop())
            other.commit()
            other.close()
        return results_for(filter_)

    return _fake_openalex(monkeypatch, backfill, results)


CHANGED_LINE = "work(s) changed while the batch was fetched; left for the next run"


def _two_doi_rows(tmp_path, backfill):
    db_path, conn = _backfill_db(tmp_path, backfill)
    _insert(conn, doi="10.1000/ONE", metadata_status="matched")
    _insert(conn, doi="10.1000/TWO", metadata_status="matched")
    conn.close()
    return db_path


def _doi_answers(filter_):
    if not filter_.startswith("doi:"):
        return []  # pass 1 (an id the change gave a row) finds nothing
    return [_work("W1", "10.1000/one", "T1"), _work("W2", "10.1000/two", "T2")]


def test_backfill_pass2_skips_a_row_whose_doi_changed_during_the_request(tmp_path, monkeypatch, capsys, backfill):
    db_path = _two_doi_rows(tmp_path, backfill)
    _fake_openalex_changing(
        monkeypatch, backfill, db_path, "UPDATE works SET doi = '10.1000/OTHER' WHERE id = 1", _doi_answers
    )
    _run(monkeypatch, backfill, db_path)

    assert _topic_row(db_path, 1) == (None, None, None)
    assert _topic_row(db_path, 2) == ("T2", "T2-field", 1)
    assert f"[doi] 1 {CHANGED_LINE}" in capsys.readouterr().err


def test_backfill_pass2_skips_a_row_that_gained_an_openalex_id(tmp_path, monkeypatch, capsys, backfill):
    db_path = _two_doi_rows(tmp_path, backfill)
    _fake_openalex_changing(
        monkeypatch, backfill, db_path, "UPDATE works SET openalex_id = 'W9' WHERE id = 1", _doi_answers
    )
    _run(monkeypatch, backfill, db_path)

    assert _topic_row(db_path, 1) == (None, None, None)
    assert _topic_row(db_path, 2) == ("T2", "T2-field", 1)
    assert f"[doi] 1 {CHANGED_LINE}" in capsys.readouterr().err


def test_backfill_pass1_skips_a_row_whose_openalex_id_changed(tmp_path, monkeypatch, capsys, backfill):
    db_path, conn = _backfill_db(tmp_path, backfill)
    _insert(conn, openalex_id="W1", metadata_status="matched")
    _insert(conn, openalex_id="W2", metadata_status="matched")
    conn.close()

    def answers(filter_):
        return [_work("W1", "10.1000/one", "T1"), _work("W2", "10.1000/two", "T2")] if filter_ == "openalex_id:W1|W2" else []

    _fake_openalex_changing(
        monkeypatch, backfill, db_path, "UPDATE works SET openalex_id = 'W9' WHERE id = 1", answers
    )
    _run(monkeypatch, backfill, db_path)

    assert _topic_row(db_path, 1) == (None, None, None)
    assert _topic_row(db_path, 2) == ("T2", "T2-field", 1)
    assert f"[id] 1 {CHANGED_LINE}" in capsys.readouterr().err


def test_backfill_skips_a_row_requeued_out_of_the_default_scope(tmp_path, monkeypatch, capsys, backfill):
    db_path = _two_doi_rows(tmp_path, backfill)
    _fake_openalex_changing(
        monkeypatch, backfill, db_path, "UPDATE works SET metadata_status = 'pending' WHERE id = 1", _doi_answers
    )
    _run(monkeypatch, backfill, db_path)

    assert _topic_row(db_path, 1) == (None, None, None)
    assert _topic_row(db_path, 2) == ("T2", "T2-field", 1)
    assert f"[doi] 1 {CHANGED_LINE}" in capsys.readouterr().err


@pytest.mark.parametrize("pass_label, identifier, value_one, value_two", [
    ("id", "openalex_id", "W1", "W2"),
    ("doi", "doi", "10.1000/ONE", "10.1000/TWO"),
])
def test_backfill_never_overwrites_a_row_another_run_finished_during_the_request(
    tmp_path, monkeypatch, capsys, backfill, pass_label, identifier, value_one, value_two
):
    db_path, conn = _backfill_db(tmp_path, backfill)
    _insert(conn, **{identifier: value_one, "metadata_status": "matched"})
    _insert(conn, **{identifier: value_two, "metadata_status": "matched"})
    conn.close()

    def answers(filter_):
        return [_work("W1", "10.1000/one", "T1"), _work("W2", "10.1000/two", "T2")]

    # Another backfill writes row 1's topics while this run's request is still out.
    _fake_openalex_changing(
        monkeypatch, backfill, db_path,
        "UPDATE works SET primary_topic = 'FIRST', primary_field = 'FIRST-field', topics_fetched = 1 WHERE id = 1",
        answers,
    )
    _run(monkeypatch, backfill, db_path)

    assert _topic_row(db_path, 1) == ("FIRST", "FIRST-field", 1)
    assert _topic_row(db_path, 2) == ("T2", "T2-field", 1)
    assert f"[{pass_label}] 1 {CHANGED_LINE}" in capsys.readouterr().err


def test_backfill_unchanged_rows_log_no_changed_line(tmp_path, monkeypatch, capsys, backfill):
    db_path, conn = _backfill_db(tmp_path, backfill)
    _insert(conn, doi="10.1000/ONE", metadata_status="matched")
    conn.close()
    _fake_openalex(monkeypatch, backfill, lambda _f: [_work("W1", "10.1000/one", "T1")])
    _run(monkeypatch, backfill, db_path)

    assert _topic_row(db_path, 1) == ("T1", "T1-field", 1)
    assert "changed while the batch was fetched" not in capsys.readouterr().err


# --- export_bundle output names (BE-53) --------------------------------------


@pytest.mark.parametrize("fmt", ["json", "bibtex", "bundle_zip"])
def test_two_exports_in_the_same_second_get_their_own_files(tmp_path, monkeypatch, capsys, export_bundle, fmt):
    import datetime as real_datetime
    import zipfile

    db_path = _make_db(tmp_path)
    conn = _connect(db_path)
    first = _insert(conn, title="Corpus one work", doi="10.1000/ONE")
    second = _insert(conn, title="Corpus two work", doi="10.1000/TWO")
    conn.execute("INSERT INTO corpus_works (corpus_id, work_id) VALUES (1, ?), (2, ?)", (first, second))
    conn.commit()
    conn.close()

    class FrozenDatetime(real_datetime.datetime):
        @classmethod
        def utcnow(cls):
            return cls(2026, 11, 22, 12, 0, 0)

    monkeypatch.setattr(export_bundle, "datetime", FrozenDatetime)
    out_dir = tmp_path / "exports"
    capsys.readouterr()  # drop DatabaseManager setup output
    payloads = []
    for corpus_id in (1, 2):
        monkeypatch.setattr(sys, "argv", [
            "export_bundle", "--db-path", str(db_path), "--output-dir", str(out_dir),
            "--format", fmt, "--corpus-id", str(corpus_id),
        ])
        export_bundle.main()
        payloads.append(json.loads(capsys.readouterr().out))

    paths = [Path(p["output_path"]) for p in payloads]
    assert paths[0] != paths[1]
    ext = {"json": ".json", "bibtex": ".bib", "bundle_zip": ".zip"}[fmt]
    for payload, path in zip(payloads, paths):
        assert payload["filename"] == f"export_20261122_120000{ext}"  # the download name stays as before
        assert path.name.startswith("export_20261122_120000_") and path.suffix == ext

    def text(path):
        if fmt == "bundle_zip":
            with zipfile.ZipFile(path) as zf:
                return zf.read("metadata.json").decode("utf-8")
        return path.read_text(encoding="utf-8")

    assert "Corpus one work" in text(paths[0]) and "Corpus two work" not in text(paths[0])
    assert "Corpus two work" in text(paths[1]) and "Corpus one work" not in text(paths[1])


def test_export_writers_never_overwrite_an_existing_file(tmp_path, export_bundle):
    existing = tmp_path / "taken.json"
    existing.write_text("other corpus", encoding="utf-8")
    with pytest.raises(FileExistsError):
        export_bundle.write_json_export([], existing)
    with pytest.raises(FileExistsError):
        export_bundle.write_bibtex_export([], existing)
    with pytest.raises(FileExistsError):
        export_bundle.write_bundle_zip([], existing)
    assert existing.read_text(encoding="utf-8") == "other corpus"


def test_export_render_failure_leaves_no_file(tmp_path, monkeypatch, export_bundle):
    json_path = tmp_path / "out.json"
    with pytest.raises(TypeError):
        export_bundle.write_json_export([{"id": 1, "title": "T", "bad": object()}], json_path)
    assert not json_path.exists()

    def boom(_item):
        raise RuntimeError("render failed")

    monkeypatch.setattr(export_bundle, "to_bibtex_entry", boom)
    bib_path = tmp_path / "out.bib"
    with pytest.raises(RuntimeError):
        export_bundle.write_bibtex_export([{"id": 1, "title": "T"}], bib_path)
    assert not bib_path.exists()


# --- a failed export write removes its own partial file (F5) ------------------


def test_a_failed_export_write_removes_its_partial_file(tmp_path, export_bundle):
    pdf = tmp_path / "a.pdf"
    pdf.write_bytes(b"%PDF")
    items = [{"title": "T", "file_path": str(pdf)}]
    gone = tmp_path / "gone.pdf"
    gone.write_bytes(b"%PDF")
    vanishing = items + [{"title": "U", "file_path": str(gone)}]

    for writer in (export_bundle.write_pdf_zip, export_bundle.write_bundle_zip):
        out = tmp_path / f"{writer.__name__}.zip"
        real_collect = export_bundle.collect_pdf_files

        def collect(its):
            got = real_collect(its)
            gone.unlink()  # vanishes between collecting and writing
            return got

        export_bundle.collect_pdf_files = collect
        try:
            with pytest.raises(OSError):
                writer(vanishing, out)
        finally:
            export_bundle.collect_pdf_files = real_collect
        assert not out.exists()
        gone.write_bytes(b"%PDF")

    out = tmp_path / "x.json"
    with pytest.raises(TypeError):
        export_bundle.write_json_export([{"title": object()}], out)  # rendering fails before the file
    assert not out.exists()

    # An existing file under the name is someone else's export: never removed.
    taken = tmp_path / "taken.zip"
    taken.write_bytes(b"other")
    with pytest.raises(FileExistsError):
        export_bundle.write_pdf_zip(items, taken)
    assert taken.read_bytes() == b"other"


def test_a_failed_text_export_write_removes_its_partial_file(tmp_path, export_bundle):
    class _FullDisk(type(tmp_path)):
        def open(self, *args, **kwargs):
            fh = super().open(*args, **kwargs)
            real_write = fh.write

            def write(text):
                real_write(text[:3])
                raise OSError(28, "No space left on device")

            fh.write = write
            return fh

    for writer in (export_bundle.write_json_export, export_bundle.write_bibtex_export):
        out = _FullDisk(tmp_path / f"{writer.__name__}.out")
        with pytest.raises(OSError):
            writer([{"title": "T"}], out)
        assert not out.exists()
