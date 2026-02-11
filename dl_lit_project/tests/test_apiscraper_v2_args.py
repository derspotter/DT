from pathlib import Path

from dl_lit import APIscraper_v2 as scraper


def test_parser_accepts_positional_input():
    parser = scraper._build_arg_parser()
    args = parser.parse_args(["/tmp/input.pdf", "--db-path", "/tmp/literature.db"])
    assert args.input == "/tmp/input.pdf"
    assert args.input_pdf is None
    assert args.input_dir is None
    assert args.db_path == "/tmp/literature.db"


def test_parser_accepts_input_pdf_flag():
    parser = scraper._build_arg_parser()
    args = parser.parse_args(["--input-pdf", "/tmp/input.pdf", "--db-path", "/tmp/literature.db"])
    assert args.input is None
    assert args.input_pdf == "/tmp/input.pdf"
    assert args.input_dir is None


def test_resolve_input_prefers_input_pdf():
    parser = scraper._build_arg_parser()
    args = parser.parse_args(
        [
            "positional.pdf",
            "--input-dir",
            "/tmp/dir",
            "--input-pdf",
            "/tmp/single.pdf",
            "--db-path",
            "/tmp/literature.db",
        ]
    )
    assert scraper._resolve_input_path(args) == "/tmp/single.pdf"


def test_main_routes_single_pdf(monkeypatch, tmp_path):
    pdf_path = tmp_path / "single.pdf"
    pdf_path.write_text("dummy")
    db_path = tmp_path / "literature.db"

    calls = {"single": None}

    monkeypatch.setattr(scraper, "configure_api_client", lambda: True)

    class DummyDB:
        def __init__(self, db_path):
            self.db_path = Path(db_path)

        def close_connection(self):
            return None

    monkeypatch.setattr(scraper, "DatabaseManager", DummyDB)

    def _process_single(pdf_path_arg, *_args, **_kwargs):
        calls["single"] = pdf_path_arg

    monkeypatch.setattr(scraper, "process_single_pdf", _process_single)

    rc = scraper.main(["--input-pdf", str(pdf_path), "--db-path", str(db_path)])
    assert rc == 0
    assert calls["single"] == str(pdf_path)


def test_main_routes_directory(monkeypatch, tmp_path):
    input_dir = tmp_path / "in"
    input_dir.mkdir()
    (input_dir / "a.pdf").write_text("dummy")
    db_path = tmp_path / "literature.db"

    calls = {"dir": None}

    monkeypatch.setattr(scraper, "configure_api_client", lambda: True)

    class DummyDB:
        def __init__(self, db_path):
            self.db_path = Path(db_path)

        def close_connection(self):
            return None

    monkeypatch.setattr(scraper, "DatabaseManager", DummyDB)

    def _process_dir(input_dir_arg, *_args, **_kwargs):
        calls["dir"] = input_dir_arg

    monkeypatch.setattr(scraper, "process_directory_v2", _process_dir)

    rc = scraper.main(["--input-dir", str(input_dir), "--db-path", str(db_path)])
    assert rc == 0
    assert calls["dir"] == str(input_dir)
