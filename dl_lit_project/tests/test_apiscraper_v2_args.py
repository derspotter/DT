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


def test_build_page_batches_groups_consecutive_pages(monkeypatch):
    saved_outputs = []

    class _Ctx:
        def __init__(self, obj):
            self.obj = obj

        def __enter__(self):
            return self.obj

        def __exit__(self, exc_type, exc, tb):
            return False

    class _OpenedPdf:
        def __init__(self, path):
            self.pages = [f"page:{path}"]

    class _MergedPdf:
        def __init__(self):
            self.pages = []

        def save(self, out_path):
            saved_outputs.append(out_path)

    class _PdfFacade:
        @staticmethod
        def open(path):
            return _Ctx(_OpenedPdf(path))

        @staticmethod
        def new():
            return _MergedPdf()

    class _Pike:
        Pdf = _PdfFacade

    monkeypatch.setattr(scraper, "pikepdf", _Pike)
    monkeypatch.setattr(scraper.tempfile, "mkdtemp", lambda prefix: "/tmp/page_batches")

    pdf_files = [
        "/in/doc_refs_physical_p1.pdf",
        "/in/doc_refs_physical_p2.pdf",
        "/in/doc_refs_physical_p3.pdf",
        "/in/doc_refs_physical_p5.pdf",
    ]
    work_items, temp_dir = scraper._build_page_batches(pdf_files, page_batch_size=3)

    assert temp_dir == "/tmp/page_batches"
    assert len(saved_outputs) == 1
    assert saved_outputs[0].endswith("doc_refs_batch_p1-3.pdf")
    assert any(item[0].endswith("doc_refs_batch_p1-3.pdf") and item[1].endswith("doc_refs_physical_p1.pdf") for item in work_items)
    assert any(item[0].endswith("doc_refs_physical_p5.pdf") for item in work_items)


def test_build_page_batches_disabled_keeps_original_files():
    pdf_files = ["/in/a.pdf", "/in/b.pdf"]
    work_items, temp_dir = scraper._build_page_batches(pdf_files, page_batch_size=1)
    assert temp_dir is None
    assert work_items == [("/in/a.pdf", "/in/a.pdf"), ("/in/b.pdf", "/in/b.pdf")]
