"""Run standalone with python3 -m unittest discover -s dl_lit_project/tests -p test_upstream_text_scan.py."""
import argparse
import contextlib
import importlib.util
import io
import json
import subprocess
from pathlib import Path
import sys
import tempfile
import threading
import types
import unittest
from unittest.mock import patch


SPEC = importlib.util.spec_from_file_location(
    "upstream_update", Path(__file__).resolve().parents[2] / "backend/scripts/upstream_update.py"
)
upstream = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(upstream)


class FakeTools:
    def __init__(self):
        self.errors = True
        self.warnings = False
        self.buffer = "stale warning from another document"

    def mupdf_display_errors(self, value=None):
        if value is not None:
            self.errors = value
        return self.errors

    def mupdf_display_warnings(self, value=None):
        if value is not None:
            self.warnings = value
        return self.warnings

    def reset_mupdf_warnings(self):
        self.buffer = ""

    def mupdf_warnings(self, reset=True):
        value = self.buffer
        if reset:
            self.buffer = ""
        return value


def fake_fitz(messages=None, failure=None, open_warning="", close_failure=False):
    tools = FakeTools()

    class Document:
        def __len__(self):
            return 2

        def __getitem__(self, index):
            tools.buffer += (messages or {}).get(index, "")
            if failure == index:
                raise ValueError("broken page")
            return types.SimpleNamespace(get_text=lambda mode: "word " * 120)

        def close(self):
            if close_failure:
                tools.buffer += "close warning"
                raise ValueError("close failed")

    def open_doc(path):
        tools.buffer += open_warning
        if failure == "open":
            raise ValueError("broken document")
        return Document()

    return types.SimpleNamespace(TOOLS=tools, open=open_doc, VersionBind="test", VersionFitz="test")


class TextScanTests(unittest.TestCase):
    def scan(self, module):
        stderr = io.StringIO()
        with patch.dict(sys.modules, fitz=module), contextlib.redirect_stderr(stderr):
            result = upstream.scan_pdf_text(Path("file.pdf"), work_id=20590)
        self.assertTrue(module.TOOLS.errors)
        self.assertFalse(module.TOOLS.warnings)
        self.assertEqual(module.TOOLS.buffer, "")
        return result, stderr.getvalue()

    def test_clean_and_stale_buffer_is_cleared(self):
        result, log = self.scan(fake_fitz())
        self.assertEqual(upstream.text_scan_reason(result, 500, .25), "ok")
        self.assertEqual(result["warning_count"], 0)
        self.assertEqual(log, "")

    def test_page_warning_retains_raw_and_context(self):
        raw = "unknown colorspace: R364\nunknown keyword: 'EI'"
        result, log = self.scan(fake_fitz({1: raw}))
        self.assertEqual(upstream.text_scan_reason(result, 500, .25), "ok_with_warnings")
        self.assertEqual(result["warnings"][0]["page"], 2)
        self.assertEqual(result["warnings"][0]["message"], raw)
        self.assertEqual(result["warnings"][0]["category"], "colorspace")
        for expected in ("20590", "file.pdf", "PDF-Seite 2", "Grafiken können fehlen"):
            self.assertIn(expected, log)

    def test_open_warning_has_no_invented_page(self):
        result, _ = self.scan(fake_fitz(open_warning="cmsOpenProfileFromMem failed"))
        self.assertIsNone(result["warnings"][0]["page"])
        self.assertEqual(result["warnings"][0]["category"], "color_profile")

    def test_failed_page_retains_warnings_and_partial_text(self):
        result, log = self.scan(fake_fitz({1: "syntax error"}, failure=1))
        self.assertEqual(upstream.text_scan_reason(result, 500, .25), "error")
        self.assertEqual(result["error_page"], 2)
        self.assertGreater(result["text_chars"], 500)
        self.assertEqual(result["warning_count"], 1)
        self.assertIn("abgebrochen", log)

    def test_open_and_close_errors_restore_flags(self):
        for module in (fake_fitz(failure="open", open_warning="bad xref"), fake_fitz(close_failure=True)):
            with self.subTest(module=module):
                result, _ = self.scan(module)
                self.assertEqual(upstream.text_scan_reason(result, 500, .25), "error")
                self.assertEqual(result["warning_count"], 1)

    def test_weak_text_still_selected_for_ocr(self):
        for chars, reason in ((0, "empty_text"), (50, "low_text")):
            self.assertEqual(upstream.text_scan_reason({
                "text_chars": chars, "pages": 1, "nonempty_pages": 1, "warning_count": 1,
            }, 500, .25), reason)

    def test_summary_separates_warnings_without_changing_weak_failure_policy(self):
        results = [
            {"reason": "ok"}, {"reason": "ok_with_warnings", "warning_count": 1},
            {"reason": "low_text", "warning_count": 2}, {"reason": "error", "warning_count": 1},
            {"reason": "empty_text"}, {"reason": "missing"},
        ]
        summary = upstream.summarize_text_scan(results)
        self.assertEqual(summary["ok"], 1)
        self.assertEqual(summary["ok_with_warnings"], 1)
        self.assertEqual(summary["warning_files"], 3)
        self.assertEqual(summary["problematic_count"], 4)
        self.assertEqual(len(summary["warned"]), 3)

    def test_classification_is_cautious(self):
        for message, category in (
            ("cannot find page 0 in page tree", "page_reference"),
            ("unknown keyword: 'EI'", "content_syntax"),
            ("unexpected problem", "pdf_structure"),
        ):
            self.assertEqual(upstream.explain_pdf_warning(message)[0], category)

    def test_cli_json_and_report_keep_warnings_without_failing(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "test.pdf").touch()
            (root / "manifest.json").write_text(json.dumps({"items": [{
                "work_id": 42, "title": "Example", "staged_file": "test.pdf", "target_file": "test.pdf",
            }]}))
            stdout, stderr = io.StringIO(), io.StringIO()
            args = argparse.Namespace(draft_dir=directory, min_text_chars=500, min_text_page_ratio=.25,
                                      write=True, include_items=False, fail_on_weak=True)
            with patch.dict(sys.modules, fitz=fake_fitz({1: "unknown keyword: EI"})), \
                    contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                upstream.command_scan_text(args)
            output = json.loads(stdout.getvalue())
            self.assertEqual(output["summary"]["ok_with_warnings"], 1)
            report = json.loads((root / "text-scan.json").read_text())
            self.assertEqual(report["items"][0]["warnings"][0]["page"], 2)
            self.assertIn("Lauf geht weiter", stderr.getvalue())
            self.assertIn("[Textprüfung] 1/1 PDFs (100.0%)", stderr.getvalue())

    def test_missing_file_is_reported_with_context(self):
        with tempfile.TemporaryDirectory() as directory, contextlib.redirect_stderr(io.StringIO()) as stderr:
            result = upstream.scan_item_text({"work_id": 12, "staged_file": "absent.pdf"}, Path(directory), 500, .25)
        self.assertEqual(result["reason"], "missing")
        self.assertIn("12", stderr.getvalue())
        self.assertIn("absent.pdf", stderr.getvalue())

    def test_ocr_selects_weak_and_broken_pdfs_not_warning_only_files(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "files").mkdir()
            items = [{"work_id": index, "target_file": f"{index}.pdf", "staged_file": f"files/{index}.pdf"}
                     for index in range(3)]
            (root / "manifest.json").write_text(json.dumps({"items": items}))
            results = [{"target_file": f"{index}.pdf", "reason": reason, "warning_count": 1}
                       for index, reason in enumerate(("ok_with_warnings", "low_text", "error"))]
            args = argparse.Namespace(draft_dir=directory, min_text_chars=500, min_text_page_ratio=.25,
                                      ocr_url="http://unused.invalid", all=False, overwrite=False,
                                      timeout=1, keep_going=True)
            with patch.object(upstream, "scan_draft_items", return_value=(root, results, {})), \
                    patch.object(upstream, "ocr_pdf", return_value={"full_text": "OCR text"}) as ocr, \
                    contextlib.redirect_stdout(io.StringIO()) as stdout, \
                    contextlib.redirect_stderr(io.StringIO()) as stderr:
                upstream.command_ocr(args)
            self.assertEqual(ocr.call_count, 2)
            self.assertEqual([call.args[1] for call in ocr.call_args_list],
                             [root / "files/1.pdf", root / "files/2.pdf"])
            self.assertEqual(json.loads(stdout.getvalue())["selected_count"], 2)
            self.assertIn("[OCR] 2/2 PDFs (100.0%)", stderr.getvalue())
            self.assertIn("erfolgreich=2", stderr.getvalue())

    def test_page_progress_is_forwarded(self):
        progress = types.SimpleNamespace(update_page=lambda page, total: pages.append((page, total)))
        pages = []
        with patch.dict(sys.modules, fitz=fake_fitz()):
            upstream.scan_pdf_text(Path("file.pdf"), progress=progress)
        self.assertEqual(pages, [(1, 2), (2, 2)])

    def test_heartbeat_while_no_file_has_completed(self):
        seen = threading.Event()

        class Sink(io.StringIO):
            def write(self, text):
                result = super().write(text)
                if "In Arbeit" in text:
                    seen.set()
                return result

        sink = Sink()
        with contextlib.redirect_stderr(sink), contextlib.redirect_stdout(io.StringIO()) as stdout:
            with upstream.ProgressReporter("OCR", 2, interval=.01) as progress:
                progress.start_item({"work_id": 42, "target_file": "test.pdf"})
                progress.update_page(3, 8)
                self.assertTrue(seen.wait(2), "No heartbeat while work is pending")
                progress.advance("erfolgreich")
            self.assertEqual(stdout.getvalue(), "")
        self.assertIn("0/2 PDFs (0.0%)", sink.getvalue())
        self.assertIn("PDF-Seite 3/8", sink.getvalue())
        self.assertIn("work_id=42", sink.getvalue())
        self.assertIn("1/2 PDFs (50.0%)", sink.getvalue())
        self.assertFalse(progress.thread.is_alive())

    def test_progress_stops_on_interrupt_without_claiming_completion(self):
        with contextlib.redirect_stderr(io.StringIO()) as stderr:
            with self.assertRaises(KeyboardInterrupt):
                with upstream.ProgressReporter("Textprüfung", 2) as progress:
                    progress.start_item({"work_id": 42})
                    raise KeyboardInterrupt()
        self.assertFalse(progress.thread.is_alive())
        self.assertIn("Abgebrochen", stderr.getvalue())
        self.assertNotIn("100.0%", stderr.getvalue())

    def test_empty_progress_does_not_divide_by_zero(self):
        with contextlib.redirect_stderr(io.StringIO()) as stderr:
            with upstream.ProgressReporter("OCR", 0):
                pass
        self.assertIn("0/0 PDFs (100.0%)", stderr.getvalue())

    def test_failed_output_preserves_original_exception(self):
        class BrokenOutput(io.StringIO):
            def write(self, value):
                raise BrokenPipeError("closed pipe")

        with contextlib.redirect_stderr(BrokenOutput()):
            with self.assertRaisesRegex(RuntimeError, "original OCR error"):
                with upstream.ProgressReporter("OCR", 1) as progress:
                    raise RuntimeError("original OCR error")
        self.assertFalse(progress.thread.is_alive())
        self.assertTrue(progress.output_failed.is_set())

    def test_final_output_failure_does_not_mask_original_exception(self):
        class BrokenFinal(io.StringIO):
            def write(self, value):
                if "Abgebrochen" in value:
                    raise ValueError("closed stream")
                return super().write(value)

        with contextlib.redirect_stderr(BrokenFinal()):
            with self.assertRaisesRegex(RuntimeError, "original OCR error"):
                with upstream.ProgressReporter("OCR", 1):
                    raise RuntimeError("original OCR error")

    def test_lines_are_serialized_and_final_is_last(self):
        writes = []

        class RecordingOutput(io.StringIO):
            def write(self, value):
                writes.append(value)
                return super().write(value)

        with contextlib.redirect_stderr(RecordingOutput()):
            with upstream.ProgressReporter("Textprüfung", 2, interval=.01) as progress:
                upstream.pdf_scan_log(Path("file.pdf"), 1, 2, "test warning", progress=progress)
                progress.update_page(2, 3)
                progress.advance("ok")
                progress.start_item({"work_id": 2})
                progress.emit("In Arbeit")
                progress.advance("ok")
        self.assertTrue(all(line.startswith("[") and line.endswith("\n") for line in writes))
        self.assertIn("Beendet", writes[-1])
        self.assertIn("2/2 PDFs", writes[-1])
        self.assertIn("ok=2", writes[-1])
        second_file = next(line for line in writes if "work_id=2" in line)
        self.assertNotIn("PDF-Seite", second_file)
        self.assertFalse(progress.thread.is_alive())

    def test_full_stderr_pipe_does_not_hold_up_process_exit(self):
        # Do not drain stderr until exit: the child really has a blocked consumer.
        code = '''
import os, runpy, sys, threading
m = runpy.run_path(sys.argv[1])
os.set_blocking(2, False)
try:
    while True:
        os.write(2, b'x' * 4096)
except BlockingIOError:
    pass
os.set_blocking(2, True)
entered = threading.Event()
original = m['ProgressReporter']._heartbeat
def heartbeat(self):
    entered.set()
    original(self)
m['ProgressReporter']._heartbeat = heartbeat
with m['ProgressReporter']('OCR', 1) as progress:
    assert entered.wait(1)
    progress.advance('ok')
print('processing returned')
'''
        process = subprocess.Popen([sys.executable, "-c", code, str(Path(upstream.__file__))],
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            self.assertEqual(process.wait(timeout=5), 0)
            self.assertIn(b"processing returned", process.stdout.read())
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()
            process.stdout.close()
            process.stderr.close()

    def test_ocr_pre_scan_and_ocr_run_sequentially_without_mocking_scan(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "files").mkdir()
            (root / "files/test.pdf").touch()
            (root / "manifest.json").write_text(json.dumps({"items": [{
                "work_id": 1, "target_file": "test.pdf", "staged_file": "files/test.pdf",
            }]}))
            args = argparse.Namespace(draft_dir=directory, min_text_chars=500, min_text_page_ratio=.25,
                                      ocr_url="http://unused.invalid", all=True, overwrite=False,
                                      timeout=1, keep_going=False)
            with patch.dict(sys.modules, fitz=fake_fitz()), \
                    patch.object(upstream, "ocr_pdf", return_value={"full_text": "OCR result", "page_count": 2}), \
                    contextlib.redirect_stdout(io.StringIO()) as stdout, \
                    contextlib.redirect_stderr(io.StringIO()) as stderr:
                upstream.command_ocr(args)
            lines = stderr.getvalue().splitlines()
            scan_end = next(i for i, line in enumerate(lines) if "vor OCR" in line and "Beendet" in line)
            ocr_start = next(i for i, line in enumerate(lines) if line.startswith("[OCR]") and "Start" in line)
            self.assertLess(scan_end, ocr_start)
            self.assertEqual(json.loads(stdout.getvalue())["completed_count"], 1)

    def test_ocr_progress_counts_skips_and_failures_and_stops(self):
        for keep_going in (True, False):
            with self.subTest(keep_going=keep_going), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                (root / "files").mkdir()
                (root / "files/0.txt").write_text("existing text")
                items = [{"work_id": index, "target_file": f"{index}.pdf", "staged_file": f"files/{index}.pdf"}
                         for index in range(2)]
                (root / "manifest.json").write_text(json.dumps({"items": items}))
                scans = [{"target_file": item["target_file"], "reason": "empty_text"} for item in items]
                args = argparse.Namespace(draft_dir=directory, min_text_chars=500, min_text_page_ratio=.25,
                                          ocr_url="http://unused.invalid", all=False, overwrite=False,
                                          timeout=1, keep_going=keep_going)
                with patch.object(upstream, "scan_draft_items", return_value=(root, scans, {})), \
                        patch.object(upstream, "ocr_pdf", side_effect=RuntimeError("test failure")), \
                        contextlib.redirect_stdout(io.StringIO()) as stdout, \
                        contextlib.redirect_stderr(io.StringIO()) as stderr:
                    with self.assertRaises(SystemExit if keep_going else RuntimeError):
                        upstream.command_ocr(args)
                self.assertIn("übersprungen=1", stderr.getvalue())
                self.assertIn("fehlgeschlagen=1", stderr.getvalue())
                if keep_going:
                    self.assertEqual(json.loads(stdout.getvalue())["failed_count"], 1)
                else:
                    self.assertIn("Abgebrochen", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
