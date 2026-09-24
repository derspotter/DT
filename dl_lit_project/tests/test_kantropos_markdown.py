"""No OCR/model/runtime dependencies needed for failure-path regression tests."""
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location('dt_markdown', Path(__file__).resolve().parents[2] / 'backend/kantropos/markdown_util.py')
mod = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(mod)


class MarkdownTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.md = self.root / 'markdown'
        self.md.mkdir()
        self.dest = self.md / 'sample.txt'
        self.job = (str(self.root), str(self.md), 'sample.pdf')

    def test_existing_nonempty_is_untouched(self):
        self.dest.write_text('original')
        with patch.object(mod, 'extract_markdown') as extract:
            self.assertEqual(mod.create_markdown_file(self.job)['status'], 'existing')
        extract.assert_not_called()
        self.assertEqual(self.dest.read_text(), 'original')

    def test_empty_or_whitespace_is_retried(self):
        for old in ['', ' \n']:
            self.dest.write_text(old)
            with patch.object(mod, 'extract_markdown', return_value='recovered'):
                self.assertEqual(mod.create_markdown_file(self.job)['status'], 'created')
            self.assertEqual(self.dest.read_text(), 'recovered')

    def test_empty_markdown_uses_plain_text(self):
        with patch.object(mod, 'extract_markdown', return_value=''), patch.object(mod, 'extract_plain_text', return_value='page text'):
            self.assertEqual(mod.create_markdown_file(self.job)['status'], 'fallback')
        self.assertEqual(self.dest.read_text(), 'page text')

    def test_converter_error_uses_plain_text(self):
        with patch.object(mod, 'extract_markdown', side_effect=ValueError('bad layout')), patch.object(mod, 'extract_plain_text', return_value='page text'):
            self.assertEqual(mod.create_markdown_file(self.job)['status'], 'fallback')

    def test_targeted_text_recovery_does_not_repeat_layout_conversion(self):
        with patch.object(mod, 'extract_markdown') as layout, patch.object(mod, 'extract_plain_text', return_value='all pages'):
            result = mod.create_markdown_file(self.job, text_only=True)
        layout.assert_not_called()
        self.assertEqual(result['status'], 'fallback')
        self.assertEqual(self.dest.read_text(), 'all pages')

    def test_surrogates_are_visible_replacements_in_valid_utf8(self):
        with patch.object(mod, 'extract_markdown', return_value='before\udc91after'):
            result = mod.create_markdown_file(self.job)
        self.assertEqual(result['replacement_characters'], 1)
        self.assertEqual(self.dest.read_text(encoding='utf-8'), 'before\ufffdafter')

    def test_empty_fallback_fails_without_creating_output(self):
        with patch.object(mod, 'extract_markdown', return_value=''), patch.object(mod, 'extract_plain_text', return_value=' '):
            result = mod.create_markdown_file(self.job)
        self.assertEqual(result['status'], 'failed')
        self.assertFalse(self.dest.exists())
        with self.assertRaisesRegex(RuntimeError, 'embedding must not start'):
            mod.require_success([result])

    def test_page_error_fails_without_partial_output(self):
        with patch.object(mod, 'extract_markdown', return_value=''), patch.object(mod, 'extract_plain_text', side_effect=RuntimeError('bad page')):
            self.assertEqual(mod.create_markdown_file(self.job)['status'], 'failed')
        self.assertFalse(self.dest.exists())

    def test_atomic_failure_preserves_existing_and_cleans_temporary(self):
        self.dest.write_text('original')
        with patch.object(mod.os, 'replace', side_effect=OSError('disk error')):
            with self.assertRaises(OSError):
                mod.atomic_text(self.dest, 'new')
        self.assertEqual(self.dest.read_text(), 'original')
        self.assertEqual(list(self.md.iterdir()), [self.dest])

    def test_empty_sidecar_does_not_overwrite_good_text(self):
        (self.root / 'sample.txt').write_text('')
        self.dest.write_text('good text')
        with self.assertRaisesRegex(ValueError, 'Empty source'):
            mod.copy_txt_files_to_markdown_directory(self.root, self.md)
        self.assertEqual(self.dest.read_text(), 'good text')

    def test_valid_sidecar_replaces_invalid_utf8_output(self):
        (self.root / 'sample.txt').write_text('valid OCR text', encoding='utf-8')
        self.dest.write_bytes(b'\xff')
        mod.copy_txt_files_to_markdown_directory(self.root, self.md)
        self.assertEqual(self.dest.read_text(encoding='utf-8'), 'valid OCR text')

    def test_invalid_utf8_sidecar_does_not_overwrite_good_text(self):
        (self.root / 'sample.txt').write_bytes(b'\xff')
        self.dest.write_text('good text', encoding='utf-8')
        with self.assertRaises(UnicodeError):
            mod.copy_txt_files_to_markdown_directory(self.root, self.md)
        self.assertEqual(self.dest.read_text(encoding='utf-8'), 'good text')

    def test_corpus_path_cannot_escape_root(self):
        with self.assertRaisesRegex(ValueError, 'Invalid corpus'):
            mod.create_markdown_files('../outside')

    def test_invalid_utf8_output_is_retried(self):
        self.dest.write_bytes(b'\xff')
        with patch.object(mod, 'extract_markdown', return_value='valid'):
            self.assertEqual(mod.create_markdown_file(self.job)['status'], 'created')
        self.assertEqual(self.dest.read_text(), 'valid')


if __name__ == '__main__':
    unittest.main()
