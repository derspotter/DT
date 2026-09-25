"""Offline regression tests for strictly manifest-scoped embedding."""
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

SPEC = importlib.util.spec_from_file_location('embed_draft', Path(__file__).resolve().parents[2] / 'backend/kantropos/embed_draft.py')
mod = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(mod)


class ScopedTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        (self.root / 'markdown').mkdir()
        self.manifest = {'target': {'name': 'Corpus'}, 'items': [
            {'target_file': 'existing.pdf'}, {'target_file': 'new.pdf'}]}
        self.vectors = {'existing.pdf'}
        self.metadata = {'existing.pdf'}
        self.runtime = Mock(root=self.root)
        self.runtime.existing_ids.side_effect = lambda names: set(self.vectors)
        self.runtime.metadata_ids.side_effect = lambda: set(self.metadata)
        self.runtime.prepare.side_effect = lambda name: [name]
        self.runtime.insert.side_effect = self.insert
        self.factory = Mock(return_value=self.runtime)
        self.file('existing.pdf', 'already done')
        self.file('new.pdf', 'new text')
        self.file('legacy.pdf', '')

    def file(self, name, text):
        (self.root / name).write_bytes(b'fixture')
        (self.root / 'markdown' / (name[:-4] + '.txt')).write_text(text)

    def insert(self, name, nodes):
        self.vectors.add(name)
        self.metadata.add(name)

    def run_draft(self, yes=True):
        with contextlib.redirect_stdout(io.StringIO()):
            mod.run(self.manifest, yes=yes, runtime_factory=self.factory)

    def test_incremental_only_processes_missing_manifest_documents(self):
        self.run_draft()
        self.runtime.prepare.assert_called_once_with('new.pdf')
        self.runtime.insert.assert_called_once_with('new.pdf', ['new.pdf'])
        self.runtime.close.assert_called_once()
        self.assertNotIn('legacy.pdf', self.vectors)
        self.assertEqual(self.runtime.existing_ids.call_args.args[0], ['existing.pdf', 'new.pdf'])

    def test_preview_does_not_embed_or_write(self):
        self.run_draft(yes=False)
        self.runtime.prepare.assert_called_once_with('new.pdf')
        self.runtime.insert.assert_not_called()

    def test_complete_draft_does_not_repeat_embeddings(self):
        self.vectors.add('new.pdf')
        self.metadata.add('new.pdf')
        self.run_draft()
        self.runtime.prepare.assert_not_called()
        self.runtime.insert.assert_not_called()

    def test_empty_in_scope_stops_before_any_writes(self):
        self.manifest['items'].append({'target_file': 'legacy.pdf'})
        with self.assertRaisesRegex(ValueError, 'legacy.pdf: empty'):
            self.run_draft()
        self.runtime.insert.assert_not_called()
        self.runtime.close.assert_called_once()

    def test_invalid_utf8_and_missing_text_fail_closed(self):
        path = self.root / 'markdown/new.txt'
        path.write_bytes(b'\xff')
        with self.assertRaisesRegex(ValueError, 'new.pdf: text is not valid UTF-8'):
            self.run_draft()
        path.unlink()
        with self.assertRaisesRegex(ValueError, 'new.pdf: missing file'):
            self.run_draft()
        self.runtime.insert.assert_not_called()

    def test_mismatched_vectors_and_metadata_block_resume(self):
        for vectors, metadata in [({'existing.pdf'}, set()), (set(), {'existing.pdf'})]:
            self.vectors, self.metadata = vectors, metadata
            with self.assertRaisesRegex(ValueError, 'Vector/metadata mismatch'):
                self.run_draft()
        self.runtime.insert.assert_not_called()

    def test_per_file_failure_names_document_and_closes_connection(self):
        self.runtime.insert.side_effect = ValueError('bad embedding')
        with self.assertRaisesRegex(RuntimeError, 'new.pdf: ValueError: bad embedding'):
            self.run_draft()
        self.runtime.close.assert_called_once()

    def test_final_audit_catches_missing_writes(self):
        self.runtime.insert.side_effect = None
        with self.assertRaisesRegex(RuntimeError, 'Completion audit failed'):
            self.run_draft()

    def test_scope_rejects_invalid_names_and_duplicates(self):
        for name in ['../x.pdf', '/x.pdf', 'a\\x.pdf', 'x.txt', None, 'x\x00.pdf']:
            self.manifest['items'] = [{'target_file': name}]
            with self.assertRaises(ValueError):
                self.run_draft()
        self.manifest['items'] = [{'target_file': 'x.pdf'}] * 2
        with self.assertRaisesRegex(ValueError, 'Duplicate'):
            self.run_draft()
        self.factory.assert_not_called()

    def test_scope_rejects_empty_selection_and_corpus_escape(self):
        for corpus in ['../Corpus', '/Corpus', '', '.', '..', None]:
            self.manifest['target']['name'] = corpus
            with self.assertRaises(ValueError):
                self.run_draft()
        self.manifest['target']['name'] = 'Corpus'
        self.manifest['items'] = []
        with self.assertRaisesRegex(ValueError, 'nonempty'):
            self.run_draft()

    def test_text_symlink_cannot_escape_corpus(self):
        with tempfile.TemporaryDirectory() as outside:
            path = Path(outside) / 'text.txt'
            path.write_text('outside')
            text = self.root / 'markdown/new.txt'
            text.unlink()
            text.symlink_to(path)
            with self.assertRaisesRegex(ValueError, 'outside corpus'):
                self.run_draft()

    def test_empty_input_never_calls_ollama(self):
        request = Mock()
        with self.assertRaisesRegex(ValueError, 'Empty embedding input'):
            mod.checked_embedding(' \n', request)
        request.assert_not_called()

    def test_empty_invalid_or_multiple_vectors_are_actionable_errors(self):
        for vectors in [[], [[]], [[float('nan')]], [[float('inf')]], [[1], [2]]]:
            with self.assertRaisesRegex(ValueError, 'no valid embedding'):
                mod.checked_embedding('text', lambda text: SimpleNamespace(embeddings=vectors))
        self.assertEqual(mod.checked_embedding('text', lambda text: SimpleNamespace(embeddings=[[1., 2.]])), [1., 2.])

    def test_index_inventory_returns_only_manifest_ids(self):
        runtime = mod.Runtime.__new__(mod.Runtime)
        runtime.collection = 'Corpus'
        response = io.BytesIO(json.dumps({'result': {'hits': [
            {'value': 'new.pdf'}, {'value': 'legacy.pdf'}]}}).encode())
        with patch.dict(mod.os.environ, {'QDRANT__SERVICE__API_KEY': 'test'}), \
                patch.object(mod.urllib.request, 'urlopen', return_value=response) as query:
            self.assertEqual(runtime.existing_ids(['new.pdf']), {'new.pdf'})
        self.assertTrue(json.loads(query.call_args.args[0].data)['exact'])
        self.assertNotIn('filter', json.loads(query.call_args.args[0].data))

    def test_index_auth_failure_is_not_treated_as_empty_index(self):
        runtime = mod.Runtime.__new__(mod.Runtime)
        runtime.collection = 'Corpus'
        error = mod.urllib.error.HTTPError('http://qdrant', 401, 'unauthorized', {}, None)
        with patch.dict(mod.os.environ, {'QDRANT__SERVICE__API_KEY': 'test'}), \
                patch.object(mod.urllib.request, 'urlopen', side_effect=error), \
                self.assertRaises(mod.urllib.error.HTTPError):
            runtime.existing_ids(['new.pdf'])


if __name__ == '__main__':
    unittest.main()
