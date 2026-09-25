"""Regression coverage for resumable OCR and fail-closed corpus imports."""
import argparse
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

REPO = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location('upstream_recovery', REPO / 'backend/scripts/upstream_update.py')
upstream = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(upstream)


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.draft = self.root / 'draft'
        self.target = self.root / 'target'
        (self.draft / 'files').mkdir(parents=True)
        self.target.mkdir()
        self.item = dict(work_id=1, target_file='1.pdf', staged_file='files/1.pdf', bibtex_key='one')
        self.manifest = dict(target={'path': str(self.target)}, items=[self.item])
        (self.draft / 'files/1.pdf').write_bytes(b'PDF1')
        (self.draft / 'metadata.current.bib').write_text('old')
        (self.target / 'metadata.bib').write_text('old')
        (self.draft / 'metadata.bib.new').write_text('new')
        (self.draft / 'metadata.pending-additions.bib').write_text('@article{one, file={1.pdf:PDF}}')
        self.save()
        self.args = argparse.Namespace(draft_dir=str(self.draft), target_path='', yes=True,
                                       overwrite_files=False, require_text_ready=False, require_applied=False)
        self.quiet = contextlib.ExitStack()
        self.quiet.enter_context(contextlib.redirect_stdout(io.StringIO()))
        self.quiet.enter_context(contextlib.redirect_stderr(io.StringIO()))
        self.addCleanup(self.quiet.close)

    def save(self):
        (self.draft / 'manifest.json').write_text(json.dumps(self.manifest))

    def test_changed_live_metadata_is_never_overwritten(self):
        (self.target / 'metadata.bib').write_text('someone else added entries')
        with self.assertRaisesRegex(SystemExit, 'Live metadata changed'):
            upstream.command_apply(self.args)
        self.assertFalse((self.target / '1.pdf').exists())

    def test_same_size_different_pdf_is_not_skipped(self):
        (self.target / '1.pdf').write_bytes(b'PDF2')
        with self.assertRaisesRegex(SystemExit, 'different content'):
            upstream.command_apply(self.args)

    def test_import_is_resumable_and_existing_text_is_not_rewritten(self):
        self.item['ocr_text_file'] = 'files/1.txt'
        (self.draft / 'files/1.txt').write_text('text')
        self.save()
        upstream.command_apply(self.args)
        sidecar = self.target / '1.txt'
        os.utime(sidecar, (123, 123))
        upstream.command_apply(self.args)
        self.assertEqual(sidecar.stat().st_mtime, 123)
        self.assertEqual((self.target / 'metadata.bib').read_text(), 'new')

    def test_skip_apply_requires_actual_import(self):
        self.args.require_applied = True
        with self.assertRaisesRegex(SystemExit, 'not fully imported'):
            upstream.command_apply(self.args)

    def test_unresolved_pdf_blocks_import_even_if_ocr_was_skipped(self):
        self.args.require_text_ready = True
        (self.draft / 'text-scan.json').write_text(json.dumps({'items': [{'target_file': '1.pdf', 'reason': 'error'}]}))
        with self.assertRaisesRegex(SystemExit, 'Unresolved PDF'):
            upstream.command_apply(self.args)

    def test_unknown_work_id_is_not_silently_ignored(self):
        args = argparse.Namespace(draft_dir=str(self.draft), work_id=[42])
        with self.assertRaisesRegex(SystemExit, 'Unknown work ids'):
            upstream.scan_draft_items(args)

    def test_markdown_coverage_rejects_missing_and_empty_files(self):
        for content in (None, '', '   '):
            directory = self.target / 'markdown'
            directory.mkdir(exist_ok=True)
            if content is not None:
                (directory / '1.txt').write_text(content)
            with self.assertRaisesRegex(SystemExit, 'coverage incomplete'):
                upstream.command_check_markdown(self.args)
        (directory / '1.txt').write_text('usable text')
        upstream.command_check_markdown(self.args)

    def test_generated_commands_do_not_suggest_corpus_wide_embedding(self):
        output = io.StringIO()
        with patch.object(upstream, 'resolve_target', return_value={'name': 'Corpus'}), contextlib.redirect_stdout(output):
            upstream.command_commands(argparse.Namespace())
        self.assertIn('embed-draft', output.getvalue())
        self.assertNotIn('sync_mode=INSERT', output.getvalue())

    def test_empty_sidecar_is_retried_and_success_is_checkpointed(self):
        (self.draft / 'files/1.txt').write_text('  ')
        args = argparse.Namespace(draft_dir=str(self.draft), work_id=[1], min_text_chars=500,
                                  min_text_page_ratio=.25, all=False, overwrite=False,
                                  ocr_url='http://unused', timeout=3660, keep_going=True)
        results = [{'target_file': '1.pdf', 'reason': 'empty_text'}]
        with patch.object(upstream, 'scan_draft_items', return_value=(self.draft, results, {})), \
                patch.object(upstream, 'ocr_pdf', return_value={'full_text': 'recovered'}) as ocr:
            upstream.command_ocr(args)
        ocr.assert_called_once()
        self.assertEqual((self.draft / 'files/1.txt').read_text(), 'recovered\n')
        report = json.loads((self.draft / 'ocr-report.json').read_text())
        self.assertEqual(report['completed_count'], 1)


class FlowTests(unittest.TestCase):
    def run_flow(self, extra=(), fail='', command='rag-flow'):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            docker = root / 'docker'
            docker.write_text('''#!/usr/bin/env python3
import json, os, sys
args = sys.argv[1:]
with open(os.environ['CALLS'], 'a') as f: f.write(json.dumps(args) + '\\n')
stage = next((s for s in ('draft', 'scan-text', 'ocr', 'apply', 'check-markdown') if s in args), '')
if stage and stage == os.environ.get('FAIL_STAGE'): sys.exit(42)
if '-i' in args and 'kantropos-corpus-updater' in args:
    payload = json.load(sys.stdin)
    assert payload['items'] == [{'target_file':'new.pdf'}]
    if os.environ.get('FAIL_STAGE') == 'embedding': sys.exit(43)
elif 'draft' in args: print(json.dumps({'draft_dir':'/saved', 'target':{'name':'Corpus'}}))
elif 'print(json.dumps(json.loads' in ' '.join(args): print(json.dumps({'target':{'name':'Corpus'},'items':[{'target_file':'new.pdf'}]}))
elif 'manifest.json' in ' '.join(args): print('Corpus')
elif 'urllib.parse' in ' '.join(args): print('Corpus')
''')
            docker.chmod(0o755)
            curl = root / 'curl'
            curl.write_text('#!/bin/sh\nexit 0\n')
            curl.chmod(0o755)
            calls = root / 'calls'
            env = {**os.environ, 'PATH': f'{root}:{os.environ["PATH"]}', 'CALLS': str(calls), 'FAIL_STAGE': fail}
            arguments = (['rag-flow', '--draft-dir', '/saved', '--ocr-url', 'http://unused']
                         if command == 'rag-flow' else ['embed-draft', '/saved'])
            result = subprocess.run(['bash', str(REPO / 'backend/scripts/kantropos_upstream.sh'), *arguments, *extra],
                                    env=env, text=True, capture_output=True)
            return result, [json.loads(line) for line in calls.read_text().splitlines()]

    def test_resume_does_not_create_a_new_draft(self):
        result, calls = self.run_flow()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(any('draft' in args for args in calls))
        self.assertFalse(any('kantropos-corpus-updater' in args for args in calls))

    def test_ocr_failure_never_reaches_import_or_embedding(self):
        result, calls = self.run_flow(('--yes',), fail='ocr')
        self.assertEqual(result.returncode, 42)
        self.assertIn('stage OCR failed', result.stderr)
        self.assertIn('Saved draft: /saved', result.stderr)
        self.assertFalse(any('apply' in args or 'kantropos-corpus-updater' in args for args in calls))

    def test_skip_apply_without_yes_is_still_a_dry_run(self):
        result, calls = self.run_flow(('--skip-apply',))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(any('kantropos-corpus-updater' in args for args in calls))

    def test_skip_apply_checks_the_files_were_imported(self):
        result, calls = self.run_flow(('--yes', '--skip-apply', '--skip-markdown', '--skip-embed'))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(any('--require-applied' in args for args in calls))

    def test_embedding_passes_manifest_to_scoped_runner_not_http(self):
        result, calls = self.run_flow(('--yes', '--skip-apply', '--skip-markdown'))
        self.assertEqual(result.returncode, 0, result.stderr)
        scoped = [args for args in calls if '-i' in args and 'kantropos-corpus-updater' in args]
        self.assertEqual(len(scoped), 1)
        self.assertIn('--yes', scoped[0])
        self.assertFalse(any('http://localhost:8001/embeddings/' in ' '.join(args) for args in calls))

    def test_coverage_failure_never_invokes_scoped_runner(self):
        result, calls = self.run_flow(('--yes', '--skip-apply', '--skip-markdown'), fail='check-markdown')
        self.assertEqual(result.returncode, 42)
        self.assertFalse(any('kantropos-corpus-updater' in args for args in calls))

    def test_scoped_runner_failure_propagates_to_flow(self):
        result, calls = self.run_flow(('--yes', '--skip-apply', '--skip-markdown'), fail='embedding')
        self.assertEqual(result.returncode, 43)
        self.assertIn('stage embedding failed', result.stderr)

    def test_direct_embedding_command_defaults_to_preview_with_preflights(self):
        result, calls = self.run_flow(command='embed-draft')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(any('--require-applied' in args for args in calls))
        self.assertTrue(any('check-markdown' in args for args in calls))
        scoped = next(args for args in calls if '-i' in args and 'kantropos-corpus-updater' in args)
        self.assertNotIn('--yes', scoped)

    def test_direct_embedding_write_requires_yes(self):
        result, calls = self.run_flow(('--yes',), command='embed-draft')
        self.assertEqual(result.returncode, 0, result.stderr)
        scoped = next(args for args in calls if '-i' in args and 'kantropos-corpus-updater' in args)
        self.assertIn('--yes', scoped)

    def test_direct_embedding_cannot_skip_import_validation(self):
        result, calls = self.run_flow(('--yes',), fail='apply', command='embed-draft')
        self.assertEqual(result.returncode, 42)
        self.assertFalse(any('kantropos-corpus-updater' in args for args in calls))


if __name__ == '__main__':
    unittest.main()
