"""Dependency-free tests of DT's Debian OCR profile and process wiring."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2] / 'backend/ocr'
SPEC = importlib.util.spec_from_file_location('dt_test_runtime', ROOT / 'runtime.py')
runtime = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runtime)


class RuntimeTests(unittest.TestCase):
    def test_backend_is_exact_debian_snapshot(self):
        self.assertEqual(hashlib.sha256((ROOT / 'ocr_service_hibernate.py').read_bytes()).hexdigest(),
                         runtime.profile()['source_sha256'])

    def test_profile_matches_verified_debian_models(self):
        env = runtime.profile()['environment']
        self.assertEqual(env['OCR_DET_MODEL_NAME'], 'PP-OCRv6_medium_det')
        self.assertEqual(env['OCR_REC_MODEL_NAME'], 'PP-OCRv6_medium_rec')
        self.assertEqual(env['OCR_USE_UNWARPING'], 'true')
        self.assertEqual(env['OCR_ENABLE_HPI'], '1')
        self.assertGreater(int(env['OCR_HTTP_TIMEOUT_SECONDS']), 300)

    def test_wrong_dependency_version_fails_closed(self):
        with patch.object(runtime.importlib.metadata, 'version', return_value='wrong'):
            with self.assertRaisesRegex(RuntimeError, 'runtime mismatch'):
                runtime.configure_environment()

    def test_library_paths_include_nvjpeg_before_engine_import(self):
        with tempfile.TemporaryDirectory() as directory:
            lib = Path(directory) / 'lib/python3.11/site-packages/nvidia/nvjpeg/lib'
            lib.mkdir(parents=True)
            versions = runtime.profile()['packages']
            with patch.object(runtime.importlib.metadata, 'version', side_effect=versions.__getitem__), \
                    patch.object(sys, 'prefix', directory), patch.dict(os.environ, {}, clear=True):
                runtime.configure_environment()
                self.assertIn(str(lib), os.environ['LD_LIBRARY_PATH'])
                self.assertEqual(os.environ['OCR_USE_UNWARPING'], 'true')

    def test_default_python_lives_in_dt_not_legacy_checkout(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(runtime.python_path(), ROOT.parents[1] / '.runtime/ocr/debian-20260918/bin/python')

    def test_manager_rejects_test_port(self):
        spec = importlib.util.spec_from_file_location('dt_test_manager', ROOT / 'launch_manager.py')
        module = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, runtime=runtime):
            spec.loader.exec_module(module)
        with patch.dict(os.environ, DT_OCR_BACKEND_PORT='19003'):
            with self.assertRaisesRegex(RuntimeError, 'test backend port'):
                module.load_manager()

    def test_manager_adapter_points_to_dt_without_editing_legacy_source(self):
        spec = importlib.util.spec_from_file_location('dt_test_manager_adapter', ROOT / 'launch_manager.py')
        module = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, runtime=runtime):
            spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as directory:
            legacy = Path(directory) / 'service_manager.py'
            original = ('import os\n'
                        'KEEP_SERVICES_RUNNING = os.environ.get("KEEP_SERVICES_RUNNING") == "1"\n'
                        'SERVICES = {"ocr": {"use_http_service": True, "port": 9003, '
                        '"process_name": "ocr_service_hibernate.py"}}\n')
            legacy.write_text(original)
            with patch.object(module, 'configure_environment'), \
                    patch.dict(os.environ, {'RAG_FEEDER_OCR_HOME': directory,
                                           'KEEP_SERVICES_RUNNING': '0'}, clear=True), \
                    patch.dict(sys.modules):
                manager = module.load_manager()
                service = manager.SERVICES['ocr']
                self.assertEqual(service['venv'], str(runtime.python_path()))
                self.assertEqual(service['start_cmd'][1], str(ROOT / 'launch_backend.py'))
                self.assertIn('--port 9003', service['process_match'])
                self.assertEqual(service['process_name'], 'ocr_service_hibernate:app')
                self.assertIn(service['process_name'], service['process_match'])
                self.assertTrue(manager.KEEP_SERVICES_RUNNING)
                self.assertEqual(os.environ['SERVICE_MANAGER_ROLE'], 'ocr')
            self.assertEqual(legacy.read_text(), original)


if __name__ == '__main__':
    unittest.main()
