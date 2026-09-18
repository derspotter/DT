import asyncio
import importlib.util
import os
from pathlib import Path
import subprocess
import types
import unittest
from unittest.mock import AsyncMock, Mock, patch

ROOT = Path(__file__).resolve().parents[2] / 'backend/ocr'
SPEC = importlib.util.spec_from_file_location('dt_gpu_guard', ROOT / 'gpu_guard.py')
guard = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(guard)


class GpuGuardTests(unittest.TestCase):
    def test_free_gpu_passes(self):
        with patch.dict(os.environ, {}, clear=True), patch.object(guard.subprocess, 'run', return_value=types.SimpleNamespace(stdout='16000\n')) as run:
            guard.require_gpu_headroom()
            self.assertEqual(run.call_args.kwargs['timeout'], 5)
            self.assertIn('--id=0', run.call_args.args[0])

    def test_busy_gpu_rejected_with_actionable_message(self):
        with patch.dict(os.environ, {}, clear=True), patch.object(guard.subprocess, 'run', return_value=types.SimpleNamespace(stdout='1200\n')):
            with self.assertRaisesRegex(RuntimeError, '1200 MiB free.*6144 MiB required.*untouched'):
                guard.require_gpu_headroom()

    def test_unknown_gpu_status_fails_closed(self):
        for value in [FileNotFoundError(), subprocess.TimeoutExpired('nvidia-smi', 5), None]:
            with self.subTest(value=value), patch.dict(os.environ, {}, clear=True), patch.object(guard.subprocess, 'run', side_effect=value, return_value=types.SimpleNamespace(stdout='N/A')):
                with self.assertRaisesRegex(RuntimeError, 'cannot verify'):
                    guard.require_gpu_headroom()

    def test_invalid_threshold_rejected(self):
        with patch.dict(os.environ, DT_OCR_MIN_FREE_VRAM_MIB='0'):
            with self.assertRaisesRegex(RuntimeError, 'must be positive'):
                guard.require_gpu_headroom()

    def manager(self, running=True):
        class Queue:
            def _ensure_service_ready(self, service):
                ready(service)
        class HTTPException(Exception):
            def __init__(self, status_code, detail):
                self.status_code = status_code
                super().__init__(detail)
        ready = Mock()
        manager = types.SimpleNamespace(ServiceQueue=Queue, service_queue=Queue(),
                                        HTTPException=HTTPException, run_http_ocr=AsyncMock(return_value={'ok': True}),
                                        is_service_running=Mock(return_value=running))
        return manager, ready

    def test_cold_request_checks_before_start(self):
        manager, ready = self.manager()
        guard.install_gpu_guard(manager)
        with patch.object(guard, 'require_gpu_headroom', side_effect=RuntimeError('GPU busy')):
            with self.assertRaisesRegex(RuntimeError, 'GPU busy'):
                manager.service_queue._ensure_service_ready('ocr')
        ready.assert_not_called()

    def test_warm_request_is_checked_and_does_not_restart(self):
        manager, ready = self.manager()
        http = manager.run_http_ocr
        guard.install_gpu_guard(manager)
        with patch.object(guard, 'require_gpu_headroom') as check:
            self.assertEqual(asyncio.run(manager.run_http_ocr('url', 'name', b'pdf', 'id')), {'ok': True})
            check.assert_called_once()
        ready.assert_not_called()
        http.assert_awaited_once_with('url', 'name', b'pdf', 'id')

    def test_busy_warm_request_returns_503_without_dispatch(self):
        manager, ready = self.manager()
        http = manager.run_http_ocr
        guard.install_gpu_guard(manager)
        with patch.object(guard, 'require_gpu_headroom', side_effect=RuntimeError('GPU busy')):
            with self.assertRaises(manager.HTTPException) as caught:
                asyncio.run(manager.run_http_ocr())
        self.assertEqual(caught.exception.status_code, 503)
        ready.assert_not_called()
        http.assert_not_awaited()

    def test_dead_child_restarted_before_dispatch(self):
        manager, ready = self.manager(running=False)
        guard.install_gpu_guard(manager)
        with patch.object(guard, 'require_gpu_headroom') as check:
            asyncio.run(manager.run_http_ocr())
            self.assertEqual(check.call_count, 2)
        ready.assert_called_once_with('ocr')


if __name__ == '__main__':
    unittest.main()
