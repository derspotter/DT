"""Import ``backend/scripts/daemon/worker.py`` as a module without leaking its stream wrappers.

worker.py wraps ``sys.stdout``/``sys.stderr`` in ``_LockedTextStream`` on import (it is a
process entry point).  Inside pytest that would replace the capture streams for every later
test, so the helper restores both streams once the module has executed.
"""

import importlib.util
import sys
from pathlib import Path

WORKER_PATH = Path(__file__).resolve().parents[2] / "backend" / "scripts" / "daemon" / "worker.py"


def load_worker(module_name, monkeypatch=None):
    if monkeypatch is not None:
        monkeypatch.syspath_prepend(str(WORKER_PATH.parent))
    spec = importlib.util.spec_from_file_location(module_name, WORKER_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    saved_stdout, saved_stderr = sys.stdout, sys.stderr
    try:
        spec.loader.exec_module(module)
    finally:
        sys.stdout, sys.stderr = saved_stdout, saved_stderr
    return module
