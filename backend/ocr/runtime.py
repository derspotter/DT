"""DT-owned runtime profile; no modification of the legacy manager checkout."""
import importlib.metadata
import json
import os
from pathlib import Path
import sys

DIRECTORY = Path(__file__).resolve().parent
REPO = DIRECTORY.parents[1]


def profile():
    return json.loads((DIRECTORY / "profile.json").read_text())


def python_path():
    return Path(os.environ.get("DT_OCR_VENV", str(REPO / ".runtime/ocr" / profile()["profile"]))) / "bin/python"


def configure_environment():
    config = profile()
    for package, expected in config["packages"].items():
        actual = importlib.metadata.version(package)
        if actual != expected:
            raise RuntimeError(f"OCR runtime mismatch: {package}={actual}, expected {expected}")
    os.environ.update(config["environment"])
    libraries = []
    for site in Path(sys.prefix).glob("lib/python*/site-packages"):
        for pattern in ("nvidia/*/lib", "nvidia/*/lib64", "ultra_infer/libs/**/lib", "paddle/libs"):
            libraries.extend(str(path) for path in site.glob(pattern) if path.is_dir())
    os.environ["LD_LIBRARY_PATH"] = ":".join(sorted(set(libraries)) + [os.environ.get("LD_LIBRARY_PATH", "")])
    return config
