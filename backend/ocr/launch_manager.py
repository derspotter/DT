"""Keep the existing HTTP/queue contract while DT owns the OCR backend and profile."""
import importlib.util
import os
from pathlib import Path
import sys
from runtime import DIRECTORY, configure_environment, python_path


def load_manager():
    if os.environ.get("DT_OCR_BACKEND_PORT", "9003") != "9003":
        raise RuntimeError("A test backend port must not be used for the production manager")
    configure_environment()
    os.environ["SERVICE_MANAGER_ROLE"] = "ocr"
    source = Path(os.environ.get("RAG_FEEDER_OCR_HOME", "/home/spott/rechtmaschine-debian-rag-ocr")) / "service_manager.py"
    spec = importlib.util.spec_from_file_location("dt_ocr_manager", source)
    manager = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = manager
    spec.loader.exec_module(manager)
    manager.SERVICES["ocr"].update({
        "venv": str(python_path()),
        "start_cmd": [str(python_path()), str(DIRECTORY / "launch_backend.py")],
        "cwd": str(DIRECTORY),
        "process_match": str(python_path()) + " -m uvicorn ocr_service_hibernate:app --host 127.0.0.1 --port 9003",
        "env": {},
    })
    return manager


if __name__ == "__main__":
    import uvicorn
    manager = load_manager()
    uvicorn.run(manager.app, host=manager.SERVICE_MANAGER_HOST, port=manager.SERVICE_MANAGER_PORT)
