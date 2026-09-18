"""Launch the Debian-matched DT backend, optionally on a loopback test port."""
import os
from runtime import DIRECTORY, configure_environment, python_path

if __name__ == "__main__":
    configure_environment()
    os.chdir(DIRECTORY)
    python = str(python_path())
    os.execv(python, [python, "-m", "uvicorn", "ocr_service_hibernate:app", "--host", "127.0.0.1",
                     "--port", os.environ.get("DT_OCR_BACKEND_PORT", "9003")])
