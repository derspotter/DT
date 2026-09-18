# DT OCR runtime (Debian profile, 2026-09-18)

DT owns the backend code, pinned OCR profile and launch configuration in this directory.
`ocr_service_hibernate.py` is an unchanged snapshot of the running Debian backend:
SHA-256 `545934f9fd7a37a15467e0386c7614efd03e65a7834351dabfc6c8987bf6f5ff`.
It includes named v6 models, bounded images and rotation-coordinate handling.

The existing HTTP/queue manager is still loaded from
`RAG_FEEDER_OCR_HOME/service_manager.py` (default `/home/spott/rechtmaschine-debian-rag-ocr`).
This is a runtime dependency, not a new push or modification to that repository.
The manager must include the long-request timeout fix (local production commit `1948e87`):
3600 seconds to its backend and no legacy retry on timeout. DT's OCR client waits 3660 seconds.
Only the `ocr` role is enabled. The launcher points the manager at this DT backend and runtime.

## Dependencies

The production runtime is `.runtime/ocr/debian-20260918` under DT, ignored by git.
It was copied from the existing Python 3.11 OCR environment (no hard links), then upgraded:

```bash
.runtime/ocr/debian-20260918/bin/python -m pip install \
  'paddleocr==3.7.0' 'paddlex[ocr]==3.7.2' 'pypdfium2==5.3.0' \
  'nvidia-nvjpeg-cu12==12.4.0.76'
.runtime/ocr/debian-20260918/bin/python -m pip check
```

Paddle GPU remains 3.2.0, UltraInfer 1.2.0, and the existing CUDA 12.6 libraries and host driver
are retained. This matches Debian's OCR engine/models, not every system/CUDA package.
Use `python -m ...`, not copied console-script shebangs which may still reference the old environment.
`DT_OCR_VENV` can select a different prepared environment. The launcher refuses mismatched core packages.
CUDA shared-library paths (including nvJPEG) are set before importing the inference engine.

`profile.json` explicitly enables v6 medium detection/recognition, GPU/HPI, orientation and
paper unwarping. Unwarping improves difficult-page handling but may add time to each page;
this is not a promised speed upgrade. Existing sidecars are not automatically regenerated.

## Start / test / rollback

The upstream wrapper starts `launch_manager.py` if port 8004 is unavailable. Its backend listens
only on loopback port 9003. Manager HTTP/queue behavior remains on port 8004. Logs are
`logs/ocr-manager.log` and `/tmp/ocr_service.log` on the Docker host.

An isolated backend test can use `DT_OCR_BACKEND_PORT=19003` with `launch_backend.py`;
do not leave that override set when starting the production manager. Validate a small existing PDF
through `/ocr`, not just `/health`. Check page count, text, model log, HPI and confidence.

Before switching, verify manager `/status` reports no active or queued jobs. Record the exact old
manager/backend PIDs and environments; stop only those processes, not app containers or other GPU users.
The original checkout, original virtualenv and previous source remain untouched for rollback.
To roll back, stop the DT manager/backend while idle and restart the recorded original manager command
from the old checkout. Reverting the wrapper restores its old automatic startup path.

This change does not start corpus import, markdown generation or embeddings.

## Shared-GPU limitation

The production GPU is also used by other processes. During the first isolated test, two Ollama
models occupied most of its 20 GB and the test process aborted with a CUDA illegal-access error.
Those models were not stopped or reconfigured. Check GPU headroom before large OCR runs;
the OCR queue only serializes OCR requests, not workloads submitted to other GPU services.
