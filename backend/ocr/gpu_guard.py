"""Reject OCR admission when the shared GPU is busy, without evicting models."""
import asyncio
import os
import subprocess


def require_gpu_headroom():
    minimum = int(os.environ.get("DT_OCR_MIN_FREE_VRAM_MIB", "6144"))
    if minimum <= 0:
        raise RuntimeError("DT_OCR_MIN_FREE_VRAM_MIB must be positive")
    try:
        result = subprocess.run(
            ["nvidia-smi", "--id=0", "--query-gpu=memory.free", "--format=csv,noheader,nounits"],
            check=True, capture_output=True, text=True, timeout=5,
        )
        free = int(result.stdout.strip())
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        raise RuntimeError("OCR unavailable: cannot verify free GPU memory; retry later") from exc
    if free < minimum:
        raise RuntimeError(
            f"OCR GPU busy: {free} MiB free, at least {minimum} MiB required. "
            "Other GPU services were left untouched. Retry when GPU memory is available."
        )


def install_gpu_guard(manager):
    original_ready = manager.ServiceQueue._ensure_service_ready
    original_http = manager.run_http_ocr

    def ensure_ready(queue, service):
        if service == "ocr":
            require_gpu_headroom()
        return original_ready(queue, service)

    async def run_http(*args, **kwargs):
        try:
            # Check every request, including warm requests that skip a queue switch.
            await asyncio.to_thread(require_gpu_headroom)
            # The legacy queue remembers its service even after a child crashes.
            if not manager.is_service_running("ocr"):
                await asyncio.to_thread(manager.service_queue._ensure_service_ready, "ocr")
        except RuntimeError as exc:
            raise manager.HTTPException(status_code=503, detail=str(exc)) from exc
        return await original_http(*args, **kwargs)

    manager.ServiceQueue._ensure_service_ready = ensure_ready
    manager.run_http_ocr = run_http
