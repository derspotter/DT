"""The shared download step: enhance a reference, take its checksum, keep its trace.

Callers (the daemon's queue and raw-download paths, ``dl_lit download-pdfs``) build their own
``ref`` and handle their own result shapes; only the enhance -> checksum -> trace sequence lives
here. Enhancer exceptions propagate to the caller.
"""
from __future__ import annotations

import hashlib


def download_ref(enhancer, ref: dict, download_dir) -> tuple[dict | None, str, list[dict]]:
    """Return ``(download_result_or_None, checksum, trace)``.

    ``download_result`` is the enhancer's returned dict when it names a ``downloaded_file``;
    otherwise ``None`` and ``checksum == ""``. The trace comes from the returned dict, or from
    ``ref`` (which the enhancer updates in place) when it returned nothing. The
    checksum is the downloader's own (``result["checksum"]``); only without one is the file
    hashed, in chunks. An unreadable file gives ``""``.
    """
    result = enhancer.enhance_bibliography(ref, download_dir)
    download_result = result if isinstance(result, dict) else ref
    trace = list(download_result.get("download_trace") or [])
    if not (isinstance(result, dict) and result.get("downloaded_file")):
        return None, "", trace
    checksum = result.get("checksum") or ""
    if not checksum:
        try:
            with open(str(result["downloaded_file"]), "rb") as fh:
                checksum = hashlib.file_digest(fh, "sha256").hexdigest()
        except OSError:
            checksum = ""
    return result, checksum, trace
