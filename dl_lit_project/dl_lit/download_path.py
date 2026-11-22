"""The shared download step: enhance a reference, take its checksum, keep its trace.

Callers (the daemon's queue and raw-download paths, ``dl_lit download-pdfs``) build their own
``ref`` and handle their own result shapes; only the enhance -> checksum -> trace sequence lives
here. Enhancer exceptions propagate to the caller.
"""
from __future__ import annotations

import hashlib
import os
import re
import secrets
import sys

# The staged name of one attempt: ``<final>.part-<hex>`` in the final file's directory.
_PART_SUFFIX_RE = re.compile(r"\.part-[0-9a-zA-Z]+$")


def download_ref(enhancer, ref: dict, download_dir, *, part_suffix: str | None = None) -> tuple[dict | None, str, list[dict]]:
    """Return ``(download_result_or_None, checksum, trace)``.

    With ``part_suffix`` the builders write ``<final><part_suffix>`` (``ref["part_suffix"]``)
    and report that staged path; the writer publishes it with :func:`publish_download`.

    ``download_result`` is the enhancer's returned dict when it names a ``downloaded_file``;
    otherwise ``None`` and ``checksum == ""``. The trace comes from the returned dict, or from
    ``ref`` (which the enhancer updates in place) when it returned nothing. The
    checksum is the downloader's own (``result["checksum"]``); only without one is the file
    hashed, in chunks. An unreadable file is logged to stderr and gives ``""``.
    """
    if part_suffix:
        ref["part_suffix"] = part_suffix
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
        except OSError as exc:
            print(f"[download] cannot read {result['downloaded_file']} for its checksum: {exc}", file=sys.stderr)
            checksum = ""
    return result, checksum, trace


def fit_stem(name: str, reserve: int = 32, limit: int = 255) -> str:
    """Shorten the stem of ``name`` until its UTF-8 name plus ``reserve`` bytes fits ``limit``.

    ``reserve`` covers the suffixes a staged download may get (``.part-<8 hex>``, or
    ``-<work id>-<8 hex>`` when publishing). A name with room for them is returned unchanged.
    Cuts on a character boundary.
    """
    def size(text):
        return len(text.encode("utf-8", "surrogateescape"))

    if size(name) + reserve <= limit:
        return name
    stem, ext = os.path.splitext(name)
    while stem and size(stem + ext) + reserve > limit:
        stem = stem[:-1]
    return stem + ext


def write_staged(final_path, content: bytes, part_suffix: str):
    """Write ``content`` to a new staged file ``<final><part_suffix>`` and return its path.

    The file is created exclusively (``'xb'``): an existing stage name, which may be a hard
    link to a published PDF, is never truncated. On ``FileExistsError`` one fresh
    ``.part-<8 hex>`` name is tried, then ``FileExistsError`` propagates. A write that fails
    removes the file it created before re-raising.
    """
    final_path = os.fspath(final_path)
    directory, name = os.path.split(final_path)
    final_path = os.path.join(directory, fit_stem(name))
    path = final_path + part_suffix
    try:
        fh = open(path, "xb")
    except FileExistsError:
        path = final_path + f".part-{secrets.token_hex(4)}"
        fh = open(path, "xb")
    try:
        with fh:
            fh.write(content)
    except BaseException:
        discard_download(path)
        raise
    return path


def publish_download(part_path: str, work_id: int) -> str:
    """Hard-link a staged download to its final name and return that name; never overwrites.

    Tries ``<final>`` (the staged name without ``.part-…``), then ``<stem>-<work id>.pdf``,
    then ``<stem>-<work id>-<8 hex>.pdf``. The successful link is the last fallible step; the
    staged file is left for the caller's :func:`discard_download`.
    """
    part = os.fspath(part_path)
    final = _PART_SUFFIX_RE.sub("", part)
    if final == part:
        raise ValueError(f"not a staged download path: {part}")
    stem, _ext = os.path.splitext(final)
    candidates = [final, f"{stem}-{int(work_id)}.pdf", f"{stem}-{int(work_id)}-{secrets.token_hex(4)}.pdf"]
    for index, candidate in enumerate(candidates):
        try:
            os.link(part, candidate)
        except FileExistsError:
            if index == len(candidates) - 1:
                raise
            continue
        return candidate
    raise AssertionError("unreachable")


def is_staged_path(path) -> bool:
    return bool(path) and bool(_PART_SUFFIX_RE.search(os.path.basename(os.fspath(path))))


def discard_download(part_path: str | None) -> None:
    """Remove a staged download (missing is fine). A failure is logged, never raised."""
    if not part_path:
        return
    try:
        os.unlink(os.fspath(part_path))
    except FileNotFoundError:
        pass
    except OSError as exc:
        print(f"[download] could not remove staged file {part_path}: {exc}", file=sys.stderr)


def remove_published(path: str | None) -> None:
    """Remove a PDF a writer published but did not keep. A failure is logged, never raised."""
    if not path:
        return
    try:
        os.unlink(os.fspath(path))
    except FileNotFoundError:
        pass
    except OSError as exc:
        print(f"[download] could not remove unreferenced PDF {path}: {exc}", file=sys.stderr)
