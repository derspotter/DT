from __future__ import annotations

import os


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        parsed = int(value)
        return parsed
    except ValueError:
        return default


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return str(value).strip().lower() not in {'0', 'false', 'no', 'off'}


# Shared defaults across keyword search and upload enrichment expansion.
DEFAULT_RELATED_DEPTH = _env_int('RAG_FEEDER_RELATED_DEPTH', 0)
DEFAULT_RELATED_DEPTH_UPSTREAM = _env_int('RAG_FEEDER_RELATED_DEPTH_UPSTREAM', 0)
DEFAULT_MAX_RELATED = _env_int('RAG_FEEDER_MAX_RELATED', 30)
DEFAULT_INCLUDE_DOWNSTREAM = _env_bool('RAG_FEEDER_INCLUDE_DOWNSTREAM', False)
DEFAULT_INCLUDE_UPSTREAM = _env_bool('RAG_FEEDER_INCLUDE_UPSTREAM', False)
