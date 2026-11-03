from __future__ import annotations

try:
    from dl_lit.utils import env_bool, env_int
except ImportError:  # imported without a caller that already put dl_lit_project on sys.path
    from _bootstrap import ensure_import_paths

    ensure_import_paths(__file__)
    from dl_lit.utils import env_bool, env_int


# Shared defaults across keyword search and upload enrichment expansion.
DEFAULT_RELATED_DEPTH = env_int('RAG_FEEDER_RELATED_DEPTH', 0)
DEFAULT_RELATED_DEPTH_UPSTREAM = env_int('RAG_FEEDER_RELATED_DEPTH_UPSTREAM', 0)
DEFAULT_MAX_RELATED = env_int('RAG_FEEDER_MAX_RELATED', 30)
DEFAULT_INCLUDE_DOWNSTREAM = env_bool('RAG_FEEDER_INCLUDE_DOWNSTREAM', False)
DEFAULT_INCLUDE_UPSTREAM = env_bool('RAG_FEEDER_INCLUDE_UPSTREAM', False)
