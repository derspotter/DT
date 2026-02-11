import os
import sys
from pathlib import Path


def _is_valid_project_dir(candidate: Path | None) -> bool:
    if not candidate:
        return False
    dl_lit_dir = candidate / "dl_lit"
    return dl_lit_dir.exists() and (dl_lit_dir / "db_manager.py").exists()


def resolve_project_dir(anchor_file: str) -> Path | None:
    env_dir = os.environ.get("RAG_FEEDER_DL_LIT_PROJECT_DIR")
    if env_dir:
        env_candidate = Path(env_dir).resolve()
        if _is_valid_project_dir(env_candidate):
            return env_candidate

    cursor = Path(anchor_file).resolve().parent
    while True:
        direct = cursor if cursor.name == "dl_lit_project" else None
        if _is_valid_project_dir(direct):
            return direct

        nested = cursor / "dl_lit_project"
        if _is_valid_project_dir(nested):
            return nested

        if cursor.parent == cursor:
            return None
        cursor = cursor.parent


def ensure_import_paths(anchor_file: str) -> Path | None:
    project_dir = resolve_project_dir(anchor_file)
    if not project_dir:
        return None
    for p in (project_dir, project_dir.parent):
        as_str = str(p)
        if as_str not in sys.path:
            sys.path.insert(0, as_str)
    return project_dir
