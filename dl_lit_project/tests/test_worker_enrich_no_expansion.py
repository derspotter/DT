import inspect

from dl_lit.db_manager import DatabaseManager
from tests.test_worker_settings import _daemon


def test_enrich_single_never_asks_for_references_or_citations(monkeypatch, tmp_path):
    mod, daemon = _daemon(monkeypatch, tmp_path, lambda self, *a: object())
    seen = {}

    def fake(ref, searcher, limiter, **kw):
        seen.update(kw)
        return {"title": "x"}, {}

    monkeypatch.setattr(mod, "process_single_reference", fake)
    wid, enriched, info = daemon._enrich_single({"id": 1, "title": "x"}, True, True, 100, 2, 1, 40)
    assert wid == 1 and enriched == {"title": "x"} and info["status"] == "matched"
    assert seen["fetch_references"] is False and seen["fetch_citations"] is False


def test_expansion_request_logs_disabled_notice_once(monkeypatch, tmp_path):
    mod, daemon = _daemon(monkeypatch, tmp_path, lambda self, *a: object())
    lines = []
    monkeypatch.setattr(mod, "log", lines.append)
    monkeypatch.setattr(mod, "_EXPANSION_NOTICE_LOGGED", False)
    mod._note_expansion_disabled(False, False)
    assert lines == []
    mod._note_expansion_disabled(True, False)
    mod._note_expansion_disabled(False, True)
    assert len(lines) == 1 and "Expansion during enrichment is disabled" in lines[0]


def test_apply_enriched_metadata_has_no_expansion_parameters():
    params = inspect.signature(DatabaseManager.apply_enriched_metadata).parameters
    assert "expand_related" not in params and "max_related_per_source" not in params
