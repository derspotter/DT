import json
from datetime import datetime, timezone

from dl_lit import utils


class _Resp:
    def __init__(self, headers):
        self.headers = headers


def test_quota_path_defaults_next_to_db(monkeypatch, tmp_path):
    monkeypatch.delenv("RAG_FEEDER_OPENALEX_QUOTA_PATH", raising=False)
    monkeypatch.setenv("RAG_FEEDER_DB_PATH", str(tmp_path / "data" / "literature.db"))
    assert utils.openalex_quota_path() == tmp_path / "data" / "openalex_quota.json"


def test_quota_path_env_override(monkeypatch, tmp_path):
    monkeypatch.setenv("RAG_FEEDER_OPENALEX_QUOTA_PATH", str(tmp_path / "q.json"))
    assert utils.openalex_quota_path() == tmp_path / "q.json"


def test_record_writes_snapshot(monkeypatch, tmp_path):
    target = tmp_path / "q.json"
    monkeypatch.setenv("RAG_FEEDER_OPENALEX_QUOTA_PATH", str(target))
    snapshot = utils.record_openalex_quota(_Resp({
        "X-RateLimit-Limit": "100000",
        "X-RateLimit-Remaining": "87412",
        "X-RateLimit-Credits-Used": "1",
        "X-RateLimit-Reset": "18720",
    }))
    assert snapshot["limit"] == 100000
    assert snapshot["remaining"] == 87412
    assert snapshot["credits_used"] == 1
    assert snapshot["reset_seconds"] == 18720
    assert snapshot["api_key_present"] is True
    on_disk = json.loads(target.read_text())
    assert on_disk == snapshot
    observed = datetime.fromisoformat(on_disk["observed_at"].replace("Z", "+00:00"))
    reset_at = datetime.fromisoformat(on_disk["reset_at"].replace("Z", "+00:00"))
    assert (reset_at - observed).total_seconds() == 18720
    assert observed.tzinfo == timezone.utc


def test_record_without_headers_marks_no_key(monkeypatch, tmp_path):
    target = tmp_path / "q.json"
    monkeypatch.setenv("RAG_FEEDER_OPENALEX_QUOTA_PATH", str(target))
    snapshot = utils.record_openalex_quota(_Resp({}))
    assert snapshot == {"api_key_present": False, "observed_at": snapshot["observed_at"]}
    assert json.loads(target.read_text())["api_key_present"] is False


def test_record_never_raises(monkeypatch, tmp_path):
    # Directory that cannot be created: a file where the parent should be.
    blocker = tmp_path / "blocker"
    blocker.write_text("x")
    monkeypatch.setenv("RAG_FEEDER_OPENALEX_QUOTA_PATH", str(blocker / "q.json"))
    assert utils.record_openalex_quota(_Resp({"X-RateLimit-Limit": "5"})) is None
