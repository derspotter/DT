import base64
import json

import pytest

from dl_lit import llm_provider


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    # Keep the developer's .env out of provider-selection tests
    monkeypatch.setattr(llm_provider, "load_dotenv", lambda *a, **k: None)
    for key in (
        "RAG_FEEDER_LLM_PROVIDER",
        "RAG_FEEDER_OPENAI_BASE_URL",
        "OPENAI_API_KEY",
        "RAG_FEEDER_OPENAI_MODEL",
        "GEMINI_API_KEY",
        "GOOGLE_API_KEY",
    ):
        monkeypatch.delenv(key, raising=False)


def test_default_provider_is_gemini(monkeypatch):
    sentinel = object()
    captured = {}

    class FakeGenaiClient:
        def __new__(cls, api_key=None):
            captured["api_key"] = api_key
            return sentinel

    monkeypatch.setattr(llm_provider.genai, "Client", FakeGenaiClient)
    monkeypatch.setenv("GEMINI_API_KEY", "gk-123")

    client = llm_provider.get_client()
    assert client is sentinel
    assert captured["api_key"] == "gk-123"


def test_gemini_without_key_optional_returns_none():
    assert llm_provider.get_client() is None


def test_gemini_without_key_required_raises():
    with pytest.raises(ValueError):
        llm_provider.get_client(required=True)


def test_unknown_provider_raises(monkeypatch):
    monkeypatch.setenv("RAG_FEEDER_LLM_PROVIDER", "banana")
    with pytest.raises(ValueError):
        llm_provider.get_client()


def test_openai_requires_api_key(monkeypatch):
    monkeypatch.setenv("RAG_FEEDER_LLM_PROVIDER", "openai")
    monkeypatch.setenv("RAG_FEEDER_OPENAI_MODEL", "gpt-test")
    with pytest.raises(ValueError, match="OPENAI_API_KEY"):
        llm_provider.get_client(required=True)


def test_openai_requires_model(monkeypatch):
    monkeypatch.setenv("RAG_FEEDER_LLM_PROVIDER", "openai")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    with pytest.raises(ValueError, match="RAG_FEEDER_OPENAI_MODEL"):
        llm_provider.get_client(required=True)


@pytest.fixture
def openai_client(monkeypatch):
    monkeypatch.setenv("RAG_FEEDER_LLM_PROVIDER", "openai")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    monkeypatch.setenv("RAG_FEEDER_OPENAI_MODEL", "gpt-test")
    return llm_provider.get_client(required=True)


def test_openai_files_upload_returns_local_handle(openai_client, tmp_path):
    pdf = tmp_path / "sample.pdf"
    pdf.write_bytes(b"%PDF-1.4 test")

    handle = openai_client.files.upload(file=str(pdf), config={"mime_type": "application/pdf"})
    assert handle.name == f"local:{pdf.resolve()}"

    fetched = openai_client.files.get(name=handle.name)
    assert fetched.name == handle.name

    # delete is a harmless no-op for local handles
    openai_client.files.delete(name=handle.name)


def test_openai_files_get_missing_raises(openai_client, tmp_path):
    with pytest.raises(FileNotFoundError):
        openai_client.files.get(name=f"local:{tmp_path / 'nope.pdf'}")


class _FakeHTTPResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code
        self.text = json.dumps(payload)

    def json(self):
        return self._payload


def test_openai_generate_content_posts_chat_completion(openai_client, monkeypatch, tmp_path):
    pdf = tmp_path / "doc.pdf"
    pdf.write_bytes(b"%PDF-1.4 body")
    handle = openai_client.files.upload(file=str(pdf), config={"mime_type": "application/pdf"})

    captured = {}

    def fake_post(url, headers=None, json=None, timeout=None):
        captured.update(url=url, headers=headers, body=json, timeout=timeout)
        return _FakeHTTPResponse({
            "choices": [{"message": {"content": "{\"ok\": true}"}}],
            "usage": {"total_tokens": 42},
        })

    monkeypatch.setattr(llm_provider.requests, "post", fake_post)

    class Config:
        temperature = 0.7
        response_mime_type = "application/json"
        response_schema = None

    response = openai_client.models.generate_content(
        model="gemini-3-flash-preview",  # ignored in openai mode
        contents=["find the references", handle],
        config=Config(),
    )

    assert captured["url"] == "https://api.openai.com/v1/chat/completions"
    assert captured["headers"]["Authorization"] == "Bearer sk-test"
    assert captured["body"]["model"] == "gpt-test"
    assert captured["body"]["temperature"] == 0.7
    assert captured["body"]["response_format"] == {"type": "json_object"}

    parts = captured["body"]["messages"][0]["content"]
    text_parts = [p for p in parts if p["type"] == "text"]
    file_parts = [p for p in parts if p["type"] == "file"]
    assert text_parts[0]["text"] == "find the references"
    encoded = base64.b64encode(b"%PDF-1.4 body").decode("ascii")
    assert file_parts[0]["file"]["file_data"] == f"data:application/pdf;base64,{encoded}"

    assert response.text == "{\"ok\": true}"
    assert response.usage_metadata.total_token_count == 42


def test_openai_generate_content_timeout_message(openai_client, monkeypatch):
    def fake_post(url, headers=None, json=None, timeout=None):
        raise llm_provider.requests.exceptions.Timeout("boom")

    monkeypatch.setattr(llm_provider.requests, "post", fake_post)

    with pytest.raises(Exception, match="timed out"):
        openai_client.models.generate_content(
            model=None,
            contents=["hello"],
            config=None,
        )


def test_openai_generate_content_http_error(openai_client, monkeypatch):
    def fake_post(url, headers=None, json=None, timeout=None):
        return _FakeHTTPResponse({"error": {"message": "bad request"}}, status_code=400)

    monkeypatch.setattr(llm_provider.requests, "post", fake_post)

    with pytest.raises(RuntimeError, match="bad request"):
        openai_client.models.generate_content(model=None, contents=["hello"], config=None)


def test_openai_custom_base_url(monkeypatch):
    monkeypatch.setenv("RAG_FEEDER_LLM_PROVIDER", "openai")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-local")
    monkeypatch.setenv("RAG_FEEDER_OPENAI_MODEL", "qwen-local")
    monkeypatch.setenv("RAG_FEEDER_OPENAI_BASE_URL", "http://localhost:8000/v1/")

    client = llm_provider.get_client(required=True)
    captured = {}

    def fake_post(url, headers=None, json=None, timeout=None):
        captured["url"] = url
        return _FakeHTTPResponse({"choices": [{"message": {"content": "hi"}}], "usage": {}})

    monkeypatch.setattr(llm_provider.requests, "post", fake_post)
    client.models.generate_content(model=None, contents=["hello"], config=None)
    assert captured["url"] == "http://localhost:8000/v1/chat/completions"
