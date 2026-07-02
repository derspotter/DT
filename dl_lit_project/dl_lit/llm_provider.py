"""LLM provider selection: Gemini (default) or any OpenAI-compatible endpoint.

Callers use the google-genai client surface (``client.models.generate_content``,
``client.files.upload/get/delete``) regardless of provider. ``get_client()``
returns either a real ``genai.Client`` or an :class:`OpenAICompatClient`
adapter that speaks that same surface but talks to an OpenAI-style
``/chat/completions`` endpoint.

Configuration (environment):

- ``RAG_FEEDER_LLM_PROVIDER``: ``gemini`` (default) or ``openai``
- Gemini: ``GEMINI_API_KEY`` or ``GOOGLE_API_KEY``
- OpenAI-compatible: ``OPENAI_API_KEY``, ``RAG_FEEDER_OPENAI_MODEL``,
  ``RAG_FEEDER_OPENAI_BASE_URL`` (default ``https://api.openai.com/v1``)

In OpenAI mode there is no remote Files API: ``files.upload`` returns a
``local:<path>`` handle and the PDF is inlined as base64 into the request at
generate time. Handles therefore stay valid across processes as long as the
file exists on disk.
"""

from __future__ import annotations

import base64
import os
from dataclasses import dataclass
from pathlib import Path

import requests
from dotenv import load_dotenv
from google import genai

DEFAULT_OPENAI_BASE_URL = "https://api.openai.com/v1"
LOCAL_HANDLE_PREFIX = "local:"
# Socket-level guard only; caller-side timeout/retry wrappers stay in charge.
HTTP_TIMEOUT_SECONDS = float(os.getenv("RAG_FEEDER_OPENAI_HTTP_TIMEOUT_SECONDS", "600"))


@dataclass
class LocalFileHandle:
    """Files-API-shaped handle pointing at a PDF on local disk."""

    name: str
    display_name: str = ""
    mime_type: str = "application/pdf"

    @property
    def path(self) -> Path:
        return Path(self.name[len(LOCAL_HANDLE_PREFIX):])


@dataclass
class _UsageMetadata:
    total_token_count: int = 0


@dataclass
class _GenerateResponse:
    text: str
    usage_metadata: _UsageMetadata


class _LocalFiles:
    """files.upload/get/delete against local paths instead of a Files API."""

    def upload(self, file, config=None):
        path = Path(file).resolve()
        if not path.is_file():
            raise FileNotFoundError(f"Cannot upload missing file: {path}")
        config = config or {}
        return LocalFileHandle(
            name=f"{LOCAL_HANDLE_PREFIX}{path}",
            display_name=str(config.get("display_name") or path.name),
            mime_type=str(config.get("mime_type") or "application/pdf"),
        )

    def get(self, name):
        handle = LocalFileHandle(name=str(name))
        if not str(name).startswith(LOCAL_HANDLE_PREFIX) or not handle.path.is_file():
            raise FileNotFoundError(f"Unknown local file handle: {name}")
        return handle

    def delete(self, name):
        # Local handles borrow the file; nothing to clean up remotely.
        return None


class _OpenAIModels:
    def __init__(self, base_url: str, api_key: str, model: str):
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._model = model

    def generate_content(self, model=None, contents=None, config=None):
        # The Gemini model names used by callers have no meaning here; the
        # endpoint's model always comes from RAG_FEEDER_OPENAI_MODEL.
        parts = []
        for item in contents if isinstance(contents, (list, tuple)) else [contents]:
            if item is None:
                continue
            if isinstance(item, str):
                parts.append({"type": "text", "text": item})
                continue
            name = getattr(item, "name", None)
            if isinstance(name, str) and name.startswith(LOCAL_HANDLE_PREFIX):
                handle = item if isinstance(item, LocalFileHandle) else LocalFileHandle(name=name)
                encoded = base64.b64encode(handle.path.read_bytes()).decode("ascii")
                parts.append({
                    "type": "file",
                    "file": {
                        "filename": handle.display_name or handle.path.name,
                        "file_data": f"data:{handle.mime_type};base64,{encoded}",
                    },
                })
                continue
            raise TypeError(
                f"Unsupported content item for the OpenAI-compatible provider: {item!r}"
            )

        body = {
            "model": self._model,
            "messages": [{"role": "user", "content": parts}],
        }
        temperature = getattr(config, "temperature", None)
        if temperature is not None:
            body["temperature"] = temperature
        if getattr(config, "response_mime_type", None) == "application/json":
            body["response_format"] = {"type": "json_object"}

        try:
            http_response = requests.post(
                f"{self._base_url}/chat/completions",
                headers={
                    "Authorization": f"Bearer {self._api_key}",
                    "Content-Type": "application/json",
                },
                json=body,
                timeout=HTTP_TIMEOUT_SECONDS,
            )
        except requests.exceptions.Timeout as exc:
            raise TimeoutError(
                f"OpenAI-compatible request timed out after {HTTP_TIMEOUT_SECONDS}s"
            ) from exc

        if http_response.status_code != 200:
            detail = http_response.text
            try:
                detail = http_response.json().get("error", {}).get("message", detail)
            except Exception:
                pass
            raise RuntimeError(
                f"OpenAI-compatible endpoint returned {http_response.status_code}: {detail}"
            )

        payload = http_response.json()
        try:
            text = payload["choices"][0]["message"]["content"] or ""
        except (KeyError, IndexError, TypeError) as exc:
            raise RuntimeError(f"Unexpected response shape from endpoint: {payload!r}") from exc
        usage = payload.get("usage") or {}
        return _GenerateResponse(
            text=text,
            usage_metadata=_UsageMetadata(total_token_count=int(usage.get("total_tokens") or 0)),
        )


class OpenAICompatClient:
    """genai.Client-shaped adapter for OpenAI-compatible chat endpoints."""

    provider_name = "openai"

    def __init__(self, base_url: str, api_key: str, model: str):
        self.files = _LocalFiles()
        self.models = _OpenAIModels(base_url, api_key, model)


def get_client(required: bool = False):
    """Return the configured LLM client, or None if optional and unconfigured.

    Raises ValueError on unknown provider, or on missing OpenAI configuration
    (OpenAI mode fails fast even when ``required`` is False, since a partial
    configuration is always a mistake).
    """
    load_dotenv()
    provider = (os.getenv("RAG_FEEDER_LLM_PROVIDER") or "gemini").strip().lower()

    if provider == "gemini":
        api_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
        if not api_key:
            if required:
                raise ValueError(
                    "Gemini provider selected but GEMINI_API_KEY/GOOGLE_API_KEY is not set"
                )
            return None
        try:
            return genai.Client(api_key=api_key)
        except Exception:
            if required:
                raise
            return None

    if provider == "openai":
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("OpenAI-compatible provider selected but OPENAI_API_KEY is not set")
        model = os.getenv("RAG_FEEDER_OPENAI_MODEL")
        if not model:
            raise ValueError(
                "OpenAI-compatible provider selected but RAG_FEEDER_OPENAI_MODEL is not set"
            )
        base_url = os.getenv("RAG_FEEDER_OPENAI_BASE_URL") or DEFAULT_OPENAI_BASE_URL
        return OpenAICompatClient(base_url=base_url, api_key=api_key, model=model)

    raise ValueError(
        f"Unknown RAG_FEEDER_LLM_PROVIDER: {provider!r} (expected 'gemini' or 'openai')"
    )
