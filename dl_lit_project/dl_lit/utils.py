import json
import math
import re
import sys
import tempfile
from pathlib import Path
import time
import threading
from collections import deque
from datetime import datetime, timedelta, timezone
import os
from urllib.parse import parse_qsl, urlsplit, urlunsplit

import requests
from requests.adapters import HTTPAdapter


OPENALEX_BASE_URL = 'https://api.openalex.org'


class OpenAlexRateLimitExceeded(RuntimeError):
    """Raised when OpenAlex is unavailable due to throttling or daily quota exhaustion."""


_missing_openalex_api_key_warned = False


class ServiceRateLimiter:
    """
    A thread-safe rate limiter for multiple services with different rate limits.
    Each service can have a limit defined by a number of requests per time window (in seconds).
    Example config:
    service_config = {
        'openalex': {'limit': 100000, 'window': 86400},  # 100k requests per day
        'unpaywall': {'limit': 100000, 'window': 86400}, # 100k requests per day
        'default': {'limit': 5, 'window': 1} # 5 requests per second as a fallback
    }
    """
    def __init__(self, service_config):
        self.service_config = service_config
        # Per-service rolling logs of (timestamp, units). "units" is usually
        # request count, but can also represent token usage for token-based limits.
        self.request_logs = {service: deque() for service in service_config}
        self.request_totals = {service: 0 for service in service_config}
        self.locks = {service: threading.Lock() for service in service_config}
        self.last_request_ts = {service: 0.0 for service in service_config}
        self.blocked_until_ts = {service: 0.0 for service in service_config}

    def wait_if_needed(self, service_name, units=1):
        """
        Blocks until a request can be made to the specified service without exceeding its rate limit.
        """
        # Get the config for the service, or fall back to default
        config = self.service_config.get(service_name, self.service_config.get('default'))
        if not config:
            return True  # If no config, allow the request to proceed

        # Ensure the service is initialized in our logs and locks
        if service_name not in self.locks:
            self.locks[service_name] = threading.Lock()
            self.request_logs[service_name] = deque()
            self.request_totals[service_name] = 0
            self.last_request_ts[service_name] = 0.0
            self.blocked_until_ts[service_name] = 0.0

        limit = config['limit']
        window = timedelta(seconds=config['window'])
        min_interval = float(config.get('min_interval', 0) or 0)
        try:
            units = int(units)
        except Exception:
            units = 1
        if units <= 0:
            units = 1

        with self.locks[service_name]:
            now = datetime.now()
            now_ts = time.monotonic()

            blocked_until = self.blocked_until_ts.get(service_name, 0.0)
            if blocked_until > now_ts:
                wait_seconds = blocked_until - now_ts
                print(
                    f"Service '{service_name}' is temporarily unavailable. Waiting for {wait_seconds:.2f} seconds."
                )
                time.sleep(wait_seconds)
                now = datetime.now()
                now_ts = time.monotonic()

            # Some APIs enforce strict per-request spacing and will still 429
            # even when an average RPS bucket is respected.
            if min_interval > 0:
                elapsed = now_ts - self.last_request_ts[service_name]
                if elapsed < min_interval:
                    wait_seconds = min_interval - elapsed
                    print(
                        f"Rate spacing for '{service_name}' reached. Waiting for {wait_seconds:.2f} seconds."
                    )
                    time.sleep(wait_seconds)
                    now = datetime.now()
                    now_ts = time.monotonic()

            # Remove old requests from the log that are outside the time window
            while self.request_logs[service_name] and (now - self.request_logs[service_name][0][0]) > window:
                _, old_units = self.request_logs[service_name].popleft()
                self.request_totals[service_name] = max(0, self.request_totals[service_name] - old_units)

            # If the log is full, we need to wait
            if self.request_totals[service_name] + units > limit and self.request_logs[service_name]:
                time_of_oldest_request, _ = self.request_logs[service_name][0]
                time_to_wait = (time_of_oldest_request + window) - now

                if time_to_wait.total_seconds() > 0:
                    print(f"Rate limit for '{service_name}' reached. Waiting for {time_to_wait.total_seconds():.2f} seconds.")
                    time.sleep(time_to_wait.total_seconds())
                    # Recalculate after waiting to keep totals accurate.
                    now = datetime.now()
                    while self.request_logs[service_name] and (now - self.request_logs[service_name][0][0]) > window:
                        _, old_units = self.request_logs[service_name].popleft()
                        self.request_totals[service_name] = max(0, self.request_totals[service_name] - old_units)

            # Log the new request time
            self.request_logs[service_name].append((datetime.now(), units))
            self.request_totals[service_name] += units
            self.last_request_ts[service_name] = time.monotonic()
            
            # Return True to indicate the request can proceed
            return True

    def impose_block(self, service_name, wait_seconds):
        if wait_seconds is None:
            return
        try:
            delay = max(0.0, float(wait_seconds))
        except Exception:
            return
        if delay <= 0:
            return

        if service_name not in self.locks:
            self.locks[service_name] = threading.Lock()
            self.request_logs[service_name] = deque()
            self.request_totals[service_name] = 0
            self.last_request_ts[service_name] = 0.0
            self.blocked_until_ts[service_name] = 0.0

        with self.locks[service_name]:
            blocked_until = time.monotonic() + delay
            self.blocked_until_ts[service_name] = max(
                self.blocked_until_ts.get(service_name, 0.0),
                blocked_until,
            )

    def remaining_block_seconds(self, service_name):
        blocked_until = self.blocked_until_ts.get(service_name, 0.0)
        return max(0.0, blocked_until - time.monotonic())

    def backoff(self, service_name, wait_seconds):
        if wait_seconds is None:
            return
        try:
            delay = max(0.0, float(wait_seconds))
        except Exception:
            return
        if delay <= 0:
            return
        print(f"Backing off '{service_name}' for {delay:.2f} seconds after upstream throttling.")
        time.sleep(delay)


# Global shared rate limiter instance for the entire application
_global_rate_limiter = None


def _env_number(name, default, parse, kind, minimum, maximum):
    raw = os.getenv(name)
    value = default
    if raw is not None and raw.strip() != "":
        try:
            value = parse(raw.strip())
        except ValueError:
            print(f"[env] {name}={raw} is not a valid {kind}; using {default}", file=sys.stderr)
    if minimum is not None:
        value = max(minimum, value)
    if maximum is not None:
        value = min(maximum, value)
    return value


def env_int(name: str, default: int, *, minimum: int | None = None, maximum: int | None = None) -> int:
    """Read an int env var; unset/empty -> default, invalid -> stderr warning + default, then clamp."""
    return _env_number(name, int(default), int, "int", minimum, maximum)


def env_float(name: str, default: float, *, minimum: float | None = None, maximum: float | None = None) -> float:
    """Read a float env var; unset/empty -> default, invalid or non-finite -> stderr warning + default, then clamp."""
    return float(_env_number(name, float(default), _finite_float, "float", minimum, maximum))


def _finite_float(raw: str) -> float:
    value = float(raw)
    if not math.isfinite(value):
        raise ValueError(f"not finite: {raw}")
    return value


_TRUTHY = {"1", "true", "yes", "on"}
_FALSY = {"0", "false", "no", "off"}
_env_bool_warned = set()  # (name, raw): env_bool also runs in per-candidate paths


def env_bool(name: str, default: bool = False) -> bool:
    """Read a bool env var: 1/true/yes/on and 0/false/no/off (case-insensitive, stripped).

    Unset/empty -> default; anything else -> one stderr warning per name and value + default.
    """
    raw = os.getenv(name)
    value = raw.strip().lower() if raw is not None else ""
    if not value:
        return bool(default)
    if value in _TRUTHY:
        return True
    if value in _FALSY:
        return False
    if (name, raw) not in _env_bool_warned:
        _env_bool_warned.add((name, raw))
        print(f"[env] {name}={raw} is not a valid bool; using {bool(default)}", file=sys.stderr)
    return bool(default)


# Private names kept as aliases for one release.
_env_int = env_int
_env_float = env_float


def _env_str(name: str, default: str = "") -> str:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip()


def get_openalex_api_key() -> str | None:
    api_key = _env_str('OPENALEX_API_KEY') or _env_str('RAG_FEEDER_OPENALEX_API_KEY')
    return api_key or None


def get_openalex_mailto() -> str | None:
    mailto = _env_str('RAG_FEEDER_MAILTO')
    return mailto or None


OPENALEX_QUOTA_FILENAME = 'openalex_quota.json'


def openalex_quota_path() -> Path:
    override = _env_str('RAG_FEEDER_OPENALEX_QUOTA_PATH')
    if override:
        return Path(override)
    db_path = _env_str('RAG_FEEDER_DB_PATH')
    if db_path:
        return Path(db_path).resolve().parent / OPENALEX_QUOTA_FILENAME
    return Path(__file__).resolve().parents[1] / 'data' / OPENALEX_QUOTA_FILENAME


def _header_int(headers, name: str) -> int | None:
    try:
        raw = headers.get(name)
    except Exception:
        return None
    if raw is None:
        return None
    try:
        return int(str(raw).strip())
    except (TypeError, ValueError):
        return None


def record_openalex_quota(response) -> dict | None:
    """Persist the daily-budget headers OpenAlex sends with keyed requests.

    The rate limiter lives per Python process, so this file is how the Node
    backend learns where the budget stands. Never raises: a failed write is
    logged and ignored because it must not sink the request that produced it.

    A response without the rate-limit headers (a gateway error page, a 429
    that carries no headers, ...) is not necessarily proof that no API key
    is configured. When a key *is* configured, such a response is treated as
    "no data this time" and the last good snapshot is left on disk instead
    of being overwritten with a misleading no-key snapshot.
    """
    now = datetime.now(timezone.utc).replace(microsecond=0)
    observed_at = now.isoformat().replace('+00:00', 'Z')
    headers = getattr(response, 'headers', None) or {}
    limit = _header_int(headers, 'X-RateLimit-Limit')
    if limit is None:
        if get_openalex_api_key():
            # A key is configured but this particular response carried no
            # rate-limit headers (gateway error, headerless 429, ...).
            # Keep whatever snapshot is already on disk.
            return None
        snapshot = {'api_key_present': False, 'observed_at': observed_at}
    else:
        reset_seconds = _header_int(headers, 'X-RateLimit-Reset')
        snapshot = {
            'limit': limit,
            'remaining': _header_int(headers, 'X-RateLimit-Remaining'),
            'credits_used': _header_int(headers, 'X-RateLimit-Credits-Used'),
            'reset_seconds': reset_seconds,
            'observed_at': observed_at,
            'reset_at': (
                (now + timedelta(seconds=reset_seconds)).isoformat().replace('+00:00', 'Z')
                if reset_seconds is not None else None
            ),
            'api_key_present': True,
        }

    target = openalex_quota_path()
    tmp_name = None
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(prefix='.openalex_quota-', dir=str(target.parent))
        with os.fdopen(fd, 'w', encoding='utf-8') as handle:
            json.dump(snapshot, handle)
        os.chmod(tmp_name, 0o644)
        os.replace(tmp_name, target)
        tmp_name = None
    except Exception as exc:  # noqa: BLE001 - deliberately broad, see docstring
        print(f'[OpenAlex WARN] Could not write quota snapshot to {target}: {exc}', file=sys.stderr)
        return None
    finally:
        if tmp_name is not None:
            try:
                os.unlink(tmp_name)
            except OSError:
                pass
    return snapshot


def _warn_missing_openalex_api_key() -> None:
    global _missing_openalex_api_key_warned
    if _missing_openalex_api_key_warned:
        return
    print(
        "[WARN] OPENALEX_API_KEY not set. OpenAlex daily credits may be severely limited without an API key."
    )
    _missing_openalex_api_key_warned = True


def _merge_query_params(url: str, extra_params: dict[str, object] | None) -> tuple[str, dict[str, object]]:
    if not extra_params:
        return url, {}
    parsed = urlsplit(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    merged_params = {}
    for key, value in extra_params.items():
        if value is None:
            continue
        if key in query:
            continue
        merged_params[key] = value
    if not merged_params:
        return url, {}
    cleaned_url = urlunsplit((parsed.scheme, parsed.netloc, parsed.path, parsed.query, parsed.fragment))
    return cleaned_url, merged_params


def _normalize_openalex_params(url: str, params: dict[str, object] | None) -> tuple[str, dict[str, str]]:
    parsed = urlsplit(url)
    query_items = dict(parse_qsl(parsed.query, keep_blank_values=True))
    normalized = {str(key): str(value) for key, value in query_items.items()}
    for key, value in (params or {}).items():
        if value is None:
            continue
        normalized[str(key)] = str(value)
    return parsed.path, normalized


def _is_openalex_lookup_request(url: str, params: dict[str, object] | None) -> bool:
    path, normalized = _normalize_openalex_params(url, params)
    path_parts = [part for part in path.split('/') if part]

    if len(path_parts) >= 2 and path_parts[0] in {'works', 'authors', 'sources', 'institutions', 'topics', 'publishers'}:
        return True

    search_value = (normalized.get('search') or '').strip()
    if search_value:
        return False

    filter_value = (normalized.get('filter') or '').strip()
    if not filter_value:
        return False

    allowed_lookup_prefixes = ('doi:', 'openalex_id:')
    for clause in filter_value.split(','):
        clause = clause.strip()
        if not clause:
            continue
        if any(clause.startswith(prefix) for prefix in allowed_lookup_prefixes):
            continue
        return False

    return True


def _parse_header_seconds(value) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(str(value).strip())
    except Exception:
        return None
    if parsed < 0:
        return None
    return parsed


def _openalex_quota_message(wait_seconds: float | None) -> str:
    if wait_seconds is None:
        return 'OpenAlex rate limit exceeded.'
    return f'OpenAlex rate limit exceeded. Retry after {wait_seconds:.0f}s.'


def _apply_openalex_response_limits(
    response,
    rate_limiter: ServiceRateLimiter | None,
    *,
    billable_request: bool,
) -> float | None:
    if rate_limiter is None:
        return None

    retry_after = _parse_header_seconds(response.headers.get('Retry-After'))
    reset_seconds = _parse_header_seconds(response.headers.get('X-RateLimit-Reset'))
    remaining = response.headers.get('X-RateLimit-Remaining')

    wait_seconds = retry_after if retry_after is not None else reset_seconds
    try:
        remaining_value = int(str(remaining).strip()) if remaining is not None else None
    except Exception:
        remaining_value = None

    if billable_request and remaining_value is not None and remaining_value <= 0 and reset_seconds:
        rate_limiter.impose_block('openalex_billable', reset_seconds)
        return reset_seconds

    if response.status_code == 429 and wait_seconds:
        rate_limiter.impose_block('openalex', wait_seconds)
        return wait_seconds

    return wait_seconds


_API_KEY_VALUE = r"[^&#\s'\"]*"
# Two passes, neither consuming the separator the next match needs: first every "&api_key=..."
# (adjacent ones each start at their own "&"), then a leading "?api_key=..." with the "&" after
# it, so "?api_key=a&page=2" -> "?page=2" and a lone "?api_key=a" -> "".
_API_KEY_AMP_RE = re.compile(r"&api_key=" + _API_KEY_VALUE)
_API_KEY_QMARK_RE = re.compile(r"\?api_key=" + _API_KEY_VALUE + r"(&?)")


def redact_openalex_key(text) -> str:
    """Drop every ``api_key=...`` query parameter from a printed URL or message."""
    text = _API_KEY_AMP_RE.sub("", str(text))
    return _API_KEY_QMARK_RE.sub(lambda m: "?" if m.group(1) else "", text)


def redact_openalex_request_error(exc: requests.RequestException) -> requests.RequestException:
    """``exc`` with the api key dropped from its message: same type, ``response``/``request`` kept.

    requests puts the prepared URL (query string and all) into HTTPError / ConnectionError /
    Timeout messages. Returns ``exc`` itself when its message carries no key.
    """
    message = redact_openalex_key(str(exc))
    if message == str(exc) and "api_key=" not in repr(exc):
        return exc
    kwargs = {}
    if getattr(exc, 'response', None) is not None:
        kwargs['response'] = exc.response
    if getattr(exc, 'request', None) is not None:
        kwargs['request'] = exc.request
    try:
        return type(exc)(message, **kwargs)
    except TypeError:  # a subclass with its own constructor signature
        return requests.RequestException(message, **kwargs)


def openalex_request_json(
    *,
    endpoint: str | None = None,
    url: str | None = None,
    params: dict | None = None,
    headers: dict | None = None,
    timeout: float = 30,
    session=None,
    rate_limiter: ServiceRateLimiter | None = None,
    retries: int = 3,
    mailto: str | None = None,
):
    if endpoint and url:
        raise ValueError('Provide either endpoint or url, not both')
    if not endpoint and not url:
        raise ValueError('Either endpoint or url is required')

    rate_limiter = rate_limiter or get_global_rate_limiter()
    request_headers = {'Accept': 'application/json'}
    if headers:
        request_headers.update(headers)

    api_key = get_openalex_api_key()
    if not api_key:
        _warn_missing_openalex_api_key()

    extra_params = dict(params or {})
    if api_key and 'api_key' not in extra_params:
        extra_params['api_key'] = api_key
    if mailto and 'mailto' not in extra_params:
        extra_params['mailto'] = mailto

    request_url = url or f"{OPENALEX_BASE_URL}/{endpoint.lstrip('/')}"
    request_url, request_params = _merge_query_params(request_url, extra_params)
    wait_cap_seconds = env_float('RAG_FEEDER_OPENALEX_MAX_WAIT_SEC', 15.0, minimum=0.0)
    http = session or requests
    billable_request = not _is_openalex_lookup_request(request_url, request_params)

    remaining_block = rate_limiter.remaining_block_seconds('openalex_billable') if (rate_limiter and billable_request) else 0.0
    if remaining_block > wait_cap_seconds:
        raise OpenAlexRateLimitExceeded(_openalex_quota_message(remaining_block))

    last_error = None
    for attempt in range(retries):
        try:
            if rate_limiter:
                remaining_block = rate_limiter.remaining_block_seconds('openalex_billable') if billable_request else 0.0
                if remaining_block > wait_cap_seconds:
                    raise OpenAlexRateLimitExceeded(_openalex_quota_message(remaining_block))
                rate_limiter.wait_if_needed('openalex')

            response = http.get(
                request_url,
                headers=request_headers,
                params=request_params,
                timeout=timeout,
            )
            record_openalex_quota(response)
            wait_seconds = _apply_openalex_response_limits(
                response,
                rate_limiter,
                billable_request=billable_request,
            )

            if response.status_code == 429:
                if wait_seconds is None:
                    wait_seconds = 2 ** attempt
                if billable_request and wait_seconds > wait_cap_seconds:
                    raise OpenAlexRateLimitExceeded(_openalex_quota_message(wait_seconds))
                if attempt == retries - 1:
                    raise OpenAlexRateLimitExceeded(_openalex_quota_message(wait_seconds))
                time.sleep(wait_seconds)
                continue

            if response.status_code in (500, 502, 503, 504):
                if attempt == retries - 1:
                    response.raise_for_status()
                time.sleep(2 ** attempt)
                continue

            response.raise_for_status()
            return response.json()
        except OpenAlexRateLimitExceeded:
            raise
        except requests.RequestException as exc:
            last_error = exc
            if attempt == retries - 1:
                # The message holds the request URL, api_key included: never let that escape.
                raise redact_openalex_request_error(exc) from None
            time.sleep(2 ** attempt)

    if last_error is not None:
        raise redact_openalex_request_error(last_error) from None
    raise RuntimeError('OpenAlex request failed after retries')


_thread_sessions_local = threading.local()


def get_thread_session(
    kind: str,
    *,
    proxies: dict | None = None,
    trust_env: bool = False,
    pool_maxsize: int = 16,
) -> requests.Session:
    """Return this thread's keep-alive requests.Session for ``kind`` (and ``proxies``).

    requests.Session is not guaranteed thread-safe, so sessions are never shared
    across threads; within a thread the connection pool is reused across calls.
    The cache key is ``(kind, proxies)``; ``trust_env`` and ``pool_maxsize`` apply
    when the session is first created for that key.
    """
    cache = getattr(_thread_sessions_local, 'sessions', None)
    if cache is None:
        cache = {}
        _thread_sessions_local.sessions = cache
    key = (kind, tuple(sorted(proxies.items())) if proxies else None)
    session = cache.get(key)
    if session is None:
        session = requests.Session()
        session.trust_env = trust_env
        if proxies:
            session.proxies.update(proxies)
        adapter = HTTPAdapter(pool_connections=pool_maxsize, pool_maxsize=pool_maxsize)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        cache[key] = session
    return session


def get_openalex_session() -> requests.Session:
    """Return this thread's shared requests.Session for OpenAlex calls.

    Unlike the download kinds it honours the environment (proxies,
    REQUESTS_CA_BUNDLE, .netrc), as the former plain session did.
    """
    return get_thread_session('openalex', trust_env=True, pool_maxsize=64)


def reset_global_rate_limiter():
    """Drop the shared limiter so the next get_global_rate_limiter() re-reads the environment."""
    global _global_rate_limiter
    _global_rate_limiter = None


def set_global_rate_limiter(limiter):
    """Re-register a limiter as the shared instance (e.g. restore after a failed rebuild)."""
    global _global_rate_limiter
    _global_rate_limiter = limiter


def get_global_rate_limiter():
    """Get the shared rate limiter instance for the entire application."""
    global _global_rate_limiter
    if _global_rate_limiter is None:
        default_rps = env_int('RAG_FEEDER_API_DEFAULT_RPS', 40, minimum=1)
        openalex_rps = env_int('RAG_FEEDER_OPENALEX_RPS', 30, minimum=1)
        crossref_rps = env_int('RAG_FEEDER_CROSSREF_RPS', 20, minimum=1)
        semantic_scholar_rps = env_int('RAG_FEEDER_SEMANTIC_SCHOLAR_RPS', 1, minimum=1)
        semantic_scholar_min_interval = env_float('RAG_FEEDER_SEMANTIC_SCHOLAR_MIN_INTERVAL_SEC', 1.1, minimum=0.0)
        unpaywall_rps = env_int('RAG_FEEDER_UNPAYWALL_RPS', 10, minimum=1)
        scihub_rps = env_int('RAG_FEEDER_SCIHUB_RPS', 8, minimum=1)
        libgen_rps = env_int('RAG_FEEDER_LIBGEN_RPS', 6, minimum=1)
        libgen_min_interval = env_float('RAG_FEEDER_LIBGEN_MIN_INTERVAL_SEC', 0.4, minimum=0.0)
        gemini_per_minute = env_int('RAG_FEEDER_GEMINI_PER_MIN', 2000, minimum=1)
        gemini_daily = env_int('RAG_FEEDER_GEMINI_DAILY', 100000, minimum=1)
        gemini_tokens_per_min = env_int('RAG_FEEDER_GEMINI_TOKENS_PER_MIN', 3000000, minimum=1)
        _global_rate_limiter = ServiceRateLimiter({
            'default': {'limit': default_rps, 'window': 1},
            'openalex': {'limit': openalex_rps, 'window': 1},
            'crossref': {'limit': crossref_rps, 'window': 1},
            'semantic_scholar': {
                'limit': semantic_scholar_rps,
                'window': 1,
                'min_interval': semantic_scholar_min_interval,
            },
            'unpaywall': {'limit': unpaywall_rps, 'window': 1},
            'scihub': {'limit': scihub_rps, 'window': 1},
            'libgen': {
                'limit': libgen_rps,
                'window': 1,
                'min_interval': libgen_min_interval,
            },
            'gemini': {'limit': gemini_per_minute, 'window': 60},
            'gemini_daily': {'limit': gemini_daily, 'window': 86400},
            'gemini_tokens': {'limit': gemini_tokens_per_min, 'window': 60},
        })
    return _global_rate_limiter


def parse_bibtex_file_field(file_field_str: str | None) -> str | None:
    """Parses the BibTeX 'file' field to extract the file path.

    Handles formats like 'Description:filepath:Type', 'filepath:Type', or just 'filepath'.
    It processes the first file entry if multiple are specified (separated by ';').
    Also handles surrounding curly braces.

    Args:
        file_field_str: The raw string from the BibTeX 'file' field.

    Returns:
        The extracted file path as a string, or None if input is invalid.
    """
    if not file_field_str or not isinstance(file_field_str, str):
        return None

    # If multiple files are linked (e.g. Zotero format), process the first one.
    current_file_entry_str = file_field_str.split(';')[0]

    # Remove surrounding curly braces if present
    cleaned_str = current_file_entry_str.strip('{}')

    parts = cleaned_str.split(':')

    if not parts:
        return None

    # Determine path based on number of parts from splitting by ':'
    # 1. "filepath.pdf" -> parts = ['filepath.pdf']
    # 2. "filepath.pdf:PDF" -> parts = ['filepath.pdf', 'PDF']
    # 3. ":filepath.pdf:PDF" -> parts = ['', 'filepath.pdf', 'PDF']
    # 4. "Description:filepath.pdf:PDF" -> parts = ['Description', 'filepath.pdf', 'PDF']

    if len(parts) == 1:
        # Case 1: Just the filepath
        return parts[0].strip() if parts[0] else None
    elif len(parts) == 2:
        # Case 2: "filepath:Type"
        return parts[0].strip() if parts[0] else None
    elif len(parts) >= 3:
        # Case 3 or 4: ":filepath:Type" or "Description:filepath:Type"
        # The filepath is the second component.
        return parts[1].strip() if parts[1] else None
    
    return None # Should be covered by the logic above
