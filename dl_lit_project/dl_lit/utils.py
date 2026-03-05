import re
from pathlib import Path
import time
import threading
from collections import deque
from datetime import datetime, timedelta
import os


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

        limit = config['limit']
        window = timedelta(seconds=config['window'])
        try:
            units = int(units)
        except Exception:
            units = 1
        if units <= 0:
            units = 1

        with self.locks[service_name]:
            now = datetime.now()

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
            
            # Return True to indicate the request can proceed
            return True


# Global shared rate limiter instance for the entire application
_global_rate_limiter = None


def _env_int(name: str, default: int, minimum: int = 1) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return max(minimum, int(default))
    try:
        return max(minimum, int(raw))
    except ValueError:
        return max(minimum, int(default))


def get_global_rate_limiter():
    """Get the shared rate limiter instance for the entire application."""
    global _global_rate_limiter
    if _global_rate_limiter is None:
        default_rps = _env_int('RAG_FEEDER_API_DEFAULT_RPS', 40)
        openalex_rps = _env_int('RAG_FEEDER_OPENALEX_RPS', 30)
        crossref_rps = _env_int('RAG_FEEDER_CROSSREF_RPS', 20)
        unpaywall_rps = _env_int('RAG_FEEDER_UNPAYWALL_RPS', 10)
        scihub_rps = _env_int('RAG_FEEDER_SCIHUB_RPS', 8)
        libgen_rps = _env_int('RAG_FEEDER_LIBGEN_RPS', 6)
        gemini_per_minute = _env_int('RAG_FEEDER_GEMINI_PER_MIN', 2000)
        gemini_daily = _env_int('RAG_FEEDER_GEMINI_DAILY', 100000)
        gemini_tokens_per_min = _env_int('RAG_FEEDER_GEMINI_TOKENS_PER_MIN', 3000000)
        _global_rate_limiter = ServiceRateLimiter({
            'default': {'limit': default_rps, 'window': 1},
            'openalex': {'limit': openalex_rps, 'window': 1},
            'crossref': {'limit': crossref_rps, 'window': 1},
            'unpaywall': {'limit': unpaywall_rps, 'window': 1},
            'scihub': {'limit': scihub_rps, 'window': 1},
            'libgen': {'limit': libgen_rps, 'window': 1},
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
