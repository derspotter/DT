import re
from pathlib import Path
import time
import threading
from collections import deque
from datetime import datetime, timedelta


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
        self.request_logs = {service: deque() for service in service_config}
        self.locks = {service: threading.Lock() for service in service_config}

    def wait_if_needed(self, service_name):
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

        limit = config['limit']
        window = timedelta(seconds=config['window'])

        with self.locks[service_name]:
            now = datetime.now()

            # Remove old requests from the log that are outside the time window
            while self.request_logs[service_name] and (now - self.request_logs[service_name][0]) > window:
                self.request_logs[service_name].popleft()

            # If the log is full, we need to wait
            if len(self.request_logs[service_name]) >= limit:
                time_of_oldest_request = self.request_logs[service_name][0]
                time_to_wait = (time_of_oldest_request + window) - now

                if time_to_wait.total_seconds() > 0:
                    print(f"Rate limit for '{service_name}' reached. Waiting for {time_to_wait.total_seconds():.2f} seconds.")
                    time.sleep(time_to_wait.total_seconds())

            # Log the new request time
            self.request_logs[service_name].append(datetime.now())
            
            # Return True to indicate the request can proceed
            return True


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
