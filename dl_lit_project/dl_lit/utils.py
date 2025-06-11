import re
from pathlib import Path

def parse_bibtex_file_field(file_field_str: str | None) -> str | None:
    """Parses the BibTeX 'file' field to extract the file path.

    Handles formats like ':/path/to/file.pdf:PDF' or just 'file.pdf'.

    Args:
        file_field_str: The raw string from the BibTeX 'file' field.

    Returns:
        The extracted file path as a string, or None if input is invalid.
    """
    if not file_field_str or not isinstance(file_field_str, str):
        return None

    # Regex to find the path between colons, or the whole string if no colons
    # It looks for the longest string that doesn't contain a colon, 
    # which is often the file path itself in formats like 'Description:path:Type'
    match = re.search(r'[^:]+', file_field_str)
    if match:
        # Further clean up if it's a typical Zotero/Mendeley format
        parts = file_field_str.split(':')
        if len(parts) >= 2 and parts[1]:
             # Typically the path is the second component in ':path:type'
             return parts[1]
        # If not the typical format, return the first non-empty part
        return parts[0] if parts[0] else None

    return file_field_str # Fallback to the original string if regex fails
