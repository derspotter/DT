import re
from pathlib import Path

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
