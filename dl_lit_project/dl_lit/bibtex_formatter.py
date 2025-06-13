# dl_lit/bibtex_formatter.py
import json
import re
import hashlib
import sys
import traceback
from pathlib import Path
from bibtexparser.model import Entry



# Copied and adapted from new_dl.py BibliographyEnhancer.escape_bibtex
def escape_bibtex_string(text: str | None) -> str:
    """Escapes characters with special meaning in BibTeX/LaTeX."""
    if text is None:
        return ""
    # Simple replacements - may need refinement based on actual BibTeX processor
    replacements = {
        '\\': r'\textbackslash{}',  # Must be first
        '{': r'\{',
        '}': r'\}',
        '$': r'\$',
        '&': r'\&',
        '%': r'\%',
        '#': r'\#',
        '_': r'\_',
        '^': r'\^{}',  # Escaping caret often needs braces
        '~': r'\~{}',  # Escaping tilde often needs braces
        '"': r'{''}',  # Double quotes can cause issues
    }
    escaped_text = str(text)
    for char, escaped_char in replacements.items():
        escaped_text = escaped_text.replace(char, escaped_char)
    return escaped_text

class BibTeXFormatter:
    def __init__(self):
        pass

    def _generate_bibtex_key(self, entry_data: dict, db_id: int) -> str:
        """Generates a BibTeX key using a fallback strategy."""
        # Priority 1: Use existing key if present (e.g., from DOI or original entry)
        bib_key = entry_data.get('ID')
        if bib_key:
            return re.sub(r'[^a-zA-Z0-9_\-:]+', '', bib_key) or f"entry_{db_id}"

        # Priority 2: Author-Year-Title
        authors_str = entry_data.get('author', '')
        year_str = str(entry_data.get('year', '')).strip()
        title_str = entry_data.get('title', '')

        first_author_surname = ''
        if authors_str and isinstance(authors_str, str):
            first_author_full = authors_str.split(' and ')[0].strip()
            if ',' in first_author_full:
                first_author_surname = first_author_full.split(',')[0].strip()
            else:
                parts = first_author_full.split()
                if parts:
                    first_author_surname = parts[-1]
        
        first_author_surname = re.sub(r'[^a-zA-Z]', '', first_author_surname)
        year_str = re.sub(r'[^0-9]', '', year_str)

        if first_author_surname and year_str:
            bib_key = f"{first_author_surname}{year_str}"
            if title_str:
                title_words = re.findall(r'\b[a-zA-Z]{3,}\b', title_str)
                if title_words:
                    bib_key += f"_{title_words[0].lower()}"
            # Sanitize and return
            return re.sub(r'[^a-zA-Z0-9_\-:]+', '', bib_key) or f"entry_{db_id}"

        # Priority 3: Hash of title
        if title_str:
            hash_input = f"{title_str}".encode('utf-8')
            return f"ref_{hashlib.sha1(hash_input).hexdigest()[:8]}"

        # Final fallback: database ID
        return f"entry_{db_id}"

    def format_entry(self, db_entry_dict: dict) -> str | None:
        """
        Formats a dictionary (representing a row from 'downloaded_references')
        into a BibTeX entry string.
        Relies on 'bibtex_entry_json' for most fields, but 'file_path' from
        the database is the source of truth for the 'file' field in the output.
        """
        entry_db_id = db_entry_dict.get('id', 'N/A')
        # print(f"[Formatter Debug] Processing entry ID: {entry_db_id}", file=sys.stderr)
        raw_bibtex_json = db_entry_dict.get('bibtex_entry_json', '{}')
        if entry_db_id != 'N/A': # Print for first few entries
            try:
                # Attempt to limit printing for brevity in logs, e.g. first 5 entries
                if int(entry_db_id) < 5 : # Assuming IDs are integers and start from 1 or similar small number
                    # print(f"[Formatter Debug] ID {entry_db_id} - raw_bibtex_json: {raw_bibtex_json[:200]}...", file=sys.stderr)
                    pass # Fix IndentationError
            except ValueError: # If ID is not an int, print for 'N/A' or other non-int IDs if it's the first time
                if not hasattr(self, '_printed_non_int_id_debug'):
                    # print(f"[Formatter Debug] ID {entry_db_id} (non-int) - raw_bibtex_json: {raw_bibtex_json[:200]}...", file=sys.stderr)
                    # self._printed_non_int_id_debug = True # Assuming this was also meant to be commented
                    pass # Fix IndentationError
        try:
            # 1. Load base entry data from JSON
            raw_bibtex_json = db_entry_dict.get('bibtex_entry_json', '{}')
            entry_data = json.loads(raw_bibtex_json)
            if not isinstance(entry_data, dict):
                print(f"Warning: bibtex_entry_json for ID {db_entry_dict.get('id', 'N/A')} is not a dict. Skipping entry.")
                return None

            # 2. Determine Entry Type
            # Prefer ENTRYTYPE from JSON, fallback to 'entry_type' column, then 'misc'
            entry_type = entry_data.get('ENTRYTYPE', db_entry_dict.get('entry_type', 'misc')).lower()

            # 3. Determine BibTeX Key
            # Create a temporary dict for key generation, preferring values from bibtex_entry_json
            # then supplementing with direct DB fields if JSON is missing them.
            key_gen_data = entry_data.copy() # Start with JSON data
            if 'ID' not in key_gen_data and db_entry_dict.get('bibtex_key'): # if JSON has no ID, use db's bibtex_key
                key_gen_data['ID'] = db_entry_dict.get('bibtex_key')
            if 'title' not in key_gen_data and db_entry_dict.get('title'):
                key_gen_data['title'] = db_entry_dict.get('title')
            if 'author' not in key_gen_data and db_entry_dict.get('authors'):
                authors_json = db_entry_dict.get('authors')
                try:
                    authors_list = json.loads(authors_json)
                    if isinstance(authors_list, list):
                        key_gen_data['author'] = " and ".join(authors_list)
                    else: # If not a list, use as string
                         key_gen_data['author'] = str(authors_json)
                except (json.JSONDecodeError, TypeError): # If not valid JSON, use as string
                    key_gen_data['author'] = str(authors_json)
            if 'year' not in key_gen_data and db_entry_dict.get('year'):
                key_gen_data['year'] = db_entry_dict.get('year')
            
            bib_key = self._generate_bibtex_key(key_gen_data, db_entry_dict.get('id'))

            # 4. Prepare fields for the BibTeX entry
            # Start with fields from bibtex_entry_json, converting keys to lowercase
            # and ensuring values are strings.
            fields = {}
            for key, value in entry_data.items():
                # Exclude special keys that are handled separately or will be overridden.
                if key.upper() not in ['ENTRYTYPE', 'ID', 'FILE'] and value is not None:
                    fields[key.lower()] = str(value)

            # 5. Handle the 'file' field using the database 'file_path' as source of truth
            db_file_path_str = db_entry_dict.get('file_path')
            if db_file_path_str and db_file_path_str.strip():
                try:
                    file_name_only = Path(db_file_path_str.strip()).name
                    if file_name_only: # Ensure filename is not empty
                        escaped_filename = escape_bibtex_string(file_name_only)
                        # Format for JabRef: ":filename.pdf:PDF"
                        fields['file'] = f":{escaped_filename}:PDF"
                    else: # Filename was empty (e.g. path was '/'), so remove field
                        if 'file' in fields: del fields['file']
                except Exception as e_path:
                    print(f"Warning: Could not process file_path '{db_file_path_str}' for entry ID {db_entry_dict.get('id', 'N/A')}: {e_path}. 'file' field will be omitted.")
                    if 'file' in fields: del fields['file']
            else:
                # No valid file_path in DB, so ensure 'file' field is not in the output
                if 'file' in fields:
                    del fields['file']
            
            # 6. Create BibTeX Entry object
            # Ensure all field keys are suitable for bibtexparser (e.g. lowercase)
            # Values should be strings.
            processed_fields = {k.lower(): v for k,v in fields.items()}
            bib_entry = Entry(entry_type, bib_key, processed_fields)

            # 7. Manually format the entry string for consistent output
            lines = [f"@{bib_entry.entry_type}{{{bib_entry.key}}},"]
            indent = '  '
            
            # Sort fields alphabetically for consistent output.
            sorted_field_items = sorted(bib_entry.fields.items())

            for i, (key, value_from_entry_obj) in enumerate(sorted_field_items):
                # value_from_entry_obj is what bibtexparser.model.Entry stored.
                # It should be a string.
                
                # For the 'file' field, we want the {{::PDF}} structure.
                # The value_from_entry_obj should be ':{escaped_filename}:PDF'
                if key == 'file':
                    # Format as: file = {{:filename.pdf:PDF}}
                    # value_from_entry_obj here is already ':{escaped_filename}:PDF'
                    line = f'{indent}{key} = {{{value_from_entry_obj}}}'
                else:
                    # For other fields, add single braces if not already fully braced.
                    if isinstance(value_from_entry_obj, str) and \
                       value_from_entry_obj.startswith('{') and \
                       value_from_entry_obj.endswith('}') and \
                       value_from_entry_obj.count('{') == value_from_entry_obj.count('}'): # Basic check for balanced outer braces
                        line = f'{indent}{key} = {value_from_entry_obj}'
                    else:
                        line = f'{indent}{key} = {{{value_from_entry_obj}}}'
                
                if i < len(sorted_field_items) - 1:
                    line += ','
                lines.append(line)
            lines.append('}')
            return '\n'.join(lines)

        except json.JSONDecodeError:
            print(f"Error: bibtex_entry_json for ID {db_entry_dict.get('id', 'N/A')} is not valid JSON: '{db_entry_dict.get('bibtex_entry_json', '')[:100]}...'", file=sys.stderr)
            return None
        except Exception as e:
            print(f"Critical error formatting BibTeX for entry ID {db_entry_dict.get('id', 'N/A')}: {e}", file=sys.stderr)
            traceback.print_exc(file=sys.stderr) # For debugging
            return None
