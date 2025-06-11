#!/usr/bin/env python3
"""
Handle duplicate file references in BibTeX:
1. Move duplicate files to ~/output/duplicates
2. Remove entries referencing those files from metadata.bib
3. Save removed entries to ~/output/duplicates/missing.bib
"""

import os
import re
import sys
import shutil
from pathlib import Path

def extract_entries_from_bibtex(bibtex_file):
    """Extract all entries from a BibTeX file as a dictionary."""
    entries = {}
    current_entry = []
    current_key = None
    entry_pattern = re.compile(r'@\w+{([^,]+),')
    
    with open(bibtex_file, 'r', encoding='utf-8') as f:
        for line in f:
            # Start of new entry
            if line.startswith('@'):
                # Save previous entry if exists
                if current_key and current_entry:
                    entries[current_key] = current_entry
                
                # Start new entry
                match = entry_pattern.match(line)
                if match:
                    current_key = match.group(1)
                    current_entry = [line]
                else:
                    current_key = None
                    current_entry = []
            # Continue current entry
            elif current_key:
                current_entry.append(line)
                
    # Save last entry
    if current_key and current_entry:
        entries[current_key] = current_entry
    
    return entries

def find_duplicate_references(bibtex_file):
    """Find entries with duplicate file references and their file paths."""
    entries = extract_entries_from_bibtex(bibtex_file)
    file_pattern = re.compile(r'file\s*=\s*\{\{([^}]+)\}:PDF\}')
    
    # Map each file to its entries
    file_to_entries = {}
    entry_to_file = {}
    
    for key, lines in entries.items():
        entry_text = ''.join(lines)
        match = file_pattern.search(entry_text)
        if match:
            file_path = match.group(1).strip()
            basename = os.path.basename(file_path)
            
            if basename not in file_to_entries:
                file_to_entries[basename] = []
            
            file_to_entries[basename].append(key)
            entry_to_file[key] = file_path
    
    # Filter to files with duplicates
    duplicates = {file: entries for file, entries in file_to_entries.items() 
                if len(entries) > 1}
    
    return duplicates, entries, entry_to_file

def main():
    # Paths
    bibtex_file = str(Path.home() / "output" / "downloads" / "metadata.bib")
    output_dir = Path.home() / "output"
    downloads_dir = output_dir / "downloads"
    duplicates_dir = output_dir / "duplicates"
    missing_bib = duplicates_dir / "missing.bib"
    
    # Create duplicates directory
    duplicates_dir.mkdir(exist_ok=True)
    
    # Find duplicates
    duplicates, all_entries, entry_to_file = find_duplicate_references(bibtex_file)
    
    # Keep track of entries to remove and files to move
    entries_to_remove = set()
    for filename, entry_keys in duplicates.items():
        for entry_key in entry_keys:
            entries_to_remove.add(entry_key)
    
    print(f"Found {len(duplicates)} files with duplicate references")
    print(f"Will remove {len(entries_to_remove)} entries from metadata.bib")
    
    # Move files to duplicates directory
    files_moved = 0
    for filename in duplicates.keys():
        source = downloads_dir / filename
        dest = duplicates_dir / filename
        if source.exists():
            try:
                shutil.move(str(source), str(dest))
                files_moved += 1
            except Exception as e:
                print(f"Error moving {filename}: {e}")
    
    print(f"Moved {files_moved} duplicate files to {duplicates_dir}")
    
    # Save removed entries to missing.bib
    with open(missing_bib, 'w', encoding='utf-8') as f:
        for key in entries_to_remove:
            f.writelines(all_entries[key])
            f.write("\n\n")
    
    print(f"Saved {len(entries_to_remove)} removed entries to {missing_bib}")
    
    # Create new metadata.bib without duplicates
    temp_bibtex = bibtex_file + ".temp"
    with open(temp_bibtex, 'w', encoding='utf-8') as f:
        for key, lines in all_entries.items():
            if key not in entries_to_remove:
                f.writelines(lines)
                f.write("\n\n")
    
    # Replace original with cleaned version
    if os.path.exists(temp_bibtex):
        os.replace(temp_bibtex, bibtex_file)
        
    print(f"Updated {bibtex_file} with {len(all_entries) - len(entries_to_remove)} entries")
    print("Done!")

if __name__ == "__main__":
    main()
