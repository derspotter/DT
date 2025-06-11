#!/usr/bin/env python3
"""
Handle orphaned files in the downloads directory:
Files that exist in the downloads directory but aren't referenced in metadata.bib
"""

import os
import re
import sys
import shutil
from pathlib import Path

def extract_referenced_files(bibtex_file):
    """Extract all file paths referenced in the BibTeX file."""
    referenced_files = set()
    file_pattern = re.compile(r'file\s*=\s*\{\{([^}]+)\}:PDF\}')
    
    with open(bibtex_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    for match in file_pattern.finditer(content):
        file_path = match.group(1).strip()
        # Get just the filename
        basename = os.path.basename(file_path)
        referenced_files.add(basename)
    
    return referenced_files

def find_orphaned_files(downloads_dir, referenced_files):
    """Find files in downloads_dir that aren't in referenced_files."""
    orphaned_files = []
    
    for file_path in downloads_dir.glob("*.pdf"):
        if file_path.name not in referenced_files:
            orphaned_files.append(file_path)
    
    return orphaned_files

def main():
    # Paths
    bibtex_file = str(Path.home() / "output" / "downloads" / "metadata.bib")
    output_dir = Path.home() / "output"
    downloads_dir = output_dir / "downloads"
    orphaned_dir = output_dir / "orphaned_files"
    
    # Create orphaned files directory
    orphaned_dir.mkdir(exist_ok=True)
    
    # Get files referenced in BibTeX
    referenced_files = extract_referenced_files(bibtex_file)
    print(f"Found {len(referenced_files)} files referenced in BibTeX.")
    
    # Find orphaned files
    orphaned_files = find_orphaned_files(downloads_dir, referenced_files)
    print(f"Found {len(orphaned_files)} orphaned files in {downloads_dir}.")
    
    # Move orphaned files
    for source in orphaned_files:
        dest = orphaned_dir / source.name
        try:
            shutil.move(str(source), str(dest))
            print(f"Moved {source.name}")
        except Exception as e:
            print(f"Error moving {source.name}: {e}")
    
    print(f"\nMoved {len(orphaned_files)} orphaned files to {orphaned_dir}")
    print("Done!")

if __name__ == "__main__":
    main()
