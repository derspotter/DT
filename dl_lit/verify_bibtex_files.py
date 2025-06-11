#!/usr/bin/env python3
"""
Verify BibTeX file entries against actual files in directory.
This script checks if all file references in metadata.bib exist in the specified directory.
"""

import os
import re
import sys
from pathlib import Path

def extract_file_paths(bibtex_file):
    """Extract file paths from BibTeX entries."""
    file_paths = []
    entry_keys = []
    entry_count = 0
    
    with open(bibtex_file, 'r', encoding='utf-8') as f:
        current_key = None
        for line in f:
            # Extract key from first line of each entry
            if line.startswith('@'):
                entry_count += 1
                key_match = re.search(r'@\w+{([^,]+),', line)
                if key_match:
                    current_key = key_match.group(1)
            
            # Extract file path
            file_match = re.search(r'file\s*=\s*\{\{([^}]+)\}:PDF\}', line)
            if file_match:
                file_path = file_match.group(1).strip()
                file_paths.append((current_key, file_path))
                entry_keys.append(current_key)
    
    print(f"Total BibTeX entries: {entry_count}")
    print(f"Entries with file fields: {len(file_paths)}")
    
    return file_paths

def verify_file_existence(file_paths, base_dir):
    """Verify that the files exist."""
    missing_files = []
    existing_files = []
    absolute_count = 0
    relative_count = 0
    
    for entry_key, file_path in file_paths:
        # Check if it's already an absolute path
        if os.path.isabs(file_path):
            absolute_count += 1
            full_path = Path(file_path)
        else:
            relative_count += 1
            # For relative paths, try both with and without 'downloads' prefix
            full_path = Path(base_dir) / file_path
            if not full_path.exists():
                # If file_path already contains 'downloads/', avoid double-nesting
                if not file_path.startswith('downloads/'):
                    full_path = Path(base_dir) / 'downloads' / file_path
        
        if full_path.exists():
            existing_files.append((entry_key, file_path))
        else:
            missing_files.append((entry_key, file_path))
    
    print(f"Absolute paths: {absolute_count}")
    print(f"Relative paths: {relative_count}")
    
    return existing_files, missing_files

def find_duplicate_references(file_paths):
    """Find entries that reference the same file."""
    # Create a dictionary of file paths to entry keys
    file_to_entries = {}
    for entry_key, file_path in file_paths:
        # Strip any directory prefix for comparison
        basename = os.path.basename(file_path) if file_path else None
        if basename:
            if basename not in file_to_entries:
                file_to_entries[basename] = []
            file_to_entries[basename].append(entry_key)
    
    # Filter to only files referenced by multiple entries
    duplicates = {file: entries for file, entries in file_to_entries.items() 
                 if len(entries) > 1}
    
    return duplicates

def main():
    # Default paths
    bibtex_file = str(Path.home() / "output" / "downloads" / "metadata.bib")
    base_dir = str(Path.home() / "output")
    
    # Override with command-line arguments if provided
    if len(sys.argv) > 1:
        bibtex_file = sys.argv[1]
    if len(sys.argv) > 2:
        base_dir = sys.argv[2]
    
    # Count actual files in the downloads directory
    downloads_dir = Path(base_dir) / "downloads"
    actual_files = list(downloads_dir.glob("*.pdf"))
    print(f"Checking file references in {bibtex_file}...")
    print(f"Actual PDF files in {downloads_dir}: {len(actual_files)}")
    
    file_paths = extract_file_paths(bibtex_file)
    
    existing, missing = verify_file_existence(file_paths, base_dir)
    print(f"Existing files: {len(existing)} ({len(existing)/len(file_paths)*100:.1f}%)")
    print(f"Missing files: {len(missing)} ({len(missing)/len(file_paths)*100:.1f}%)")
    
    if missing:
        print("\nMissing files (showing first 10):")
        for i, (entry_key, path) in enumerate(missing[:10]):
            print(f"{i+1}. Entry key: {entry_key} | Path: {path}")
        if len(missing) > 10:
            print(f"... and {len(missing) - 10} more.")
    
    print("\nSummary:")
    if not missing:
        print("✅ All file references in the BibTeX exist!")
    else:
        print(f"❌ {len(missing)} file references do not exist.")
        
    # Check for orphaned files (files that exist but aren't referenced in BibTeX)
    existing_paths = set(path for _, path in existing)
    orphaned = 0
    for pdf_file in actual_files:
        rel_path = f"downloads/{pdf_file.name}"
        if rel_path not in existing_paths:
            orphaned += 1
    
    if orphaned > 0:
        print(f"⚠️ Found {orphaned} PDF files in the downloads directory that aren't referenced in BibTeX.")
    else:
        print("✅ All PDF files in downloads directory are referenced in BibTeX.")
    
    # Check for duplicate file references
    duplicates = find_duplicate_references(file_paths)
    print(f"\nFound {len(duplicates)} files referenced by multiple entries.")
    if duplicates:
        # Sort by number of referencing entries (most duplicated first)
        sorted_dupes = sorted(duplicates.items(), key=lambda x: len(x[1]), reverse=True)
        print("Top 10 most duplicated files:")
        for i, (filename, entries) in enumerate(sorted_dupes[:10]):
            print(f"{i+1}. {filename} - referenced by {len(entries)} entries:")
            for j, entry_key in enumerate(entries[:3]):
                print(f"   {j+1}. {entry_key}")
            if len(entries) > 3:
                print(f"   ... and {len(entries) - 3} more")
        
        # Save full duplicate information to file
        output_file = Path(bibtex_file).parent / "duplicate_references.txt"
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(f"Found {len(duplicates)} files referenced by multiple entries:\n\n")
            for filename, entries in sorted_dupes:
                f.write(f"{filename} - referenced by {len(entries)} entries:\n")
                for entry_key in entries:
                    f.write(f"  - {entry_key}\n")
                f.write("\n")
        print(f"\nComplete duplicate reference information saved to {output_file}")
        
        # Count entries affected
        total_affected = sum(len(entries) for entries in duplicates.values())
        print(f"Total BibTeX entries affected by duplication: {total_affected}")



if __name__ == "__main__":
    main()
