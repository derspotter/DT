#!/usr/bin/env python3
"""Check why PDF count is stagnating"""

import sqlite3
from collections import Counter
from pathlib import Path

conn = sqlite3.connect('data/literature.db')
cursor = conn.cursor()

# Check for duplicate file paths in downloaded_references
cursor.execute("""
    SELECT file_path, COUNT(*) as cnt 
    FROM downloaded_references 
    WHERE file_path IS NOT NULL AND file_path != ''
    GROUP BY file_path 
    HAVING COUNT(*) > 1
    ORDER BY cnt DESC
    LIMIT 20
""")

print('Duplicate file paths in database:')
duplicates = cursor.fetchall()
total_dups = 0
if duplicates:
    for path, count in duplicates:
        filename = Path(path).name if path else "NO PATH"
        print(f'  {filename}: {count} entries')
        total_dups += (count - 1)  # Extra entries beyond the first
    print(f'\nTotal duplicate entries: {total_dups}')
else:
    print('  No duplicate file paths found')

# Check recent downloads to see if they're using the same filenames
cursor.execute("""
    SELECT id, title, year, file_path, date_processed
    FROM downloaded_references 
    WHERE date_processed > datetime('now', '-1 hour')
    ORDER BY date_processed DESC
    LIMIT 20
""")

print('\n\nRecent downloads:')
recent_files = []
for row in cursor.fetchall():
    id_, title, year, file_path, date = row
    if file_path:
        filename = Path(file_path).name
        recent_files.append(filename)
        print(f'  {date}: {filename[:60]}...')
    else:
        print(f'  {date}: NO FILE PATH - {title[:40]}...')

# Check for name collisions
print('\n\nChecking for filename patterns...')

# Get all filenames
cursor.execute("""
    SELECT file_path 
    FROM downloaded_references 
    WHERE file_path IS NOT NULL AND file_path != ''
""")

all_paths = cursor.fetchall()
filenames = [Path(p[0]).name for p in all_paths if p[0]]
filename_counts = Counter(filenames)

# Show most common filenames
print('\nMost common filenames:')
for name, count in filename_counts.most_common(10):
    if count > 1:
        print(f'  {name[:60]}...: {count} times')

# Check year distribution
cursor.execute("""
    SELECT year, COUNT(*) 
    FROM downloaded_references 
    WHERE file_path IS NOT NULL 
    GROUP BY year 
    ORDER BY COUNT(*) DESC 
    LIMIT 10
""")

print('\n\nPapers by year (with files):')
for year, count in cursor.fetchall():
    print(f'  {year}: {count} papers')

conn.close()

# Check actual files
pdf_dir = Path('pdf_library')
if pdf_dir.exists():
    actual_files = list(pdf_dir.glob('*.pdf'))
    print(f'\n\nActual PDF files: {len(actual_files)}')
    
    # Check for very long filenames
    long_names = [f for f in actual_files if len(f.name) > 200]
    if long_names:
        print(f'Files with very long names: {len(long_names)}')