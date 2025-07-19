#!/usr/bin/env python3
"""Debug check_if_exists with title/author/year"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from dl_lit.db_manager import DatabaseManager

# Initialize database
db_manager = DatabaseManager("data/literature.db")

# Test data
title = "Underdevelopment and unregulated markets: why free markets produce junk growth in developing countries"
authors = ["Hansjörg Herr"]
year = 2018

print("Testing check_if_exists with title/author/year:")
print(f"  Title: {title[:50]}...")
print(f"  Authors: {authors}")
print(f"  Year: {year}")

# Test with all parameters
result = db_manager.check_if_exists(
    doi=None,
    openalex_id=None,
    title=title,
    authors=authors,
    year=year
)

print(f"\nResult: {result}")

# Debug: manually check what's in the database
conn = db_manager.conn
cursor = conn.cursor()

import json
normalized_title = db_manager._normalize_text(title)
normalized_authors = db_manager._normalize_text(json.dumps(authors))

print(f"\nNormalized values:")
print(f"  Title: {normalized_title[:50]}...")
print(f"  Authors: {normalized_authors[:50]}...")

# Check if these exist in downloaded_references
cursor.execute("""
    SELECT id, title, normalized_title, normalized_authors, year 
    FROM downloaded_references 
    WHERE normalized_title = ? AND normalized_authors = ? AND CAST(year AS TEXT) = ?
""", (normalized_title, normalized_authors, str(year)))

row = cursor.fetchone()
if row:
    print(f"\nFound matching entry: ID {row[0]}")
    print(f"  DB normalized_title: {row[2][:50]}...")
    print(f"  DB normalized_authors: {row[3][:50]}...")
else:
    print("\nNo matching entry found")
    
    # Try to find why
    cursor.execute("""
        SELECT id, title, normalized_title, year 
        FROM downloaded_references 
        WHERE normalized_title = ?
    """, (normalized_title,))
    
    title_matches = cursor.fetchall()
    if title_matches:
        print(f"\nFound {len(title_matches)} entries with matching title")
        for match in title_matches[:3]:
            print(f"  ID {match[0]}: year={match[3]}")