#!/usr/bin/env python3
"""Debug normalization issues"""

import sqlite3
import json
from pathlib import Path

db_path = Path("data/literature.db")
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Check the test entry
cursor.execute("""
    SELECT id, title, authors, year, normalized_title, normalized_authors 
    FROM downloaded_references 
    WHERE id = 1
""")
row = cursor.fetchone()

if row:
    id_, title, authors, year, norm_title, norm_authors = row
    print("Entry ID 1:")
    print(f"  Title: {title[:50]}...")
    print(f"  Authors: {authors[:100]}...")
    print(f"  Year: {year}")
    print(f"  Normalized title: {norm_title[:50]}...")
    print(f"  Normalized authors: {norm_authors[:50]}...")
    
    # Test normalization
    import re
    def normalize_text(text):
        if not text:
            return None
        return re.sub(r'[^a-zA-Z0-9]', '', str(text)).upper()
    
    print("\nTest normalization:")
    print(f"  Title normalized: {normalize_text(title)[:50]}...")
    print(f"  Authors normalized: {normalize_text(authors)[:50]}...")
    
    # Check if normalized values match
    print("\nDo they match?")
    print(f"  Title match: {norm_title == normalize_text(title)}")
    print(f"  Authors match: {norm_authors == normalize_text(authors)}")

# Check entry without IDs
cursor.execute("""
    SELECT id, title, authors, year, normalized_title, normalized_authors 
    FROM downloaded_references 
    WHERE (doi IS NULL OR doi = '') 
    AND (openalex_id IS NULL OR openalex_id = '')
    LIMIT 1
""")
row2 = cursor.fetchone()

if row2:
    id2, title2, authors2, year2, norm_title2, norm_authors2 = row2
    print(f"\n\nEntry without IDs (ID {id2}):")
    print(f"  Title: {title2[:50]}...")
    print(f"  Authors: {authors2[:100]}...")
    print(f"  Year: {year2}")
    print(f"  Normalized title: {norm_title2[:50]}...")
    print(f"  Normalized authors: {norm_authors2[:50]}...")

conn.close()