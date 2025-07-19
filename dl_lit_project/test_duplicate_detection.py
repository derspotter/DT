#!/usr/bin/env python3
"""
Diagnostic script to test duplicate detection in dl_lit pipeline.
This will help identify where duplicate detection is failing.
"""

import sys
import sqlite3
from pathlib import Path
from collections import Counter
import json

# Add the project directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

from dl_lit.db_manager import DatabaseManager


def print_section(title):
    """Print a formatted section header."""
    print(f"\n{'='*60}")
    print(f" {title}")
    print('='*60)


def analyze_database_state(db_path):
    """Analyze the current state of the database."""
    print_section("Database State Analysis")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Count entries in each table
    tables = ['no_metadata', 'with_metadata', 'to_download_references', 
              'downloaded_references', 'duplicate_references', 'failed_downloads']
    
    print("\nTable row counts:")
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"  {table:30} {count:,} entries")
    
    # Check for entries without DOI/OpenAlex ID in downloaded_references
    print("\n\nEntries in downloaded_references without identifiers:")
    cursor.execute("""
        SELECT COUNT(*) FROM downloaded_references 
        WHERE (doi IS NULL OR doi = '') 
        AND (openalex_id IS NULL OR openalex_id = '')
    """)
    no_id_count = cursor.fetchone()[0]
    print(f"  Without DOI or OpenAlex ID: {no_id_count:,} entries")
    
    cursor.execute("""
        SELECT COUNT(*) FROM downloaded_references 
        WHERE doi IS NOT NULL AND doi != ''
    """)
    with_doi = cursor.fetchone()[0]
    print(f"  With DOI: {with_doi:,} entries")
    
    cursor.execute("""
        SELECT COUNT(*) FROM downloaded_references 
        WHERE openalex_id IS NOT NULL AND openalex_id != ''
    """)
    with_oaid = cursor.fetchone()[0]
    print(f"  With OpenAlex ID: {with_oaid:,} entries")
    
    # Check for potential duplicates based on title
    print("\n\nPotential duplicates by title in downloaded_references:")
    cursor.execute("""
        SELECT LOWER(title) as norm_title, COUNT(*) as cnt 
        FROM downloaded_references 
        WHERE title IS NOT NULL 
        GROUP BY LOWER(title) 
        HAVING COUNT(*) > 1 
        ORDER BY cnt DESC 
        LIMIT 10
    """)
    
    duplicates = cursor.fetchall()
    if duplicates:
        for title, count in duplicates:
            print(f"  '{title[:50]}...' appears {count} times")
    else:
        print("  No duplicate titles found")
    
    conn.close()


def test_check_if_exists(db_manager):
    """Test the check_if_exists method with various inputs."""
    print_section("Testing check_if_exists Method")
    
    # Get a sample entry from downloaded_references
    conn = db_manager.conn
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT id, doi, openalex_id, title 
        FROM downloaded_references 
        WHERE doi IS NOT NULL 
        LIMIT 1
    """)
    sample = cursor.fetchone()
    
    if sample:
        entry_id, doi, openalex_id, title = sample
        print(f"\nTesting with existing entry:")
        print(f"  ID: {entry_id}")
        print(f"  DOI: {doi}")
        print(f"  OpenAlex ID: {openalex_id}")
        print(f"  Title: {title[:50]}...")
        
        # Test 1: Check with exact DOI
        result = db_manager.check_if_exists(doi=doi, openalex_id=None)
        print(f"\n  Test 1 - Exact DOI lookup:")
        print(f"    Result: {result}")
        
        # Test 2: Check with normalized DOI
        norm_doi = db_manager._normalize_doi(doi) if doi else None
        result = db_manager.check_if_exists(doi=norm_doi, openalex_id=None)
        print(f"\n  Test 2 - Normalized DOI lookup:")
        print(f"    Normalized DOI: {norm_doi}")
        print(f"    Result: {result}")
        
        # Test 3: Check with OpenAlex ID
        if openalex_id:
            result = db_manager.check_if_exists(doi=None, openalex_id=openalex_id)
            print(f"\n  Test 3 - OpenAlex ID lookup:")
            print(f"    Result: {result}")
        
        # Test 4: Check with both
        result = db_manager.check_if_exists(doi=doi, openalex_id=openalex_id)
        print(f"\n  Test 4 - Both DOI and OpenAlex ID:")
        print(f"    Result: {result}")
    else:
        print("\nNo entries with DOI found in downloaded_references!")
    
    # Test with non-existent values
    print("\n\nTesting with non-existent values:")
    result = db_manager.check_if_exists(doi="10.99999/fake-doi-12345", openalex_id="W9999999999")
    print(f"  Result: {result} (should be (None, None, None))")


def test_duplicate_stages(db_manager):
    """Test duplicate detection at each pipeline stage."""
    print_section("Testing Duplicate Detection at Each Stage")
    
    conn = db_manager.conn
    cursor = conn.cursor()
    
    # Get a sample from downloaded_references to use for testing
    cursor.execute("""
        SELECT doi, openalex_id, title, authors, year 
        FROM downloaded_references 
        WHERE doi IS NOT NULL 
        LIMIT 1
    """)
    sample = cursor.fetchone()
    
    if not sample:
        print("No suitable test entry found in downloaded_references!")
        return
    
    doi, openalex_id, title, authors_json, year = sample
    authors = json.loads(authors_json) if authors_json else []
    
    print(f"\nUsing test entry:")
    print(f"  Title: {title[:50]}...")
    print(f"  DOI: {doi}")
    print(f"  OpenAlex ID: {openalex_id}")
    
    # Test 1: Try to insert into no_metadata WITH DOI
    print("\n1. Testing insert_no_metadata WITH DOI:")
    test_entry = {
        'title': title,
        'authors': authors,
        'year': year,
        'doi': doi,
        'source_pdf': 'test.pdf'
    }
    
    row_id, error = db_manager.insert_no_metadata(test_entry)
    if error:
        print(f"   ✓ Correctly rejected: {error}")
    else:
        print(f"   ✗ ERROR: Accepted duplicate! (ID: {row_id})")
        # Clean up
        cursor.execute("DELETE FROM no_metadata WHERE id = ?", (row_id,))
        conn.commit()
    
    # Test 2: Try to insert WITHOUT DOI (title/author/year match)
    print("\n2. Testing insert_no_metadata WITHOUT DOI (title/author/year match):")
    test_entry_no_doi = {
        'title': title,
        'authors': authors,
        'year': year,
        'doi': None,  # No DOI
        'source_pdf': 'test2.pdf'
    }
    
    row_id, error = db_manager.insert_no_metadata(test_entry_no_doi)
    if error and "title_authors_year" in error:
        print(f"   ✓ Correctly rejected by title/author/year: {error}")
    elif error:
        print(f"   ⚠️  Rejected but not by title/author/year: {error}")
    else:
        print(f"   ✗ ERROR: Accepted duplicate without DOI! (ID: {row_id})")
        # Clean up
        cursor.execute("DELETE FROM no_metadata WHERE id = ?", (row_id,))
        conn.commit()
    
    # Test 3: Test add_entry_to_download_queue
    print("\n3. Testing add_entry_to_download_queue:")
    test_data = {
        'title': title,
        'authors': authors,
        'year': year,
        'doi': doi,
        'openalex_id': openalex_id
    }
    
    status, item_id, message = db_manager.add_entry_to_download_queue(test_data)
    print(f"   Status: {status}")
    print(f"   Message: {message}")
    if status == 'duplicate':
        print("   ✓ Correctly detected as duplicate")
    else:
        print("   ✗ ERROR: Failed to detect duplicate!")
        if item_id:
            # Clean up
            cursor.execute("DELETE FROM to_download_references WHERE id = ?", (item_id,))
            conn.commit()
    
    # Test 4: Test title/author/year detection for entry without identifiers
    print("\n4. Testing title/author/year detection:")
    # Find an entry without DOI/OpenAlex ID
    cursor.execute("""
        SELECT title, authors, year 
        FROM downloaded_references 
        WHERE (doi IS NULL OR doi = '') 
        AND (openalex_id IS NULL OR openalex_id = '')
        LIMIT 1
    """)
    no_id_sample = cursor.fetchone()
    
    if no_id_sample:
        title2, authors2_json, year2 = no_id_sample
        authors2 = json.loads(authors2_json) if authors2_json else []
        
        print(f"\n   Testing with entry without IDs:")
        print(f"   Title: {title2[:50]}...")
        print(f"   Year: {year2}")
        
        test_entry_no_id = {
            'title': title2,
            'authors': authors2,
            'year': year2,
            'source_pdf': 'test3.pdf'
        }
        
        row_id, error = db_manager.insert_no_metadata(test_entry_no_id)
        if error and "title_authors_year" in error:
            print(f"   ✓ Correctly detected by title/author/year!")
        else:
            print(f"   ✗ Failed to detect by title/author/year")
            if not error:
                cursor.execute("DELETE FROM no_metadata WHERE id = ?", (row_id,))
                conn.commit()
    else:
        print("\n   No entries without DOI/OpenAlex ID found to test")


def check_duplicate_references_log(db_path):
    """Analyze the duplicate_references table."""
    print_section("Duplicate References Log Analysis")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Count duplicates by source table
    cursor.execute("""
        SELECT existing_entry_table, COUNT(*) as cnt 
        FROM duplicate_references 
        GROUP BY existing_entry_table 
        ORDER BY cnt DESC
    """)
    
    print("\nDuplicates by source table:")
    results = cursor.fetchall()
    if results:
        for table, count in results:
            print(f"  {table:30} {count:,} duplicates")
    else:
        print("  No duplicates logged")
    
    # Recent duplicates
    cursor.execute("""
        SELECT title, existing_entry_table, matched_on_field, date_detected 
        FROM duplicate_references 
        ORDER BY date_detected DESC 
        LIMIT 5
    """)
    
    print("\nMost recent duplicates:")
    recent = cursor.fetchall()
    if recent:
        for title, table, field, date in recent:
            print(f"  {date}: '{title[:40]}...' matched on {field} in {table}")
    else:
        print("  No recent duplicates")
    
    conn.close()


def main():
    """Run all diagnostic tests."""
    # Use default database path or accept as argument
    if len(sys.argv) > 1:
        db_path = Path(sys.argv[1])
    else:
        db_path = Path("data/literature.db")
    
    if not db_path.exists():
        print(f"Error: Database not found at {db_path}")
        print("Usage: python test_duplicate_detection.py [path/to/database.db]")
        sys.exit(1)
    
    print(f"Testing duplicate detection with database: {db_path}")
    
    # Run analyses
    analyze_database_state(db_path)
    
    # Initialize DatabaseManager for method tests
    db_manager = DatabaseManager(db_path)
    
    test_check_if_exists(db_manager)
    test_duplicate_stages(db_manager)
    check_duplicate_references_log(db_path)
    
    print_section("Test Complete")
    print("\nIf duplicate detection is failing, look for:")
    print("  - Entries without DOI/OpenAlex ID in downloaded_references")
    print("  - check_if_exists returning (None, None, None) for known entries")
    print("  - Duplicate entries being accepted at various stages")
    print("  - Empty or sparse duplicate_references log")


if __name__ == "__main__":
    main()