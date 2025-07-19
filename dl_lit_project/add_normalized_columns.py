#!/usr/bin/env python3
"""
Script to add normalized_title and normalized_authors columns to existing tables.
This is a one-time migration to enable title/author/year duplicate detection.
"""

import sys
import sqlite3
from pathlib import Path
import re


def normalize_text(text):
    """Normalize text by removing non-alphanumeric characters and converting to uppercase."""
    if not text:
        return None
    # Remove all non-alphanumeric characters and convert to uppercase
    return re.sub(r'[^a-zA-Z0-9]', '', str(text)).upper()


def add_normalized_columns(db_path):
    """Add normalized columns to existing tables and populate them."""
    print(f"Adding normalized columns to database: {db_path}")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # List of tables that need normalized columns
    tables_to_update = [
        'downloaded_references',
        'with_metadata', 
        'no_metadata',
        'to_download_references'
    ]
    
    for table in tables_to_update:
        print(f"\nProcessing table: {table}")
        
        # Check which columns exist
        cursor.execute(f"PRAGMA table_info({table})")
        existing_columns = [col[1] for col in cursor.fetchall()]
        
        # Add normalized_title if missing
        if 'normalized_title' not in existing_columns:
            try:
                cursor.execute(f"ALTER TABLE {table} ADD COLUMN normalized_title TEXT")
                print(f"  ✓ Added normalized_title column")
                
                # Populate normalized_title from existing title
                cursor.execute(f"""
                    UPDATE {table} 
                    SET normalized_title = LOWER(
                        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                            title, ' ', ''), '-', ''), '.', ''), ',', ''), ':', '')
                    )
                    WHERE normalized_title IS NULL AND title IS NOT NULL
                """)
                print(f"  ✓ Populated normalized_title values")
            except sqlite3.Error as e:
                print(f"  ⚠️  Could not add normalized_title: {e}")
        else:
            print(f"  - normalized_title already exists")
        
        # Add normalized_authors if missing
        if 'normalized_authors' not in existing_columns:
            try:
                cursor.execute(f"ALTER TABLE {table} ADD COLUMN normalized_authors TEXT")
                print(f"  ✓ Added normalized_authors column")
                
                # Populate normalized_authors from existing authors
                # This is more complex since authors is a JSON field
                cursor.execute(f"SELECT id, authors FROM {table} WHERE authors IS NOT NULL")
                rows = cursor.fetchall()
                
                for row_id, authors_json in rows:
                    if authors_json:
                        normalized = normalize_text(authors_json)
                        cursor.execute(
                            f"UPDATE {table} SET normalized_authors = ? WHERE id = ?",
                            (normalized, row_id)
                        )
                
                print(f"  ✓ Populated normalized_authors values ({len(rows)} rows)")
            except sqlite3.Error as e:
                print(f"  ⚠️  Could not add normalized_authors: {e}")
        else:
            print(f"  - normalized_authors already exists")
    
    # Create indexes
    print("\nCreating indexes...")
    indexes = [
        ("idx_downloaded_title_year", "downloaded_references", "normalized_title, year"),
        ("idx_downloaded_authors", "downloaded_references", "normalized_authors"),
        ("idx_with_metadata_title_year", "with_metadata", "normalized_title, year"),
        ("idx_with_metadata_authors", "with_metadata", "normalized_authors"),
        ("idx_no_metadata_title_year", "no_metadata", "normalized_title, year"),
        ("idx_no_metadata_authors", "no_metadata", "normalized_authors"),
        ("idx_to_download_title_year", "to_download_references", "normalized_title, year"),
        ("idx_to_download_authors", "to_download_references", "normalized_authors"),
    ]
    
    for idx_name, table, columns in indexes:
        try:
            cursor.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table}({columns})")
            print(f"  ✓ Created index {idx_name}")
        except sqlite3.Error as e:
            print(f"  ⚠️  Could not create index {idx_name}: {e}")
    
    conn.commit()
    conn.close()
    print("\n✓ Migration complete!")


def main():
    """Run the migration."""
    if len(sys.argv) > 1:
        db_path = Path(sys.argv[1])
    else:
        db_path = Path("data/literature.db")
    
    if not db_path.exists():
        print(f"Error: Database not found at {db_path}")
        print("Usage: python add_normalized_columns.py [path/to/database.db]")
        sys.exit(1)
    
    add_normalized_columns(db_path)


if __name__ == "__main__":
    main()