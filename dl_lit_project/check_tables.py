#!/usr/bin/env python3
"""Simple script to check table contents directly."""

import sqlite3
from pathlib import Path

def check_table_contents():
    db_path = Path("data/literature.db")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # Check each table individually
    tables = ["no_metadata", "with_metadata", "to_download_references", "downloaded_references"]
    
    for table in tables:
        print(f"\n=== {table.upper()} ===")
        cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
        count = cursor.fetchone()[0]
        print(f"Row count: {count}")
        
        if count > 0:
            cursor.execute(f"SELECT * FROM {table} LIMIT 3")
            rows = cursor.fetchall()
            for i, row in enumerate(rows, 1):
                print(f"\nRow {i}:")
                for key in row.keys():
                    value = row[key]
                    if isinstance(value, str) and len(value) > 100:
                        value = value[:97] + "..."
                    print(f"  {key}: {value}")
    
    conn.close()

if __name__ == "__main__":
    check_table_contents()