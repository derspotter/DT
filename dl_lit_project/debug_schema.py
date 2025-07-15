#!/usr/bin/env python3
"""Debug script to check actual table schemas."""

import sqlite3
from pathlib import Path

def debug_schema():
    db_path = Path("data/literature.db")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    tables = ["with_metadata", "to_download_references", "no_metadata"]
    
    for table in tables:
        print(f"=== {table.upper()} TABLE SCHEMA ===")
        cursor.execute(f"PRAGMA table_info({table})")
        columns = cursor.fetchall()
        for col in columns:
            print(f"  {col[1]} {col[2]} (pk={col[5]}, nn={col[3]}, default={col[4]})")
        print()
    
    conn.close()

if __name__ == "__main__":
    debug_schema()