#!/usr/bin/env python3
"""
Simple script to inspect the contents of the literature database tables.
This helps debug data flow issues between enrichment stages.
"""

import sqlite3
import json
from pathlib import Path
from collections import defaultdict

def inspect_database(db_path="data/literature.db"):
    """Inspect all tables in the literature database and show their contents."""
    
    db_file = Path(db_path)
    if not db_file.exists():
        print(f"❌ Database file not found: {db_file.resolve()}")
        return
    
    print(f"🔍 Inspecting database: {db_file.resolve()}")
    print("=" * 80)
    
    conn = sqlite3.connect(db_file)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # Get all table names
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [row[0] for row in cursor.fetchall()]
    
    print(f"📊 Found {len(tables)} tables:")
    for table in tables:
        print(f"  • {table}")
    print()
    
    # Inspect each table
    table_counts = {}
    
    for table_name in tables:
        print(f"🔍 Table: {table_name}")
        print("-" * 40)
        
        # Get row count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        table_counts[table_name] = count
        
        if count == 0:
            print(f"  📭 Empty (0 rows)")
        else:
            print(f"  📈 Contains {count} rows")
            
            # Get column info
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            col_names = [col[1] for col in columns]
            
            # Show first few rows
            limit = min(3, count)
            cursor.execute(f"SELECT * FROM {table_name} LIMIT {limit}")
            sample_rows = cursor.fetchall()
            
            print(f"  📋 Columns: {', '.join(col_names)}")
            print(f"  📄 Sample rows (showing {limit} of {count}):")
            
            for i, row in enumerate(sample_rows, 1):
                print(f"    Row {i}:")
                for col_name in col_names:
                    value = row[col_name]
                    # Truncate long values
                    if isinstance(value, str) and len(value) > 50:
                        value = value[:47] + "..."
                    print(f"      {col_name}: {value}")
                print()
        
        print()
    
    # Summary
    print("=" * 80)
    print("📊 SUMMARY:")
    print("-" * 40)
    
    workflow_tables = [
        ("no_metadata", "Raw extracted references (before enrichment)"),
        ("with_metadata", "Enriched references (after OpenAlex/Crossref lookup)"),
        ("to_download_references", "Queue for PDF downloads"),
        ("downloaded_references", "Successfully downloaded papers"),
        ("failed_enrichments", "References that failed metadata enrichment"),
        ("failed_downloads", "References that failed PDF download"),
        ("duplicate_references", "Detected duplicates")
    ]
    
    total_items = 0
    for table_name, description in workflow_tables:
        count = table_counts.get(table_name, 0)
        total_items += count
        status = "✅" if count > 0 else "📭"
        print(f"  {status} {table_name:25} {count:6} rows - {description}")
    
    print(f"\n📊 Total items across workflow: {total_items}")
    
    # Check for potential issues
    print("\n🔍 POTENTIAL ISSUES:")
    print("-" * 40)
    
    issues_found = False
    
    # Issue 1: Items stuck in no_metadata
    no_meta_count = table_counts.get("no_metadata", 0)
    with_meta_count = table_counts.get("with_metadata", 0)
    if no_meta_count > 0 and with_meta_count == 0:
        print(f"  ⚠️  {no_meta_count} items in no_metadata but none promoted to with_metadata")
        print(f"      → Enrichment may not be working properly")
        issues_found = True
    
    # Issue 2: Items stuck in with_metadata 
    queue_count = table_counts.get("to_download_references", 0)
    if with_meta_count > 0 and queue_count == 0:
        print(f"  ⚠️  {with_meta_count} items in with_metadata but none in download queue")
        print(f"      → process-downloads command may not be working")
        issues_found = True
    
    # Issue 3: High failure rates
    failed_enrich = table_counts.get("failed_enrichments", 0)
    failed_downloads = table_counts.get("failed_downloads", 0)
    success_rate_enrich = 0
    success_rate_download = 0
    
    if no_meta_count + with_meta_count + failed_enrich > 0:
        success_rate_enrich = (with_meta_count / (no_meta_count + with_meta_count + failed_enrich)) * 100
    
    if queue_count + table_counts.get("downloaded_references", 0) + failed_downloads > 0:
        success_rate_download = (table_counts.get("downloaded_references", 0) / 
                                (queue_count + table_counts.get("downloaded_references", 0) + failed_downloads)) * 100
    
    if success_rate_enrich < 50 and (no_meta_count + with_meta_count + failed_enrich) > 5:
        print(f"  ⚠️  Low enrichment success rate: {success_rate_enrich:.1f}%")
        print(f"      → Check failed_enrichments table for common failure reasons")
        issues_found = True
    
    if success_rate_download < 50 and (queue_count + table_counts.get("downloaded_references", 0) + failed_downloads) > 5:
        print(f"  ⚠️  Low download success rate: {success_rate_download:.1f}%")
        print(f"      → Check failed_downloads table for common failure reasons")
        issues_found = True
    
    if not issues_found:
        print("  ✅ No obvious issues detected")
    
    conn.close()

if __name__ == "__main__":
    inspect_database()