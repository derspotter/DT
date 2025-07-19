#!/usr/bin/env python3
"""
Script to analyze sync between PDF library files and database entries.
This helps identify why PDFs might be re-downloaded.
"""

import sys
import sqlite3
from pathlib import Path
import hashlib
import re

# Add the project directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

from dl_lit.db_manager import DatabaseManager


def calculate_file_hash(file_path):
    """Calculate SHA256 hash of a file."""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


def extract_year_from_filename(filename):
    """Extract year from filename like '2023_Title_of_Paper.pdf'."""
    match = re.match(r'^(\d{4})_', filename)
    return match.group(1) if match else None


def analyze_pdf_library(pdf_dir, db_path):
    """Compare PDF files in library with database entries."""
    print(f"\nAnalyzing PDF library: {pdf_dir}")
    print(f"Database: {db_path}")
    print("="*60)
    
    pdf_dir = Path(pdf_dir)
    if not pdf_dir.exists():
        print(f"Error: PDF directory not found: {pdf_dir}")
        return
    
    # Get all PDF files
    pdf_files = list(pdf_dir.glob("*.pdf"))
    print(f"\nFound {len(pdf_files)} PDF files in library")
    
    # Connect to database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get all entries from downloaded_references
    cursor.execute("""
        SELECT id, title, year, doi, openalex_id, file_path, checksum_pdf 
        FROM downloaded_references
    """)
    db_entries = cursor.fetchall()
    print(f"Found {len(db_entries)} entries in downloaded_references table")
    
    # Create lookup structures
    db_by_filepath = {}
    db_by_checksum = {}
    db_files = set()
    
    for entry in db_entries:
        id_, title, year, doi, openalex_id, file_path, checksum = entry
        if file_path:
            db_files.add(Path(file_path).name)
            db_by_filepath[Path(file_path).name] = entry
        if checksum:
            db_by_checksum[checksum] = entry
    
    # Analyze each PDF file
    print("\n" + "="*60)
    print("ANALYSIS RESULTS")
    print("="*60)
    
    files_in_db = 0
    files_not_in_db = []
    files_with_checksum_mismatch = []
    
    print("\nChecking each PDF file...")
    for pdf_file in pdf_files[:10]:  # Check first 10 for brevity
        filename = pdf_file.name
        file_hash = calculate_file_hash(pdf_file)
        
        status = "❓"
        details = []
        
        # Check if filename exists in database
        if filename in db_files:
            db_entry = db_by_filepath[filename]
            files_in_db += 1
            
            # Check if checksum matches
            if db_entry[6] == file_hash:
                status = "✓"
                details.append("In DB with matching checksum")
            elif db_entry[6] is None:
                status = "⚠️"
                details.append("In DB but no checksum recorded")
            else:
                status = "❌"
                details.append("In DB but checksum mismatch!")
                files_with_checksum_mismatch.append((filename, db_entry))
        else:
            # Check if file exists with same checksum but different name
            if file_hash in db_by_checksum:
                status = "🔄"
                db_entry = db_by_checksum[file_hash]
                details.append(f"Same file in DB as: {Path(db_entry[5]).name if db_entry[5] else 'unknown'}")
            else:
                status = "❌"
                details.append("NOT in database")
                files_not_in_db.append(filename)
        
        print(f"  {status} {filename[:50]:50} {' | '.join(details)}")
    
    if len(pdf_files) > 10:
        print(f"\n  ... and {len(pdf_files) - 10} more files")
    
    # Summary statistics
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    
    # Sample files not in DB for detailed check
    if files_not_in_db:
        print(f"\n❌ {len(files_not_in_db)} files NOT in database")
        print("   Examples:")
        for f in files_not_in_db[:5]:
            year = extract_year_from_filename(f)
            title_part = f[5:-4] if year else f[:-4]  # Remove year prefix and .pdf
            title_part = title_part.replace('_', ' ')
            print(f"     - {f}")
            
            # Try to find similar titles in DB
            cursor.execute("""
                SELECT title, doi, openalex_id 
                FROM downloaded_references 
                WHERE title LIKE ? 
                LIMIT 3
            """, (f"%{title_part[:20]}%",))
            
            similar = cursor.fetchall()
            if similar:
                print(f"       Possible matches in DB:")
                for title, doi, oaid in similar:
                    print(f"         • {title[:50]}... (DOI: {doi or 'None'})")
    
    # Check for DB entries without files
    print(f"\n📊 Database entries with file paths: {len([e for e in db_entries if e[5]])}")
    
    cursor.execute("""
        SELECT COUNT(*) FROM downloaded_references 
        WHERE file_path IS NULL OR file_path = ''
    """)
    no_filepath = cursor.fetchone()[0]
    print(f"📊 Database entries WITHOUT file paths: {no_filepath}")
    
    # Check how many DB entries point to non-existent files
    missing_files = 0
    for entry in db_entries:
        if entry[5]:  # Has file_path
            if not Path(entry[5]).exists():
                missing_files += 1
    
    print(f"📊 Database entries pointing to non-existent files: {missing_files}")
    
    conn.close()
    
    print("\n" + "="*60)
    print("LIKELY ISSUES")
    print("="*60)
    
    if files_not_in_db:
        print("\n1. Many PDF files are not recorded in the database")
        print("   This explains why they get re-downloaded!")
        print("   Possible causes:")
        print("   - Files were downloaded before database tracking was implemented")
        print("   - Database was cleared/reset but PDF files remained")
        print("   - Download process failed to record entries in downloaded_references")
    
    if missing_files > 0:
        print(f"\n2. {missing_files} database entries point to non-existent files")
        print("   The database thinks files exist but they don't")
    
    if no_filepath > 0:
        print(f"\n3. {no_filepath} database entries have no file_path")
        print("   These entries can't be matched to PDF files")


def main():
    """Run the PDF library sync analysis."""
    # Default paths
    pdf_dir = Path("pdf_library")
    db_path = Path("data/literature.db")
    
    # Accept custom paths as arguments
    if len(sys.argv) > 1:
        pdf_dir = Path(sys.argv[1])
    if len(sys.argv) > 2:
        db_path = Path(sys.argv[2])
    
    if not db_path.exists():
        print(f"Error: Database not found at {db_path}")
        print("Usage: python test_pdf_library_sync.py [pdf_dir] [database.db]")
        sys.exit(1)
    
    analyze_pdf_library(pdf_dir, db_path)


if __name__ == "__main__":
    main()