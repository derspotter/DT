import sqlite3
import json
import sys

def inspect_failed_downloads(db_path):
    """Connects to the SQLite DB and prints the contents of the failed_downloads table."""
    print(f"--- Inspecting failed_downloads in {db_path} ---")
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Check if table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='failed_downloads';")
        if cursor.fetchone() is None:
            print("Table 'failed_downloads' does not exist.")
            return

        cursor.execute("SELECT * FROM failed_downloads;")
        rows = cursor.fetchall()

        if not rows:
            print("Table 'failed_downloads' is empty.")
        else:
            print(f"Found {len(rows)} entries in failed_downloads:")
            for i, row in enumerate(rows):
                print(f"\n--- Entry {i+1} ---")
                row_dict = dict(row)
                # Pretty print authors if it's a JSON string
                if 'authors' in row_dict and row_dict['authors']:
                    try:
                        authors_data = json.loads(row_dict['authors'])
                        row_dict['authors'] = authors_data
                    except (json.JSONDecodeError, TypeError):
                        pass # keep as string if not valid json
                print(json.dumps(row_dict, indent=2))

    except sqlite3.Error as e:
        print(f"Database error: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        db_path = sys.argv[1]
    else:
        db_path = 'data/literature.db' # Default path
    
    inspect_failed_downloads(db_path)
