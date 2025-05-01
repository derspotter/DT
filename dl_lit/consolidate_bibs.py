import os
import json
import argparse
from pathlib import Path

def consolidate_bibliographies(input_dir, output_file):
    """Scans input_dir for *_bibliography.json files, consolidates their
    contents (assuming they are lists of JSON objects), and writes the
    result to output_file.
    """
    consolidated_entries = []
    input_path = Path(input_dir)
    output_path = Path(output_file)

    print(f"Scanning directory: {input_path}")

    if not input_path.is_dir():
        print(f"Error: Input directory not found: {input_path}")
        return False

    file_count = 0
    for root, _, files in os.walk(input_path):
        for filename in files:
            if filename.endswith('_bibliography.json') and filename != output_path.name:
                file_path = Path(root) / filename
                file_count += 1
                print(f"Processing file: {file_path}")
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read().strip()
                        if not content:
                            print(f"  Skipping empty file: {file_path}")
                            continue
                        data = json.loads(content)
                        if isinstance(data, list):
                            consolidated_entries.extend(data)
                            print(f"  Added {len(data)} entries.")
                        else:
                            print(f"  Warning: File does not contain a JSON list: {file_path}")
                except json.JSONDecodeError:
                    print(f"  Error: Invalid JSON in file: {file_path}")
                except Exception as e:
                    print(f"  Error reading file {file_path}: {e}")

    print(f"\nProcessed {file_count} files.")
    print(f"Total entries before deduplication: {len(consolidated_entries)}")

    # --- Deduplication based on entire object --- 
    unique_entries = []
    seen_entries = set()

    for entry in consolidated_entries:
        # Convert dict to a canonical string representation for hashing/comparison
        # Sorting keys ensures order doesn't matter {a:1, b:2} == {b:2, a:1}
        entry_str = json.dumps(entry, sort_keys=True)
        if entry_str not in seen_entries:
            unique_entries.append(entry)
            seen_entries.add(entry_str)

    print(f"Total unique entries after deduplication: {len(unique_entries)}")

    try:
        # Ensure the output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(unique_entries, f, indent=2, ensure_ascii=False) # Save unique entries
        print(f"Successfully wrote consolidated bibliography to: {output_path}")
        return True
    except Exception as e:
        print(f"Error writing output file {output_path}: {e}")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Consolidate bibliography JSON files.')
    parser.add_argument('input_dir', type=str, help='Directory containing the bibliography JSON files.')
    parser.add_argument('output_file', type=str, help='Path for the consolidated output JSON file.')
    args = parser.parse_args()

    if consolidate_bibliographies(args.input_dir, args.output_file):
        print("Consolidation successful.")
    else:
        print("Consolidation failed.")
        exit(1)
