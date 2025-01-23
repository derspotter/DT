import json
from pathlib import Path

def calculate_download_stats(directory: str) -> None:
    """Calculate aggregate download statistics for all bibliography JSON files in a directory."""
    directory = Path(directory)
    total_refs = 0
    downloaded_refs = 0
    
    # Find all JSON files recursively
    json_files = directory.glob('**/*.json')
    
    # Process each file
    for file_path in json_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Handle both array and object formats
            references = data['references'] if isinstance(data, dict) else data
            
            # Count references and successful downloads
            total_refs += len(references)
            downloaded_refs += sum(1 for ref in references if 'downloaded_file' in ref)
                
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
            continue
    
    # Calculate and print statistics
    success_rate = (downloaded_refs / total_refs * 100) if total_refs > 0 else 0
    print("\nDownload Statistics:")
    print(f"Total references: {total_refs:,}")
    print(f"Successfully downloaded: {downloaded_refs:,}")
    print(f"Success rate: {success_rate:.1f}%")

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python script.py /path/to/bibliography/files")
        sys.exit(1)
    
    calculate_download_stats(sys.argv[1])