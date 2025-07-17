#!/usr/bin/env python3
"""
Automated PDF Processing Watcher
Continuously monitors the inbox folder and processes new PDFs automatically.
"""

import os
import sys
from pathlib import Path

# Add the project root to Python path so we can import dl_lit
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from dl_lit.pipeline import PipelineOrchestrator

def main():
    """Start the automated PDF processing watcher."""
    
    # Default configuration
    inbox_folder = "inbox"
    check_interval = 5  # seconds
    
    # Processing options
    options = {
        'fetch_references': True,
        'fetch_citations': False,  # Set to True if you want citing works too
        'max_citations': 100,
        'move_on_complete': True,
        'completed_folder': 'completed',
        'failed_folder': 'failed'
    }
    
    # Create inbox folder if it doesn't exist
    Path(inbox_folder).mkdir(parents=True, exist_ok=True)
    Path('completed').mkdir(parents=True, exist_ok=True)
    Path('failed').mkdir(parents=True, exist_ok=True)
    
    print("🔬 DL-LIT Automated PDF Processing Watcher")
    print("=" * 50)
    print(f"📂 Watching folder: {Path(inbox_folder).absolute()}")
    print(f"⏰ Check interval: {check_interval} seconds")
    print(f"📚 Fetch references: {options['fetch_references']}")
    print(f"📖 Fetch citations: {options['fetch_citations']}")
    print(f"✅ Move completed to: completed/")
    print(f"❌ Move failed to: failed/")
    print(f"📦 Downloaded PDFs: pdf_library/")
    print("\n💡 Drop PDF files into the inbox/ folder to process them automatically!")
    print("🛑 Press Ctrl+C to stop watching\n")
    
    try:
        # Initialize the pipeline orchestrator
        orchestrator = PipelineOrchestrator()
        
        # Start watching the folder
        orchestrator.watch_folder(
            folder_path=inbox_folder,
            options=options,
            interval=check_interval
        )
        
    except KeyboardInterrupt:
        print("\n\n👋 Shutting down PDF watcher. Goodbye!")
    except Exception as e:
        print(f"\n❌ Error starting watcher: {e}")
        print("Make sure you're in the virtual environment and have all dependencies installed.")
        sys.exit(1)

if __name__ == "__main__":
    main()