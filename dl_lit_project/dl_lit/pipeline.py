"""
Automated PDF processing pipeline orchestrator.
Coordinates all steps from PDF input to downloaded papers.
"""

import os
import time
import shutil
import traceback
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import json

from .db_manager import DatabaseManager
from .get_bib_pages import extract_reference_sections
from .APIscraper_v2 import process_single_pdf
from .OpenAlexScraper import OpenAlexCrossrefSearcher
from .new_dl import BibliographyEnhancer
from .utils import get_global_rate_limiter

# ANSI color codes for console output
RESET = "\033[0m"
RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
CYAN = "\033[96m"


class ProgressReporter:
    """Simple progress reporter for console output."""
    
    def __init__(self, pdf_name: str):
        self.pdf_name = Path(pdf_name).name
        self.start_time = time.time()
        self.steps_completed = []
        
    def update(self, step: str, status: str, details: str = ""):
        """Update progress with current step status."""
        elapsed = time.time() - self.start_time
        
        # Color code based on status
        if status.lower() == "success":
            color = GREEN
            symbol = "✓"
        elif status.lower() == "failed":
            color = RED
            symbol = "✗"
        elif status.lower() == "processing":
            color = YELLOW
            symbol = "→"
        else:
            color = CYAN
            symbol = "•"
            
        print(f"{color}[{elapsed:5.1f}s] {symbol} {self.pdf_name} - {step}: {status} {details}{RESET}")
        
        if status.lower() in ["success", "failed"]:
            self.steps_completed.append((step, status, details))
    
    def summary(self) -> Dict:
        """Return summary of processing."""
        total_time = time.time() - self.start_time
        successful_steps = sum(1 for _, status, _ in self.steps_completed if status.lower() == "success")
        failed_steps = sum(1 for _, status, _ in self.steps_completed if status.lower() == "failed")
        
        return {
            "pdf_name": self.pdf_name,
            "total_time": total_time,
            "successful_steps": successful_steps,
            "failed_steps": failed_steps,
            "steps": self.steps_completed
        }


class PipelineOrchestrator:
    """Orchestrates the complete PDF processing pipeline."""
    
    def __init__(self, db_path: Optional[str] = None, output_folder: Optional[str] = None):
        """
        Initialize the pipeline orchestrator.
        
        Args:
            db_path: Path to SQLite database (uses default if None)
            output_folder: Folder for downloaded PDFs (uses default if None)
        """
        self.db_manager = DatabaseManager(db_path)
        self.rate_limiter = get_global_rate_limiter()
        self.output_folder = output_folder or "pdf_library"
        
        # Ensure output folder exists
        Path(self.output_folder).mkdir(parents=True, exist_ok=True)
        
    def process_pdf_complete(self, pdf_path: str, options: Optional[Dict] = None) -> Dict:
        """
        Process a single PDF through the entire pipeline.
        
        Args:
            pdf_path: Path to the PDF file
            options: Dictionary of options:
                - fetch_references: bool (default True)
                - fetch_citations: bool (default False)
                - max_citations: int (default 100)
                - move_on_complete: bool (default False)
                - completed_folder: str (default "completed")
                - failed_folder: str (default "failed")
                
        Returns:
            Dictionary with processing results and statistics
        """
        # Default options
        options = options or {}
        fetch_references = options.get('fetch_references', True)
        fetch_citations = options.get('fetch_citations', False)
        max_citations = options.get('max_citations', 100)
        move_on_complete = options.get('move_on_complete', False)
        completed_folder = options.get('completed_folder', 'completed')
        failed_folder = options.get('failed_folder', 'failed')
        
        # Initialize progress reporter
        progress = ProgressReporter(pdf_path)
        
        # Processing statistics
        stats = {
            'pdf_path': pdf_path,
            'start_time': datetime.now().isoformat(),
            'success': False,
            'extracted_refs': 0,
            'parsed_refs': 0,
            'enriched_refs': 0,
            'queued_downloads': 0,
            'successful_downloads': 0,
            'failed_downloads': 0,
            'errors': []
        }
        
        try:
            # Step 1: Extract bibliography pages
            progress.update("Extract Bibliography", "processing")
            extracted_refs_dir = Path("data/extracted_refs")
            extracted_refs_dir.mkdir(parents=True, exist_ok=True)
            
            try:
                extract_reference_sections(pdf_path, str(extracted_refs_dir))
                extracted_files = list(extracted_refs_dir.glob(f"{Path(pdf_path).stem}_refs_*.pdf"))
                stats['extracted_refs'] = len(extracted_files)
                progress.update("Extract Bibliography", "success", f"{len(extracted_files)} pages")
            except Exception as e:
                progress.update("Extract Bibliography", "failed", str(e))
                stats['errors'].append(f"Bibliography extraction: {str(e)}")
                raise
            
            # Step 2: Parse references to database
            progress.update("Parse References", "processing")
            try:
                import threading
                
                # Process the extracted reference pages
                cursor = self.db_manager.conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM no_metadata")
                initial_count = cursor.fetchone()[0]
                
                # Create dummy summary dict and lock for the function call
                summary_dict = {}
                summary_lock = threading.Lock()
                
                process_single_pdf(str(extracted_refs_dir), self.db_manager, summary_dict, summary_lock)
                
                cursor.execute("SELECT COUNT(*) FROM no_metadata")
                final_count = cursor.fetchone()[0]
                
                parsed_refs = final_count - initial_count
                stats['parsed_refs'] = parsed_refs
                progress.update("Parse References", "success", f"{parsed_refs} references")
            except Exception as e:
                progress.update("Parse References", "failed", str(e))
                stats['errors'].append(f"Reference parsing: {str(e)}")
                
            # Step 3: Enrich metadata
            progress.update("Enrich Metadata", "processing")
            try:
                from .OpenAlexScraper import process_single_reference, OpenAlexCrossrefSearcher
                
                # Initialize searcher
                searcher = OpenAlexCrossrefSearcher()
                
                # Get unprocessed entries
                cursor = self.db_manager.conn.cursor()
                cursor.execute("SELECT * FROM no_metadata ORDER BY date_added DESC LIMIT 100")
                entries = cursor.fetchall()
                
                enriched_count = 0
                for entry in entries:
                    try:
                        # Convert row to dict format
                        entry_dict = {
                            'id': entry[0],
                            'title': entry[3] if len(entry) > 3 else '',
                            'authors': entry[4] if len(entry) > 4 else '[]',
                            'year': entry[5] if len(entry) > 5 else None
                        }
                        
                        result = process_single_reference(
                            entry_dict, 
                            searcher,
                            self.rate_limiter,
                            fetch_references=fetch_references,
                            fetch_citations=fetch_citations,
                            max_citations=max_citations
                        )
                        if result:
                            enriched_count += 1
                    except Exception as e:
                        print(f"Error enriching entry {entry[0]}: {e}")
                        
                stats['enriched_refs'] = enriched_count
                progress.update("Enrich Metadata", "success", f"{enriched_count} references")
            except Exception as e:
                progress.update("Enrich Metadata", "failed", str(e))
                stats['errors'].append(f"Metadata enrichment: {str(e)}")
                
            # Step 4: Queue for download
            progress.update("Queue Downloads", "processing")
            try:
                # Move enriched entries to download queue
                cursor = self.db_manager.conn.cursor()
                cursor.execute("SELECT * FROM with_metadata LIMIT 100")
                with_meta_entries = cursor.fetchall()
                
                queued = 0
                for entry in with_meta_entries:
                    try:
                        result = self.db_manager.enqueue_for_download(entry[0])
                        if result[0]:  # If successful
                            queued += 1
                    except Exception as e:
                        print(f"Error queueing entry {entry[0]}: {e}")
                
                stats['queued_downloads'] = queued
                progress.update("Queue Downloads", "success", f"{queued} papers")
            except Exception as e:
                progress.update("Queue Downloads", "failed", str(e))
                stats['errors'].append(f"Download queueing: {str(e)}")
                
            # Step 5: Download PDFs
            progress.update("Download PDFs", "processing")
            try:
                enhancer = BibliographyEnhancer(
                    db_manager=self.db_manager,
                    rate_limiter=self.rate_limiter,
                    output_folder=self.output_folder
                )
                
                # Get entries from download queue
                download_entries = self.db_manager.get_entries_to_download(limit=50)
                
                successful = 0
                failed = 0
                
                for entry in download_entries:
                    try:
                        # Convert entry to dictionary format expected by enhancer
                        ref = {
                            'id': entry['id'],
                            'title': entry['title'],
                            'authors': json.loads(entry['authors']) if entry['authors'] else [],
                            'year': entry['year'],
                            'doi': entry['doi'],
                            'openalex_id': entry.get('openalex_id'),
                            'openalex_json': entry.get('openalex_json')
                        }
                        
                        result = enhancer.enhance_bibliography(ref, Path(self.output_folder))
                        
                        if result and 'downloaded_file' in result:
                            # Move to downloaded_references using existing method
                            self.db_manager.move_queue_entry_to_downloaded(
                                entry['id'], result, result['downloaded_file'], ''
                            )
                            successful += 1
                        else:
                            # Move to failed_downloads using existing method
                            self.db_manager.move_queue_entry_to_failed(
                                entry['id'], 'download_failed'
                            )
                            failed += 1
                            
                    except Exception as e:
                        print(f"Error downloading entry {entry['id']}: {e}")
                        failed += 1
                        
                stats['successful_downloads'] = successful
                stats['failed_downloads'] = failed
                progress.update("Download PDFs", "success", 
                              f"{successful} downloaded, {failed} failed")
                
            except Exception as e:
                progress.update("Download PDFs", "failed", str(e))
                stats['errors'].append(f"PDF downloads: {str(e)}")
                
            # Mark as successful if we got through all steps
            stats['success'] = True
            
        except Exception as e:
            stats['errors'].append(f"Pipeline error: {str(e)}")
            traceback.print_exc()
            
        finally:
            # Clean up extracted reference pages
            try:
                for ref_file in extracted_refs_dir.glob(f"{Path(pdf_path).stem}_refs_*.pdf"):
                    ref_file.unlink()
            except:
                pass
                
            # Move PDF to completed or failed folder if requested
            if move_on_complete:
                try:
                    source = Path(pdf_path)
                    if stats['success'] and stats['errors'] == []:
                        dest_folder = Path(completed_folder)
                    else:
                        dest_folder = Path(failed_folder)
                        
                    dest_folder.mkdir(parents=True, exist_ok=True)
                    dest = dest_folder / source.name
                    
                    # Handle duplicate filenames
                    if dest.exists():
                        base = dest.stem
                        ext = dest.suffix
                        counter = 1
                        while dest.exists():
                            dest = dest_folder / f"{base}_{counter}{ext}"
                            counter += 1
                            
                    shutil.move(str(source), str(dest))
                    stats['final_location'] = str(dest)
                    
                except Exception as e:
                    stats['errors'].append(f"File move error: {str(e)}")
        
        # Add summary
        stats['end_time'] = datetime.now().isoformat()
        stats['processing_summary'] = progress.summary()
        
        return stats
    
    def process_folder(self, folder_path: str, options: Optional[Dict] = None) -> List[Dict]:
        """
        Process all PDFs in a folder.
        
        Args:
            folder_path: Path to folder containing PDFs
            options: Processing options (passed to process_pdf_complete)
            
        Returns:
            List of processing results for each PDF
        """
        folder = Path(folder_path)
        if not folder.exists():
            raise ValueError(f"Folder does not exist: {folder_path}")
            
        pdf_files = list(folder.glob("*.pdf"))
        if not pdf_files:
            print(f"No PDF files found in {folder_path}")
            return []
            
        print(f"\nFound {len(pdf_files)} PDF files to process")
        results = []
        
        for i, pdf_file in enumerate(pdf_files, 1):
            print(f"\n{'='*60}")
            print(f"Processing {i}/{len(pdf_files)}: {pdf_file.name}")
            print(f"{'='*60}")
            
            result = self.process_pdf_complete(str(pdf_file), options)
            results.append(result)
            
            # Brief pause between PDFs to avoid overwhelming APIs
            if i < len(pdf_files):
                time.sleep(2)
                
        return results
    
    def watch_folder(self, folder_path: str, options: Optional[Dict] = None, 
                     interval: int = 5) -> None:
        """
        Watch a folder and process new PDFs as they appear.
        
        Args:
            folder_path: Path to folder to watch
            options: Processing options (passed to process_pdf_complete)
            interval: Check interval in seconds
        """
        folder = Path(folder_path)
        folder.mkdir(parents=True, exist_ok=True)
        
        # Keep track of processed files
        processed_files = set()
        
        # Add move_on_complete to options
        if options is None:
            options = {}
        options['move_on_complete'] = True
        
        print(f"\nWatching folder: {folder_path}")
        print(f"Check interval: {interval} seconds")
        print("Press Ctrl+C to stop...\n")
        
        try:
            while True:
                # Get current PDF files
                current_files = set(folder.glob("*.pdf"))
                
                # Find new files
                new_files = current_files - processed_files
                
                for pdf_file in new_files:
                    print(f"\nNew PDF detected: {pdf_file.name}")
                    
                    # Process the PDF
                    result = self.process_pdf_complete(str(pdf_file), options)
                    
                    # Mark as processed
                    processed_files.add(pdf_file)
                    
                    # Print summary
                    if result['success']:
                        print(f"{GREEN}✓ Successfully processed: {pdf_file.name}{RESET}")
                    else:
                        print(f"{RED}✗ Failed to process: {pdf_file.name}{RESET}")
                        if result['errors']:
                            print(f"  Errors: {'; '.join(result['errors'])}")
                
                # Wait before next check
                time.sleep(interval)
                
        except KeyboardInterrupt:
            print("\n\nStopping folder watch...")
            print(f"Processed {len(processed_files)} files total.")