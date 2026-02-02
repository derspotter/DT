"""
Automated PDF processing pipeline orchestrator.
Coordinates all steps from PDF input to downloaded papers.
"""

import os
import time
import shutil
import traceback
import sqlite3
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import json

from .db_manager import DatabaseManager
from .get_bib_pages import extract_reference_sections
from .APIscraper_v2 import process_single_pdf, configure_api_client, upload_pdf_and_extract_bibliography
from .OpenAlexScraper import OpenAlexCrossrefSearcher, process_single_reference
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
                - related_depth: int (default 2)
                - max_related: int (default 40)
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
        related_depth = options.get('related_depth', 2)
        max_related = options.get('max_related', 40)
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

                for ref_pdf in extracted_files:
                    process_single_pdf(str(ref_pdf), self.db_manager, summary_dict, summary_lock)

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
                            'title': entry[2] if len(entry) > 2 else '',
                            'authors': entry[3] if len(entry) > 3 else '[]',
                            'year': entry[5] if len(entry) > 5 else None
                        }
                        
                        result = process_single_reference(
                            entry_dict, 
                            searcher,
                            self.rate_limiter,
                            fetch_references=fetch_references,
                            fetch_citations=fetch_citations,
                            max_citations=max_citations,
                            related_depth=related_depth,
                            max_related_per_reference=max_related,
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


def run_pipeline(
    input_path: str | Path,
    db_path: str | Path,
    output_dir: str | Path,
    json_dir: str | Path | None = None,
    batch_size: int = 50,
    queue_batch: int = 50,
    mailto: str = "spott@wzb.eu",
    fetch_references: bool = True,
    fetch_citations: bool = False,
    max_citations: int = 100,
    related_depth: int = 2,
    max_related: int = 40,
    max_ref_pages: int | None = None,
    max_entries: int | None = None,
    run_enrichment: bool = True,
) -> dict:
    """Run extract → enrich → queue pipeline for PDFs (no download step)."""
    input_path = Path(input_path)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    json_dir = Path(json_dir) if json_dir else output_dir / "bib_jsons"
    json_dir.mkdir(parents=True, exist_ok=True)

    if not configure_api_client():
        raise RuntimeError("API client configuration failed. Check GOOGLE_API_KEY.")

    db_manager = DatabaseManager(db_path=db_path)
    rate_limiter = get_global_rate_limiter()
    searcher = OpenAlexCrossrefSearcher(mailto=mailto, rate_limiter=rate_limiter)

    run_id = None
    summary = {
        "processed_files": 0,
        "extracted_pages": 0,
        "extracted_entries": 0,
        "inserted_entries": 0,
        "skipped_entries": 0,
        "enriched_promoted": 0,
        "enriched_failed": 0,
        "queued": 0,
        "skipped_queue": 0,
        "errors": [],
    }

    def _chunked(items: list[int], size: int) -> list[list[int]]:
        if size <= 0:
            return [items]
        return [items[i:i + size] for i in range(0, len(items), size)]

    try:
        params = {
            "output_dir": str(output_dir),
            "json_dir": str(json_dir),
            "batch_size": batch_size,
            "queue_batch": queue_batch,
            "mailto": mailto,
            "fetch_references": fetch_references,
            "fetch_citations": fetch_citations,
            "max_citations": max_citations,
            "related_depth": related_depth,
            "max_related": max_related,
            "max_ref_pages": max_ref_pages,
            "max_entries": max_entries,
            "run_enrichment": run_enrichment,
        }
        cur = db_manager.conn.cursor()
        cur.execute(
            "INSERT INTO pipeline_runs (input_path, parameters_json, status) VALUES (?, ?, ?)",
            (str(input_path), json.dumps(params), "running"),
        )
        run_id = cur.lastrowid
        db_manager.conn.commit()

        if input_path.is_dir():
            pdf_files = list(input_path.glob("**/*.pdf"))
        else:
            pdf_files = [input_path]

        if not pdf_files:
            return summary

        inserted_ids: list[int] = []
        with_metadata_ids: list[int] = []

        for pdf_file in pdf_files:
            if max_entries and summary["inserted_entries"] >= max_entries:
                break
            summary["processed_files"] += 1
            extract_reference_sections(str(pdf_file), str(output_dir))
            extracted_pages = sorted(output_dir.glob(f"{pdf_file.stem}_refs_*.pdf"))
            if max_ref_pages and max_ref_pages > 0:
                extracted_pages = extracted_pages[:max_ref_pages]
            summary["extracted_pages"] += len(extracted_pages)

            for ref_pdf in extracted_pages:
                if max_entries and summary["inserted_entries"] >= max_entries:
                    break
                entries = upload_pdf_and_extract_bibliography(str(ref_pdf))
                if not isinstance(entries, list):
                    entries = []

                summary["extracted_entries"] += len(entries)

                json_path = json_dir / f"{ref_pdf.stem}.json"
                with open(json_path, "w", encoding="utf-8") as handle:
                    json.dump(entries, handle, indent=2)

                for entry in entries:
                    if max_entries and summary["inserted_entries"] >= max_entries:
                        break
                    minimal_ref = {
                        "title": entry.get("title"),
                        "authors": entry.get("authors"),
                        "editors": entry.get("editors"),
                        "year": entry.get("year"),
                        "doi": entry.get("doi"),
                        "source": entry.get("source"),
                        "volume": entry.get("volume"),
                        "issue": entry.get("issue"),
                        "pages": entry.get("pages"),
                        "publisher": entry.get("publisher"),
                        "type": entry.get("type"),
                        "url": entry.get("url"),
                        "isbn": entry.get("isbn"),
                        "issn": entry.get("issn"),
                        "abstract": entry.get("abstract"),
                        "keywords": entry.get("keywords"),
                        "source_pdf": str(ref_pdf),
                        "translated_title": entry.get("translated_title"),
                        "translated_year": entry.get("translated_year"),
                        "translated_language": entry.get("translated_language"),
                        "translation_relationship": entry.get("translation_relationship"),
                        "ingest_source": "pipeline",
                        "run_id": run_id,
                    }
                    row_id, err = db_manager.insert_no_metadata(
                        minimal_ref,
                        searcher=searcher,
                        rate_limiter=rate_limiter,
                        resolve_potential_duplicates=run_enrichment,
                    )
                    if err:
                        summary["skipped_entries"] += 1
                    else:
                        summary["inserted_entries"] += 1
                        if row_id:
                            inserted_ids.append(row_id)

        if run_enrichment and inserted_ids:
            db_manager.conn.row_factory = sqlite3.Row
            for batch in _chunked(inserted_ids, batch_size):
                for entry_id in batch:
                    row_cursor = db_manager.conn.cursor()
                    row_cursor.execute("SELECT * FROM no_metadata WHERE id = ?", (entry_id,))
                    row = row_cursor.fetchone()
                    if not row:
                        continue
                    entry = dict(row)
                    authors = []
                    if entry.get("authors"):
                        try:
                            authors = json.loads(entry["authors"])
                        except json.JSONDecodeError:
                            authors = [entry["authors"]]

                    ref_for_scraper = {
                        "title": entry.get("title"),
                        "authors": authors,
                        "doi": entry.get("doi"),
                        "year": entry.get("year"),
                        "container-title": entry.get("source"),
                        "volume": entry.get("volume"),
                        "issue": entry.get("issue"),
                        "pages": entry.get("pages"),
                        "abstract": entry.get("abstract"),
                        "keywords": entry.get("keywords"),
                        "source_work_id": entry.get("source_work_id"),
                        "relationship_type": entry.get("relationship_type"),
                        "ingest_source": entry.get("ingest_source"),
                        "run_id": entry.get("run_id"),
                    }

                    enriched_data = process_single_reference(
                        ref_for_scraper,
                        searcher,
                        rate_limiter,
                        fetch_references=fetch_references,
                        fetch_citations=fetch_citations,
                        max_citations=max_citations,
                        related_depth=related_depth,
                        max_related_per_reference=max_related,
                    )

                    if enriched_data:
                        new_id, err = db_manager.promote_to_with_metadata(
                            entry_id,
                            enriched_data,
                            expand_related=related_depth > 1,
                            max_related_per_source=max_related,
                        )
                        if new_id:
                            summary["enriched_promoted"] += 1
                            with_metadata_ids.append(new_id)
                        else:
                            summary["skipped_entries"] += 1
                    else:
                        reason = "Metadata fetch failed (no match found in OpenAlex/Crossref)"
                        db_manager.move_no_meta_entry_to_failed(entry_id, reason)
                        summary["enriched_failed"] += 1

        if run_enrichment:
            for wid in with_metadata_ids:
                qid, _ = db_manager.enqueue_for_download(wid)
                if qid:
                    summary["queued"] += 1
                else:
                    summary["skipped_queue"] += 1

        if run_id:
            cur = db_manager.conn.cursor()
            cur.execute(
                "UPDATE pipeline_runs SET finished_at = CURRENT_TIMESTAMP, status = ?, notes = ? WHERE id = ?",
                ("completed", json.dumps(summary), run_id),
            )
            db_manager.conn.commit()
        return summary
    except Exception as exc:
        summary["errors"].append(str(exc))
        if run_id:
            cur = db_manager.conn.cursor()
            cur.execute(
                "UPDATE pipeline_runs SET finished_at = CURRENT_TIMESTAMP, status = ?, notes = ? WHERE id = ?",
                ("failed", json.dumps(summary), run_id),
            )
            db_manager.conn.commit()
        raise
    finally:
        db_manager.close_connection()
