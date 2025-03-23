#!/usr/bin/env python3
"""
PDF OCR processor using Google Gemini API

This script processes PDF files by:
1. Slicing them into 4-page chunks
2. Uploading them to Gemini concurrently for OCR
3. Rate limiting to respect the 15 RPM API limit
4. Extracting text with tables in markdown format
5. Stitching the results back together

Usage: python gem_ocr.py input_directory [output_directory]
"""

import os
import sys
import time
import argparse
import tempfile
import threading
import concurrent.futures
import queue
from pathlib import Path
import shutil
import google.generativeai as genai
import pikepdf

class RateLimiter:
    """
    Implements a sliding window rate limiter to enforce RPM limits
    with proper concurrency control
    """
    def __init__(self, rpm_limit=15):
        self.rpm_limit = rpm_limit
        self.tpm_limit = 0  # We don't need to track tokens with standard limits
        self.timestamps = []
        self.token_usage = []  # Kept for compatibility but not used with standard limits
        self.lock = threading.Lock()
        
        # With standard limits, use a safety margin of 90%
        self.effective_rpm_limit = max(10, int(rpm_limit * 0.9))
        self.effective_tpm_limit = 0
        
        # Lower concurrency limit for standard rate limits
        max_concurrent = min(8, int(rpm_limit * 0.5))  # Max 8 workers, about half the RPM limit
        self.concurrency_limit = threading.Semaphore(max_concurrent)
        
        print(f"Rate limiter initialized with {rpm_limit} RPM limit (effective: {self.effective_rpm_limit})")
        print(f"Maximum concurrent requests: {max_concurrent}")
    
    def wait_if_needed(self):
        """Wait if either rate limit would be exceeded - with concurrency control"""
        # First acquire the semaphore to limit concurrent requests
        self.concurrency_limit.acquire()
        try:
            with self.lock:
                current_time = time.time()
                
                # Clean up timestamps older than 60 seconds
                self.timestamps = [ts for ts in self.timestamps if current_time - ts < 60]
                self.token_usage = [(ts, tokens) for ts, tokens in self.token_usage if current_time - ts < 60]
                
                # Count current requests and tokens in the sliding window
                current_rpm = len(self.timestamps)
                current_tpm = sum(tokens for _, tokens in self.token_usage)
                
                # Graduated delay as we approach limits
                if current_rpm > 0.8 * self.effective_rpm_limit or current_tpm > 0.8 * self.effective_tpm_limit:
                    rpm_ratio = current_rpm / self.effective_rpm_limit
                    tpm_ratio = current_tpm / self.effective_tpm_limit if self.effective_tpm_limit > 0 else 0
                    ratio = max(rpm_ratio, tpm_ratio)
                    # Apply a small delay only when getting close to limits
                    if ratio > 0.8:
                        delay = 0.1 + (ratio - 0.8) * 2  # 0.1s to 0.5s depending on proximity to limit
                        time.sleep(delay)
                
                # If we've hit RPM limit, wait until we have capacity
                while len(self.timestamps) >= self.effective_rpm_limit:
                    if self.timestamps:
                        oldest = self.timestamps[0]
                        wait_time = oldest + 60 - current_time + 0.1
                        print(f"RPM limit reached ({current_rpm}/{self.effective_rpm_limit}). "
                              f"Waiting {wait_time:.2f}s...")
                        
                        self.lock.release()
                        time.sleep(wait_time)
                        self.lock.acquire()
                        
                        current_time = time.time()
                        self.timestamps = [ts for ts in self.timestamps if current_time - ts < 60]
                    else:
                        break
                
                # If we've hit TPM limit, wait until we have capacity
                current_tpm = sum(tokens for _, tokens in self.token_usage)
                while current_tpm >= self.effective_tpm_limit:
                    if self.token_usage:
                        oldest = self.token_usage[0][0]  # Get timestamp of oldest token usage
                        wait_time = oldest + 60 - current_time + 0.1
                        print(f"TPM limit reached ({current_tpm:,}/{self.effective_tpm_limit:,}). "
                              f"Waiting {wait_time:.2f}s...")
                        
                        self.lock.release()
                        time.sleep(wait_time)
                        self.lock.acquire()
                        
                        current_time = time.time()
                        self.token_usage = [(ts, tokens) for ts, tokens in self.token_usage if current_time - ts < 60]
                        current_tpm = sum(tokens for _, tokens in self.token_usage)
                    else:
                        break
                
                # Add current request timestamp
                self.timestamps.append(current_time)
                # Token usage will be added after the API call when we know the count
                
                return True
        finally:
            # Always release the semaphore when done
            self.concurrency_limit.release()
    
    def record_token_usage(self, token_count):
        """Record the token usage of a completed request"""
        with self.lock:
            self.token_usage.append((time.time(), token_count))
    
    def get_usage_stats(self):
        """Get current usage statistics"""
        with self.lock:
            current_time = time.time()
            # Clean up expired entries
            self.timestamps = [ts for ts in self.timestamps if current_time - ts < 60]
            self.token_usage = [(ts, tokens) for ts, tokens in self.token_usage if current_time - ts < 60]
            
            current_rpm = len(self.timestamps)
            current_tpm = sum(tokens for _, tokens in self.token_usage)
            rpm_pct = (current_rpm / self.rpm_limit) * 100 if self.rpm_limit > 0 else 0
            tpm_pct = (current_tpm / self.tpm_limit) * 100 if self.tpm_limit > 0 else 0
            
            return {
                "rpm": current_rpm,
                "rpm_limit": self.rpm_limit,
                "rpm_percent": rpm_pct,
                "tpm": current_tpm,
                "tpm_limit": self.tpm_limit,
                "tpm_percent": tpm_pct
            }

class GeminiOCR:
    """
    Process PDFs using Gemini for OCR with appropriate rate limiting
    """
    def __init__(self, model_name="gemini-2.0-flash-exp", pages_per_batch=4, max_concurrency=8):
        self.model_name = model_name
        self.pages_per_batch = pages_per_batch
        self.max_concurrency = max_concurrency
        self.rate_limiter = RateLimiter(rpm_limit=15)
        self.setup_gemini()
        self.token_stats = {"total": 0, "count": 0, "avg": 0}
    
    def setup_gemini(self):
        """Initialize the Gemini API"""
        api_key = os.environ.get('GOOGLE_API_KEY')
        
        if not api_key:
            print("ERROR: GOOGLE_API_KEY environment variable not found!")
            print("Please set your Google API key with:")
            print("  export GOOGLE_API_KEY='your-api-key-here'")
            sys.exit(1)
        
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel(self.model_name)
        print(f"Initialized Gemini model: {self.model_name}")
    
    def process_file(self, pdf_path, output_path):
        """Process a single PDF file"""
        try:
            pdf_basename = os.path.basename(pdf_path)
            print(f"\nProcessing: {pdf_basename}")
            
            # Create temporary directory for batch PDFs
            with tempfile.TemporaryDirectory() as temp_dir:
                # Get PDF information
                with pikepdf.open(pdf_path) as pdf:
                    total_pages = len(pdf.pages)
                    print(f"Total pages: {total_pages}")
                
                # Create batches
                batches = []
                for start_page in range(1, total_pages + 1, self.pages_per_batch):
                    end_page = min(start_page + self.pages_per_batch - 1, total_pages)
                    batch_path = self._create_batch_pdf(pdf_path, start_page, end_page, temp_dir)
                    batches.append((start_page, end_page, batch_path))
                
                print(f"Created {len(batches)} batches of {self.pages_per_batch} pages each")
                
                # Use more conservative concurrency with standard rate limits
                dynamic_concurrency = min(
                    self.max_concurrency,
                    min(8, max(4, total_pages // 4))  # Between 4-8 based on PDF size
                )
                print(f"Using {dynamic_concurrency} concurrent workers for this document")
                
                # Process batches with concurrent workers
                results = []
                with concurrent.futures.ThreadPoolExecutor(max_workers=dynamic_concurrency) as executor:
                    futures = {
                        executor.submit(self._process_batch, batch_path, start_page, end_page): 
                        (start_page, end_page) 
                        for start_page, end_page, batch_path in batches
                    }
                    
                    completed = 0
                    for future in concurrent.futures.as_completed(futures):
                        start_page, end_page, text, token_count = future.result()
                        completed += 1
                        
                        # Print periodic status updates
                        if completed % 10 == 0 or completed == len(futures):
                            # Get rate limiter stats
                            stats = self.rate_limiter.get_usage_stats()
                            print(f"Progress: {completed}/{len(futures)} batches | "
                                  f"RPM: {stats['rpm']}/{stats['rpm_limit']} | "
                                  f"TPM: {stats['tpm']:,}/{stats['tpm_limit']:,}")
                            
                        if text:
                            results.append((start_page, text))
                            print(f"✓ Completed pages {start_page}-{end_page} ({token_count} tokens)")
                        else:
                            print(f"✗ Failed to extract text from pages {start_page}-{end_page}")
                
                # Sort results by page number and combine
                results.sort(key=lambda x: x[0])
                combined_text = "\n\n".join([text for _, text in results])
                
                # Write output
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
                with open(output_path, 'w', encoding='utf-8') as f:
                    f.write(combined_text)
                
                # Print final statistics
                if self.token_stats["count"] > 0:
                    print(f"\nTokens per page statistics:")
                    print(f"  Average: {self.token_stats['avg']:.1f} tokens/page")
                    print(f"  Total tokens: {self.token_stats['total']:,}")
                
                print(f"OCR complete! Results saved to: {output_path}")
                return True
                
        except Exception as e:
            print(f"Error processing {pdf_path}: {str(e)}")
            return False
    
    def _create_batch_pdf(self, pdf_path, start_page, end_page, temp_dir):
        """Create a PDF batch with specified page range"""
        batch_filename = f"batch_{start_page}_to_{end_page}.pdf"
        batch_path = os.path.join(temp_dir, batch_filename)
        
        # Extract pages into a new PDF
        with pikepdf.open(pdf_path) as source_pdf:
            batch_pdf = pikepdf.Pdf.new()
            for i in range(start_page - 1, min(end_page, len(source_pdf.pages))):
                batch_pdf.pages.append(source_pdf.pages[i])
            batch_pdf.save(batch_path)
        
        return batch_path
    
    def _process_batch(self, batch_path, start_page, end_page):
        """Process a batch PDF with Gemini OCR"""
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # Apply rate limiting with both RPM and TPM tracking
                self.rate_limiter.wait_if_needed()
                
                # Upload PDF to Gemini
                pdf_content = genai.upload_file(batch_path)
                
                # Create prompt for OCR
                prompt = f"""
Extract ALL text from this PDF document (pages {start_page} to {end_page}).

INSTRUCTIONS:
- Extract ALL visible text including headers, footers, and body
- Maintain the original formatting structure as much as possible
- Convert tables to markdown format
- Preserve paragraph structure and indentation
- Include page numbers or markers if present
- Do not add any additional commentary or explanations
- Only output the extracted text
"""
                
                # Make the API request
                response = self.model.generate_content([prompt, pdf_content])
                
                if response.text and response.text.strip():
                    # Format with page range header
                    formatted_text = f"--- Batch: Pages {start_page} to {end_page} ---\n{response.text.strip()}"
                    
                    # Calculate token count and add to rate limiter tracking
                    try:
                        # Count tokens in the output
                        token_count = self.model.count_tokens([formatted_text]).total_tokens
                        
                        # Track token usage for rate limiting
                        self.rate_limiter.record_token_usage(token_count)
                        
                        # Update token statistics
                        pages_in_batch = end_page - start_page + 1
                        tokens_per_page = token_count / pages_in_batch
                        
                        with threading.Lock():
                            self.token_stats["total"] += token_count
                            self.token_stats["count"] += pages_in_batch
                            self.token_stats["avg"] = self.token_stats["total"] / self.token_stats["count"]
                            
                    except Exception as tc_error:
                        # If token counting fails, use an estimated count
                        token_count = len(formatted_text.split()) * 1.3  # Rough estimate
                        self.rate_limiter.record_token_usage(int(token_count))
                        print(f"  Token counting error: {str(tc_error)}")
                    
                    return start_page, end_page, formatted_text, token_count
                else:
                    print(f"  Empty response for pages {start_page}-{end_page}, retrying...")
                    retry_count += 1
                    time.sleep(2 * retry_count)  # Exponential backoff
            
            except Exception as e:
                print(f"  Error processing pages {start_page}-{end_page}: {str(e)}")
                retry_count += 1
                
                # More aggressive backoff for rate limit errors
                if "429" in str(e) or "Resource has been exhausted" in str(e):
                    wait_time = 30 * (2 ** retry_count)
                    print(f"  Rate limit error. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    time.sleep(5 * retry_count)
        
        print(f"  Failed to process pages {start_page}-{end_page} after {max_retries} attempts")
        return start_page, end_page, None, 0
    
    def process_directory(self, input_dir, output_dir):
        """Process all PDF files in a directory"""
        input_path = Path(input_dir)
        output_path = Path(output_dir)
        
        # Create output directory if it doesn't exist
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Find all PDF files
        pdf_files = list(input_path.glob("*.pdf"))
        
        if not pdf_files:
            print(f"No PDF files found in {input_dir}")
            return 0
        
        print(f"Found {len(pdf_files)} PDF files to process")
        
        # Process each PDF
        successful = 0
        for pdf_file in pdf_files:
            output_file = output_path / f"{pdf_file.stem}.txt"
            if self.process_file(str(pdf_file), str(output_file)):
                successful += 1
        
        print(f"\nProcessed {successful} of {len(pdf_files)} files successfully")
        return successful

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="PDF OCR using Google Gemini")
    parser.add_argument("input_dir", help="Directory containing PDF files")
    parser.add_argument("output_dir", nargs="?", default="ocr_results",
                        help="Directory for output text files (default: ocr_results)")
    parser.add_argument("--pages", type=int, default=4,
                        help="Pages per batch (default: 4)")
    parser.add_argument("--concurrency", type=int, default=8,
                        help="Maximum concurrent requests (default: 8)")
    parser.add_argument("--rpm", type=int, default=15,
                        help="Rate limit in requests per minute (default: 15)")
    
    args = parser.parse_args()
    
    # Initialize OCR processor
    ocr = GeminiOCR(pages_per_batch=args.pages, max_concurrency=args.concurrency)
    ocr.rate_limiter.rpm_limit = args.rpm
    ocr.rate_limiter.effective_rpm_limit = int(args.rpm * 0.9)
    
    # Process directory
    ocr.process_directory(args.input_dir, args.output_dir)

if __name__ == "__main__":
    main()
