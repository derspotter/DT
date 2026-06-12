#!/usr/bin/env python3
"""
PDF OCR processor using OpenAI GPT-5-mini Responses API

This script processes PDF files by:
1. Slicing them into N-page chunks
2. Uploading each chunk to OpenAI (user_data) for OCR
3. Rate limiting with concurrency control
4. Extracting text with tables in markdown format
5. Stitching the results back together

Usage: python ocr/gpt5-ocr.py input_directory [output_directory]
"""

import os
import sys
import time
import argparse
import tempfile
import threading
import concurrent.futures
from pathlib import Path

import pikepdf
from openai import OpenAI


class RateLimiter:
    """
    Sliding window rate limiter with concurrency control.
    Tracks requests/minute (RPM) and tokens/minute (TPM) approximately.
    """

    def __init__(self, rpm_limit=500, tpm_limit=500_000):
        self.rpm_limit = rpm_limit
        self.tpm_limit = tpm_limit
        self.timestamps = []
        self.token_usage = []
        self.lock = threading.Lock()

        # Safety margins
        self.effective_rpm_limit = max(10, int(rpm_limit * 0.85))
        self.effective_tpm_limit = int(tpm_limit * 0.85) if tpm_limit else 0

        # Concurrency guard (cap based on RPM)
        max_concurrent = min(16, max(2, int(self.effective_rpm_limit * 0.25)))
        self.concurrency_limit = threading.Semaphore(max_concurrent)

        print(
            f"Rate limiter: {rpm_limit} RPM (eff {self.effective_rpm_limit}), "
            f"{tpm_limit:,} TPM (eff {self.effective_tpm_limit:,}); "
            f"max concurrent: {max_concurrent}"
        )

    def record_token_usage(self, tokens):
        with self.lock:
            self.token_usage.append((time.time(), tokens))

    def get_usage_stats(self):
        with self.lock:
            now = time.time()
            self.timestamps = [ts for ts in self.timestamps if now - ts < 60]
            self.token_usage = [(ts, t) for ts, t in self.token_usage if now - ts < 60]
            return {
                "rpm": len(self.timestamps),
                "rpm_limit": self.effective_rpm_limit,
                "tpm": sum(t for _, t in self.token_usage),
                "tpm_limit": self.effective_tpm_limit or 0,
            }

    def wait_if_needed(self):
        self.concurrency_limit.acquire()
        try:
            with self.lock:
                now = time.time()
                self.timestamps = [ts for ts in self.timestamps if now - ts < 60]
                self.token_usage = [(ts, t) for ts, t in self.token_usage if now - ts < 60]

                # Soft backoff near limits
                rpm = len(self.timestamps)
                tpm = sum(t for _, t in self.token_usage)
                if self.effective_tpm_limit:
                    near_tpm = tpm / self.effective_tpm_limit > 0.8
                else:
                    near_tpm = False
                if rpm / self.effective_rpm_limit > 0.8 or near_tpm:
                    time.sleep(0.1)

                # Hard wait on RPM
                while len(self.timestamps) >= self.effective_rpm_limit:
                    oldest = self.timestamps[0]
                    wait_time = oldest + 60 - now + 0.05
                    if wait_time > 0:
                        self.lock.release()
                        time.sleep(min(wait_time, 1.0))
                        self.lock.acquire()
                        now = time.time()
                        self.timestamps = [ts for ts in self.timestamps if now - ts < 60]
                # Record this request
                self.timestamps.append(time.time())
        finally:
            # Release immediately so the API call happens outside the lock
            self.concurrency_limit.release()


class GPT5MiniOCR:
    """
    Process PDFs using OpenAI GPT-5-mini (Responses API) for OCR.
    """

    def __init__(self, pages_per_batch=4, max_concurrency=8, rpm=500, tpm=500_000, use_flex=True):
        self.pages_per_batch = pages_per_batch
        self.max_concurrency = max_concurrency
        self.use_flex = use_flex

        # OpenAI client
        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            print("ERROR: OPENAI_API_KEY environment variable not found!")
            print("Please set your OpenAI API key, e.g.:")
            print("  export OPENAI_API_KEY='sk-...'")
            sys.exit(1)
        self.client = OpenAI()
        self.model_name = "gpt-5-mini"

        # Rate limiter tuned for GPT-5-mini
        self.rate_limiter = RateLimiter(rpm_limit=rpm, tpm_limit=tpm)

    def process_file(self, pdf_path, output_path):
        """Process a single PDF file by batching pages and OCR-ing each batch."""
        try:
            pdf_basename = os.path.basename(pdf_path)
            print(f"\nProcessing: {pdf_basename}")

            with tempfile.TemporaryDirectory() as temp_dir:
                # Determine total pages
                with pikepdf.open(pdf_path) as pdf:
                    total_pages = len(pdf.pages)
                print(f"Total pages: {total_pages}")

                # Build batches
                batches = []
                for start_page in range(1, total_pages + 1, self.pages_per_batch):
                    end_page = min(start_page + self.pages_per_batch - 1, total_pages)
                    batch_path = self._create_batch_pdf(pdf_path, start_page, end_page, temp_dir)
                    batches.append((start_page, end_page, batch_path))
                print(f"Created {len(batches)} batches of {self.pages_per_batch} pages each")

                # Concurrency tuned to avoid spikes
                dynamic_concurrency = min(self.max_concurrency, max(2, total_pages // self.pages_per_batch))
                print(f"Using {dynamic_concurrency} concurrent workers for this document")

                results = []
                with concurrent.futures.ThreadPoolExecutor(max_workers=dynamic_concurrency) as executor:
                    future_map = {
                        executor.submit(self._process_batch, batch_path, start_page, end_page): (start_page, end_page)
                        for start_page, end_page, batch_path in batches
                    }
                    completed = 0
                    for future in concurrent.futures.as_completed(future_map):
                        start_page, end_page, text, token_count = future.result()
                        completed += 1
                        if text:
                            results.append((start_page, text))
                            print(f"✓ Completed pages {start_page}-{end_page} ({int(token_count):,} tokens)")
                        else:
                            print(f"✗ Failed to extract text from pages {start_page}-{end_page}")

                        # Periodic status
                        if completed % 10 == 0 or completed == len(future_map):
                            stats = self.rate_limiter.get_usage_stats()
                            print(
                                f"Progress: {completed}/{len(future_map)} batches | "
                                f"RPM: {stats['rpm']}/{stats['rpm_limit']} | "
                                f"TPM: {stats['tpm']:,}/{stats['tpm_limit']:,}"
                            )

                # Assemble
                results.sort(key=lambda x: x[0])
                combined_text = "\n\n".join([text for _, text in results])

                # Write output
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
                with open(output_path, "w", encoding="utf-8") as f:
                    f.write(combined_text)

                print(f"OCR complete! Results saved to: {output_path}")
                return True

        except Exception as e:
            print(f"Error processing {pdf_path}: {e}")
            return False

    def _create_batch_pdf(self, pdf_path, start_page, end_page, temp_dir):
        """Create a PDF containing the given page range (inclusive)."""
        batch_filename = f"batch_{start_page}_to_{end_page}.pdf"
        batch_path = os.path.join(temp_dir, batch_filename)
        with pikepdf.open(pdf_path) as source_pdf:
            out = pikepdf.Pdf.new()
            for i in range(start_page - 1, min(end_page, len(source_pdf.pages))):
                out.pages.append(source_pdf.pages[i])
            out.save(batch_path)
        return batch_path

    def _process_batch(self, batch_path, start_page, end_page):
        """OCR a batch using GPT-5-mini via Responses API with file upload."""
        prompt = f"""
Extract ALL text from this PDF document (pages {start_page} to {end_page}).

INSTRUCTIONS:
- Extract all visible text including headers, footers, and body
- Maintain original formatting where helpful for readability
- Convert tables to GitHub-Flavored Markdown
- Preserve paragraph structure and indentation
- Include page markers if present
- Do not add commentary; only output the extracted text
"""

        max_retries = 3
        backoff = 2
        attempt = 0

        while attempt < max_retries:
            try:
                # Rate limit before upload + request
                self.rate_limiter.wait_if_needed()

                # Upload this batch as a file
                with open(batch_path, "rb") as f:
                    uploaded = self.client.files.create(file=f, purpose="user_data")

                # Build Responses API request
                content_items = [
                    {"type": "input_file", "file_id": uploaded.id},
                    {"type": "input_text", "text": prompt},
                ]

                request = {
                    "model": self.model_name,
                    "input": [
                        {
                            "role": "user",
                            "content": content_items,
                        }
                    ],
                }
                if self.use_flex:
                    request["service_tier"] = "flex"

                # Call API
                response = self.client.responses.create(**request)

                # Best-effort usage tracking
                token_count = 0
                if hasattr(response, "usage") and response.usage:
                    token_count = int(getattr(response.usage, "total_tokens", 0) or 0)
                    if token_count:
                        self.rate_limiter.record_token_usage(token_count)

                # Extract text
                output_text = getattr(response, "output_text", None)
                if output_text and output_text.strip():
                    formatted = f"--- Batch: Pages {start_page} to {end_page} ---\n{output_text.strip()}"
                    # Clean up remote file
                    try:
                        self.client.files.delete(uploaded.id)
                    except Exception:
                        pass
                    return start_page, end_page, formatted, token_count

                # Empty output — retry
                print(f"  Empty response for pages {start_page}-{end_page}, retrying...")
                try:
                    self.client.files.delete(uploaded.id)
                except Exception:
                    pass
                attempt += 1
                time.sleep(backoff * attempt)

            except Exception as e:
                print(f"  Error processing pages {start_page}-{end_page}: {e}")
                attempt += 1
                time.sleep(backoff * attempt)

        print(f"  Failed to process pages {start_page}-{end_page} after {max_retries} attempts")
        return start_page, end_page, None, 0

    def process_directory(self, input_dir, output_dir):
        """Process all PDFs in a directory."""
        input_path = Path(input_dir)
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        pdf_files = list(input_path.glob("*.pdf"))
        if not pdf_files:
            print(f"No PDF files found in {input_dir}")
            return 0

        print(f"Found {len(pdf_files)} PDF files to process")
        success = 0
        for pdf in pdf_files:
            out_file = output_path / f"{pdf.stem}.txt"
            if self.process_file(str(pdf), str(out_file)):
                success += 1
        print(f"\nProcessed {success} of {len(pdf_files)} files successfully")
        return success


def main():
    parser = argparse.ArgumentParser(description="PDF OCR using OpenAI GPT-5-mini Responses API")
    parser.add_argument("input_dir", help="Directory containing PDF files")
    parser.add_argument(
        "output_dir",
        nargs="?",
        default="ocr_results",
        help="Directory for output text files (default: ocr_results)",
    )
    parser.add_argument("--pages", type=int, default=4, help="Pages per batch (default: 4)")
    parser.add_argument("--concurrency", type=int, default=8, help="Maximum concurrent requests (default: 8)")
    parser.add_argument("--rpm", type=int, default=500, help="Requests per minute limit (default: 500)")
    parser.add_argument("--tpm", type=int, default=500000, help="Tokens per minute limit (default: 500000)")
    parser.add_argument(
        "--no-flex",
        action="store_true",
        help="Disable flex service tier (use standard)",
    )

    args = parser.parse_args()

    ocr = GPT5MiniOCR(
        pages_per_batch=args.pages,
        max_concurrency=args.concurrency,
        rpm=args.rpm,
        tpm=args.tpm,
        use_flex=not args.no_flex,
    )
    ocr.process_directory(args.input_dir, args.output_dir)


if __name__ == "__main__":
    main()
