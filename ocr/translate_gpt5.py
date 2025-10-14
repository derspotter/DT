#!/usr/bin/env python3
"""
Translate OCR text files to German using OpenAI GPT-5-mini.

Reads all ``.txt`` files from the input directory, sends their content to
GPT-5-mini via the Responses API, and writes translated output files into the
target directory. Large files are processed in manageable chunks to avoid
token limits while preserving ordering.

Usage: python ocr/translate_gpt5.py input_dir [output_dir]
"""

import argparse
import os
import sys
import textwrap
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Iterable, List, Tuple

from openai import OpenAI


class RateLimiter:
    """RPM/TPM limiter with optional concurrency control."""

    def __init__(
        self,
        rpm_limit: int = 500,
        tpm_limit: int = 500_000,
        max_concurrency: int = 4,
    ) -> None:
        self.rpm_limit = rpm_limit
        self.tpm_limit = tpm_limit
        self.lock = threading.Lock()
        self.timestamps: List[float] = []
        self.token_usage: List[tuple[float, int]] = []
        self.concurrency_limit = threading.Semaphore(max(1, max_concurrency))

        self.effective_rpm = max(10, int(rpm_limit * 0.85))
        self.effective_tpm = int(tpm_limit * 0.85) if tpm_limit else 0

    def acquire(self) -> None:
        self.concurrency_limit.acquire()
        with self.lock:
            now = time.time()
            self.timestamps = [ts for ts in self.timestamps if now - ts < 60]
            self.token_usage = [(ts, t) for ts, t in self.token_usage if now - ts < 60]

            while len(self.timestamps) >= self.effective_rpm:
                oldest = self.timestamps[0]
                wait_time = oldest + 60 - now + 0.05
                if wait_time > 0:
                    self.lock.release()
                    time.sleep(min(wait_time, 1.0))
                    self.lock.acquire()
                    now = time.time()
                    self.timestamps = [ts for ts in self.timestamps if now - ts < 60]

            self.timestamps.append(time.time())

    def release(self) -> None:
        self.concurrency_limit.release()

    def record_tokens(self, token_count: int) -> None:
        if not token_count:
            return
        with self.lock:
            self.token_usage.append((time.time(), token_count))


def chunk_text(text: str, max_chars: int) -> Iterable[str]:
    """Yield text chunks bounded by ``max_chars`` keeping paragraph breaks."""

    paragraphs = text.split("\n\n")
    buffer: List[str] = []
    current_len = 0

    for para in paragraphs:
        candidate = para if not buffer else f"\n\n{para}"

        if current_len + len(candidate) <= max_chars:
            buffer.append(candidate)
            current_len += len(candidate)
            continue

        if buffer:
            yield "".join(buffer)
            buffer = []
            current_len = 0

        if len(para) <= max_chars:
            buffer.append(para)
            current_len = len(para)
            continue

        # Paragraph itself exceeds chunk size; split hard to respect limit.
        for start in range(0, len(para), max_chars):
            slice_text = para[start : start + max_chars]
            yield slice_text
        buffer = []
        current_len = 0

    if buffer:
        yield "".join(buffer)


class GPT5Translator:
    """Translate plain text documents to German using GPT-5-mini."""

    def __init__(
        self,
        chunk_chars: int = 6000,
        use_flex: bool = True,
        rpm: int = 500,
        tpm: int = 500_000,
        max_workers: int = 4,
    ) -> None:
        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            print("ERROR: OPENAI_API_KEY environment variable not set.")
            print("Set it via: export OPENAI_API_KEY='sk-...'")
            sys.exit(1)

        self.client = OpenAI()
        self.model = "gpt-5-mini"
        self.chunk_chars = chunk_chars
        self.use_flex = use_flex
        self.max_workers = max(1, max_workers)
        self.rate_limiter = RateLimiter(
            rpm_limit=rpm,
            tpm_limit=tpm,
            max_concurrency=self.max_workers,
        )

    def translate_text(self, text: str, filename: str) -> str:
        chunks = list(chunk_text(text, self.chunk_chars)) or [""]
        total = len(chunks)
        translations: List[Tuple[int, str]] = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures: List[Tuple[int, object]] = []
            for idx, chunk in enumerate(chunks, start=1):
                print(
                    f"   Translating chunk {idx}/{total} "
                    f"for {filename} ({len(chunk)} chars)"
                )
                future = executor.submit(
                    self._translate_chunk,
                    chunk,
                    filename,
                    idx,
                    total,
                )
                futures.append((idx, future))

            results = {}
            for idx, future in futures:
                try:
                    results[idx] = future.result()
                except Exception as exc:
                    raise RuntimeError(
                        f"Translation failed for {filename} chunk {idx}/{total}: {exc}"
                    ) from exc

        return "\n\n".join(
            results[idx].strip() for idx in range(1, total + 1) if results.get(idx)
        )

    def _translate_chunk(self, chunk: str, filename: str, idx: int, total: int) -> str:
        self.rate_limiter.acquire()
        try:
            instruction = textwrap.dedent(
                f"""
                Translate the provided document segment into German.
                - Maintain the original structure, lists, and headings when possible.
                - Preserve numbers, dates, and proper names faithfully.
                - Do not include commentary or explanations; only return the German text.
                - The text comes from {filename}, segment {idx} of {total}.
                """
            ).strip()

            request = {
                "model": self.model,
                "input": [
                    {
                        "role": "user",
                        "content": [
                            {"type": "input_text", "text": instruction},
                            {"type": "input_text", "text": chunk},
                        ],
                    }
                ],
            }
            if self.use_flex:
                request["service_tier"] = "flex"

            response = self.client.responses.create(**request)

            if hasattr(response, "usage") and response.usage:
                total_tokens = int(getattr(response.usage, "total_tokens", 0) or 0)
                self.rate_limiter.record_tokens(total_tokens)

            output_text = getattr(response, "output_text", None)
            if output_text and output_text.strip():
                return output_text

            raise RuntimeError(
                f"Received empty translation for {filename} chunk {idx}/{total}."
            )
        finally:
            self.rate_limiter.release()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Translate OCR text files to German using GPT-5-mini"
    )
    parser.add_argument("input_dir", help="Directory containing .txt files to translate")
    parser.add_argument(
        "output_dir",
        nargs="?",
        default="ocr/translated",
        help="Directory to store translated files (default: ocr/translated)",
    )
    parser.add_argument(
        "--chunk-chars",
        type=int,
        default=6000,
        help="Maximum characters per chunk (default: 6000)",
    )
    parser.add_argument(
        "--no-flex",
        action="store_true",
        help="Disable flex tier for GPT-5-mini requests",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=4,
        help="Number of parallel translation workers (default: 4)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing translated files",
    )

    args = parser.parse_args()

    input_path = Path(args.input_dir)
    if not input_path.exists() or not input_path.is_dir():
        print(f"ERROR: Input directory '{input_path}' does not exist or is not a directory.")
        sys.exit(1)

    output_path = Path(args.output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    translator = GPT5Translator(
        chunk_chars=args.chunk_chars,
        use_flex=not args.no_flex,
        max_workers=args.concurrency,
    )

    txt_files = sorted(input_path.glob("*.txt"))
    if not txt_files:
        print(f"No .txt files found in {input_path}")
        return

    for txt_file in txt_files:
        relative_name = txt_file.name
        output_file = output_path / relative_name

        if output_file.exists() and not args.overwrite:
            print(f"Skipping {relative_name}: output already exists. Use --overwrite to replace.")
            continue

        print(f"\nTranslating {relative_name}...")

        with txt_file.open("r", encoding="utf-8") as f:
            original_text = f.read()

        try:
            translated_text = translator.translate_text(original_text, relative_name)
        except Exception as exc:
            print(f"❌ Failed to translate {relative_name}: {exc}")
            continue

        with output_file.open("w", encoding="utf-8") as f:
            f.write(translated_text)

        print(f"✅ Saved translation to {output_file}")


if __name__ == "__main__":
    main()
