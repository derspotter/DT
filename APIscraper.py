import os
import json
import time
import anthropic
from pathlib import Path
from collections import deque
from datetime import datetime, timedelta
from pdfminer.high_level import extract_text

API_KEY = os.getenv("API_KEY")

class RateLimiter:
    def __init__(self, requests_per_minute, input_tokens_per_minute, output_tokens_per_minute):
        self.rpm_limit = requests_per_minute
        self.input_tpm_limit = input_tokens_per_minute
        self.output_tpm_limit = output_tokens_per_minute
        self.request_times = deque()
        self.input_tokens = deque()
        self.output_tokens = deque()
    
    def wait_if_needed(self, input_tokens):
        now = datetime.now()
        minute_ago = now - timedelta(minutes=1)
        while self.request_times and self.request_times[0] < minute_ago:
            self.request_times.popleft()
        while self.input_tokens and self.input_tokens[0][0] < minute_ago:
            self.input_tokens.popleft()
        while self.output_tokens and self.output_tokens[0][0] < minute_ago:
            self.output_tokens.popleft()
        if len(self.request_times) >= self.rpm_limit:
            sleep_time = (self.request_times[0] - minute_ago).total_seconds() + 0.1
            if sleep_time > 0:
                print(f"[DEBUG] Rate limit approached, waiting {sleep_time:.1f} seconds...")
                time.sleep(sleep_time)
        current_input_tokens = sum(tokens for _, tokens in self.input_tokens)
        if current_input_tokens + input_tokens > self.input_tpm_limit:
            sleep_time = (self.input_tokens[0][0] - minute_ago).total_seconds() + 0.1
            if sleep_time > 0:
                print(f"[DEBUG] Input token limit approached, waiting {sleep_time:.1f} seconds...")
                time.sleep(sleep_time)
    
    def record_request(self, input_tokens, output_tokens):
        now = datetime.now()
        self.request_times.append(now)
        self.input_tokens.append((now, input_tokens))
        self.output_tokens.append((now, output_tokens))
        print(f"[DEBUG] Recorded request: input_tokens={input_tokens}, output_tokens={output_tokens}")

def extract_text_from_pdf(pdf_path):
    try:
        print(f"[DEBUG] Extracting text from PDF: {pdf_path}")
        text = extract_text(pdf_path)
        if text:
            print(f"[DEBUG] Text successfully extracted from {pdf_path}")
        else:
            print(f"[DEBUG] No text found in {pdf_path}")
        return text
    except Exception as e:
        print(f"[ERROR] Error extracting text from {pdf_path}: {e}")
        return None

def extract_bibliography(api_key, text, rate_limiter):
    """Use Claude to extract bibliography from text."""
    prompt = """
Human: Please extract the bibliography or references section from the following text and format it as a JSON array.
    Include as many of these fields as you can find in each reference:
    - title: full title of the work
    - authors: array of author names or organisation like G-20
    - year: publication year
    - journal: journal name if applicable
    - volume: journal volume if applicable
    - issue: journal issue if applicable
    - pages: page numbers if applicable
    - doi: DOI if present
    - url: URL if present
    - publisher: publisher name if present
    - place: place of publication if present
    - type: type of document (e.g., 'journal article', 'book', 'book chapter', 'conference paper', etc.)
    - isbn: ISBN if present for books
    - issn: ISSN if present for journals
    - abstract: abstract if present
    - keywords: array of keywords if present
    - language: language of publication if not English
    
    Text to process:
    {text}
    
    Return ONLY the JSON array, no other text.
    
Assistant:"""
    
    # Estimate input tokens (rough estimate)
    input_tokens = len(text.split()) + len(prompt.split())
    print(f"[DEBUG] Estimated input tokens: {input_tokens}")
    
    # Wait if we need to respect rate limits
    rate_limiter.wait_if_needed(input_tokens)
    
    client = anthropic.Anthropic(api_key=api_key)
    try:
        print(f"[DEBUG] Sending request to Claude API...")
        message = client.messages.create(
            model="claude-3-5-sonnet-20241022",
            max_tokens=2048,
            messages=[
                {
                    "role": "user",
                    "content": prompt.format(text=text)
                }
            ]
        )
        print(f"[DEBUG] Received response from Claude API")
        # Extract the JSON string from the response
        try:
            message_content = message.content[0].text  # Extract the JSON string from TextBlock
            # Load the JSON string into a Python dictionary to validate it (optional)
            bibliography_list = json.loads(message_content)
            return bibliography_list  # Return this to be saved later, avoiding multiple saves
            
        except json.JSONDecodeError as e:
            print(f"[ERROR] Failed to decode JSON: {e}")
            return []

    except Exception as e:
        print(f"[ERROR] Error calling Claude API: {e}")
        return []

def process_pdf_directory(api_key, input_dir, output_dir):
    rate_limiter = RateLimiter(requests_per_minute=50, input_tokens_per_minute=40000, output_tokens_per_minute=8000)
    os.makedirs(output_dir, exist_ok=True)
    
    for pdf_path in Path(input_dir).glob("*.pdf"):
        print(f"Processing {pdf_path.name}...")
        try:
            text = extract_text_from_pdf(pdf_path)
            if not text or not text.strip():
                print(f"No text extracted from {pdf_path.name}. Skipping...")
                continue

            # Extract bibliography as a list of references
            bibliography = extract_bibliography(api_key, text, rate_limiter)
            if bibliography:
                output_path = Path(output_dir) / f"{pdf_path.stem}_bibliography.json"
                with open(output_path, "w", encoding="utf-8") as f:
                    json.dump(bibliography, f, indent=2, ensure_ascii=False)
                print(f"Saved bibliography to {output_path}")
            else:
                print(f"No bibliography found for {pdf_path.name}")
        except Exception as e:
            print(f"Error processing {pdf_path.name}: {str(e)}")

if __name__ == "__main__":
    INPUT_DIR = "papers"
    OUTPUT_DIR = "bibliographies"
    process_pdf_directory(API_KEY, INPUT_DIR, OUTPUT_DIR)
