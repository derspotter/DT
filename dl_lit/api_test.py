import os
import time
from dotenv import load_dotenv
from openai import OpenAI
import google.generativeai as genai

def configure_clients():
    """Loads API keys and configures API clients."""
    load_dotenv()
    
    deepseek_api_key = os.getenv('DEEPSEEK_API_KEY')
    google_api_key = os.getenv('GOOGLE_API_KEY')

    deepseek_client = None
    google_model = None

    if not deepseek_api_key:
        print("[ERROR] DEEPSEEK_API_KEY not found in .env file.")
    else:
        try:
            deepseek_client = OpenAI(api_key=deepseek_api_key, base_url="https://api.deepseek.com")
            print("[INFO] DeepSeek client configured.")
        except Exception as e:
            print(f"[ERROR] Failed to configure DeepSeek client: {e}")

    if not google_api_key:
        print("[ERROR] GOOGLE_API_KEY not found in .env file.")
    else:
        try:
            genai.configure(api_key=google_api_key)
            google_model = genai.GenerativeModel('gemini-2.5-flash-preview-04-17')
            print("[INFO] Google Gemini client configured with 'gemini-2.5-flash-preview-04-17'.")
        except Exception as e:
            print(f"[ERROR] Failed to configure Google client: {e}")

    return deepseek_client, google_model

def test_deepseek(client, prompt):
    """Sends a prompt to DeepSeek and measures response time."""
    if not client:
        print("[SKIP] Skipping DeepSeek test - client not configured.")
        return

    print("\n--- Testing DeepSeek ('deepseek-chat') ---")
    start_time = time.time()
    try:
        response = client.chat.completions.create(
            model="deepseek-chat",
            messages=[{"role": "user", "content": prompt}],
            stream=False,
            temperature=0.1
        )
        end_time = time.time()
        duration = end_time - start_time
        print(f"[TIMER] DeepSeek call took {duration:.2f} seconds.")
        print(f"[RESPONSE] {response.choices[0].message.content[:100]}...")
    except Exception as e:
        end_time = time.time()
        duration = end_time - start_time
        print(f"[ERROR] DeepSeek API call failed after {duration:.2f} seconds: {e}")

def test_google_flash(model, prompt):
    """Sends a prompt to Google Gemini Flash and measures response time."""
    if not model:
        print("[SKIP] Skipping Google Gemini test - model not configured.")
        return
        
    print("\n--- Testing Google Gemini ('gemini-2.5-flash-preview-04-17') ---")
    start_time = time.time()
    try:
        response = model.generate_content(prompt)
        end_time = time.time()
        duration = end_time - start_time
        print(f"[TIMER] Google Gemini Flash call took {duration:.2f} seconds.")
        # Accessing text might vary slightly based on response structure, adjust if needed
        print(f"[RESPONSE] {response.text[:100]}...") 
    except Exception as e:
        end_time = time.time()
        duration = end_time - start_time
        print(f"[ERROR] Google Gemini Flash API call failed after {duration:.2f} seconds: {e}")

if __name__ == "__main__":
    print("--- API Benchmark Test --- (Requires .env in current dir)")
    deepseek_client, google_model = configure_clients()

    # Generate a long placeholder prompt (approx. 5000 chars)
    placeholder_paragraph = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum. "
    # Repeat the paragraph to reach roughly 5000 characters
    repetitions = 5000 // len(placeholder_paragraph) + 1
    long_placeholder_text = (placeholder_paragraph * repetitions)[:5000] # Ensure it's not excessively long

    # Define the task within the long prompt
    test_prompt = f"Please analyze the following text and answer this question concisely: What is the capital of France?\n\nText:\n------\n{long_placeholder_text}\n------\n\nAnswer concisely:"
    print(f"[INFO] Using test prompt of length: {len(test_prompt)} characters.")

    test_deepseek(deepseek_client, test_prompt)
    test_google_flash(google_model, test_prompt)

    print("\n--- Test Complete ---")
