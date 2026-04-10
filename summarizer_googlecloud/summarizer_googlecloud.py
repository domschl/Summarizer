import os
import sys
import json
import argparse
import time
import math
import yaml
from google import genai
from google.genai import types
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception

# Constants
VERSION = "0.0.1"

def get_config():
    config_file = os.path.expanduser("~/.config/summarizer/summarizer_config.json")
    try:
        with open(config_file, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading config: {e}")
        sys.exit(1)

def is_rate_limit_error(exception):
    """Returns True if the exception is a 429 Rate Limit error or 503 Unavailable."""
    msg = str(exception).lower()
    return "429" in msg or "resource_exhausted" in msg or "rate limit" in msg or "503" in msg or "unavailable" in msg

class GeminiEngine:
    def __init__(self, api_key: str, model_name: str):
        self.client = genai.Client(api_key=api_key)
        self.model_name = model_name
        self.last_request_time = 0
        self.min_delay = 4.1  # 15 RPM is 4s, 4.1s for safety

    def _wait_for_rpm(self):
        """Ensures we don't exceed 15 RPM by waiting at least 4.1s between calls."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_delay:
            wait_time = self.min_delay - elapsed
            time.sleep(wait_time)

    @retry(
        retry=retry_if_exception(is_rate_limit_error),
        wait=wait_exponential(multiplier=2, min=10, max=120),
        stop=stop_after_attempt(10),
        before_sleep=lambda retry_state: print(f"Rate limit hit ({retry_state.outcome.exception()}). Retrying in {retry_state.next_action.sleep}s... (Attempt {retry_state.attempt_number})")
    )
    def generate(self, prompt: str, max_output_tokens: int = 1500) -> str:
        self._wait_for_rpm()
        
        try:
            response = self.client.models.generate_content(
                model=self.model_name,
                contents=prompt,
                config=types.GenerateContentConfig(
                    max_output_tokens=max_output_tokens,
                    temperature=0.2,
                )
            )
            self.last_request_time = time.time()
            
            if not response.text:
                # Handle cases where safety filters might have blocked the response
                return "[Summary blocked or empty response from Gemini]"
            
            return response.text.strip()
        except Exception as e:
            if "quota" in str(e).lower() and "daily" in str(e).lower():
                print("\nCRITICAL: Daily API Quota (1,500 requests) has been reached.")
                print("Please wait 24 hours before resuming summarization with the free tier.")
                sys.exit(10) # Exit with code 10 to signal orchestrator to stop
            raise e

def chunked_summarize(engine, content: str, filepath: str, chunk_size: int = 50000) -> str:
    num_chunks = math.ceil(len(content) / chunk_size)
    if num_chunks == 0:
        return ""
        
    if num_chunks == 1:
        print("\n--> Document fits in one chunk. Summarizing directly...")
        instruction = "Please provide a detailed summary of this document:"
        prompt = f"The following is the full text of '{filepath}'. {instruction}\n\n{content}"
        return engine.generate(prompt, max_output_tokens=1500)
        
    chunk_summaries = []
    for i in range(num_chunks):
        start = i * chunk_size
        end = start + chunk_size
        chunk = content[start:end]

        chunk_start = time.time()
        print(f"\r--> Summarizing chunk {i+1}/{num_chunks}...", end="", flush=True)
        
        instruction = "Briefly summarize this part of the document:"
        prompt = f"{instruction}\n\n{chunk}"
        
        output = engine.generate(prompt, max_output_tokens=500)
        duration = time.time() - chunk_start
        print(f" ({duration:.1f}s)", end="", flush=True)
        chunk_summaries.append(output)

    print("\n--> Consolidating final summary...")
    consolidated_text = "\n\n".join(chunk_summaries)

    base_instruction = "The following are summaries of segments from a document. Please combine them into a single coherent, detailed summary:"
    final_prompt = f"File: {filepath}\n\n{base_instruction}\n\n{consolidated_text}"

    return engine.generate(final_prompt, max_output_tokens=1500)

def split_header_content(text: str) -> tuple[str, str]:
    separator = "---\n"
    if not text.startswith(separator):
        return ("", text)
    
    parts = text.split(separator, 2)
    if len(parts) < 3:
        return ("", text)
    
    return (parts[1], parts[2])

def parse_markdown(md_text: str):
    frontmatter, content = split_header_content(md_text)
    try:
        yaml_metadata = yaml.safe_load(frontmatter) if frontmatter else None
    except Exception:
        yaml_metadata = None
    return yaml_metadata, content

def assemble_markdown(metadata, md_text: str, model_name: str) -> str:
    if metadata is None:
        metadata = {}
        
    filtered_metadata = {}
    for k, v in metadata.items():
        if isinstance(v, list) and len(v) == 0: continue
        if isinstance(v, str) and v == "": continue
        filtered_metadata[k] = v
    
    # Add summary version info
    filtered_metadata['summary_version'] = f"{model_name} {VERSION}"
        
    header = yaml.dump(filtered_metadata, default_flow_style=False, indent=2)
    return f"---\n{header}---\n{md_text}"

def summarize_file(source_file: str, destination_file: str):
    if not os.path.exists(source_file):
        print(f"Error: Source file does not exist: {source_file}")
        sys.exit(1)
        
    config = get_config()
    api_key = config.get("api_key")
    model_name = config.get("model_name", "gemini-1.5-flash")
    chunk_size = config.get("chunk_size", 25000)

    if not api_key:
        print("Error: No api_key found in summarizer_config.json")
        sys.exit(1)

    try:
        with open(source_file, 'r', encoding='utf-8') as f:
            content = f.read()
            
        metadata, md_text = parse_markdown(content)
        
        print(f"Initializing Gemini Engine ({model_name})...")
        engine = GeminiEngine(api_key, model_name)
        
        summary_text = chunked_summarize(engine, md_text, os.path.basename(source_file), chunk_size=chunk_size)
        
        sum_metadata = {}
        if metadata:
            for key in ['title', 'authors', 'tags', 'uuid']:
                if key in metadata:
                    sum_metadata[key] = metadata[key]
                    
        full_summary = assemble_markdown(sum_metadata, summary_text, model_name)
        
        target_dir = os.path.dirname(destination_file)
        if target_dir and not os.path.exists(target_dir):
            os.makedirs(target_dir, exist_ok=True)
            
        temp_file = destination_file + ".tmp"
        with open(temp_file, 'w', encoding='utf-8') as f:
            f.write(full_summary)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_file, destination_file)
            
        print(f"Successfully wrote summary to: {destination_file}")
    except Exception as e:
        print(f"Error during summarization: {e}")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="Summarize Markdown using Gemini API")
    parser.add_argument("source", help="Path to the source markdown file")
    parser.add_argument("destination", help="Path to the destination summary markdown file")
    
    args = parser.parse_args()
    summarize_file(args.source, args.destination)

if __name__ == "__main__":
    main()
