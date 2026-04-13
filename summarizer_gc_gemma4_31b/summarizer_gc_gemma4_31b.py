import os
import sys
import json
import argparse
import time
import math
import re
import yaml
import logging
import threading
import hashlib
from datetime import datetime, timedelta
from google import genai
from google.genai import types

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("gc_gemma4_31b")

# Noise filter for external libraries
class NoiseFilter(logging.Filter):
    def filter(self, record):
        msg = record.getMessage()
        # Suppress AFC enabled messages
        if "AFC is enabled" in msg:
            return False
        # Suppress successful HTTP 200 OK logs, but keep others
        if "HTTP Request" in msg and "200 OK" in msg:
            return False
        return True

for handler in logging.root.handlers:
    handler.addFilter(NoiseFilter())

# Constants
VERSION = "0.1.0"
MODEL_NAME = "gemma-4-31b-it"  # Specifically for this implementation

def watchdog():
    """Exits the process if the parent process dies (PPID becomes 1)."""
    while True:
        if os.getppid() == 1:
            logger.warning("Parent process died. Exiting...")
            os._exit(1)
        time.sleep(0.1)

def get_platform_config():
    config_file = os.path.expanduser("~/.config/summarizer/summarizer_config_gc_gemma4_31b.json")
    try:
        if os.path.exists(config_file):
            with open(config_file, 'r') as f:
                return json.load(f)
    except Exception as e:
        logger.error(f"Error loading platform config: {e}")
    
    # Defaults
    return {
        "chunk_size": 200000,
        "api_key": "",
        "rate_blocked_until": ""
    }

def update_block_until(timestamp_str: str):
    config_path = os.path.expanduser("~/.config/summarizer/summarizer_config_gc_gemma4_31b.json")
    try:
        config = get_platform_config()
        # Only update if the new block is further in the future
        current_blocked = config.get("rate_blocked_until", "")
        if timestamp_str > current_blocked:
            config["rate_blocked_until"] = timestamp_str
            
            # Atomic write
            temp_path = config_path + ".tmp"
            with open(temp_path, 'w') as f:
                json.dump(config, f, indent=4)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, config_path)
            logger.info(f"Updated rate limit block until: {timestamp_str}")
    except Exception as e:
        logger.error(f"Failed to update config file: {e}")

def check_rate_limit():
    """Checks if we are currently blocked and sleeps if so."""
    while True:
        config = get_platform_config()
        blocked_until_str = config.get("rate_blocked_until", "")
        if not blocked_until_str:
            break
            
        try:
            blocked_until = datetime.fromisoformat(blocked_until_str)
            now = datetime.now()
            if now < blocked_until:
                wait_seconds = (blocked_until - now).total_seconds()
                if wait_seconds > 0:
                    logger.info(f"Rate limited. Waiting {wait_seconds:.1f}s until {blocked_until_str}...")
                    time.sleep(min(wait_seconds, 10)) # Check again every 10s or when done
                    continue
        except Exception as e:
            logger.error(f"Error parsing rate_blocked_until: {e}")
        
        break

def parse_retry_delay(exception):
    """Attempts to parse the recommended retry delay from a Google API exception."""
    try:
        if hasattr(exception, "details") and exception.details:
            for detail in exception.details:
                if isinstance(detail, dict) and "retry_delay" in detail:
                    delay_str = str(detail["retry_delay"])
                    match = re.search(r"([\d\.]+)", delay_str)
                    if match:
                        return float(match.group(1))
        
        msg = str(exception)
        match = re.search(r"'retryDelay':\s*'([\d\.]+)s'", msg)
        if match:
            return float(match.group(1))
    except Exception:
        pass
    return None

class GemmaEngine:
    def __init__(self, api_key: str):
        self.client = genai.Client(api_key=api_key)
        self.last_request_time = 0
        self.min_delay = 4.1  # Global safety delay (approx 15 RPM)

    def generate(self, prompt: str, max_output_tokens: int = 1500) -> str:
        attempts = 0
        max_attempts = 10
        backoff = 10

        while attempts < max_attempts:
            check_rate_limit()
            
            # Local RPM safety
            elapsed = time.time() - self.last_request_time
            if elapsed < self.min_delay:
                time.sleep(self.min_delay - elapsed)

            try:
                response = self.client.models.generate_content(
                    model=MODEL_NAME,
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        max_output_tokens=max_output_tokens,
                        temperature=0.2,
                    )
                )
                self.last_request_time = time.time()
                
                if not response.text:
                    return "[Summary blocked or empty response]"
                
                return response.text.strip()

            except Exception as e:
                attempts += 1
                msg = str(e).lower()
                
                # 1. Check for specific daily quota exhaustion first (often a 429 or 403)
                # Google's API response is (currently?) broken, doesn't give correct 
                # retry times or responses. A 'rate limit' in 429 seems do indicate 
                # that entire daily quota is exhausted, at least for the 'free' tier.
                if "Rate limit" in msg or ("quota" in msg and "daily" in msg):  
                    logger.error("Daily API Quota reached.")
                    # Set block until tomorrow at 08:00 (conservative safety)
                    from datetime import timedelta
                    tomorrow = datetime.now().replace(hour=8, minute=0, second=0, microsecond=0) + timedelta(days=1)
                    update_block_until(tomorrow.isoformat())
                    sys.exit(10)
                
                # 2. Check for generic transient rate limit (429)
                if "429" in msg or "resource_exhausted" in msg or "rate limit" in msg:
                    delay = parse_retry_delay(e) or backoff
                    logger.warning(f"Rate limit hit. Suggest retry in {delay}s. Attempt {attempts}/{max_attempts}")
                    time.sleep(delay)
                    backoff *= 2
                    continue
                
                # Check for transient errors (503, etc)
                if "503" in msg or "unavailable" in msg or "deadline_exceeded" in msg:
                    logger.warning(f"Transient error: {e}. Retrying in {backoff}s...")
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                
                raise e

        raise Exception("Max retry attempts reached.")

class WorkCache:
    def __init__(self, cache_dir: str = "~/.cache/summarizer/work_cache"):
        self.cache_dir = os.path.expanduser(cache_dir)
        os.makedirs(self.cache_dir, exist_ok=True)

    def _get_path(self, doc_hash: str, chunk_size: int) -> str:
        return os.path.join(self.cache_dir, f"{doc_hash}_{chunk_size}.json")

    def load_progress(self, doc_hash: str, chunk_size: int) -> tuple[list[str], int]:
        path = self._get_path(doc_hash, chunk_size)
        if os.path.exists(path):
            try:
                with open(path, 'r') as f:
                    data = json.load(f)
                    last_updated = datetime.fromisoformat(data['last_updated'])
                    if (datetime.now() - last_updated).days < 14:
                        return data.get('chunk_summaries', []), data.get('next_index', 0)
                    else:
                        logger.info(f"Cache entry too old, discarding: {path}")
                        os.remove(path)
            except Exception as e:
                logger.warning(f"Failed to load cache {path}: {e}")
        return [], 0

    def save_progress(self, doc_hash: str, chunk_size: int, chunk_summaries: list[str], next_index: int, filepath: str):
        path = self._get_path(doc_hash, chunk_size)
        data = {
            "doc_hash": doc_hash,
            "chunk_size": chunk_size,
            "filepath": filepath,
            "chunk_summaries": chunk_summaries,
            "next_index": next_index,
            "last_updated": datetime.now().isoformat()
        }
        temp_path = path + ".tmp"
        try:
            with open(temp_path, 'w') as f:
                json.dump(data, f, indent=4)
            os.replace(temp_path, path)
        except Exception as e:
            logger.error(f"Failed to save work cache: {e}")

    def clear_progress(self, doc_hash: str, chunk_size: int):
        path = self._get_path(doc_hash, chunk_size)
        if os.path.exists(path):
            try:
                os.remove(path)
            except Exception as e:
                logger.warning(f"Failed to remove cache entry: {e}")

    def cleanup_old_entries(self, max_age_days: int = 14):
        now = datetime.now()
        count = 0
        try:
            for filename in os.listdir(self.cache_dir):
                if not filename.endswith(".json"):
                    continue
                path = os.path.join(self.cache_dir, filename)
                try:
                    with open(path, 'r') as f:
                        data = json.load(f)
                        last_updated = datetime.fromisoformat(data['last_updated'])
                        if (now - last_updated).days >= max_age_days:
                            os.remove(path)
                            count += 1
                except Exception:
                    # If we can't read it or it's malformed, maybe keep it or delete it. 
                    # Let's just skip for now to be safe.
                    pass
            if count > 0:
                logger.info(f"Cleaned up {count} old work-cache entries.")
        except Exception as e:
            logger.error(f"Error during cache cleanup: {e}")

def chunked_summarize(engine, content: str, filepath: str, chunk_size: int, doc_hash: str) -> str:
    num_chunks = math.ceil(len(content) / chunk_size)
    filename = os.path.basename(filepath)
    
    if num_chunks == 0: return ""
    if num_chunks == 1:
        logger.info(f"[{filename}] Summarizing directly...")
        prompt = f"The following is text from '{filepath}'. Please provide a detailed summary:\n\n{content}"
        return engine.generate(prompt, max_output_tokens=1500)
        
    cache = WorkCache()
    chunk_summaries, start_index = cache.load_progress(doc_hash, chunk_size)
    if start_index > 0:
        logger.info(f"[{filename}] Resuming from chunk {start_index+1}/{num_chunks}...")

    for i in range(start_index, num_chunks):
        start = i * chunk_size
        end = start + chunk_size
        chunk = content[start:end]

        logger.info(f"[{filename}] Summarizing chunk {i+1}/{num_chunks}...")
        prompt = f"Briefly summarize this part of document '{filepath}':\n\n{chunk}"
        output = engine.generate(prompt, max_output_tokens=500)
        chunk_summaries.append(output)
        cache.save_progress(doc_hash, chunk_size, chunk_summaries, i + 1, filepath)

    logger.info(f"[{filename}] Consolidating...")
    consolidated_text = "\n\n".join(chunk_summaries)
    final_prompt = f"The following are summaries of segments from '{filepath}'. Please combine them into a single coherent summary:\n\n{consolidated_text}"
    summary = engine.generate(final_prompt, max_output_tokens=1500)
    
    cache.clear_progress(doc_hash, chunk_size)
    return summary

def split_header_content(text: str) -> tuple[str, str]:
    if not text.startswith("---\n"): return ("", text)
    parts = text.split("---\n", 2)
    if len(parts) < 3: return ("", text)
    return (parts[1], parts[2])

def parse_markdown(md_text: str):
    frontmatter, content = split_header_content(md_text)
    try:
        yaml_metadata = yaml.safe_load(frontmatter) if frontmatter else {}
    except:
        yaml_metadata = {}
    return yaml_metadata, content

def summarize_file(source_file: str, destination_file: str):
    config = get_platform_config()
    api_key = config.get("api_key")
    chunk_size = config.get("chunk_size", 200000)

    if not api_key:
        logger.error("No api_key found in platform config.")
        sys.exit(1)

    with open(source_file, 'r', encoding='utf-8') as f:
        content = f.read()
    metadata, md_text = parse_markdown(content)
    
    # Calculate document hash early for work-cache and metadata
    doc_hash = hashlib.sha256(md_text.encode('utf-8')).hexdigest()
    
    # Initialize cache and perform background cleanup
    cache = WorkCache()
    cache.cleanup_old_entries()

    engine = GemmaEngine(api_key)
    summary_text = chunked_summarize(engine, md_text, os.path.basename(source_file), chunk_size, doc_hash)
    
    # Simple metadata preservation
    sum_metadata = {}
    for key in ['title', 'authors', 'tags', 'uuid']:
        if key in metadata: sum_metadata[key] = metadata[key]
    sum_metadata['summary_version'] = f"{MODEL_NAME} {VERSION}"
    # Track which version of the markdown was used for this summary
    sum_metadata['source_md_hash'] = doc_hash
    
    header = yaml.dump(sum_metadata, default_flow_style=False, indent=2)
    full_summary = f"---\n{header}---\n{summary_text}"
    
    os.makedirs(os.path.dirname(destination_file), exist_ok=True)
    temp_file = destination_file + ".tmp"
    with open(temp_file, 'w', encoding='utf-8') as f:
        f.write(full_summary)
        f.flush()
        os.fsync(f.fileno())
    os.replace(temp_file, destination_file)
    logger.info(f"Successfully wrote summary: {destination_file}")

def main():
    # Start watchdog thread
    w = threading.Thread(target=watchdog, daemon=True)
    w.start()

    parser = argparse.ArgumentParser(description="Summarize using Gemma 4 31B")
    parser.add_argument("source", help="Source markdown file")
    parser.add_argument("destination", help="Destination summary file")
    args = parser.parse_args()
    
    summarize_file(args.source, args.destination)

if __name__ == "__main__":
    main()
