import os
import sys
import json
import argparse
import time
import re
import logging
import threading
import hashlib
from datetime import datetime
from google import genai
from google.genai import types

from summarizer_core.engine import BaseEngine
from summarizer_core.cache import WorkCache
from summarizer_core.markdown import parse_markdown, assemble_markdown
from summarizer_core.summarizer import chunked_summarize
from summarizer_core.utils import watchdog

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("gc_gemma4_31b")

# Noise filter for external libraries
class NoiseFilter(logging.Filter):
    def filter(self, record):
        msg = record.getMessage()
        if "AFC is enabled" in msg: return False
        if "HTTP Request" in msg and "200 OK" in msg: return False
        return True

for handler in logging.root.handlers:
    handler.addFilter(NoiseFilter())

VERSION = "0.1.0"
MODEL_NAME = "gemma-4-31b-it"

def get_platform_config():
    # Local specialized config loader since it modifies the config too
    config_file = os.path.expanduser("~/.config/summarizer/summarizer_config_gc_gemma4_31b.json")
    try:
        if os.path.exists(config_file):
            with open(config_file, 'r') as f:
                return json.load(f)
    except Exception as e:
        logger.error(f"Error loading platform config: {e}")
    return { "chunk_size": 200000, "api_key": "", "rate_blocked_until": "" }

def update_block_until(timestamp_str: str):
    config_path = os.path.expanduser("~/.config/summarizer/summarizer_config_gc_gemma4_31b.json")
    try:
        config = get_platform_config()
        current_blocked = config.get("rate_blocked_until", "")
        if timestamp_str > current_blocked:
            config["rate_blocked_until"] = timestamp_str
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
    while True:
        config = get_platform_config()
        blocked_until_str = config.get("rate_blocked_until", "")
        if not blocked_until_str: break
        try:
            blocked_until = datetime.fromisoformat(blocked_until_str)
            now = datetime.now()
            if now < blocked_until:
                wait_seconds = (blocked_until - now).total_seconds()
                if wait_seconds > 0:
                    logger.info(f"Rate limited. Waiting {wait_seconds:.1f}s until {blocked_until_str}...")
                    time.sleep(min(wait_seconds, 10))
                    continue
        except Exception:
            pass
        break

def parse_retry_delay(exception):
    try:
        if hasattr(exception, "details") and exception.details:
            for detail in exception.details:
                if isinstance(detail, dict) and "retry_delay" in detail:
                    delay_str = str(detail["retry_delay"])
                    match = re.search(r"([\d\.]+)", delay_str)
                    if match: return float(match.group(1))
        match = re.search(r"'retryDelay':\s*'([\d\.]+)s'", str(exception))
        if match: return float(match.group(1))
    except Exception: pass
    return None

class GemmaEngine(BaseEngine):
    def __init__(self, api_key: str):
        self.client = genai.Client(api_key=api_key)
        self.last_request_time = 0
        self.min_delay = 4.1

    def generate(self, prompt: str, max_tokens: int = 1500, temp: float = 0.2, repetition_penalty: float = 1.1) -> str:
        attempts = 0
        max_attempts = 10
        backoff = 10

        while attempts < max_attempts:
            check_rate_limit()
            
            elapsed = time.time() - self.last_request_time
            if elapsed < self.min_delay:
                time.sleep(self.min_delay - elapsed)

            try:
                response = self.client.models.generate_content(
                    model=MODEL_NAME,
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        max_output_tokens=max_tokens,
                        temperature=temp,
                    )
                )
                self.last_request_time = time.time()
                if not response.text: return "[Summary blocked or empty response]"
                return response.text.strip()
            except Exception as e:
                attempts += 1
                msg = str(e).lower()
                if "rate limit" in msg or ("quota" in msg and "daily" in msg):  
                    logger.error("Daily API Quota reached.")
                    from datetime import timedelta
                    tomorrow = datetime.now().replace(hour=8, minute=0, second=0, microsecond=0) + timedelta(days=1)
                    update_block_until(tomorrow.isoformat())
                    sys.exit(10)
                if "429" in msg or "resource_exhausted" in msg or "rate limit" in msg:
                    delay = parse_retry_delay(e) or backoff
                    logger.warning(f"Rate limit hit. Suggest retry in {delay}s. Attempt {attempts}/{max_attempts}")
                    time.sleep(delay)
                    backoff *= 2
                    continue
                if "503" in msg or "unavailable" in msg or "deadline_exceeded" in msg:
                    logger.warning(f"Transient error: {e}. Retrying in {backoff}s...")
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                raise e
        raise Exception("Max retry attempts reached.")

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
    
    doc_hash = hashlib.sha256(md_text.encode('utf-8')).hexdigest()
    
    cache = WorkCache()
    cache.cleanup_old_entries()

    engine = GemmaEngine(api_key)
    summary_text = chunked_summarize(engine, md_text, source_file, chunk_size, doc_hash)
    
    sum_metadata = {}
    if metadata:
        for key in ['title', 'authors', 'tags', 'uuid']:
            if key in metadata: sum_metadata[key] = metadata[key]
    sum_metadata['summary_version'] = f"{MODEL_NAME} {VERSION}"
    sum_metadata['source_md_hash'] = doc_hash
    
    full_summary = assemble_markdown(sum_metadata, summary_text)
    
    os.makedirs(os.path.dirname(destination_file), exist_ok=True)
    temp_file = destination_file + ".tmp"
    with open(temp_file, 'w', encoding='utf-8') as f:
        f.write(full_summary)
        f.flush()
        os.fsync(f.fileno())
    os.replace(temp_file, destination_file)
    logger.info(f"Successfully wrote summary: {destination_file}")

def main():
    w = threading.Thread(target=watchdog, daemon=True)
    w.start()

    parser = argparse.ArgumentParser(description="Summarize using Gemma 4 31B")
    parser.add_argument("source", help="Source markdown file")
    parser.add_argument("destination", help="Destination summary file")
    args = parser.parse_args()
    
    summarize_file(args.source, args.destination)

if __name__ == "__main__":
    main()
