import os
import sys
import argparse
import signal
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("summarizer_linux")

# Noise filter for external libraries
class NoiseFilter(logging.Filter):
    def filter(self, record):
        msg = record.getMessage()
        if "AFC is enabled" in msg: return False
        if "HTTP Request" in msg and "200 OK" in msg: return False
        return True

for handler in logging.root.handlers:
    handler.addFilter(NoiseFilter())

# Suppress KeyboardInterrupt globally
signal.signal(signal.SIGINT, signal.SIG_DFL)
import threading
import hashlib
from huggingface_hub import hf_hub_download
from llama_cpp import Llama

from summarizer_core.engine import BaseEngine
from summarizer_core.cache import WorkCache
from summarizer_core.markdown import parse_markdown, assemble_markdown
from summarizer_core.summarizer import chunked_summarize
from summarizer_core.utils import get_platform_config, watchdog

# Constants
VERSION = "0.1.0"
MODEL_NAME = "gemma-4-26b-it-gguf"

class LlamaCppEngine(BaseEngine):
    def __init__(self, repo_id: str = "unsloth/gemma-4-26B-A4B-it-GGUF", filename: str = "gemma-4-26B-A4B-it-UD-Q4_K_XL.gguf"):
        logger.info(f"Loading LlamaCpp model from {repo_id}...")
        model_path = hf_hub_download(repo_id=repo_id, filename=filename)
        
        self.llm = Llama(
            model_path=model_path,
            n_gpu_layers=12,
            n_ctx=32768,
            flash_attn=True,
            verbose=False
        )

    def format_prompt(self, messages: list[dict[str, str]]) -> str:
        formatted = ""
        for msg in messages:
            role = msg["role"]
            content = msg["content"]
            formatted += f"<start_of_turn>{role}\n{content}<end_of_turn>\n"
        formatted += "<start_of_turn>model\n"
        return formatted

    def generate(self, prompt: str, max_tokens: int = 1500, temp: float = 0.2, repetition_penalty: float = 1.1) -> str:
        output = self.llm(
            prompt,
            max_tokens=max_tokens,
            temperature=temp,
            repeat_penalty=repetition_penalty,
            stop=["<end_of_turn>"]
        )
        return output["choices"][0]["text"].strip()

def summarize_file(source_file: str, destination_file: str):
    if not os.path.exists(source_file):
        logger.error(f"Source file does not exist: {source_file}")
        sys.exit(1)
        
    try:
        with open(source_file, 'r', encoding='utf-8') as f:
            content = f.read()
            
        config = get_platform_config("summarizer_config_linux.json")
        chunk_size = config.get("chunk_size", 50000)
        
        metadata, md_text = parse_markdown(content)
        
        source_md_hash = hashlib.sha256(content.encode('utf-8')).hexdigest()
        
        # Initialize cache and cleanup
        cache = WorkCache()
        cache.cleanup_old_entries()
        
        logger.info(f"Initializing LlamaCpp Engine...")
        engine = LlamaCppEngine()
        
        summary_text = chunked_summarize(engine, md_text, source_file, chunk_size, source_md_hash)
        
        sum_metadata = {}
        if metadata:
            for key in ['title', 'authors', 'tags', 'uuid']:
                if key in metadata:
                    sum_metadata[key] = metadata[key]
        
        sum_metadata['summary_version'] = f"{MODEL_NAME} {VERSION}"
        sum_metadata['source_md_hash'] = source_md_hash
                    
        full_summary = assemble_markdown(sum_metadata, summary_text)
        
        target_dir = os.path.dirname(destination_file)
        if target_dir and not os.path.exists(target_dir):
            os.makedirs(target_dir, exist_ok=True)
            
        temp_file = destination_file + ".tmp"
        with open(temp_file, 'w', encoding='utf-8') as f:
            f.write(full_summary)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_file, destination_file)
            
        logger.info(f"Successfully wrote summary to: {destination_file}")
        sys.exit(0)
    except Exception as e:
        logger.exception(f"Error during summarization: {e}")
        sys.exit(1)

def main():
    w = threading.Thread(target=watchdog, daemon=True)
    w.start()

    parser = argparse.ArgumentParser(description="Summarize Markdown using Llama.cpp (Linux)")
    parser.add_argument("source", help="Path to the source markdown file")
    parser.add_argument("destination", help="Path to the destination summary markdown file")
    
    args = parser.parse_args()
    summarize_file(args.source, args.destination)

if __name__ == "__main__":
    main()
