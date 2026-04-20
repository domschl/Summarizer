import os
import sys
import argparse
import signal
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("summarizer_macos")

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
from mlx_vlm import load, generate
from mlx_vlm.prompt_utils import apply_chat_template
from mlx_vlm.utils import load_config

from summarizer_core.engine import BaseEngine
from summarizer_core.cache import WorkCache
from summarizer_core.markdown import parse_markdown, assemble_markdown
from summarizer_core.summarizer import chunked_summarize
from summarizer_core.utils import get_platform_config, watchdog

# Constants
VERSION = "0.1.0"
MODEL_NAME = "gemma-4-26b-it-mlx"

class MLXEngine(BaseEngine):
    def __init__(self, model_id: str = "mlx-community/gemma-4-26b-a4b-it-4bit"):
        logger.info(f"Loading MLX model from {model_id}...")
        self.model, self.processor = load(model_id)
        self.config = load_config(model_id)

    def format_prompt(self, messages: list[dict[str, str]]) -> str:
        return apply_chat_template(self.processor, self.config, messages, num_images=0)

    def generate(self, prompt: str, max_tokens: int = 1500, temp: float = 0.2, repetition_penalty: float = 1.1) -> str:
        output = generate(
            self.model, self.processor, prompt, [],
            max_tokens=max_tokens,
            temp=temp,
            repetition_penalty=repetition_penalty,
            kv_bits=3.5,
            kv_quant_scheme="turboquant",
            verbose=False
        )
        if hasattr(output, "text"):
            text = str(getattr(output, "text"))
        else:
            text = str(output)
        return text

def summarize_file(source_file: str, destination_file: str):
    if not os.path.exists(source_file):
        logger.error(f"Error: Source file does not exist: {source_file}")
        sys.exit(1)
        
    try:
        with open(source_file, 'r', encoding='utf-8') as f:
            content = f.read()
            
        config = get_platform_config("summarizer_config_macos.json")
        chunk_size = config.get("chunk_size", 50000)
        
        metadata, md_text = parse_markdown(content)
        
        source_md_hash = hashlib.sha256(content.encode('utf-8')).hexdigest()
        
        cache = WorkCache()
        cache.cleanup_old_entries()
        
        logger.info(f"Initializing MLX Engine...")
        engine = MLXEngine()
        
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

    parser = argparse.ArgumentParser(description="Summarize Markdown using MLX (macOS)")
    parser.add_argument("source", help="Path to the source markdown file")
    parser.add_argument("destination", help="Path to the destination summary markdown file")
    
    args = parser.parse_args()
    summarize_file(args.source, args.destination)

if __name__ == "__main__":
    main()
