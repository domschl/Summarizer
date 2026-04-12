import os
import sys
import json
import logging
import argparse
import subprocess
import signal
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

def get_config():
    config_file = os.path.expanduser("~/.config/summarizer/summarizer_config.json")
    config = None
    try:
        if os.path.exists(config_file):
            with open(config_file, 'r') as f:
                config = json.load(f)
    except Exception as e:
        logging.error(f"Error loading config: {e}")
        
    if config is None:
        # Default summarizer name based on platform
        if sys.platform == "darwin":
            default_name = "summarizer_macos"
        else:
            default_name = "summarizer_linux"

        config = {
            "markdown_path": os.path.expanduser("~/AINotes/MarkdownBooks"),
            "target_series": ["anthropology", "music", "history"],
            "summaries_path": os.path.expanduser("~/AINotes/BookSummaries"),
            "summarizer_name": default_name
        }
        os.makedirs(os.path.dirname(config_file), exist_ok=True)
        with open(config_file, 'w') as f:
            f.write(json.dumps(config, indent=4))
    
    return config

def process_markdown_file(source_file: str, target_file: str, is_dry_run: bool):
    if os.path.exists(target_file):
        logging.info(f"Skipping summarization: Target file already exists at {target_file}")
        return

    config = get_config()
    summarizer_name = config.get("summarizer_name")
    
    # Map summarizer_name to directory and script
    summarizer_dir = os.path.join(os.path.dirname(__file__), summarizer_name)
    summarizer_script = os.path.join(summarizer_dir, f"{summarizer_name}.py")

    if not os.path.exists(summarizer_script):
        logging.error(f"Summarizer script not found: {summarizer_script}")
        return -1

    venv_python = os.path.join(summarizer_dir, ".venv", "bin", "python")
    python_exe = venv_python if os.path.exists(venv_python) else sys.executable

    logging.info(f"Dispatching Summarization: {source_file} -> {target_file} using {summarizer_name}")
    if is_dry_run:
        return
        
    try:
        target_dir = os.path.dirname(target_file)
        if target_dir:
            os.makedirs(target_dir, exist_ok=True)

        cmd = [python_exe, summarizer_script, source_file, target_file]
        # We don't capture output here so the progress bars/chunk lines natively print to stdout.
        p = subprocess.Popen(cmd)
        try:
            p.wait()
        except BaseException:
            p.kill()
            p.wait()
            raise
        
        if p.returncode != 0:
            logging.error(f"Summarization failed for {source_file} (code {p.returncode})")
        else:
            logging.info(f"Completed Summarization for: {target_file}")
            
        return p.returncode
            
    except Exception as e:
        logging.error(f"Failed handling sub-process for {source_file}: {e}")
        return -1


# Global executor to allow signal handler access
_executor = None

def signal_handler(sig, frame):
    global _executor
    logging.warning(f"Interrupt signal ({sig}) received. Shutting down...")
    if _executor:
        _executor.shutdown(wait=False, cancel_futures=True)
    # Using os._exit to bypass any blocking finally blocks and exit immediately
    os._exit(1)

def sync_summaries(concurrency: int, is_dry_run: bool):
    global _executor
    config = get_config()
    markdown_path = config.get("markdown_path")
    summaries_path = config.get("summaries_path")
    target_series = [s.lower() for s in config.get("target_series", [])]
    
    if not markdown_path or not os.path.exists(markdown_path):
        logging.error(f"Markdown path does not exist: {markdown_path}")
        return
        
    if not os.path.exists(summaries_path):
        if not is_dry_run:
            os.makedirs(summaries_path, exist_ok=True)

    markdown_files = []
    for root, dirs, files in os.walk(markdown_path):
        for file in files:
            if file.endswith(".md"):
                source_file = os.path.join(root, file)
                
                # Filter by series if configured
                if target_series:
                    rel_path = os.path.relpath(source_file, markdown_path)
                    path_parts = rel_path.split(os.sep)
                    if len(path_parts) > 1:
                        series = path_parts[0].lower()
                        if series not in target_series:
                            continue
                    else:
                        # File is in the root of markdown_path, no series
                        if "unspecified_series" not in target_series:
                            continue

                markdown_files.append(source_file)

    logging.info(f"Found {len(markdown_files)} markdown files. Processing with concurrency {concurrency}...")

    tasks = []
    for source_file in markdown_files:
        rel_path = os.path.relpath(source_file, markdown_path)
        target_file = os.path.join(summaries_path, rel_path)
        tasks.append((source_file, target_file))

    _executor = ProcessPoolExecutor(max_workers=concurrency)
    try:
        futures = {_executor.submit(process_markdown_file, src, tgt, is_dry_run): (src, tgt) for src, tgt in tasks}
            
        for future in as_completed(futures):
            try:
                ret_code = future.result()
                if ret_code == 10:
                    logging.warning("Daily quota reached signal received (code 10). Stopping further tasks...")
                    # Shutdown and cancel pending futures
                    _executor.shutdown(wait=False, cancel_futures=True)
                    break
            except Exception as e:
                logging.error(f"Task generated an exception: {e}")
    except KeyboardInterrupt:
        logging.warning("Interrupted by user. Shutting down...")
        if _executor:
            _executor.shutdown(wait=False, cancel_futures=True)
        # Re-raise to let the outer block handle exit
        raise
    finally:
        if _executor:
            # If we're here normally, wait=True is fine. 
            # If we're here due to an interrupt, it might have been shut down already.
            _executor.shutdown(wait=False, cancel_futures=True)
            _executor = None

if __name__ == "__main__":
    # Ensure subprocesses are started cleanly on all platforms
    try:
        multiprocessing.set_start_method('spawn')
    except RuntimeError:
        pass # Already set

    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    parser = argparse.ArgumentParser(description="Markdown Summarizer orchestrator")
    parser.add_argument("--concurrency", type=int, default=1, help="Number of parallel summarization processes (default 1)")
    parser.add_argument("--dry-run", action="store_true", help="Print operations without performing summaries")
    args = parser.parse_args()
    
    try:
        sync_summaries(args.concurrency, args.dry_run)
    except KeyboardInterrupt:
        sys.exit(1)
