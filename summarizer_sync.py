import os
import sys
import json
import logging
import argparse
import subprocess
from concurrent.futures import ProcessPoolExecutor, as_completed

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

def get_config():
    config_file = os.path.expanduser("~/.config/summarizer/summarizer_config.json")
    config = None
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
    except FileNotFoundError:
        pass
    if config is None:
        config = {
            "markdown_path": os.path.expanduser("~/ReferenceLibrary/MarkdownLibrary"),
            "target_series": ["anthropology", "music", "history"],
            "summaries_path": os.path.expanduser("~/ReferenceLibrary/Summaries"),
            "chunk_size": 25000,
            "api_key": "",
            "model_name": "gemma-4-31b-it",
            "hosted": True
        }
        os.makedirs(os.path.dirname(config_file), exist_ok=True)
        with open(config_file, 'w') as f:
            f.write(json.dumps(config, indent=4))
    
    if "summaries_path" not in config:
        config["summaries_path"] = os.path.expanduser("~/ReferenceLibrary/Summaries")
        with open(config_file, 'w') as f:
            f.write(json.dumps(config, indent=4))
    if "chunk_size" not in config:
        config["chunk_size"] = 25000
        with open(config_file, 'w') as f:
            f.write(json.dumps(config, indent=4))

    return config

def process_markdown_file(source_file: str, target_file: str, model_name: str, is_dry_run: bool, hosted: bool):
    if os.path.exists(target_file):
        logging.info(f"Skipping summarization: Target file already exists at {target_file}")
        return

    if hosted is True:
        summarizer_dir = os.path.join(os.path.dirname(__file__), "summarizer_googlecloud")
        summarizer_script = os.path.join(summarizer_dir, "summarizer_googlecloud.py")
    elif sys.platform == "darwin":
        summarizer_dir = os.path.join(os.path.dirname(__file__), "summarizer_macos")
        summarizer_script = os.path.join(summarizer_dir, "summarizer_macos.py")
    else:
        summarizer_dir = os.path.join(os.path.dirname(__file__), "summarizer_linux")
        summarizer_script = os.path.join(summarizer_dir, "summarizer_linux.py")

    venv_python = os.path.join(summarizer_dir, ".venv", "bin", "python")
    python_exe = venv_python if os.path.exists(venv_python) else sys.executable

    logging.info(f"Dispatching Summarization: {source_file} -> {target_file}")
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

def sync_summaries(concurrency: int, is_dry_run: bool):
    config = get_config()
    markdown_path = config.get("markdown_path")
    summaries_path = config.get("summaries_path")
    model_name = config.get("model_name", "gemini-1.5-flash")
    target_series = [s.lower() for s in config.get("target_series", [])]
    hosted = config.get("hosted", False)
    
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

    with ProcessPoolExecutor(max_workers=concurrency) as executor:
        futures = {executor.submit(process_markdown_file, src, tgt, model_name, is_dry_run, hosted): (src, tgt) for src, tgt in tasks}
            
        for future in as_completed(futures):
            try:
                ret_code = future.result()
                if ret_code == 10:
                    logging.warning("Daily quota reached signal received (code 10). Stopping further tasks...")
                    # Shutdown and cancel pending futures
                    executor.shutdown(wait=False, cancel_futures=True)
                    break
            except Exception as e:
                logging.error(f"Task generated an exception: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Markdown Summarizer orchestrator")
    parser.add_argument("--concurrency", type=int, default=1, help="Number of parallel summarization processes (default 1)")
    parser.add_argument("--dry-run", action="store_true", help="Print operations without performing summaries")
    args = parser.parse_args()
    
    sync_summaries(args.concurrency, args.dry_run)
