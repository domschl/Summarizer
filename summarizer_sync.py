import os
import sys
import json
import logging
import argparse
import subprocess
import signal
import multiprocessing
import re
import yaml
from concurrent.futures import ProcessPoolExecutor, as_completed

# Suppress KeyboardInterrupt exception stack traces globally
signal.signal(signal.SIGINT, signal.SIG_DFL)

from naming import generate_summary_filename, check_collisions, compute_file_hash
from summarizer_core.cache import WorkCache

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')


def get_config():
    config_file = os.path.expanduser("~/.config/summarizer/summarizer_config.json")
    config = None
    try:
        if os.path.exists(config_file):
            with open(config_file, 'r') as f:
                content = f.read()
            # Handle trailing commas (non-standard JSON)
            content = re.sub(r',\s*}', '}', content)
            content = re.sub(r',\s*]', ']', content)
            config = json.loads(content)
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


def split_header_content(text: str) -> tuple[str, str]:
    separator1 = "---\n"
    separator2 = "\n---\n"
    if text.startswith(separator1):
        d1 = 0
    else:
        d1 = text.find(separator2)
        if d1 > 10 or d1 == -1:
            return ("", text)
        d1 += 1

    d2 = text[d1+len(separator1):].find(separator2)
    if d2 == -1:
        return ("", text)
    d2 += d1+len(separator1)
    header = text[d1+len(separator1):d2+1]
    content = text[d2+len(separator2):]
    return (header, content)


def parse_frontmatter(filepath: str) -> dict | None:
    """Read a markdown file and return its YAML frontmatter as a dict."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
    except Exception:
        return None
    
    header, _ = split_header_content(content)
    if not header:
        return None
    try:
        return yaml.safe_load(header)
    except Exception:
        return None


def update_frontmatter_inplace(filepath: str, updates: dict):
    """Update specific fields in a file's YAML frontmatter."""
    with open(filepath, 'r', encoding='utf-8') as f:
        text = f.read()
    
    header, content = split_header_content(text)
    try:
        metadata = yaml.safe_load(header) if header else {}
    except Exception:
        metadata = {}
    if metadata is None:
        metadata = {}
    
    metadata.update(updates)
    
    filtered = {}
    for k, v in metadata.items():
        if isinstance(v, list) and len(v) == 0:
            continue
        if isinstance(v, str) and v == "":
            continue
        filtered[k] = v
    
    new_header = yaml.dump(filtered, default_flow_style=False, indent=2)
    if not new_header.endswith("\n"):
        new_header += "\n"
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(f"---\n{new_header}---\n{content}")
        f.flush()
        os.fsync(f.fileno())


# ─── Two-phase sync ───────────────────────────────────────────────────────────

def scan_existing_summaries(summaries_path: str) -> dict[str, dict]:
    """Scan existing summary files. Returns {uuid: info_dict}."""
    index = {}
    for root, dirs, files in os.walk(summaries_path):
        for filename in files:
            if not filename.endswith('.md'):
                continue
            filepath = os.path.join(root, filename)
            meta = parse_frontmatter(filepath)
            if not meta or not meta.get('uuid'):
                continue
            
            uuid = meta['uuid']
            series = os.path.relpath(root, summaries_path)
            index[uuid] = {
                'path': filepath,
                'filename': filename,
                'series': series,
                'title': meta.get('title', ''),
                'authors': meta.get('authors', []),
                'source_md_hash': meta.get('source_md_hash', ''),
                'summary_version': meta.get('summary_version', ''),
                'metadata': meta,
            }
    return index


def scan_markdown_sources(markdown_path: str, target_series: list[str]) -> dict[str, dict]:
    """Scan markdown files (source of truth for summaries). Returns {uuid: info_dict}."""
    index = {}
    for root, dirs, files in os.walk(markdown_path):
        for filename in files:
            if not filename.endswith('.md'):
                continue
            filepath = os.path.join(root, filename)
            meta = parse_frontmatter(filepath)
            if not meta or not meta.get('uuid'):
                continue
            
            uuid = meta['uuid']
            series = os.path.relpath(root, markdown_path)
            
            # Filter by series if configured
            if target_series:
                if series.lower() not in target_series and series != '.':
                    continue
                if series == '.' and 'unspecified_series' not in target_series:
                    continue
            
            title = meta.get('title', '')
            authors = meta.get('authors', [])
            first_author = authors[0] if authors else ''
            content_hash = compute_file_hash(filepath)
            
            index[uuid] = {
                'path': filepath,
                'filename': filename,
                'series': series,
                'title': title,
                'authors': authors,
                'first_author': first_author,
                'content_hash': content_hash,
                'expected_summary_filename': generate_summary_filename(title, first_author),
                'metadata': meta,
            }
    return index


def plan_summary_sync(markdown_index: dict, summary_index: dict, summaries_path: str) -> list[dict]:
    """
    Compare markdown sources and existing summaries, returning a list of actions.
    
    Actions: ADD, REMOVE, RESUMMARISE, RENAME, MOVE, SKIP
    """
    actions = []
    
    md_uuids = set(markdown_index.keys())
    sum_uuids = set(summary_index.keys())
    
    # Markdowns without a summary → ADD
    for uuid in md_uuids - sum_uuids:
        mi = markdown_index[uuid]
        target_dir = os.path.join(summaries_path, mi['series'])
        target_file = os.path.join(target_dir, mi['expected_summary_filename'])
        actions.append({
            'action': 'ADD',
            'uuid': uuid,
            'title': mi['title'],
            'source_file': mi['path'],
            'content_hash': mi['content_hash'],
            'target_file': target_file,
            'target_series': mi['series'],
        })
    
    # Summaries with no corresponding markdown → REMOVE
    for uuid in sum_uuids - md_uuids:
        si = summary_index[uuid]
        actions.append({
            'action': 'REMOVE',
            'uuid': uuid,
            'title': si['title'],
            'path': si['path'],
        })
    
    # Both exist → check for changes
    for uuid in md_uuids & sum_uuids:
        mi = markdown_index[uuid]
        si = summary_index[uuid]
        
        # Check if source markdown content has changed
        if si['source_md_hash'] and si['source_md_hash'] != mi['content_hash']:
            target_dir = os.path.join(summaries_path, mi['series'])
            target_file = os.path.join(target_dir, mi['expected_summary_filename'])
            actions.append({
                'action': 'RESUMMARISE',
                'uuid': uuid,
                'title': mi['title'],
                'old_path': si['path'],
                'source_file': mi['path'],
                'content_hash': mi['content_hash'],
                'target_file': target_file,
                'target_series': mi['series'],
            })
            continue
        
        # Check if filename changed
        expected_fn = mi['expected_summary_filename']
        needs_rename = si['filename'] != expected_fn
        
        # Check if series changed
        needs_move = si['series'] != mi['series']
        
        if needs_rename or needs_move:
            target_dir = os.path.join(summaries_path, mi['series'])
            target_file = os.path.join(target_dir, expected_fn)
            actions.append({
                'action': 'RENAME' if needs_rename else 'MOVE',
                'uuid': uuid,
                'title': mi['title'],
                'old_path': si['path'],
                'new_path': target_file,
                'old_filename': si['filename'],
                'new_filename': expected_fn,
            })
            continue
        
        # Check if source_md_hash is missing (needs backfill)
        if not si['source_md_hash']:
            actions.append({
                'action': 'UPDATE_HASH',
                'uuid': uuid,
                'title': mi['title'],
                'path': si['path'],
                'content_hash': mi['content_hash'],
            })
            continue
        
        actions.append({
            'action': 'SKIP',
            'uuid': uuid,
            'title': mi['title'],
        })
    
    return actions


def process_markdown_file(source_file: str, target_file: str, content_hash: str, is_dry_run: bool):
    """Dispatch summarisation for a single markdown file."""
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

    if is_dry_run:
        logging.info(f"Dispatching Summarization: {source_file} -> {target_file} using {summarizer_name}")
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
            # Backfill the source_md_hash into the summary
            if os.path.exists(target_file) and content_hash:
                try:
                    update_frontmatter_inplace(target_file, {'source_md_hash': content_hash})
                except Exception as e:
                    logging.error(f"Failed to update source_md_hash: {e}")
            
        return p.returncode
            
    except Exception as e:
        logging.error(f"Failed handling sub-process for {source_file}: {e}")
        return -1


# Global executor to allow signal handler access
_executor = None

def signal_handler(sig, frame):
    # This might not be hit if SIG_DFL takes over, but kept for legacy
    global _executor
    if _executor:
        _executor.shutdown(wait=False, cancel_futures=True)
    os._exit(1)

def sync_summaries(concurrency: int, is_dry_run: bool):
    global _executor
    config = get_config()
    
    # Initialize cache early
    cache = WorkCache()
    active_caches = cache.get_all_active_caches()
    if active_caches:
        logging.info(f"Found {len(active_caches)} active cache entries (in progress summaries).")
        
    markdown_path = config.get("markdown_path")
    summaries_path = config.get("summaries_path")
    target_series = [s.lower() for s in config.get("target_series", [])]
    
    if not markdown_path or not os.path.exists(markdown_path):
        logging.error(f"Markdown path does not exist: {markdown_path}")
        return
        
    if not os.path.exists(summaries_path):
        if not is_dry_run:
            os.makedirs(summaries_path, exist_ok=True)

    # ─── Phase 1: Plan ────────────────────────────────────────────────
    logging.info("Phase 1: Scanning existing summary files...")
    summary_index = scan_existing_summaries(summaries_path)
    logging.info(f"  Found {len(summary_index)} existing summaries with UUIDs")

    logging.info("Phase 1: Scanning markdown sources...")
    markdown_index = scan_markdown_sources(markdown_path, target_series)
    logging.info(f"  Found {len(markdown_index)} markdown files (matching series filter)")

    logging.info("Phase 1: Generating sync plan...")
    actions = plan_summary_sync(markdown_index, summary_index, summaries_path)
    
    # Summarise the plan
    action_counts = {}
    for a in actions:
        t = a['action']
        action_counts[t] = action_counts.get(t, 0) + 1
    
    logging.info("Phase 1: Sync plan summary:")
    for action_type in ['ADD', 'REMOVE', 'RESUMMARISE', 'RENAME', 'MOVE', 'UPDATE_HASH', 'SKIP']:
        count = action_counts.get(action_type, 0)
        if count > 0:
            logging.info(f"  {action_type}: {count}")
    
    executable_actions = [a for a in actions if a['action'] != 'SKIP']
    if not executable_actions:
        logging.info("Nothing to do. All summaries are up to date.")
        return
    
    logging.info(f"\nPhase 2: Executing {len(executable_actions)} action(s)...")

    # ─── Phase 2: Execute ─────────────────────────────────────────────
    # Sequential actions first
    sequential_types = ('REMOVE', 'RENAME', 'MOVE', 'UPDATE_HASH')
    sequential = [a for a in executable_actions if a['action'] in sequential_types]
    parallel = [a for a in executable_actions if a['action'] not in sequential_types]
    
    total = len(executable_actions)
    idx = 1
    
    for action in sequential:
        action_type = action['action']
        prefix = f"[{idx}/{total}]"
        
        if action_type == 'REMOVE':
            logging.info(f"{prefix} REMOVE: {action['title']} ({action['path']})")
            if not is_dry_run:
                try:
                    os.remove(action['path'])
                except Exception as e:
                    logging.error(f"{prefix} Failed to remove: {e}")
        
        elif action_type in ('RENAME', 'MOVE'):
            old = action['old_path']
            new = action['new_path']
            logging.info(f"{prefix} {action_type}: {action.get('old_filename', 'N/A')} -> {action.get('new_filename', 'N/A')}")
            if not is_dry_run:
                try:
                    target_dir = os.path.dirname(new)
                    os.makedirs(target_dir, exist_ok=True)
                    os.rename(old, new)
                except Exception as e:
                    logging.error(f"{prefix} {action_type} failed: {e}")
        
        elif action_type == 'UPDATE_HASH':
            logging.info(f"{prefix} UPDATE_HASH: {os.path.basename(action['path'])}")
            if not is_dry_run:
                try:
                    update_frontmatter_inplace(action['path'], {
                        'source_md_hash': action['content_hash']
                    })
                except Exception as e:
                    logging.error(f"{prefix} UPDATE_HASH failed: {e}")
        
        idx += 1
    
    # Clean up caches for files that are already completed
    for action in actions:
        if action['action'] == 'SKIP':
            mi = markdown_index.get(action['uuid'])
            if mi and mi['content_hash'] in active_caches:
                logging.info(f"Summary up-to-date for {action['title']}. Deleting stale cache entries.")
                if not is_dry_run:
                    cache.clear_by_hash_only(mi['content_hash'])

    # Parallel actions (ADD, RESUMMARISE)
    if parallel:
        # Sort parallel actions: those with an active cache go first (0), then the rest (1)
        parallel.sort(key=lambda a: 0 if a.get('content_hash') in active_caches else 1)

        _executor = ProcessPoolExecutor(max_workers=concurrency)
        try:
            futures = {}
            for action in parallel:
                action_type = action['action']
                source = action['source_file']
                target = action['target_file']
                content_hash = action.get('content_hash', '')
                
                logging.info(f"[{idx}/{total}] {action_type}: {action['title']}")
                
                if action_type == 'RESUMMARISE' and not is_dry_run:
                    old_path = action.get('old_path')
                    if old_path and os.path.exists(old_path):
                        os.remove(old_path)
                
                future = _executor.submit(process_markdown_file, source, target, content_hash, is_dry_run)
                futures[future] = action
                idx += 1
            
            for future in as_completed(futures):
                try:
                    ret_code = future.result()
                    if ret_code == 10:
                        logging.warning("Daily quota reached signal received (code 10). Stopping further tasks...")
                        _executor.shutdown(wait=False, cancel_futures=True)
                        break
                except Exception as e:
                    action = futures[future]
                    logging.error(f"Task generated an exception for '{action['title']}': {e}")
        except KeyboardInterrupt:
            logging.warning("Interrupted by user. Shutting down...")
            if _executor:
                _executor.shutdown(wait=False, cancel_futures=True)
            raise
        finally:
            if _executor:
                _executor.shutdown(wait=False, cancel_futures=True)
                _executor = None
    
    logging.info("Sync complete.")

if __name__ == "__main__":
    # Ensure subprocesses are started cleanly on all platforms
    try:
        multiprocessing.set_start_method('spawn')
    except RuntimeError:
        pass # Already set

    # Register signal handlers
    # signal.signal(signal.SIGINT, signal_handler)
    # signal.signal(signal.SIGTERM, signal_handler)

    parser = argparse.ArgumentParser(description="Markdown Summarizer orchestrator")
    parser.add_argument("--concurrency", type=int, default=1, help="Number of parallel summarization processes (default 1)")
    parser.add_argument("--dry-run", action="store_true", help="Print operations without performing summaries")
    args = parser.parse_args()
    
    try:
        sync_summaries(args.concurrency, args.dry_run)
    except KeyboardInterrupt:
        pass
