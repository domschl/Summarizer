"""
One-time migration script: Rename existing markdown and summary files
to the new wiki-compatible naming scheme.

Usage:
    python migrate_filenames.py                  # dry-run (default)
    python migrate_filenames.py --execute        # perform renames

This script will be removed after the migration is complete.
"""

import os
import sys
import json
import hashlib
import logging
import argparse
import yaml
import shutil

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from naming import generate_filename, generate_summary_filename, check_collisions

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)


def get_config():
    """Load the converter config (for calibre_path and markdown_path)."""
    config_file = os.path.expanduser("~/.config/summarizer/converter_config.json")
    try:
        with open(config_file, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        pass
    # Fallback defaults
    return {
        "calibre_path": os.path.expanduser("~/ReferenceLibrary/Calibre Library"),
        "markdown_path": os.path.expanduser("~/AINotes/MarkdownBooks"),
    }


def get_summarizer_config():
    """Load the summarizer config (for summaries_path)."""
    config_file = os.path.expanduser("~/.config/summarizer/summarizer_config.json")
    try:
        with open(config_file, 'r') as f:
            content = f.read()
        # Handle trailing commas (non-standard JSON)
        import re as _re
        content = _re.sub(r',\s*}', '}', content)
        content = _re.sub(r',\s*]', ']', content)
        return json.loads(content)
    except FileNotFoundError:
        pass
    return {
        "summaries_path": os.path.expanduser("~/AINotes/BookSummaries"),
    }


def split_header_content(text: str) -> tuple[str, str]:
    """Split a markdown file into YAML frontmatter and content."""
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
    except Exception as e:
        logger.error(f"Cannot read {filepath}: {e}")
        return None

    header, _ = split_header_content(content)
    if not header:
        return None
    try:
        return yaml.safe_load(header)
    except Exception:
        return None


def compute_file_hash(filepath: str) -> str:
    """Compute SHA-256 hash of a file."""
    sha256 = hashlib.sha256()
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            sha256.update(chunk)
    return sha256.hexdigest()


def find_source_file(opf_dir: str) -> tuple[str | None, str | None]:
    """
    Find the best source file in a Calibre book directory.
    Returns (filepath, format) or (None, None).
    Priority: md > epub > pdf/docx/pptx/xlsx
    """
    if not os.path.isdir(opf_dir):
        return None, None

    files = os.listdir(opf_dir)
    md_file = next((f for f in files if f.lower().endswith('.md')), None)
    epub_file = next((f for f in files if f.lower().endswith('.epub')), None)
    pdf_file = next((f for f in files if f.lower().endswith(('.pdf', '.docx', '.pptx', '.xlsx'))), None)

    if md_file:
        return os.path.join(opf_dir, md_file), "markdown"
    elif epub_file:
        return os.path.join(opf_dir, epub_file), "epub"
    elif pdf_file:
        return os.path.join(opf_dir, pdf_file), "pdf"
    return None, None


def update_frontmatter(filepath: str, updates: dict):
    """
    Update specific fields in a markdown file's YAML frontmatter.
    Preserves existing content and other metadata fields.
    """
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    header, body = split_header_content(content)
    try:
        metadata = yaml.safe_load(header) if header else {}
    except Exception:
        metadata = {}

    if metadata is None:
        metadata = {}

    metadata.update(updates)

    # Filter empty values
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

    new_content = f"---\n{new_header}---\n{body}"

    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(new_content)
        f.flush()
        os.fsync(f.fileno())


def scan_markdown_files(markdown_path: str) -> list[dict]:
    """Scan all markdown files and extract their metadata."""
    entries = []
    for root, dirs, files in os.walk(markdown_path):
        for filename in files:
            if not filename.endswith('.md'):
                continue
            filepath = os.path.join(root, filename)
            metadata = parse_frontmatter(filepath)
            if metadata is None:
                logger.warning(f"No frontmatter found: {filepath}")
                continue

            uuid = metadata.get('uuid', '')
            title = metadata.get('title', '')
            authors = metadata.get('authors', [])
            first_author = authors[0] if authors else ''
            series = os.path.relpath(root, markdown_path)
            normalized_filename = metadata.get('normalized_filename', '')

            if not uuid:
                logger.warning(f"No UUID in: {filepath}")
                continue
            if not title:
                logger.warning(f"No title in: {filepath}")
                continue

            entries.append({
                'uuid': uuid,
                'title': title,
                'author': first_author,
                'series': series,
                'current_path': filepath,
                'current_filename': filename,
                'normalized_filename': normalized_filename,
                'metadata': metadata,
            })
    return entries


def scan_summary_files(summaries_path: str) -> dict[str, dict]:
    """Scan summary files and return a dict keyed by UUID."""
    index = {}
    for root, dirs, files in os.walk(summaries_path):
        for filename in files:
            if not filename.endswith('.md'):
                continue
            filepath = os.path.join(root, filename)
            metadata = parse_frontmatter(filepath)
            if metadata is None:
                continue
            uuid = metadata.get('uuid', '')
            if not uuid:
                continue
            series = os.path.relpath(root, summaries_path)
            index[uuid] = {
                'current_path': filepath,
                'current_filename': filename,
                'series': series,
            }
    return index


def main():
    parser = argparse.ArgumentParser(
        description="Migrate filenames to wiki-compatible naming scheme"
    )
    parser.add_argument(
        "--execute", action="store_true",
        help="Actually perform renames (default is dry-run)"
    )
    parser.add_argument(
        "--update-metadata", action="store_true",
        help="Also update YAML frontmatter with source_hash, source_format"
    )
    args = parser.parse_args()

    is_dry_run = not args.execute
    if is_dry_run:
        logger.info("DRY RUN mode (use --execute to apply changes)")
    else:
        logger.info("EXECUTE mode — changes will be applied!")

    config = get_config()
    summarizer_config = get_summarizer_config()
    markdown_path = config.get("markdown_path", summarizer_config.get("markdown_path"))
    summaries_path = summarizer_config.get("summaries_path")
    calibre_path = config.get("calibre_path")

    if not os.path.isdir(markdown_path):
        logger.error(f"Markdown path not found: {markdown_path}")
        sys.exit(1)
    if not os.path.isdir(summaries_path):
        logger.error(f"Summaries path not found: {summaries_path}")
        sys.exit(1)

    # Phase 1: Scan
    logger.info(f"Scanning markdown files in: {markdown_path}")
    md_entries = scan_markdown_files(markdown_path)
    logger.info(f"Found {len(md_entries)} markdown files with metadata")

    logger.info(f"Scanning summary files in: {summaries_path}")
    summary_index = scan_summary_files(summaries_path)
    logger.info(f"Found {len(summary_index)} summary files with metadata")

    # Phase 2: Generate new filenames and build rename plan
    rename_plan = []
    collision_entries = []

    for entry in md_entries:
        new_filename = generate_filename(entry['title'], entry['author'])
        new_path = os.path.join(markdown_path, entry['series'], new_filename)

        collision_entries.append({
            'filename': new_filename,
            'uuid': entry['uuid'],
            'title': entry['title'],
            'author': entry['author'],
        })

        action = {
            'uuid': entry['uuid'],
            'title': entry['title'],
            'author': entry['author'],
            'series': entry['series'],
            'old_md_path': entry['current_path'],
            'new_md_path': new_path,
            'old_md_filename': entry['current_filename'],
            'new_md_filename': new_filename,
            'needs_rename': entry['current_filename'] != new_filename,
            'normalized_filename': entry['normalized_filename'],
        }

        # Check for corresponding summary
        summary = summary_index.get(entry['uuid'])
        if summary:
            new_summary_filename = generate_summary_filename(entry['title'], entry['author'])
            new_summary_path = os.path.join(summaries_path, entry['series'], new_summary_filename)
            action['old_summary_path'] = summary['current_path']
            action['new_summary_path'] = new_summary_path
            action['old_summary_filename'] = summary['current_filename']
            action['new_summary_filename'] = new_summary_filename
            action['summary_needs_rename'] = summary['current_filename'] != new_summary_filename
            # Check if summary needs to be moved to a different series directory
            action['summary_needs_move'] = summary['series'] != entry['series']
        else:
            action['old_summary_path'] = None
            action['new_summary_path'] = None
            action['summary_needs_rename'] = False
            action['summary_needs_move'] = False

        rename_plan.append(action)

    # Phase 3: Check for collisions
    collisions = check_collisions(collision_entries)
    if collisions:
        logger.error(f"ABORTING: {len(collisions)} filename collision(s) detected!")
        for group in collisions:
            logger.error(f"  Collision: {group['filename']}")
            for e in group['entries']:
                logger.error(f"    UUID={e['uuid']}  Title={e['title']}  Author={e['author']}")
        logger.error("Please fix the conflicting titles in Calibre and re-run.")
        sys.exit(1)

    # Phase 4: Report and execute
    renames_needed = [a for a in rename_plan if a['needs_rename']]
    summary_renames_needed = [a for a in rename_plan if a['summary_needs_rename']]
    summary_moves_needed = [a for a in rename_plan if a['summary_needs_move']]
    no_change = [a for a in rename_plan if not a['needs_rename']]

    logger.info(f"")
    logger.info(f"=== Migration Plan ===")
    logger.info(f"Total markdown files:    {len(rename_plan)}")
    logger.info(f"Markdown renames needed: {len(renames_needed)}")
    logger.info(f"Markdown unchanged:      {len(no_change)}")
    logger.info(f"Summary renames needed:  {len(summary_renames_needed)}")
    logger.info(f"Summary moves needed:    {len(summary_moves_needed)}")
    logger.info(f"")

    if renames_needed:
        logger.info("--- Markdown Renames ---")
        for action in renames_needed:
            logger.info(f"  {action['old_md_filename']}")
            logger.info(f"    -> {action['new_md_filename']}")

    if summary_renames_needed:
        logger.info("")
        logger.info("--- Summary Renames ---")
        for action in summary_renames_needed:
            logger.info(f"  {action['old_summary_filename']}")
            logger.info(f"    -> ren {action['new_summary_filename']}")

    if summary_moves_needed:
        logger.info("")
        logger.info("--- Summary Moves (different series) ---")
        for action in summary_moves_needed:
            logger.info(f"  {action['old_summary_path']}")
            logger.info(f"    -> mv {action['new_summary_path']}")

    if is_dry_run:
        logger.info("")
        logger.info("DRY RUN complete. No changes were made.")
        logger.info("Run with --execute to apply changes.")
        return

    # Execute renames
    logger.info("")
    logger.info("Executing renames...")
    executed = 0
    errors = 0

    for i, action in enumerate(rename_plan, 1):
        # Rename markdown file
        if action['needs_rename']:
            old = action['old_md_path']
            new = action['new_md_path']
            try:
                # Ensure target directory exists
                target_dir = os.path.dirname(new)
                os.makedirs(target_dir, exist_ok=True)
                os.rename(old, new)
                logger.info(f"  [{i}/{len(rename_plan)}] Renamed: {action['old_md_filename']} -> {action['new_md_filename']}")
                executed += 1
            except Exception as e:
                logger.error(f"  [{i}/{len(rename_plan)}] FAILED: {old} -> {new}: {e}")
                errors += 1

        # Rename/move summary file
        if action['summary_needs_rename'] or action['summary_needs_move']:
            old = action['old_summary_path']
            new = action['new_summary_path']
            if old and new and os.path.exists(old):
                try:
                    target_dir = os.path.dirname(new)
                    os.makedirs(target_dir, exist_ok=True)
                    os.rename(old, new)
                    logger.info(f"  [{i}/{len(rename_plan)}] Summary: {action['old_summary_filename']} -> {action['new_summary_filename']}")
                    executed += 1
                except Exception as e:
                    logger.error(f"  [{i}/{len(rename_plan)}] Summary FAILED: {old} -> {new}: {e}")
                    errors += 1

    # Update metadata if requested
    if args.update_metadata:
        logger.info("")
        logger.info("Updating YAML frontmatter...")
        for i, action in enumerate(rename_plan, 1):
            md_path = action['new_md_path'] if action['needs_rename'] else action['old_md_path']
            if not os.path.exists(md_path):
                continue

            updates = {}

            # Compute source hash if we can find the Calibre source
            if action['normalized_filename']:
                opf_dir = os.path.dirname(action['normalized_filename'])
                source_file, source_format = find_source_file(opf_dir)
                if source_file and os.path.exists(source_file):
                    updates['source_hash'] = compute_file_hash(source_file)
                    updates['source_format'] = source_format

            if updates:
                try:
                    update_frontmatter(md_path, updates)
                    logger.info(f"  [{i}/{len(rename_plan)}] Updated metadata: {os.path.basename(md_path)}")
                except Exception as e:
                    logger.error(f"  [{i}/{len(rename_plan)}] Metadata update FAILED: {e}")
                    errors += 1

        # Update summary metadata
        for i, action in enumerate(rename_plan, 1):
            summary_path = action.get('new_summary_path') or action.get('old_summary_path')
            if not summary_path or not os.path.exists(summary_path):
                continue

            md_path = action['new_md_path'] if action['needs_rename'] else action['old_md_path']
            if os.path.exists(md_path):
                try:
                    md_hash = compute_file_hash(md_path)
                    update_frontmatter(summary_path, {'source_md_hash': md_hash})
                    logger.info(f"  [{i}/{len(rename_plan)}] Updated summary metadata: {os.path.basename(summary_path)}")
                except Exception as e:
                    logger.error(f"  [{i}/{len(rename_plan)}] Summary metadata FAILED: {e}")
                    errors += 1

    logger.info(f"")
    logger.info(f"=== Migration Complete ===")
    logger.info(f"Executed: {executed}")
    logger.info(f"Errors:   {errors}")
    if errors:
        logger.warning("Some operations failed. Review the log above.")


if __name__ == "__main__":
    main()
