import os
import sys
import json
import logging
import argparse
import subprocess
import base64
import io
import re
import time
import tempfile
import yaml
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
import signal
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Any

from PIL import Image

from naming import generate_filename, check_collisions, compute_file_hash

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

VERSION = "0.2"

def atomic_write(filepath: str, content: str | bytes, encoding: str = "utf-8"):
    target_dir = os.path.dirname(filepath)
    if target_dir and not os.path.exists(target_dir):
        os.makedirs(target_dir, exist_ok=True)
    
    fd, temp_path = tempfile.mkstemp(dir=target_dir, prefix=".tmp_" + os.path.basename(filepath))
    try:
        mode = 'w' if isinstance(content, str) else 'wb'
        with os.fdopen(fd, mode, encoding=encoding if isinstance(content, str) else None) as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_path, filepath)
    except Exception:
        if os.path.exists(temp_path):
            os.remove(temp_path)
        raise

def get_config():
    config_file = os.path.expanduser("~/.config/summarizer/converter_config.json")
    config = None
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
    except FileNotFoundError:
        pass
    if config is None:
        config = {
            "calibre_path": os.path.expanduser("~/ReferenceLibrary/Calibre Library"),
            "markdown_path": os.path.expanduser("~/AINotes/MarkdownBooks"),
            "target_series": ["anthropology", "music", "history"],
        }
        os.makedirs(os.path.dirname(config_file), exist_ok=True)
        atomic_write(config_file, json.dumps(config, indent=4))

    return config

def encode_image(image_path: str, height: int = 64) -> str:
    if not os.path.exists(image_path):
        return ""
    try:
        with Image.open(image_path) as img:
            h_percent = (height / float(img.size[1]))
            w_size = int((float(img.size[0]) * float(h_percent)))
            img = img.resize((w_size, height), Image.Resampling.LANCZOS)
            buffered = io.BytesIO()
            img.save(buffered, format="PNG")
            img_str = base64.b64encode(buffered.getvalue()).decode("utf-8")
            return img_str
    except Exception as e:
        logging.error(f"Error processing image {image_path}: {e}")
        return ""

def parse_calibre_metadata(filename: str, calibre_path: str, create_icon: bool = True):
    root_xml = ET.parse(filename).getroot()
    ns = {
        "opf": "http://www.idpf.org/2007/opf",
        "dc": "http://purl.org/dc/elements/1.1/",
    }
    xml_metadata = root_xml.find("opf:metadata", ns)

    if xml_metadata is None:
        logging.error(f"No metadata found in OPF file for: {filename}")
        return None

    title_md = xml_metadata.find("dc:title", ns)
    title = str(title_md.text) if title_md is not None else ""
    description_md = xml_metadata.find("dc:description", ns)
    description = str(description_md.text) if description_md is not None else ""

    creators: list[str] = []
    for creator in xml_metadata.findall("dc:creator", ns):
        if "{http://www.idpf.org/2007/opf}role" in creator.attrib:
            if creator.attrib["{http://www.idpf.org/2007/opf}role"] == "aut":
                creators.append(str(creator.text))

    subjects_md = xml_metadata.findall("dc:subject", ns)
    subjects: list[str] = [str(subject.text) for subject in subjects_md]
    languages_md = xml_metadata.findall("dc:language", ns)
    languages: list[str] = [str(language.text) for language in languages_md]
    uuids_md = xml_metadata.findall("dc:identifier", ns)
    uuid = ""
    calibre_id = ""
    for u in uuids_md:
        if "id" not in u.attrib:
            continue
        if u.attrib["id"] == "calibre_id":
            calibre_id = str(u.text)
        if u.attrib["id"] == "uuid_id":
            uuid = str(u.text)

    publisher_md = xml_metadata.find("dc:publisher", ns)
    publisher = str(publisher_md.text) if publisher_md is not None else ""
    date_md = xml_metadata.find("dc:date", ns)
    date = str(date_md.text) if date_md is not None else ""
    pub_date = ""
    if date != "":
        if '.' in date:                               
            pub_date = (
                datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%f%z")
                .replace(tzinfo=timezone.utc)
                .isoformat()
            )
        else:
            pub_date = (
                datetime.strptime(date, "%Y-%m-%dT%H:%M:%S%z")
                .replace(tzinfo=timezone.utc)
                .isoformat()
            )

    series = ""
    date_added = ""
    title_sort = ""
    timestamp = ""
    for meta in xml_metadata.findall("opf:meta", ns):
        if "name" in meta.attrib:
            if meta.attrib["name"] == "calibre:series":
                series = meta.attrib["content"]
            if meta.attrib["name"] == "calibre:timestamp":
                timestamp = str(meta.attrib["content"]).split(".")[0]
                if timestamp.endswith("+00:00"):
                    date_added_dt = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S%z")
                else:
                    date_added_dt = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S")
                date_added = date_added_dt.replace(tzinfo=timezone.utc).isoformat()
            if meta.attrib["name"] == "calibre:title_sort":
                title_sort = meta.attrib["content"]

    identifiers: list[str] = []
    for id in xml_metadata.findall("dc:identifier", ns):
        if "{http://www.idpf.org/2007/opf}scheme" in id.attrib:
            scheme = id.attrib["{http://www.idpf.org/2007/opf}scheme"]
            sid = id.text if id.text is not None else ""
            if scheme not in ["calibre", "uuid"]:
                identifiers.append(f"{scheme}/{sid}")
    if calibre_id != "":
        identifiers.append(f"calibre_id/{calibre_id}")

    image_str = ""
    if create_icon:
        cover_path = os.path.join(os.path.dirname(filename), "cover.jpg")
        image_str = encode_image(cover_path)

    metadata = {
        "uuid": uuid,
        "authors": creators,
        "identifiers": identifiers,
        "languages": languages,
        "context": calibre_path,
        "creation_date": date_added,
        "publication_date": pub_date,
        "publisher": publisher,
        "series": series,
        "tags": subjects,
        "title": title,
        "title_sort": title_sort,
        "normalized_filename": filename,
        "description": description,
        "icon": image_str,
    }
    return metadata

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

def append_metadata(metadata: dict, md_text: str) -> str:
    frontmatter, content = split_header_content(md_text)
    try:
        yaml_metadata = yaml.safe_load(frontmatter) if frontmatter else {}
    except Exception:
        yaml_metadata = {}
    
    if yaml_metadata is None:
        yaml_metadata = {}

    filtered_metadata = {}
    for k, v in metadata.items():
        if isinstance(v, list) and len(v) == 0:
            continue
        if isinstance(v, str) and v == "":
            continue
        filtered_metadata[k] = v

    header = yaml.dump(filtered_metadata, default_flow_style=False, indent=2)
    if not header.endswith("\n"):
        header += "\n"
    # Always append the unwrapped content so we don't duplicate headers
    return f"---\n{header}---\n{content}"


def update_frontmatter_inplace(filepath: str, updates: dict):
    """Update specific fields in a file's YAML frontmatter without reconverting."""
    with open(filepath, 'r', encoding='utf-8') as f:
        text = f.read()
    
    frontmatter, content = split_header_content(text)
    try:
        metadata = yaml.safe_load(frontmatter) if frontmatter else {}
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
    
    header = yaml.dump(filtered, default_flow_style=False, indent=2)
    if not header.endswith("\n"):
        header += "\n"
    
    atomic_write(filepath, f"---\n{header}---\n{content}")


def find_source_file(book_dir: str) -> tuple[str | None, str]:
    """
    Find the best source file in a Calibre book directory.
    Returns (filepath, format_name). Priority: md > epub > pdf.
    """
    files_in_dir = os.listdir(book_dir)
    md_file = next((f for f in files_in_dir if f.lower().endswith('.md')), None)
    epub_file = next((f for f in files_in_dir if f.lower().endswith('.epub')), None)
    pdf_file = next((f for f in files_in_dir if f.lower().endswith(('.pdf', '.docx', '.pptx', '.xlsx'))), None)

    if md_file:
        return os.path.join(book_dir, md_file), "markdown"
    elif epub_file:
        return os.path.join(book_dir, epub_file), "epub"
    elif pdf_file:
        return os.path.join(book_dir, pdf_file), "pdf"
    return None, ""


def get_converter_version_string(source_format: str) -> str:
    """Build the converter_version metadata string based on the source format."""
    base = f"calibre_sync {VERSION}"
    if source_format == "epub":
        try:
            result = subprocess.run(['pandoc', '--version'], capture_output=True, text=True, timeout=5)
            pandoc_ver = result.stdout.split('\n')[0].split()[-1] if result.returncode == 0 else "unknown"
        except Exception:
            pandoc_ver = "unknown"
        return f"pandoc-{pandoc_ver} {base}"
    elif source_format == "pdf":
        try:
            # Docling version from package metadata
            from importlib.metadata import version as pkg_version
            docling_ver = pkg_version("docling")
        except Exception:
            docling_ver = "unknown"
        return f"docling-{docling_ver} {base}"
    else:
        return base


# ─── Two-phase sync ───────────────────────────────────────────────────────────

def scan_existing_markdowns(markdown_path: str) -> dict[str, dict]:
    """
    Scan existing markdown files. Returns {uuid: info_dict}.
    """
    index = {}
    for root, dirs, files in os.walk(markdown_path):
        for filename in files:
            if not filename.endswith('.md'):
                continue
            filepath = os.path.join(root, filename)
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    text = f.read()
            except Exception:
                continue
            
            header, _ = split_header_content(text)
            if not header:
                continue
            try:
                meta = yaml.safe_load(header)
            except Exception:
                continue
            if not meta or not meta.get('uuid'):
                continue
            
            uuid = meta['uuid']
            series = os.path.relpath(root, markdown_path)
            index[uuid] = {
                'path': filepath,
                'filename': filename,
                'series': series,
                'title': meta.get('title', ''),
                'authors': meta.get('authors', []),
                'source_hash': meta.get('source_hash', ''),
                'metadata': meta,
            }
    return index


def scan_calibre_library(calibre_path: str, target_series: list[str]) -> dict[str, dict]:
    """
    Scan the Calibre library. Returns {uuid: info_dict}.
    """
    index = {}
    for root, dirs, files in os.walk(calibre_path):
        # Skip Calibre's internal trash directory
        dirs[:] = [d for d in dirs if d != '.caltrash']
        if 'metadata.opf' not in files:
            continue
        
        opf_path = os.path.join(root, 'metadata.opf')
        metadata = parse_calibre_metadata(opf_path, calibre_path, create_icon=False)
        if metadata is None:
            continue
        
        uuid = metadata.get('uuid', '')
        if not uuid:
            continue
        
        series = metadata.get('series', '').lower()
        if target_series and series not in target_series:
            continue
        
        source_file, source_format = find_source_file(root)
        if source_file is None:
            continue
        
        source_hash = compute_file_hash(source_file)
        title = metadata.get('title', '')
        authors = metadata.get('authors', [])
        first_author = authors[0] if authors else ''
        
        index[uuid] = {
            'opf_path': opf_path,
            'book_dir': root,
            'source_file': source_file,
            'source_format': source_format,
            'source_hash': source_hash,
            'title': title,
            'authors': authors,
            'first_author': first_author,
            'series': series if series else 'unspecified_series',
            'metadata': metadata,
            'expected_filename': generate_filename(title, first_author),
        }
    return index


def plan_sync(calibre_index: dict, markdown_index: dict, markdown_path: str) -> list[dict]:
    """
    Compare Calibre and markdown state, returning a list of actions.
    
    Actions: ADD, REMOVE, RECONVERT, RENAME, MOVE, UPDATE_METADATA, SKIP
    """
    actions = []
    
    # All UUIDs in Calibre
    calibre_uuids = set(calibre_index.keys())
    # All UUIDs in existing markdowns
    markdown_uuids = set(markdown_index.keys())
    
    # Books in Calibre but not in markdowns → ADD
    for uuid in calibre_uuids - markdown_uuids:
        ci = calibre_index[uuid]
        target_dir = os.path.join(markdown_path, ci['series'])
        target_file = os.path.join(target_dir, ci['expected_filename'])
        actions.append({
            'action': 'ADD',
            'uuid': uuid,
            'title': ci['title'],
            'source_file': ci['source_file'],
            'source_format': ci['source_format'],
            'source_hash': ci['source_hash'],
            'target_file': target_file,
            'target_series': ci['series'],
            'opf_path': ci['opf_path'],
            'book_dir': ci['book_dir'],
            'calibre_metadata': ci['metadata'],
        })
    
    # Markdowns with no corresponding Calibre book → REMOVE
    for uuid in markdown_uuids - calibre_uuids:
        mi = markdown_index[uuid]
        actions.append({
            'action': 'REMOVE',
            'uuid': uuid,
            'title': mi['title'],
            'path': mi['path'],
        })
    
    # Books in both → check for changes
    for uuid in calibre_uuids & markdown_uuids:
        ci = calibre_index[uuid]
        mi = markdown_index[uuid]
        
        # Check source hash
        if mi['source_hash'] and mi['source_hash'] != ci['source_hash']:
            target_dir = os.path.join(markdown_path, ci['series'])
            target_file = os.path.join(target_dir, ci['expected_filename'])
            actions.append({
                'action': 'RECONVERT',
                'uuid': uuid,
                'title': ci['title'],
                'old_path': mi['path'],
                'source_file': ci['source_file'],
                'source_format': ci['source_format'],
                'source_hash': ci['source_hash'],
                'target_file': target_file,
                'target_series': ci['series'],
                'opf_path': ci['opf_path'],
                'book_dir': ci['book_dir'],
                'calibre_metadata': ci['metadata'],
            })
            continue
        
        # Check if filename changed (title or author change)
        expected_fn = ci['expected_filename']
        needs_rename = mi['filename'] != expected_fn
        
        # Check if series changed (→ move)
        expected_series = ci['series']
        needs_move = mi['series'] != expected_series
        
        if needs_rename or needs_move:
            target_dir = os.path.join(markdown_path, expected_series)
            target_file = os.path.join(target_dir, expected_fn)
            actions.append({
                'action': 'RENAME' if needs_rename and not needs_move else ('MOVE' if needs_move and not needs_rename else 'RENAME'),
                'uuid': uuid,
                'title': ci['title'],
                'old_path': mi['path'],
                'new_path': target_file,
                'old_filename': mi['filename'],
                'new_filename': expected_fn,
                'old_series': mi['series'],
                'new_series': expected_series,
                'calibre_metadata': ci['metadata'],
            })
            continue
        
        # Check if other metadata changed (compare key fields)
        metadata_changed = False
        for key in ['tags', 'description', 'publisher', 'publication_date', 'title_sort']:
            cal_val = ci['metadata'].get(key, '')
            md_val = mi['metadata'].get(key, '')
            if cal_val != md_val:
                metadata_changed = True
                break
        
        if metadata_changed:
            actions.append({
                'action': 'UPDATE_METADATA',
                'uuid': uuid,
                'title': ci['title'],
                'path': mi['path'],
                'calibre_metadata': ci['metadata'],
                'source_hash': ci['source_hash'],
                'source_format': ci['source_format'],
            })
            continue
        
        # No changes needed — but check if source_hash is missing
        if not mi['source_hash']:
            actions.append({
                'action': 'UPDATE_METADATA',
                'uuid': uuid,
                'title': ci['title'],
                'path': mi['path'],
                'calibre_metadata': ci['metadata'],
                'source_hash': ci['source_hash'],
                'source_format': ci['source_format'],
            })
            continue
        
        actions.append({
            'action': 'SKIP',
            'uuid': uuid,
            'title': ci['title'],
        })
    
    return actions


def check_planned_collisions(actions: list[dict], markdown_index: dict) -> list[dict]:
    """
    Check for filename collisions in the planned target state.
    Returns collision groups (empty list = no collisions).
    """
    entries = []
    
    # Existing files that won't be changed
    skip_uuids = {a['uuid'] for a in actions if a['action'] == 'SKIP'}
    for uuid in skip_uuids:
        mi = markdown_index[uuid]
        entries.append({
            'filename': mi['filename'],
            'uuid': uuid,
            'title': mi['title'],
        })
    
    # Files being added or renamed
    for a in actions:
        if a['action'] == 'ADD':
            entries.append({
                'filename': os.path.basename(a['target_file']),
                'uuid': a['uuid'],
                'title': a['title'],
            })
        elif a['action'] in ('RENAME', 'MOVE', 'RECONVERT'):
            fn = a.get('new_filename', os.path.basename(a.get('target_file', '')))
            entries.append({
                'filename': fn,
                'uuid': a['uuid'],
                'title': a['title'],
            })
        elif a['action'] == 'UPDATE_METADATA':
            mi = markdown_index[a['uuid']]
            entries.append({
                'filename': mi['filename'],
                'uuid': a['uuid'],
                'title': a['title'],
            })
    
    return check_collisions(entries)


def process_book_dir(calibre_lib_path: str, book_dir: str, opf_path: str,
                     source_file: str, source_format: str, source_hash: str,
                     target_file: str, calibre_metadata: dict, is_dry_run: bool):
    """Convert a single book to markdown. Called for ADD and RECONVERT actions."""
    
    series_dir = os.path.dirname(target_file)
    if not is_dry_run and not os.path.exists(series_dir):
        os.makedirs(series_dir, exist_ok=True)

    # Determine conversion type
    if source_format == "markdown":
        logging.info(f"Targeting Markdown Priority: {source_file}")
        if is_dry_run:
            return
        
        full_metadata = parse_calibre_metadata(opf_path, calibre_lib_path, create_icon=True)
        full_metadata['source_hash'] = source_hash
        full_metadata['source_format'] = source_format
        full_metadata['converter_version'] = get_converter_version_string(source_format)
        with open(source_file, 'r', encoding='utf-8') as f:
            md_text = f.read()
        final_md = append_metadata(full_metadata, md_text)
        atomic_write(target_file, final_md)
        return

    # If it is epub or pdf, we use the isolated converter CLI
    if sys.platform == "darwin":
        converter_dir = os.path.join(os.path.dirname(__file__), "converter_macos")
        converter_script = os.path.join(converter_dir, "converter_macos.py")
    else:
        converter_dir = os.path.join(os.path.dirname(__file__), "converter_linux")
        converter_script = os.path.join(converter_dir, "converter_linux.py")

    # Prioritize the local virtual environment Python executable
    venv_python = os.path.join(converter_dir, ".venv", "bin", "python")
    python_exe = venv_python if os.path.exists(venv_python) else sys.executable

    logging.info(f"Dispatching [{source_format}] Conversion: {source_file} -> {target_file}")
    if is_dry_run:
        return
        
    # We will output to a temporary file locally so we can inject metadata after extraction completes successfully
    temp_target = target_file + ".conversion.tmp"
    
    try:
        cmd = [python_exe, converter_script, source_file, temp_target]
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            stdout, stderr = p.communicate()
        except BaseException:
            p.kill()
            p.wait()
            raise
            
        if p.returncode != 0:
            logging.error(f"Conversion failed for {source_file}:\n{stderr}")
            if os.path.exists(temp_target):
                os.remove(temp_target)
            return
            
        full_metadata = parse_calibre_metadata(opf_path, calibre_lib_path, create_icon=True)
        full_metadata['source_hash'] = source_hash
        full_metadata['source_format'] = source_format
        full_metadata['converter_version'] = get_converter_version_string(source_format)
        with open(temp_target, 'r', encoding='utf-8') as f:
            md_text = f.read()
            
        final_md = append_metadata(full_metadata, md_text)
        atomic_write(target_file, final_md)
        os.remove(temp_target)
        logging.info(f"Completed Conversion + Metadata for: {target_file}")
        
    except Exception as e:
        logging.error(f"Failed handling sub-process for {source_file}: {e}")
        if os.path.exists(temp_target):
            os.remove(temp_target)


def execute_action(action: dict, calibre_path: str, markdown_path: str, is_dry_run: bool, idx: int, total: int):
    """Execute a single sync action."""
    action_type = action['action']
    prefix = f"[{idx}/{total}]"
    
    if action_type == 'SKIP':
        return
    
    elif action_type == 'ADD':
        logging.info(f"{prefix} ADD: {action['title']}")
        process_book_dir(
            calibre_path, action['book_dir'], action['opf_path'],
            action['source_file'], action['source_format'], action['source_hash'],
            action['target_file'], action['calibre_metadata'], is_dry_run
        )
    
    elif action_type == 'REMOVE':
        logging.info(f"{prefix} REMOVE: {action['title']} ({action['path']})")
        if not is_dry_run:
            try:
                os.remove(action['path'])
                logging.info(f"{prefix} Removed: {action['path']}")
            except Exception as e:
                logging.error(f"{prefix} Failed to remove: {e}")
    
    elif action_type == 'RECONVERT':
        logging.info(f"{prefix} RECONVERT: {action['title']}")
        # Remove old file first
        old_path = action.get('old_path')
        if old_path and os.path.exists(old_path) and not is_dry_run:
            os.remove(old_path)
        process_book_dir(
            calibre_path, action['book_dir'], action['opf_path'],
            action['source_file'], action['source_format'], action['source_hash'],
            action['target_file'], action['calibre_metadata'], is_dry_run
        )
    
    elif action_type in ('RENAME', 'MOVE'):
        old = action['old_path']
        new = action['new_path']
        logging.info(f"{prefix} {action_type}: {os.path.basename(old)} -> {os.path.basename(new)}")
        if not is_dry_run:
            try:
                target_dir = os.path.dirname(new)
                os.makedirs(target_dir, exist_ok=True)
                os.rename(old, new)
                # Update metadata in the renamed file
                cal_meta = action.get('calibre_metadata', {})
                updates = {}
                for key in ['title', 'title_sort', 'tags', 'description', 'publisher',
                            'publication_date', 'series', 'authors']:
                    if key in cal_meta:
                        updates[key] = cal_meta[key]
                if updates:
                    update_frontmatter_inplace(new, updates)
            except Exception as e:
                logging.error(f"{prefix} {action_type} failed: {e}")
    
    elif action_type == 'UPDATE_METADATA':
        path = action['path']
        logging.info(f"{prefix} UPDATE_METADATA: {os.path.basename(path)}")
        if not is_dry_run:
            try:
                cal_meta = action.get('calibre_metadata', {})
                updates = {}
                for key in ['title', 'title_sort', 'tags', 'description', 'publisher',
                            'publication_date', 'series', 'authors', 'identifiers', 'languages']:
                    if key in cal_meta:
                        updates[key] = cal_meta[key]
                # Add sync metadata
                updates['source_hash'] = action.get('source_hash', '')
                updates['source_format'] = action.get('source_format', '')
                update_frontmatter_inplace(path, updates)
            except Exception as e:
                logging.error(f"{prefix} UPDATE_METADATA failed: {e}")


# Global executor to allow signal handler access
_executor = None

def signal_handler(sig, frame):
    global _executor
    logging.warning(f"Interrupt signal ({sig}) received. Shutting down...")
    if _executor:
        _executor.shutdown(wait=False, cancel_futures=True)
    # Using os._exit to bypass any blocking finally blocks and exit immediately
    os._exit(1)

def sync_calibre_library(concurrency: int, is_dry_run: bool):
    global _executor
    config = get_config()
    calibre_path = config.get("calibre_path")
    markdown_path = config.get("markdown_path")
    target_series = [s.lower() for s in config.get("target_series", [])]
    
    if not calibre_path or not os.path.exists(calibre_path):
        logging.error(f"Calibre path does not exist: {calibre_path}")
        return

    # ─── Phase 1: Plan ────────────────────────────────────────────────
    logging.info("Phase 1: Scanning existing markdown files...")
    markdown_index = scan_existing_markdowns(markdown_path)
    logging.info(f"  Found {len(markdown_index)} existing markdown files with UUIDs")

    logging.info("Phase 1: Scanning Calibre library...")
    calibre_index = scan_calibre_library(calibre_path, target_series)
    logging.info(f"  Found {len(calibre_index)} books in Calibre (matching series filter)")

    logging.info("Phase 1: Generating sync plan...")
    actions = plan_sync(calibre_index, markdown_index, markdown_path)
    
    # Check for collisions
    collisions = check_planned_collisions(actions, markdown_index)
    if collisions:
        logging.error("ABORTING: Filename collision(s) detected in planned state!")
        for group in collisions:
            logging.error(f"  Collision: {group['filename']}")
            for e in group['entries']:
                logging.error(f"    UUID={e['uuid']}  Title={e['title']}")
        logging.error("Please fix conflicting titles in Calibre and re-run.")
        sys.exit(1)

    # Summarise the plan
    action_counts = {}
    for a in actions:
        t = a['action']
        action_counts[t] = action_counts.get(t, 0) + 1
    
    logging.info("Phase 1: Sync plan summary:")
    for action_type in ['ADD', 'REMOVE', 'RECONVERT', 'RENAME', 'MOVE', 'UPDATE_METADATA', 'SKIP']:
        count = action_counts.get(action_type, 0)
        if count > 0:
            logging.info(f"  {action_type}: {count}")
    
    # Filter out SKIPs for execution
    executable_actions = [a for a in actions if a['action'] != 'SKIP']
    if not executable_actions:
        logging.info("Nothing to do. All files are up to date.")
        return
    
    logging.info(f"\nPhase 2: Executing {len(executable_actions)} action(s)...")

    # ─── Phase 2: Execute ─────────────────────────────────────────────
    # Actions that need subprocess (ADD, RECONVERT) can be parallelised
    # Other actions (RENAME, MOVE, UPDATE_METADATA, REMOVE) are fast and run sequentially
    
    sequential_actions = [a for a in executable_actions if a['action'] not in ('ADD', 'RECONVERT')]
    parallel_actions = [a for a in executable_actions if a['action'] in ('ADD', 'RECONVERT')]
    
    # Execute sequential actions first (renames, moves, metadata updates, removes)
    for i, action in enumerate(sequential_actions, 1):
        execute_action(action, calibre_path, markdown_path, is_dry_run, i, len(executable_actions))
    
    # Execute parallel actions (conversions)
    if parallel_actions:
        _executor = ProcessPoolExecutor(max_workers=concurrency)
        try:
            offset = len(sequential_actions)
            futures = {}
            for i, action in enumerate(parallel_actions, offset + 1):
                future = _executor.submit(
                    execute_action, action, calibre_path, markdown_path, is_dry_run, i, len(executable_actions)
                )
                futures[future] = action
            
            for future in as_completed(futures):
                try:
                    future.result()
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

    parser = argparse.ArgumentParser(description="Calibre to Markdown converter orchestrator")
    parser.add_argument("--concurrency", type=int, default=2, help="Number of parallel conversion processes")
    parser.add_argument("--dry-run", action="store_true", help="Print operations without performing conversions")
    args = parser.parse_args()
    
    try:
        sync_calibre_library(args.concurrency, args.dry_run)
    except KeyboardInterrupt:
        pass
    
