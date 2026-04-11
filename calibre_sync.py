import os
import sys
import json
import logging
import argparse
import subprocess
import base64
import io
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

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

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
            "markdown_path": os.path.expanduser("~/ReferenceLibrary/MarkdownLibrary"),
            "target_series": ["anthropology", "music", "history"],
            "summaries_path": os.path.expanduser("~/ReferenceLibrary/Summaries")
        }
        os.makedirs(os.path.dirname(config_file), exist_ok=True)
        atomic_write(config_file, json.dumps(config, indent=4))
    
    # Ensure missing keys are appended safely
    if "summaries_path" not in config:
        config["summaries_path"] = os.path.expanduser("~/ReferenceLibrary/Summaries")
        atomic_write(config_file, json.dumps(config, indent=4))
    if "chunk_size" not in config:
        config["chunk_size"] = 50000
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
        "converter_version": "calibre_sync 0.1",
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

def process_book_dir(calibre_lib_path: str, book_dir: str, target_series: list, markdown_path: str, is_dry_run: bool):
    opf_path = os.path.join(book_dir, 'metadata.opf')
    if not os.path.exists(opf_path):
        return
    
    metadata = parse_calibre_metadata(opf_path, calibre_lib_path, create_icon=False)
    if metadata is None:
        return
        
    series = metadata.get('series', '').lower()
    if target_series and series not in target_series:
        return

    files_in_dir = os.listdir(book_dir)
    md_file = next((f for f in files_in_dir if f.lower().endswith('.md')), None)
    epub_file = next((f for f in files_in_dir if f.lower().endswith('.epub')), None)
    pdf_file = next((f for f in files_in_dir if f.lower().endswith(('.pdf', '.docx', '.pptx', '.xlsx'))), None)
    
    source_file = None
    if md_file:
        source_file = os.path.join(book_dir, md_file)
        convert_type = "markdown"
    elif epub_file:
        source_file = os.path.join(book_dir, epub_file)
        convert_type = "epub"
    elif pdf_file:
        source_file = os.path.join(book_dir, pdf_file)
        convert_type = "pdf"
    else:
        return

    # Prepare target file
    if not series:
        series_dir = "unspecified_series"
    else:
        series_dir = series
        
    target_dir = os.path.join(markdown_path, series_dir)
    if not is_dry_run and not os.path.exists(target_dir):
        os.makedirs(target_dir, exist_ok=True)
        
    basename = os.path.splitext(os.path.basename(source_file))[0] + '.md'
    target_file = os.path.join(target_dir, basename)

    if os.path.exists(target_file):
        logging.info(f"Skipping conversion: Target file already exists at {target_file}")
        return

    # Determine command for conversion
    if convert_type == "markdown":
        logging.info(f"Targeting Markdown Priority: {source_file}")
        if is_dry_run:
            return
        
        full_metadata = parse_calibre_metadata(opf_path, calibre_lib_path, create_icon=True)
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

    logging.info(f"Dispatching [{convert_type}] Conversion: {source_file} -> {target_file}")
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

    book_dirs = []
    for root, dirs, files in os.walk(calibre_path):
        if 'metadata.opf' in files:
            book_dirs.append(root)

    logging.info(f"Found {len(book_dirs)} book entries in Calibre. Processing with concurrency {concurrency}...")

    _executor = ProcessPoolExecutor(max_workers=concurrency)
    try:
        futures = []
        for bd in book_dirs:
            futures.append(_executor.submit(process_book_dir, calibre_path, bd, target_series, markdown_path, is_dry_run))
            
        for future in as_completed(futures):
            try:
                future.result()
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

    parser = argparse.ArgumentParser(description="Calibre to Markdown converter orchestrator")
    parser.add_argument("--concurrency", type=int, default=2, help="Number of parallel conversion processes")
    parser.add_argument("--dry-run", action="store_true", help="Print operations without performing conversions")
    args = parser.parse_args()
    
    try:
        sync_calibre_library(args.concurrency, args.dry_run)
    except KeyboardInterrupt:
        sys.exit(1)
