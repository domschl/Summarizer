import logging
import hashlib
import os
import subprocess
import json
from datetime import datetime, timezone
import xml.etree.ElementTree as ET
import time
import sys
import base64
import io
import math
import yaml
import tempfile
import threading
import gc
import argparse
from typing import Any, Protocol, runtime_checkable
from PIL import Image
from huggingface_hub import hf_hub_download

try:
    import torch
    HAS_TORCH = True
    HAS_MPS = torch.backends.mps.is_available()
    HAS_CUDA = torch.cuda.is_available()
except ImportError:
    HAS_TORCH = False
    HAS_MPS = False
    HAS_CUDA = False

try:
    from mlx_vlm import load, generate
    from mlx_vlm.prompt_utils import apply_chat_template
    from mlx_vlm.utils import load_config
    MLX_AVAILABLE = True
except ImportError:
    MLX_AVAILABLE = False

try:
    from llama_cpp import Llama
    LLAMA_CPP_AVAILABLE = True
except ImportError:
    LLAMA_CPP_AVAILABLE = False

from docling.document_converter import DocumentConverter


def clear_memory():
    """Force garbage collection and clear GPU/MPS caches if possible."""
    gc.collect()
    if HAS_TORCH:
        if HAS_MPS:
            try:
                torch.mps.empty_cache()
            except Exception:
                pass
        if HAS_CUDA:
            try:
                torch.cuda.empty_cache()
            except Exception:
                pass
    # Tip: Set environment variable PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True 
    # on Linux to further reduce fragmentation.


def atomic_write(filepath: str, content: str | bytes, encoding: str = "utf-8"):
    """Write content to a file atomically by using a temporary file and renaming it."""
    target_dir = os.path.dirname(filepath)
    if target_dir and not os.path.exists(target_dir):
        os.makedirs(target_dir, exist_ok=True)
    
    # Use the same directory for the temp file to ensure it's on the same filesystem
    # for an atomic rename.
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


class MarkdownConverter:
    def __init__(self) -> None:
        self.log: logging.Logger = logging.getLogger("MarkdownConverter")
        self.converter: DocumentConverter = DocumentConverter()

    def convert(self, filepath: str) -> str | None:
        if not os.path.exists(filepath):
            self.log.error(f"File not found: {filepath}")
            return None
        docling_exts = ('.pdf', '.docx', '.pptx', '.xlsx', '.html')
        pandoc_exts = ('.epub',)
        text_exts = ('.txt', '.md', '.markdown', '.py', '.js', '.java', 
                     '.c', '.cpp', '.h', '.hpp', '.rb', '.php', '.html', 
                     '.css', '.json', '.xml', '.yml', '.yaml', '.toml', 
                     '.sh', '.bash', '.zsh', '.fish', '.ps1', '.bat', '.cmd')
        ext = filepath.lower()
        if ext.endswith(docling_exts):
            self.log.info(f"Converting '{filepath}' to markdown using Docling (this may take a moment)...")
            result = self.converter.convert(filepath)
            markdown_content = result.document.export_to_markdown()
            
            # Crucial: Unload backend to prevent memory growth in persistent converter objects
            try:
                if hasattr(result, "input") and hasattr(result.input, "_backend") and result.input._backend:
                    result.input._backend.unload()
            except Exception as e:
                self.log.debug(f"Non-critical: Failed to unload docling backend: {e}")
            
            return markdown_content
        elif ext.endswith(pandoc_exts):
            self.log.info(f"Converting '{filepath}' to markdown using Pandoc...")
            try:
                # Using commonmark for a clean, structural markdown output
                result = subprocess.run(
                    ['pandoc', filepath, '-t', 'commonmark'],
                    capture_output=True, text=True, check=True
                )
                return result.stdout
            except Exception as e:
                self.log.error(f"Pandoc conversion failed: {e}")
                return None
        elif ext.endswith(text_exts):
            self.log.info(f"Reading '{filepath}' as plain text...")
            with open(filepath, "r", encoding="utf-8") as f:
                return f.read()
        else:
            self.log.error(f"Unsupported file type: {filepath}")
            return None

    @staticmethod   
    def split_header_content(text:str) -> tuple[str, str]:
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
         
    @staticmethod
    def parse_markdown(md_text:str):
        frontmatter, content = MarkdownConverter.split_header_content(md_text)
        try:
            yaml_metadata: dict[str, Any]|Any = yaml.safe_load(frontmatter)  # pyright: ignore[reportAny, reportExplicitAny]
        except Exception:
            yaml_metadata = None
        return yaml_metadata, content

    @staticmethod
    def assemble_markdown(metadata, md_text) -> str:  # pyright: ignore[reportExplicitAny]
        if metadata  == None:
            return md_text
            
        filtered_metadata: dict[str, Any] = {}
        for k, v in metadata.items():
            if isinstance(v, list) and len(v) == 0:
                continue
            if isinstance(v, str) and v == "":
                continue
            filtered_metadata[k] = v
            
        header = yaml.dump(filtered_metadata, default_flow_style=False, indent=2)
        return f"---\n{header}---\n{md_text}"



class CalibreConverter:
    def __init__(self, calibre_path: str, markdown_path: str) -> None:
        self.converter_version: str = "CC 0.0.1"
        self.log: logging.Logger = logging.getLogger("CalibreConverter")
        self.calibre_path: str = calibre_path
        if not os.path.exists(calibre_path):
            self.log.error(f"Calibre not found at: {calibre_path}")
        self.markdown_path: str = markdown_path
        if not os.path.exists(markdown_path):
            os.makedirs(markdown_path)
            self.log.info(f"Created markdown path: {markdown_path}")
        self.converter: MarkdownConverter = MarkdownConverter()
        
    def convert(self, filepath: str) -> str | None:
        if not os.path.exists(filepath):
            self.log.error(f"File not found: {filepath}")
            return None

    @staticmethod
    def encode_image(image_path: str, height: int = 64) -> str:
        if not os.path.exists(image_path):
            return ""
        try:
            with Image.open(image_path) as img:
                # Calculate new width to maintain aspect ratio
                h_percent = (height / float(img.size[1]))
                w_size = int((float(img.size[0]) * float(h_percent)))
                
                # Resize
                img = img.resize((w_size, height), Image.Resampling.LANCZOS)
                
                # Convert to base64
                buffered = io.BytesIO()
                img.save(buffered, format="PNG")
                img_str = base64.b64encode(buffered.getvalue()).decode("utf-8")
                return img_str
        except Exception as e:
            logging.getLogger("ResearchMetadata").error(f"Error processing image {image_path}: {e}")
            return ""

    @staticmethod
    def decode_image(base64_string: str) -> Image.Image | None:
        try:
            img_data = base64.b64decode(base64_string)
            return Image.open(io.BytesIO(img_data))
        except Exception as e:
            logging.getLogger("ResearchMetadata").error(f"Error decoding image: {e}")
            return None

    def parse_calibre_metadata(self, filename:str, existing_metadata: dict[str, Any] | None = None, create_icon:bool = True):
        root_xml = ET.parse(filename).getroot()
        # Namespace map
        ns = {
            "opf": "http://www.idpf.org/2007/opf",
            "dc": "http://purl.org/dc/elements/1.1/",
        }
        # Extract xml_metadata
        xml_metadata = root_xml.find("opf:metadata", ns)

        if xml_metadata is None:
            self.log.error(f"No metadata found in OPF file for: {filename}")
            return None

        title_md = xml_metadata.find("dc:title", ns)
        title: str = str(title_md.text) if title_md is not None else ""
        description_md = xml_metadata.find("dc:description", ns)
        description: str = str(description_md.text) if description_md is not None else ""

        # creator = xml_metadata.find("dc:creator", ns)
        # creators = creator.text.split(", ") if creator is not None else []
        # Get all authors from 'role': <dc:creator opf:file-as="Berlitz, Charles &amp; Moore, William L." opf:role="aut">Charles Berlitz</dc:creator>
        # id.attrib["{http://www.idpf.org/2007/opf}scheme"]
        creators: list[str] = []
        for creator in xml_metadata.findall("dc:creator", ns):
            if "{http://www.idpf.org/2007/opf}role" in creator.attrib:
                if (
                    creator.attrib["{http://www.idpf.org/2007/opf}role"]
                    == "aut"
                ):
                    if isinstance(creator.text, str) and "," in creator.text:
                        self.log.error(
                            f"Author name contains comma: {creator.text}"
                        )
                    creators.append(str(creator.text))

        subjects_md = xml_metadata.findall("dc:subject", ns)
        subjects: list[str] = [str(subject.text) for subject in subjects_md]
        languages_md = xml_metadata.findall("dc:language", ns)
        languages: list[str] = [str(language.text) for language in languages_md]
        uuids_md = xml_metadata.findall("dc:identifier", ns)
        uuid: str = ""
        calibre_id: str = ""
        for u in uuids_md:
            if "id" not in u.attrib:
                continue
            if u.attrib["id"] == "calibre_id":
                calibre_id = str(u.text)
            if u.attrib["id"] == "uuid_id":
                uuid = str(u.text)

        publisher_md = xml_metadata.find("dc:publisher", ns)
        publisher: str = str(publisher_md.text) if publisher_md is not None else ""
        date_md = xml_metadata.find("dc:date", ns)
        date: str = str(date_md.text) if date_md is not None else ""
        # convert to datetime, add utc timezone
        pub_date:str = ""
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

        series: str = ""
        date_added: str = ""
        title_sort: str = ""
        timestamp: str = ""
        for meta in xml_metadata.findall("opf:meta", ns):
            if "name" in meta.attrib:
                if meta.attrib["name"] == "calibre:series":
                    series = meta.attrib["content"]
                if meta.attrib["name"] == "calibre:timestamp":
                    timestamp = str(meta.attrib["content"])
                    # timestamp can be 2023-11-11T17:03:48.214591+00:00 or 2023-11-11T17:03:48+00:00
                    timestamp = timestamp.split(".")[0]
                    if timestamp.endswith("+00:00"):
                        date_added_dt = datetime.strptime(
                            timestamp, "%Y-%m-%dT%H:%M:%S%z"
                        )
                    else:
                        date_added_dt = datetime.strptime(
                            timestamp, "%Y-%m-%dT%H:%M:%S"
                        )

                    date_added = date_added_dt.replace(
                        tzinfo=timezone.utc
                    ).isoformat()
                if meta.attrib["name"] == "calibre:title_sort":
                    calibre_prefixes:dict[str, dict[str, list[str]]] = {
                        "de": {"prefixes": ["Der", "Die", "Das", "Ein", "Eine"]},
                        "en": {"prefixes": ["The", "A", "An"]},
                    }
                    title_sort = meta.attrib["content"]
                    for (lang) in (calibre_prefixes):  # remove localized prefixes ", The", ", Der", etc. (curr: DE, EN)
                        prefixes = calibre_prefixes[lang]["prefixes"]
                        for prefix in prefixes:
                            ending = f", {prefix}"
                            if title_sort.endswith(ending):
                                title_sort = title_sort[: -len(ending)]
                                break
                    # Check if starts with lowercase
                    if title_sort[0].islower():
                        # check if second character is uppercase (iPad, jQuery, etc.)
                        if len(title_sort) > 1 and title_sort[1].islower():
                            self.log.warning(
                                f"Shortened title starts with lowercase: {title_sort}, consider fixing!"
                            )
                            # title_sort = title_sort[0].upper() + title_sort[1:]  # automatic fixing can go wrong (jQuery, etc.)
        identifiers:list[str] = []
        # Find records of type:
        # <dc:identifier opf:scheme="MOBI-ASIN">B0BTX2378L</dc:identifier>
        for id in xml_metadata.findall("dc:identifier", ns):
            # self.log.info(f"ID: {id.attrib} {id.text}")
            if "{http://www.idpf.org/2007/opf}scheme" in id.attrib:
                scheme:str = id.attrib["{http://www.idpf.org/2007/opf}scheme"]
                if id.text is not None:
                    sid:str = id.text
                else:
                    sid = ""
                if scheme not in ["calibre", "uuid"]:
                    identifiers.append(f"{scheme}/{sid}")
                    # self.log.info(f"{title} Identifier: {scheme}: {sid}")
        if calibre_id != "":
            identifiers.append(f"calibre_id/{calibre_id}")

        image_str = ""
        if create_icon is True:
            # Process cover image
            if existing_metadata and existing_metadata['icon']:
                image_str = existing_metadata['icon']            
            if not image_str:
                cover_path = os.path.join(os.path.dirname(filename), "cover.jpg")
                image_str = self.encode_image(cover_path)

        metadata = {
            "uuid": uuid,
            "authors": creators,
            "identifiers": identifiers,
            "languages": languages,
            "context": self.calibre_path,
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
            "converter_version": self.converter_version,
        }
        
        return metadata

    def mirror_library(self, target_series: list[str] | None = None, worker_id: int = 0, total_workers: int = 1):
        # walk calibre library path and look for 'metadata.opf':
        for root, _dirs, files in os.walk(self.calibre_path):
            if 'metadata.opf' in files:
                # Deterministic Partitioning: hash the relative path
                rel_root = os.path.relpath(root, self.calibre_path)
                path_hash = int(hashlib.md5(rel_root.encode()).hexdigest(), 16)
                if path_hash % total_workers != worker_id:
                    continue

                opf_path = os.path.join(root, 'metadata.opf')
                metadata = self.parse_calibre_metadata(opf_path, None, create_icon=False)
                if metadata is None:
                    self.log.error(f"Could not parse metadata from {opf_path}")
                    continue
                series = metadata['series'].lower()
                if target_series is None or series in target_series:
                    source_file = None
                    for file in files:
                        ext = os.path.splitext(file)[1]
                        if ext == '.epub':
                            source_file = os.path.join(root, file)
                            break
                    if source_file is None:
                        for file in files:
                            ext = os.path.splitext(file)[1]
                            if ext == '.pdf':
                                source_file = os.path.join(root, file)
                                break
                    if source_file is None:
                        continue
                    target_path = os.path.join(self.markdown_path, series)
                    if not os.path.exists(target_path):
                        os.makedirs(target_path)
                    basename = os.path.splitext(os.path.basename(source_file))[0] + '.md'
                    target_file = os.path.join(target_path, basename)
                    if os.path.exists(target_file):
                        self.log.info(f"File already exists: {target_file}")
                        with open(target_file, 'r') as f:
                            md_text = f.read()
                        yaml_metadata, _content = MarkdownConverter.parse_markdown(md_text)
                        if yaml_metadata is None:
                            # Add metadata:
                            metadata = self.parse_calibre_metadata(opf_path, None, create_icon=True)
                            md_text = MarkdownConverter.assemble_markdown(metadata, md_text)
                            atomic_write(target_file, md_text)
                            self.log.info(f"Successfully added metadata to '{target_file}'")
                            continue
                        continue
                    self.log.info(f"Converting '{source_file}' to markdown...")
                    markdown = self.converter.convert(source_file)
                    if markdown is None:
                        self.log.error(f"Failed to convert '{source_file}' to markdown")
                        continue
                    metadata = self.parse_calibre_metadata(opf_path, None, create_icon=True)
                    markdown = MarkdownConverter.assemble_markdown(metadata, markdown)
                    atomic_write(target_file, markdown)
                    self.log.info(f"Successfully converted '{source_file}' to markdown: {target_file}")
                    
                    # Manually clear large strings and trigger GC
                    del markdown
                    del metadata
                    clear_memory()


@runtime_checkable
class InferenceEngine(Protocol):
    """Protocol for cross-platform inference engines."""
    def generate(self, prompt: str, max_tokens: int = 1500, temp: float = 0.2, repetition_penalty: float = 1.1) -> str: ...
    def format_prompt(self, messages: list[dict[str, str]]) -> str: ...
    def get_token_count(self, text: str) -> int: ...


class MLXEngine:
    def __init__(self, model_id: str = "mlx-community/gemma-4-26b-a4b-it-4bit"):
        if not MLX_AVAILABLE:
            raise ImportError("MLX is not available on this platform.")
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

    def get_token_count(self, text: str) -> int:
        # Simple heuristic or use processor.tokenizer
        return len(self.processor.tokenizer.encode(text))


class LlamaCppEngine:
    def __init__(self, repo_id: str = "unsloth/gemma-4-26B-A4B-it-GGUF", filename: str = "gemma-4-26B-A4B-it-UD-Q4_K_XL.gguf"):
        if not LLAMA_CPP_AVAILABLE:
            raise ImportError("llama-cpp-python is not installed.")
        
        print(f"Loading LlamaCpp model from {repo_id}...")
        model_path = hf_hub_download(repo_id=repo_id, filename=filename)
        
        # 11GB VRAM for 17GB model -> ~60% layers offloaded
        # 11GB VRAM for 17GB model -> ~60% layers offloaded
        # With 32k context, we need ~12 layers offloaded to fit KV cache.
        self.llm = Llama(
            model_path=model_path,
            n_gpu_layers=12,  # Further reduced to fit KV cache
            n_ctx=32768,      # Large context for summarization
            flash_attn=True,  # Significant memory savings for long context
            verbose=False     # Disabled to reduce noise; set to True to debug VRAM/loading
        )

    def format_prompt(self, messages: list[dict[str, str]]) -> str:
        # Gemma IT format: <start_of_turn>user\n...<end_of_turn>\n<start_of_turn>model\n
        formatted = ""
        for msg in messages:
            role = msg["role"]
            content = msg["content"]
            formatted += f"<start_of_turn>{role}\n{content}<end_of_turn>\n"
        formatted += "<start_of_turn>model\n"
        return formatted

    def generate(self, prompt: str, max_tokens: int = 1500, temp: float = 0.2, repetition_penalty: float = 1.1) -> str:
        # llama-cpp-python's create_completion handles strings or 
        # create_chat_completion handles message lists. 
        # But we already formatted it.
        output = self.llm(
            prompt,
            max_tokens=max_tokens,
            # cache_prompt=True,
            temperature=temp,
            repeat_penalty=repetition_penalty,
            stop=["<end_of_turn>"]
        )
        return output["choices"][0]["text"].strip()

    def get_token_count(self, text: str) -> int:
        return len(self.llm.tokenize(text.encode("utf-8")))


class Summarizer:
    def __init__(self, engine: InferenceEngine | None = None, chunk_size: int = 50000) -> None:
        self.log: logging.Logger = logging.getLogger("Summarizer")
        if engine is None:
            # Auto-detect or default
            import platform
            if platform.system() == "Darwin" and platform.machine() == "arm64" and MLX_AVAILABLE:
                self.engine = MLXEngine()
            elif LLAMA_CPP_AVAILABLE:
                self.engine = LlamaCppEngine()
            else:
                raise RuntimeError("No suitable inference engine found.")
        else:
            self.engine = engine
        self.chunk_size: int = chunk_size

    def get_answer_from_output(self, text: str) -> str:
        """Utility to strip thinking tokens and return the final answer."""
        if "<channel|>" in text:
            return text.split("<channel|>")[-1].strip()
        return text

    def chunked_summarize(self, content: str, filepath: str, extra_instructions: str = "") -> str:
        """Map-Reduce strategy for large files to avoid VRAM overflow."""
        chunk_size = self.chunk_size 
        num_chunks = math.ceil(len(content) / chunk_size)
        
        chunk_summaries = []
        for i in range(num_chunks):
            start = i * chunk_size
            end = start + chunk_size
            chunk = content[start:end]

            chunk_start = time.time()
            progress_msg = f"--> Summarizing chunk {i+1}/{num_chunks}..."
            print(f"\r{progress_msg}", end="", flush=True)
            
            instruction = f"Briefly summarize this part of the document. {extra_instructions}" if extra_instructions else "Briefly summarize this part of the document:"
            prompt = self.engine.format_prompt([{"role": "user", "content": f"{instruction}\n\n{chunk}"}])
            
            output = self.engine.generate(
                prompt,
                max_tokens=400,
                temp=0.2,
                repetition_penalty=1.1
            )
            duration = time.time() - chunk_start
            print(f" ({duration:.1f}s)", end="", flush=True)
            chunk_summaries.append(self.get_answer_from_output(output))

        print("\n--> Consolidating final summary...")
        consolidated_text = "\n\n".join(chunk_summaries)
    
        base_instruction = "Please combine them into a single coherent, detailed summary"
        final_instruction = f"{base_instruction}. {extra_instructions}" if extra_instructions else base_instruction
    
        final_prompt = self.engine.format_prompt(
            [{"role": "user", "content": f"The following are summaries of segments from '{filepath}'. {final_instruction}:\n\n{consolidated_text}"}]
        )
    
        output = self.engine.generate(
            final_prompt,
            max_tokens=1500,
            temp=0.2,
            repetition_penalty=1.1
        )
        return output

    def generate_summaries(self, markdown_path: str, summaries_path: str, worker_id: int = 0, total_workers: int = 1) -> None:
        for root, dirs, files in os.walk(markdown_path):
            for file in files:
                if file.endswith(".md"):
                    source_file = os.path.join(root, file)

                    # Deterministic Partitioning: hash the relative file path
                    rel_file = os.path.relpath(source_file, markdown_path)
                    file_hash = int(hashlib.md5(rel_file.encode()).hexdigest(), 16)
                    if file_hash % total_workers != worker_id:
                        continue
                    target_path = os.path.join(summaries_path, root[len(markdown_path)+1:])
                    if not os.path.exists(target_path):
                        os.makedirs(target_path)
                    target_file = os.path.join(target_path, file)
                    if os.path.exists(target_file):
                        self.log.info(f"Summary already exists: {target_file}")
                        continue
                    with open(source_file, 'r') as f:
                        content = f.read()
                    metadata, md_text = MarkdownConverter.parse_markdown(content)
                    if metadata is None:
                        print(f"No metadata found for {source_file}")
                        continue
                    print(f"Summarizing {source_file}...")
                    summary = self.chunked_summarize(content, source_file)
                    summary_text = self.get_answer_from_output(summary)
                    sum_metadata = {}
                    if 'title' in metadata:
                        sum_metadata['title'] = metadata['title']
                    if 'authors' in metadata:
                        sum_metadata['authors'] = metadata['authors']
                    if 'tags' in metadata:
                        sum_metadata['tags'] = metadata['tags']
                    if 'uuid' in metadata:
                        sum_metadata['uuid'] = metadata['uuid']
                    else:
                        print(f"No uuid found for {source_file}")
                    full_summary = MarkdownConverter.assemble_markdown(sum_metadata, summary_text)
                    atomic_write(target_file, full_summary)
                    self.log.info(f"Successfully summarized '{source_file}' to '{target_file}'")

                    # Manually clear large objects
                    del content
                    del summary
                    del full_summary
                    del summary_text
                    clear_memory()


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
    if "summaries_path" not in config:
        config["summaries_path"] = os.path.expanduser("~/ReferenceLibrary/Summaries")
        atomic_write(config_file, json.dumps(config, indent=4))
    if "chunk_size" not in config:
        config["chunk_size"] = 50000
        atomic_write(config_file, json.dumps(config, indent=4))
    return config

def calibre_main(config, worker_id: int = 0, total_workers: int = 1):
    converter = CalibreConverter(config['calibre_path'], config['markdown_path'])
    converter.mirror_library(config['target_series'], worker_id, total_workers)

def sum_main(config, worker_id: int = 0, total_workers: int = 1):
    converter = Summarizer(chunk_size=config['chunk_size'])
    converter.generate_summaries(config['markdown_path'], config['summaries_path'], worker_id, total_workers)
    return

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    parser = argparse.ArgumentParser(description="Summarizer: Document conversion and summary generation.")
    parser.add_argument("-c", "--calibre", action="store_true", help="Mirror Calibre library and convert to markdown")
    parser.add_argument("-s", "--summarize", action="store_true", help="Generate summaries for markdown documents")
    parser.add_argument("--worker-id", type=int, default=0, help="Worker ID for deterministic partitioning (0 to total-workers - 1)")
    parser.add_argument("--total-workers", type=int, default=1, help="Total number of workers for deterministic partitioning")
    
    args = parser.parse_args()
    config = get_config()

    if not args.calibre and not args.summarize:
        parser.print_help()
    else:
        if args.calibre and args.summarize:
            print("Please chose one option: -c or -s.")
            exit(1)
        if args.calibre:
            calibre_main(config, args.worker_id, args.total_workers)
        
        if args.summarize:
            sum_main(config, args.worker_id, args.total_workers)
