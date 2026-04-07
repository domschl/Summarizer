import logging
import os
import subprocess
import json
from datetime import datetime, timezone
import xml.etree.ElementTree as ET
import base64
import io
import yaml
from typing import Any
from PIL import Image

from docling.document_converter import DocumentConverter


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
            return result.document.export_to_markdown()
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
            if not image_str
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

    def split_header_content(self, text:str) -> tuple[str, str]:
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
         
    def parse_markdown(self, md_text:str):
        frontmatter, content = self.split_header_content(md_text)
        try:
            yaml_metadata: dict[str, Any]|Any = yaml.safe_load(frontmatter)  # pyright: ignore[reportAny, reportExplicitAny]
        except Exception as e:
            self.log.error(f"Error parsing frontmatter: {e}")
            yaml_metadata = None
        return yaml_metadata, content

    def assemble_markdown(self, metadata, md_text) -> str:  # pyright: ignore[reportExplicitAny]
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

    def mirror_library(self, target_series: list[str] | None = None):
        # walk calibre library path and look for 'metadata.opf':
        for root, _dirs, files in os.walk(self.calibre_path):
            if 'metadata.opf' in files:
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
                        yaml_metadata, _content = self.parse_markdown(md_text)
                        if yaml_metadata is None:
                            # Add metadata:
                            metadata = self.parse_calibre_metadata(opf_path, None, create_icon=True)
                            md_text = self.assemble_markdown(metadata, md_text)
                            with open(target_file, 'w') as f:
                                _ = f.write(md_text)
                            self.log.info(f"Successfully added metadata to '{target_file}'")
                            continue
                        continue
                    self.log.info(f"Converting '{source_file}' to markdown...")
                    markdown = self.converter.convert(source_file)
                    if markdown is None:
                        self.log.error(f"Failed to convert '{source_file}' to markdown")
                        continue
                    metadata = self.parse_calibre_metadata(opf_path, None, create_icon=True)
                    markdown = self.assemble_markdown(metadata, markdown)
                    with open(target_file, 'w') as f:
                        _ = f.write(markdown)
                    self.log.info(f"Successfully converted '{source_file}' to markdown: {target_file}")


def calibre_main():
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
            "target_series": ["anthropology", "music", "history"]
        }
        os.makedirs(os.path.dirname(config_file), exist_ok=True)
        with open(config_file, 'w') as f:
            json.dump(config, f, indent=4)
    calibre_path = config['calibre_path']
    markdown_path = config['markdown_path']
    target_series = config['target_series']
    converter = CalibreConverter(calibre_path, markdown_path)
    converter.mirror_library(target_series)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    calibre_main()
