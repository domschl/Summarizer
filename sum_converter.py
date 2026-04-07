import logging
import os
import subprocess
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
        
    def mirror_library(self, target_series: list[str] | None = None):
        # walk calibre library path and look for 'metadata.opf':
        for root, _dirs, files in os.walk(self.calibre_path):
            if 'metadata.opf' in files:
                opf_path = os.path.join(root, 'metadata.opf')
                # get 'series' from opf_path
                with open(opf_path, 'r') as f:
                    series = f.read()
                    try:
                        # Format is '<meta name="calibre:series" content="History"/>'
                        # or: <meta content="Programming" name="calibre:series"/>   
                        if 'name="calibre:series" content="' in series:
                            series = series.split('<meta name="calibre:series" content="')[1].split('"/>')[0]
                        elif 'content="Programming" name="calibre:series"' in series:
                            series = series.split('<meta content="')[1].split('" name="calibre:series"')[0]
                    except IndexError:
                        self.log.warning(f"Could not find series in {opf_path}")
                        continue
                series = series.lower()
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
                        continue
                    self.log.info(f"Converting '{source_file}' to markdown...")
                    markdown = self.converter.convert(source_file)
                    if markdown is None:
                        self.log.error(f"Failed to convert '{source_file}' to markdown")
                        continue
                    with open(target_file, 'w') as f:
                        _ = f.write(markdown)
                    self.log.info(f"Successfully converted '{source_file}' to markdown: {target_file}")


def calibre_main():
    calibre_path = os.path.expanduser("~/ReferenceLibrary/Calibre Library")
    markdown_path = os.path.expanduser("~/ReferenceLibrary/MarkdownLibrary")
    converter = CalibreConverter(calibre_path, markdown_path)
    converter.mirror_library(['anthropology', 'music'])

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    calibre_main()
