import os
import sys
import subprocess
import argparse
import logging
import warnings
from docling.datamodel.base_models import InputFormat
from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.datamodel.pipeline_options import PdfPipelineOptions

def convert(source_file: str, destination_file: str):
    if not os.path.exists(source_file):
        print(f"Error: Source file does not exist: {source_file}")
        sys.exit(1)
        
    ext = source_file.lower()
    
    if ext.endswith('.epub'):
        try:
            result = subprocess.run(
                ['pandoc', source_file, '-t', 'gfm', '-o', destination_file],
                capture_output=True, text=True, check=True
            )
            print(f"Successfully converted EPUB: {source_file} to {destination_file}")
            sys.exit(0)
        except subprocess.CalledProcessError as e:
            print(f"Pandoc conversion failed: {e}")
            sys.exit(1)
        except FileNotFoundError:
            print("Error: pandoc is not installed or not in PATH.")
            sys.exit(1)
            
    elif ext.endswith(('.pdf', '.docx', '.pptx', '.xlsx', '.html')):
        try:
            # Silence specific noisy loggers that spam non-fatal errors or progress
            logging.getLogger("docling.models.inference_engines.vlm.transformers_engine").setLevel(logging.WARNING)
            logging.getLogger("docling.models.inference_engines.vlm.auto_inline_engine").setLevel(logging.WARNING)
            logging.getLogger("httpx").setLevel(logging.WARNING)
            warnings.filterwarnings("ignore", message="The tied weights mapping")
            
            # Using Docling default OCR as requested (avoiding tesseract which corrupts diacritics).
            # Enable math formula translation to latex embedded in markdown
            pipeline_options = PdfPipelineOptions()
            pipeline_options.do_formula_enrichment = True
            
            converter = DocumentConverter(
                format_options={
                    InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)
                }
            )
            result = converter.convert(source_file)
            markdown_content = result.document.export_to_markdown()
            
            target_dir = os.path.dirname(destination_file)
            if target_dir and not os.path.exists(target_dir):
                os.makedirs(target_dir, exist_ok=True)
                
            with open(destination_file, 'w', encoding='utf-8') as f:
                f.write(markdown_content)
                
            print(f"Successfully converted document: {source_file} to {destination_file}")
            sys.exit(0)
            
        except Exception as e:
            print(f"Docling conversion failed: {e}")
            sys.exit(1)
    else:
        print(f"Error: Unsupported file extension for {source_file}")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="Convert PDF/EPUB to Markdown (macOS)")
    parser.add_argument("source", help="Path to the source file (e.g. .pdf, .epub)")
    parser.add_argument("destination", help="Path to the destination Markdown file")
    
    args = parser.parse_args()
    convert(args.source, args.destination)

if __name__ == "__main__":
    main()
