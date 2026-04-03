# Gemma 4 Summarizer

A powerful, local AI-powered summarization tool designed for Apple Silicon. It uses **Gemma 4-26b-it** (via `mlx-vlm`) to process and summarize documents of any size.

## Features
- **Gemma 4-26B Inference**: Optimized with **TurboQuant** (4-bit quantization) for ultra-fast performance on Mac.
- **Thinking Mode**: Shows the model's step-by-step reasoning for transparent and high-quality results.
- **Automated Chunking**: Uses a **Map-Reduce** strategy to summarize documents larger than the standard context limit (automated for files > 100k chars).
- **Document Conversion**: Built-in integration with **Docling** to automatically convert PDFs (perfect for technical ArXiv papers), DOCX, PPTX, and XLSX to clean Markdown before summarization.
- **Command Support**:
    - `/summarize <filepath> [instructions]`: Summarize a file with optional specific goals (e.g., "in German").
    - `/load <filepath> [instructions]`: Load a file and ask a question about it.
    - Supports quoted filenames for paths with spaces (e.g., `/summarize "My File.pdf"`).

## Getting Started

### Prerequisites
- Apple Silicon Mac (M1/M2/M3/M4)
- Python 3.10+ (recommended with `uv`)

### Installation
Clone the repository and install dependencies:
```bash
git clone https://codeberg.org/domschl/Summarizer.git
cd Summarizer
uv sync
```

### Usage
Run the script to start the interactive chat:
```bash
uv run summarizer.py
```

Inside the chat:
```text
User: /summarize "Deep Learning Paper.pdf" explain the math in German
```

## How It Works
- **Small Files**: Small files are read directly into the context for single-pass summarization.
- **Large Files**: For massive documents, the script splits the text into chunks, summarizes each part individually, and then executes a final "consolidation" pass to create a coherent summary.
- **Docling Integration**: Non-text files are automatically processed using Docling to ensure structural integrity (tables, LaTeX math) is maintained for the LLM.

## Model
Uses `mlx-community/gemma-4-26b-a4b-it-4bit` (via `mlx-vlm`).
