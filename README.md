# 📚 Document Summarizer

A powerful, distributed document processing and summarization pipeline optimized for high-quality AI-generated summaries. This system synchronizes a Calibre library with a Markdown-based knowledge base and generates comprehensive summaries using **Gemma 4** models.

## ✨ Features

- **🔄 Two-Phase Synchronization**: Robust "Plan then Execute" architecture ensuring data integrity and reliable state management.
- **🏗️ Distributed Architecture**: Separates document conversion from summarization, allowing for parallel processing and platform-specific optimizations.
- **🧠 Gemma 4 Summarization**: Uses state-of-the-art **Gemma 4** models with support for "Thinking Mode" and Map-Reduce strategies for large documents.
- **🛠️ Multi-Format Support**:
    - **Docling**: High-fidelity conversion for PDFs (including math/ArXiv), DOCX, PPTX, and XLSX.
    - **Pandoc**: Clean EPUB-to-Markdown conversion.
- **⚡ Parallel Processing**: Built-in concurrency support for both conversion and summarization phases.
- **🔗 Wiki-Ready Naming**: Automatic generation of deterministic, 80-character capped filenames compatible with modern wikis (e.g., Obsidian).
- **💾 Work Caching**: Persistent hash-based caching to prevent redundant work and allow resuming interrupted tasks.

## 🚀 Getting Started

### Prerequisites
- Python 3.10+ (recommended with `uv`)
- [Pandoc](https://pandoc.org/installing.html) (for EPUB support)
- macOS (Apple Silicon) or Linux for summarization engines.

### Installation
```bash
git clone https://codeberg.org/domschl/Summarizer.git
cd Summarizer
uv sync
```

## 🛠️ Core Components

### 1. Calibre Sync (`calibre_sync.py`)
Orchestrates the conversion of books from your Calibre library into a Markdown-based repository. It extracts metadata, preserves covers (as icons), and converts documents using the best available tool for each format.

**Usage:**
```bash
uv run calibre_sync.py [--concurrency N] [--dry-run]
```
- `--concurrency`: Number of parallel conversion processes.
- `--dry-run`: Scans the library and shows the sync plan without making changes.

### 2. Summarizer Sync (`summarizer_sync.py`)
Orchestrates the summarization of the Markdown repository. It compares the current state of summaries with the source Markdown files and dispatches summarization tasks to the configured AI engine.

**Usage:**
```bash
uv run summarizer_sync.py [--concurrency N] [--dry-run]
```
- `--concurrency`: Number of parallel summarization processes.
- `--dry-run`: Scans source Markdown files and shows the summarization plan.

## ⚙️ Configuration

Both scripts use JSON configuration files located in `~/.config/summarizer/`.

### Converter Configuration (`converter_config.json`)
Controls how books are pulled from Calibre.
```json
{
    "calibre_path": "~/ReferenceLibrary/Calibre Library",
    "markdown_path": "~/AINotes/MarkdownBooks",
    "target_series": ["anthropology", "music", "history"]
}
```

### Summarizer Configuration (`summarizer_config.json`)
Controls how Markdown files are summarized.
```json
{
    "markdown_path": "~/AINotes/MarkdownBooks",
    "summaries_path": "~/AINotes/BookSummaries",
    "target_series": ["anthropology", "music", "history"],
    "summarizer_name": "summarizer_macos"
}
```
Available `summarizer_name` options:
- `summarizer_macos`: Optimized for Apple Silicon (MLX).
- `summarizer_linux`: Standard Linux implementation.
- `summarizer_gc_gemma4_31b`: Google Cloud hosted Gemma 4 implementation.

## 🛡️ Data Integrity
- **SHA-256 Hashing**: Tracks source file changes to automatically trigger reconversions or re-summarizations only when content actually changes.
- **UUID Tracking**: Uses Calibre UUIDs to track documents even if titles or authors change in metadata.
- **Atomic Writes**: Ensures files are never left in a corrupted state during interruptions.
