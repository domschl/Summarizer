# Gemma 4 Summarizer

A powerful, local AI-powered summarization tool designed for Apple Silicon. It uses **Gemma 4-26b-it** (via `mlx-vlm`) to process and summarize documents of any size.

## Features
- **Gemma 4-26B Inference**: Optimized with **TurboQuant** (4-bit quantization) for ultra-fast performance on Mac.
- **Thinking Mode**: Shows the model's step-by-step reasoning for transparent and high-quality results.
- **Automated Chunking**: Uses a **Map-Reduce** strategy to summarize large texts that exceed the model context limit.
- **Document Conversion**: Built-in support for multiple formats:
    - **Docling**: Automatically converts PDFs (perfect for technical ArXiv papers), DOCX, PPTX, and XLSX.
    - **Pandoc**: Handles **EPUB** conversion into high-quality Markdown.
    - **Source Code**: Reads and structures plain text and code files for easy analysis.
- **Command Support**:
    - `/summarize <filepath> [instructions]`: Summarize a file with optional specific goals (e.g., "in German").
    - `/load <filepath> [instructions]`: Load a file and ask a question about it.
    - Supports quoted filenames for paths with spaces (e.g., `/summarize "My File.pdf"`).

## Getting Started

### Prerequisites
- Apple Silicon Mac (M1/M2/M3/M4)
- Python 3.10+ (recommended with `uv`)
- Node.js & npm (for the web interface)

### Installation
Clone the repository and install dependencies:
```bash
git clone https://codeberg.org/domschl/Summarizer.git
cd Summarizer
uv sync
```

### Usage

#### Terminal Interface
Run the script to start the interactive chat:
```bash
uv run summarizer.py
```

#### Web Interface
The project includes a modern, premium web interface. To run it, you need two terminals:

1. **Start the Backend**:
   ```bash
   cd web-interface/backend
   uv run main.py
   ```
   *The backend runs on `http://localhost:8000`.*

2. **Start the Frontend**:
   ```bash
   cd web-interface/frontend
   npm install
   npm run dev
   ```
   *The frontend runs on `http://localhost:3000`.*

## Web Interface Features
- **Modern Dark UI**: Elegant, responsive interface with glassmorphism.
- **LaTeX Math Support**: Correctly renders complex formulas and formatted Markdown.
- **Thinking Pane**: Visualizes the model's step-by-step reasoning process.
- **Direct Upload**: Easily upload files via the UI for instant loading or summarization.

## How It Works
- **Small Files**: Small files are read directly into the context for single-pass summarization.
- **Large Files**: For massive documents, the script splits the text into chunks, summarizes each part individually, and then executes a final "consolidation" pass to create a coherent summary.
- **Multi-tool Conversion**: Non-text files are automatically processed using **Docling** (PDFs/Office docs) or **Pandoc** (EPUB) to maintain structural integrity and formatting for the LLM.

## Model
Uses `mlx-community/gemma-4-26b-a4b-it-4bit` (via `mlx-vlm`).
