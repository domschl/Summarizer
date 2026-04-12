import os
import sys
import argparse
import time
import math
import yaml
import threading
from huggingface_hub import hf_hub_download
from llama_cpp import Llama

# Constants
VERSION = "0.1.0"
MODEL_NAME = "gemma-4-26b-it-gguf"

class LlamaCppEngine:
    def __init__(self, repo_id: str = "unsloth/gemma-4-26B-A4B-it-GGUF", filename: str = "gemma-4-26B-A4B-it-UD-Q4_K_XL.gguf"):
        print(f"Loading LlamaCpp model from {repo_id}...")
        model_path = hf_hub_download(repo_id=repo_id, filename=filename)
        
        self.llm = Llama(
            model_path=model_path,
            n_gpu_layers=12,
            n_ctx=32768,
            flash_attn=True,
            verbose=False
        )

    def format_prompt(self, messages: list[dict[str, str]]) -> str:
        formatted = ""
        for msg in messages:
            role = msg["role"]
            content = msg["content"]
            formatted += f"<start_of_turn>{role}\n{content}<end_of_turn>\n"
        formatted += "<start_of_turn>model\n"
        return formatted

    def generate(self, prompt: str, max_tokens: int = 1500, temp: float = 0.2, repetition_penalty: float = 1.1) -> str:
        output = self.llm(
            prompt,
            max_tokens=max_tokens,
            temperature=temp,
            repeat_penalty=repetition_penalty,
            stop=["<end_of_turn>"]
        )
        return output["choices"][0]["text"].strip()

def get_answer_from_output(text: str) -> str:
    if "<channel|>" in text:
        return text.split("<channel|>")[-1].strip()
    return text

def chunked_summarize(engine, content: str, filepath: str, chunk_size: int = 50000) -> str:
    num_chunks = math.ceil(len(content) / chunk_size)
    if num_chunks == 0:
        return ""
        
    if num_chunks == 1:
        print("\n--> Document fits in one chunk. Summarizing directly...")
        instruction = "Please provide a detailed summary of this document:"
        prompt = engine.format_prompt([{"role": "user", "content": f"The following is the full text of '{filepath}'. {instruction}\n\n{content}"}])
        
        output = engine.generate(
            prompt,
            max_tokens=1500,
            temp=0.2,
            repetition_penalty=1.1
        )
        return get_answer_from_output(output)
        
    chunk_summaries = []
    for i in range(num_chunks):
        start = i * chunk_size
        end = start + chunk_size
        chunk = content[start:end]

        chunk_start = time.time()
        progress_msg = f"--> Summarizing chunk {i+1}/{num_chunks}..."
        print(f"\r{progress_msg}", end="", flush=True)
        
        instruction = "Briefly summarize this part of the document:"
        prompt = engine.format_prompt([{"role": "user", "content": f"{instruction}\n\n{chunk}"}])
        
        output = engine.generate(
            prompt,
            max_tokens=400,
            temp=0.2,
            repetition_penalty=1.1
        )
        duration = time.time() - chunk_start
        print(f" ({duration:.1f}s)", end="", flush=True)
        chunk_summaries.append(get_answer_from_output(output))

    print("\n--> Consolidating final summary...")
    consolidated_text = "\n\n".join(chunk_summaries)

    base_instruction = "Please combine them into a single coherent, detailed summary:"
    final_prompt = engine.format_prompt(
        [{"role": "user", "content": f"The following are summaries of segments from '{filepath}'. {base_instruction}\n\n{consolidated_text}"}]
    )

    output = engine.generate(
        final_prompt,
        max_tokens=1500,
        temp=0.2,
        repetition_penalty=1.1
    )
    return get_answer_from_output(output)

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

def parse_markdown(md_text: str):
    frontmatter, content = split_header_content(md_text)
    try:
        yaml_metadata = yaml.safe_load(frontmatter)
    except Exception:
        yaml_metadata = None
    return yaml_metadata, content

def get_platform_config():
    config_file = os.path.expanduser("~/.config/summarizer/summarizer_config_linux.json")
    try:
        if os.path.exists(config_file):
            with open(config_file, 'r') as f:
                import json
                return json.load(f)
    except Exception:
        pass
    return {"chunk_size": 50000}

def watchdog():
    """Exits the process if the parent process dies (PPID becomes 1)."""
    while True:
        if os.getppid() == 1:
            print("Parent process died. Exiting...")
            os._exit(1)
        time.sleep(2)

def assemble_markdown(metadata, md_text: str) -> str:
    if metadata is None:
        return md_text
        
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
    return f"---\n{header}---\n{md_text}"

def summarize_file(source_file: str, destination_file: str):
    if not os.path.exists(source_file):
        print(f"Error: Source file does not exist: {source_file}")
        sys.exit(1)
        
    try:
        with open(source_file, 'r', encoding='utf-8') as f:
            content = f.read()
            
        config = get_platform_config()
        chunk_size = config.get("chunk_size", 50000)
        
        metadata, md_text = parse_markdown(content)
        
        print(f"Initializing LlamaCpp Engine...")
        engine = LlamaCppEngine()
        
        summary_text = chunked_summarize(engine, md_text, os.path.basename(source_file), chunk_size=chunk_size)
        
        sum_metadata = {}
        if metadata:
            for key in ['title', 'authors', 'tags', 'uuid']:
                if key in metadata:
                    sum_metadata[key] = metadata[key]
        
        sum_metadata['summary_version'] = f"{MODEL_NAME} {VERSION}"
        
        # Track which version of the markdown was used for this summary
        import hashlib
        source_md_hash = hashlib.sha256(content.encode('utf-8')).hexdigest()
        sum_metadata['source_md_hash'] = source_md_hash
                    
        full_summary = assemble_markdown(sum_metadata, summary_text)
        
        target_dir = os.path.dirname(destination_file)
        if target_dir and not os.path.exists(target_dir):
            os.makedirs(target_dir, exist_ok=True)
            
        # Write to temporary file first then move it to simulate atomic write as in sum_converter.py
        temp_file = destination_file + ".tmp"
        with open(temp_file, 'w', encoding='utf-8') as f:
            f.write(full_summary)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_file, destination_file)
            
        print(f"Successfully wrote summary to: {destination_file}")
        sys.exit(0)
    except Exception as e:
        print(f"Error during summarization: {e}")
        sys.exit(1)

def main():
    # Start watchdog thread
    w = threading.Thread(target=watchdog, daemon=True)
    w.start()

    parser = argparse.ArgumentParser(description="Summarize Markdown using Llama.cpp (Linux)")
    parser.add_argument("source", help="Path to the source markdown file")
    parser.add_argument("destination", help="Path to the destination summary markdown file")
    
    args = parser.parse_args()
    summarize_file(args.source, args.destination)

if __name__ == "__main__":
    main()
