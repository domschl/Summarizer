import os
import sys
import argparse
import time
import math
import yaml
from mlx_vlm import load, generate
from mlx_vlm.prompt_utils import apply_chat_template
from mlx_vlm.utils import load_config

class MLXEngine:
    def __init__(self, model_id: str = "mlx-community/gemma-4-26b-a4b-it-4bit"):
        print(f"Loading MLX model from {model_id}...")
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

def get_answer_from_output(text: str) -> str:
    if "<channel|>" in text:
        return text.split("<channel|>")[-1].strip()
    return text

def chunked_summarize(engine, content: str, filepath: str, chunk_size: int = 50000) -> str:
    num_chunks = math.ceil(len(content) / chunk_size)
    if num_chunks == 0:
        return ""
        
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
            
        metadata, md_text = parse_markdown(content)
        
        print(f"Initializing MLX Engine...")
        engine = MLXEngine()
        
        summary_text = chunked_summarize(engine, md_text, os.path.basename(source_file))
        
        sum_metadata = {}
        if metadata:
            for key in ['title', 'authors', 'tags', 'uuid']:
                if key in metadata:
                    sum_metadata[key] = metadata[key]
                    
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
    parser = argparse.ArgumentParser(description="Summarize Markdown using MLX (macOS)")
    parser.add_argument("source", help="Path to the source markdown file")
    parser.add_argument("destination", help="Path to the destination summary markdown file")
    
    args = parser.parse_args()
    summarize_file(args.source, args.destination)

if __name__ == "__main__":
    main()
