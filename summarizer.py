from mlx_vlm import load, generate
from mlx_vlm.prompt_utils import apply_chat_template
from mlx_vlm.utils import load_config
import math
import shlex
import logging
import json
import os

from docling.document_converter import DocumentConverter


class Summarizer:
    def __init__(self, model=None, processor=None, config=None, chunk_size=50000):
        self.log = logging.getLogger("Summarizer")
        if model is None or processor is None or config is None:
            model_id = "mlx-community/gemma-4-26b-a4b-it-4bit"
            self.model, self.processor = load(model_id)
            self.config = load_config(model_id)
        else:
            self.model = model
            self.processor = processor
            self.config = config
        self.chunk_size = chunk_size

    def get_answer_from_output(self, output):
        """Utility to strip thinking tokens and return the final answer."""
        text = str(output.text if hasattr(output, "text") else output)
        if "<channel|>" in text:
            return text.split("<channel|>")[-1].strip()
        return text

    def chunked_summarize(self, content, filepath, extra_instructions: str = ""):
        """Map-Reduce strategy for large files to avoid VRAM overflow."""
        # 50,000 chars is roughly 12k tokens - safe for 26B model on most Macs
        chunk_size = 250000 
        num_chunks = math.ceil(len(content) / chunk_size)
        
        print(f"\n[System]: '{filepath}' is too large for single-pass summarization ({len(content)} chars).")
        print(f"Switching to Map-Reduce strategy ({num_chunks} chunks)...\n")
    
        chunk_summaries = []
        for i in range(num_chunks):
            start = i * chunk_size
            end = start + chunk_size
            chunk = content[start:end]
        
        print(f"--> Summarizing chunk {i+1}/{num_chunks}...", flush=True)
        
        instruction = f"Briefly summarize this part of the document. {extra_instructions}" if extra_instructions else "Briefly summarize this part of the document:"
        prompt = apply_chat_template(
            processor, config,
            [{"role": "user", "content": f"{instruction}\n\n{chunk}"}],
            num_images=0
        )
        
        # Generate with lower max_tokens for speed during mapping
        output = generate(
            model, processor, prompt, [],
            max_tokens=400,
            temp=0.2,
            repetition_penalty=1.1,
            kv_bits=3.5,
            kv_quant_scheme="turboquant",
            verbose=False
        )
        chunk_summaries.append(get_answer_from_output(output))

        print("\n--> Consolidating final summary...")
        consolidated_text = "\n\n".join(chunk_summaries)
    
        base_instruction = "Please combine them into a single coherent, detailed summary"
        final_instruction = f"{base_instruction}. {extra_instructions}" if extra_instructions else base_instruction
    
        final_prompt = apply_chat_template(
            processor, config,
            [{"role": "user", "content": f"The following are summaries of segments from '{filepath}'. {final_instruction}:\n\n{consolidated_text}"}],
            num_images=0
        )
    
        return generate(
            model, processor, final_prompt, [],
            max_tokens=1500,
            temp=0.2,
            repetition_penalty=1.1,
            kv_bits=3.5,
            kv_quant_scheme="turboquant",
            verbose=False
        )


class ChatAgent:
    def __init__(self, chunk_size=50000, temperature=0.0, repetition_penalty=1.2):
        self.log = logging.getLogger("ChatAgent")
        self.chat_history = [
            {"role": "system", "content": "<|think|> You are a helpful AI assistant that thinks step-by-step before answering."}
        ]

        # Load model and processor
        model_id = "mlx-community/gemma-4-26b-a4b-it-4bit"
        self.model, self.processor = load(model_id)
        self.config = load_config(model_id)

        self.chunk_size = chunk_size
        self.temperature = temperature
        self.repetition_penalty = repetition_penalty
        self.summarizer = Summarizer(self.model, self.processor, self.config, self.chunk_size)

        print("Gemma 4 Chat Agent initialized (Thinking Mode & TurboQuant enabled).")
        print("Standard context limit: ~100k chars. Automated Chunking enabled for larger files.")
        print("Type 'exit' or 'quit' to end the conversation.\n")

    def run(self):
        while True:
            try:
                user_input = input("User: ")
                if user_input.lower() in ["exit", "quit"]:
                    break
            
                if user_input.startswith(("/load ", "/summarize ")):
                    try:
                        parts = shlex.split(user_input)
                    except ValueError as e:
                        print(f"Error parsing command: {e}")
                        continue
                
                    if len(parts) < 2:
                        print("Usage: /load <filepath> [instructions] or /summarize <filepath> [instructions]")
                        continue
                    
                    command = parts[0]
                    filepath = parts[1]
                    extra_info = " ".join(parts[2:]).strip()
                    is_summarize = command == "/summarize"

                    if is_summarize:
                        print(f"Loading {filepath}...")
                        print(f"Extra instructions: {extra_info}")
            
                    try:
                        # Handle non-text files using Docling
                        if filepath.lower().endswith((".pdf", ".docx", ".pptx", ".xlsx", ".html")):
                            print(f"Converting '{filepath}' to markdown using Docling (this may take a moment)...")
                            converter = DocumentConverter()
                            result = converter.convert(filepath)
                            file_content = result.document.export_to_markdown()
                        else:
                            with open(filepath, "r", encoding="utf-8") as f:
                                file_content = f.read()
                
                        # If the file is huge, use chunked summarization immediately
                        if is_summarize and len(file_content) > self.chunk_size:
                            output = self.summarizer.chunked_summarize(file_content, filepath, extra_info)
                        else:
                            if is_summarize:
                                instruction = extra_info if extra_info else "Please provide a concise summary of its main points"
                                msg = f"The following is a text file contents from '{filepath}'. {instruction}:\n\n{file_content}"
                            else:
                                instruction = extra_info if extra_info else "What would you like me to do with this text?"
                                msg = f"I have loaded the file from '{filepath}'. The content is as follows:\n\n{file_content}\n\n{instruction}"
                    
                            self.chat_history.append({"role": "user", "content": msg})
                            print(f"Loaded {len(file_content)} characters from '{filepath}'.")
                        
                            formatted_prompt = apply_chat_template(self.processor, self.config, self.chat_history, num_images=0)
                            print("\nAssistant is thinking...", flush=True)
                            output = generate(
                                self.model, self.processor, formatted_prompt, [], 
                                max_tokens=2000, 
                                temp=self.temperature,
                                repetition_penalty=self.repetition_penalty,
                                kv_bits=3.5, 
                                kv_quant_scheme="turboquant", 
                                verbose=False
                            )
                    except Exception as e:
                        print(f"Error loading file: {e}")
                        continue
                else:
                    self.chat_history.append({"role": "user", "content": user_input})
                    formatted_prompt = apply_chat_template(self.processor, self.config, self.chat_history, num_images=0)
                    print("\nAssistant is thinking...", flush=True)
                    output = generate(
                        self.model, self.processor, formatted_prompt, [], 
                        max_tokens=2000, 
                        temp=self.temperature,
                        repetition_penalty=self.repetition_penalty,
                        kv_bits=3.5, 
                        kv_quant_scheme="turboquant", 
                        verbose=False
                    )
                    
                full_response = str(output.text if hasattr(output, "text") else output)
                
                if "<channel|>" in full_response:
                    parts = full_response.split("<channel|>")
                    thought = parts[0].replace("<|channel>thought", "").strip()
                    answer = parts[1].strip()
                    print(f"\n[Thought Process]:\n{thought}\n")
                    print(f"Assistant: {answer}\n")
                    self.chat_history.append({"role": "assistant", "content": answer})
                else:
                    print(f"\nAssistant: {full_response}\n")
                    self.chat_history.append({"role": "assistant", "content": full_response})
                    
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Error: {e}")
                break

        print("\nChat ended.")


if __name__ == "__main__":
    config_file = os.path.expanduser("~/.config/summarizer/config.json")
    if os.path.exists(config_file):
        with open(config_file, "r") as f:
            config = json.load(f)
    else:
        config = {
            "chunk_size": 50000,
            "temperature": 0.0,
            "repetition_penalty": 1.2
        }
        if not os.path.exists(os.path.dirname(config_file)):
            os.makedirs(os.path.dirname(config_file))
        with open(config_file, "w") as f:
            json.dump(config, f, indent=4)
            print(f"Created default config file at {config_file}")
    ca = ChatAgent(**config)
    ca.run()
