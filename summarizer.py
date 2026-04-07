from mlx_vlm import load, generate
from mlx_vlm.prompt_utils import apply_chat_template
from mlx_vlm.utils import load_config
import math
import shlex
import logging
import json
import os
import hashlib

from sum_converter import MarkdownConverter


class Summarizer:
    def __init__(self, model=None, processor=None, config: dict[str, object] | None = None, chunk_size: int = 50000) -> None:
        self.log: logging.Logger = logging.getLogger("Summarizer")
        if model is None or processor is None or config is None:
            model_id = "mlx-community/gemma-4-26b-a4b-it-4bit"
            self.model, self.processor = load(model_id)
            self.config = load_config(model_id)
        else:
            self.model = model
            self.processor = processor
            self.config = config
        self.chunk_size: int = chunk_size

    def get_answer_from_output(self, output: object) -> str:
        """Utility to strip thinking tokens and return the final answer."""
        if hasattr(output, "text"):
            text = str(getattr(output, "text"))
        else:
            text = str(output)
        if "<channel|>" in text:
            return text.split("<channel|>")[-1].strip()
        return text

    def chunked_summarize(self, content: str, filepath: str, extra_instructions: str = "") -> object:
        """Map-Reduce strategy for large files to avoid VRAM overflow."""
        # 50,000 chars is roughly 12k tokens - safe for 26B model on most Macs
        chunk_size = 250000 
        num_chunks = math.ceil(len(content) / chunk_size)
        
        chunk_summaries = []
        for i in range(num_chunks):
            start = i * chunk_size
            end = start + chunk_size
            chunk = content[start:end]
        
            print(f"--> Summarizing chunk {i+1}/{num_chunks}...", flush=True)
            
            instruction = f"Briefly summarize this part of the document. {extra_instructions}" if extra_instructions else "Briefly summarize this part of the document:"
            prompt = apply_chat_template(
                self.processor, self.config,
                [{"role": "user", "content": f"{instruction}\n\n{chunk}"}],
                num_images=0
            )
            
            # Generate with lower max_tokens for speed during mapping
            output = generate(
                self.model, self.processor, prompt, [],
                max_tokens=400,
                temp=0.2,
                repetition_penalty=1.1,
                kv_bits=3.5,
                kv_quant_scheme="turboquant",
                verbose=False
            )
            chunk_summaries.append(self.get_answer_from_output(output))

        print("\n--> Consolidating final summary...")
        consolidated_text = "\n\n".join(chunk_summaries)
    
        base_instruction = "Please combine them into a single coherent, detailed summary"
        final_instruction = f"{base_instruction}. {extra_instructions}" if extra_instructions else base_instruction
    
        final_prompt = apply_chat_template(
            self.processor, self.config,
            [{"role": "user", "content": f"The following are summaries of segments from '{filepath}'. {final_instruction}:\n\n{consolidated_text}"}],
            num_images=0
        )
    
        return generate(
            self.model, self.processor, final_prompt, [],
            max_tokens=1500,
            temp=0.2,
            repetition_penalty=1.1,
            kv_bits=3.5,
            kv_quant_scheme="turboquant",
            verbose=False
        )

                        
class ArtifactCache:
    def __init__(self, cache_file: str | None = None) -> None:
        self.cache: dict[str, dict[str, str]] = {}
        self.log: logging.Logger = logging.getLogger("ArtifactCache")
        if cache_file is None:
            self.cache_file = os.path.expanduser("~/.config/summarizer/cache.json")
        else:
            self.cache_file = cache_file
        self.load()

    def get_hash(self, filename: str) -> str | None:
        if not os.path.exists(filename):
            self.log.error(f"File not found: {filename}")
            return None
        with open(filename, "rb") as f:
            file_content = f.read()
        return hashlib.sha256(file_content).hexdigest()

    def set(self, source_file: str, artifact_type: str, artifact: str, hash: str | None = None) -> bool:
        h = hash or self.get_hash(source_file)
        if h is None:
            return False
        if h not in self.cache:
            self.cache[h] = {}
        
        # Track source file for collision detection
        if 'source_file' in self.cache[h]:
            if self.cache[h]['source_file'] != source_file:
                if os.path.exists(self.cache[h]['source_file']):
                    self.log.error(f"Cache collision: {source_file} vs {self.cache[h]['source_file']}")
                    return False
        
        self.cache[h]['source_file'] = source_file
        self.cache[h][artifact_type] = artifact
        return True

    def get(self, source_file: str, artifact_type: str, hash: str | None = None) -> str | None:
        h = hash or self.get_hash(source_file)
        if h is not None and h in self.cache and artifact_type in self.cache[h]:
            return self.cache[h][artifact_type]
        return None

    def save(self) -> None:
        if os.path.exists(os.path.dirname(self.cache_file)) is False:
            os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
        with open(self.cache_file, "w", encoding="utf-8") as f:
            json.dump(self.cache, f, indent=4)

    def load(self) -> None:
        if os.path.exists(self.cache_file) is False:
            self.cache = {}
            return
        with open(self.cache_file, "r", encoding="utf-8") as f:
            self.cache = json.load(f)


class ChatAgent:
    def __init__(self, chunk_size: int = 50000, temperature: float = 0.0, repetition_penalty: float = 1.2) -> None:
        self.log: logging.Logger = logging.getLogger("ChatAgent")
        self.chat_history: list[dict[str, str]] = [
            {"role": "system", "content": "<|think|> You are a helpful AI assistant that thinks step-by-step before answering."}
        ]

        # Load model and processor
        model_id = "mlx-community/gemma-4-26b-a4b-it-4bit"
        self.model, self.processor = load(model_id)
        self.config = load_config(model_id)

        self.chunk_size: int = chunk_size
        self.temperature: float = temperature
        self.repetition_penalty: float = repetition_penalty
        
        self.summarizer = Summarizer(self.model, self.processor, self.config, self.chunk_size)
        self.converter = MarkdownConverter()
        self.cache = ArtifactCache()

        print("Gemma 4 Chat Agent initialized (Thinking Mode & TurboQuant enabled).")
        print("Type 'exit' or 'quit' to end the conversation.\n")

    def get_response(self, user_input: str) -> dict[str, str]:
        """Programmatic interface for the agent."""
        try:
            if user_input.startswith(("/load ", "/summarize ")):
                try:
                    parts = shlex.split(user_input)
                except ValueError as e:
                    return {"role": "assistant", "content": f"Error parsing command: {e}"}
            
                if len(parts) < 2:
                    return {"role": "assistant", "content": "Usage: /load <filepath> [instructions] or /summarize <filepath> [instructions]"}
                
                command = parts[0]
                filepath = parts[1]
                extra_info = " ".join(parts[2:]).strip()
                is_summarize = (command == "/summarize")

                file_hash = self.cache.get_hash(filepath)
                if file_hash is None:
                    return {"role": "assistant", "content": f"File not found: {filepath}"}
                
                # Try to get existing conversion
                file_content = self.cache.get(filepath, 'markdown', hash=file_hash)
                if file_content is None:
                    file_content = self.converter.convert(filepath)
                    if file_content is None:
                        return {"role": "assistant", "content": f"Failed to convert file: {filepath}"}
                    self.cache.set(filepath, 'markdown', file_content, hash=file_hash)
            
                if is_summarize:
                    msg_content = self.cache.get(filepath, 'summary', hash=file_hash)
                    if msg_content is None:
                        summary_output = self.summarizer.chunked_summarize(file_content, filepath, extra_info)
                        msg_content = self.summarizer.get_answer_from_output(summary_output)
                        self.cache.set(filepath, 'summary', msg_content, hash=file_hash)
                    msg = f"Summary of '{filepath}':\n\n{msg_content}\n\nNotes: {extra_info}" if extra_info else f"Summary of '{filepath}':\n\n{msg_content}"
                else:
                    instruction = extra_info if extra_info else "What would you like me to do with this text?"
                    msg = f"I have loaded '{filepath}'. Content:\n\n{file_content}\n\n{instruction}"
            else:
                msg = user_input
            
            # Single shared generation point
            self.chat_history.append({"role": "user", "content": msg})
            formatted_prompt = apply_chat_template(self.processor, self.config, self.chat_history, num_images=0)
            
            output = generate(
                self.model, self.processor, formatted_prompt, [], 
                max_tokens=2000, 
                temp=self.temperature,
                repetition_penalty=self.repetition_penalty,
                kv_bits=3.5, 
                kv_quant_scheme="turboquant", 
                verbose=False
            )
            self.cache.save()
                
            if hasattr(output, "text"):
                full_response = str(getattr(output, "text"))
            else:
                full_response = str(output)
            
            if "<channel|>" in full_response:
                parts = full_response.split("<channel|>")
                thought = parts[0].replace("<|channel>thought", "").strip()
                answer = parts[1].strip()
                self.chat_history.append({"role": "assistant", "content": answer})
                return {"role": "assistant", "content": answer, "thought": thought}
            else:
                self.chat_history.append({"role": "assistant", "content": full_response})
                return {"role": "assistant", "content": full_response}
                
        except Exception as e:
            self.log.error(f"Error: {e}")
            return {"role": "assistant", "content": f"An internal error occurred: {e}"}

    def run(self) -> None:
        while True:
            try:
                user_input = input("User: ")
                if user_input.lower() in ["exit", "quit"]:
                    break
                
                response = self.get_response(user_input)
                if response.get("thought"):
                    print(f"\n[Thought Process]:\n{response['thought']}\n")
                print(f"Assistant: {response['content']}\n")
                    
            except Exception as e:
                self.log.error(f"Error: {e}")
                break
        self.cache.save()
        print("\nChat ended.")


def agent_main():
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
    try:
        ca.run()
    except KeyboardInterrupt:
        print("\nChat ended.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    agent_main()
