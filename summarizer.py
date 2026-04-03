from mlx_vlm import load, generate
from mlx_vlm.prompt_utils import apply_chat_template
from mlx_vlm.utils import load_config
import math

# Load model and processor
model_id = "mlx-community/gemma-4-26b-a4b-it-4bit"
model, processor = load(model_id)
config = load_config(model_id)

def get_answer_from_output(output):
    """Utility to strip thinking tokens and return the final answer."""
    text = str(output.text if hasattr(output, "text") else output)
    if "<channel|>" in text:
        return text.split("<channel|>")[-1].strip()
    return text

def chunked_summarize(content, filepath):
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
        
        prompt = apply_chat_template(
            processor, config,
            [{"role": "user", "content": f"Briefly summarize this part of the document:\n\n{chunk}"}],
            num_images=0
        )
        
        # Generate with lower max_tokens for speed during mapping
        output = generate(
            model, processor, prompt, [],
            max_tokens=400,
            temp=0.0,
            repetition_penalty=1.2,
            kv_bits=3.5,
            kv_quant_scheme="turboquant",
            verbose=False
        )
        chunk_summaries.append(get_answer_from_output(output))

    print("\n--> Consolidating final summary...")
    consolidated_text = "\n\n".join(chunk_summaries)
    final_prompt = apply_chat_template(
        processor, config,
        [{"role": "user", "content": f"The following are summaries of segments from '{filepath}'. Please combine them into a single coherent, detailed summary:\n\n{consolidated_text}"}],
        num_images=0
    )
    
    return generate(
        model, processor, final_prompt, [],
        max_tokens=1500,
        temp=0.0,
        repetition_penalty=1.2,
        kv_bits=3.5,
        kv_quant_scheme="turboquant",
        verbose=False
    )

chat_history = [
    {"role": "system", "content": "<|think|> You are a helpful AI assistant that thinks step-by-step before answering."}
]

print("Gemma 4 Chat Agent initialized (Thinking Mode & TurboQuant enabled).")
print("Standard context limit: ~100k chars. Automated Chunking enabled for larger files.")
print("Type 'exit' or 'quit' to end the conversation.\n")

while True:
    try:
        user_input = input("User: ")
        if user_input.lower() in ["exit", "quit"]:
            break
            
        if user_input.startswith(("/load ", "/summarize ")):
            is_summarize = user_input.startswith("/summarize")
            filepath = user_input.split(" ", 1)[1].strip()
            
            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    file_content = f.read()
                
                # If the file is huge, use chunked summarization immediately
                if is_summarize and len(file_content) > 100000:
                    output = chunked_summarize(file_content, filepath)
                else:
                    if is_summarize:
                        msg = f"The following is a text file contents from '{filepath}'. Please provide a concise summary of its main points:\n\n{file_content}"
                    else:
                        msg = f"I have loaded the file from '{filepath}'. The content is as follows:\n\n{file_content}\n\nWhat would you like me to do with this text?"
                    
                    chat_history.append({"role": "user", "content": msg})
                    print(f"Loaded {len(file_content)} characters from '{filepath}'.")
                    
                    formatted_prompt = apply_chat_template(processor, config, chat_history, num_images=0)
                    print("\nAssistant is thinking...", flush=True)
                    output = generate(
                        model, processor, formatted_prompt, [], 
                        max_tokens=2000, 
                        temp=0.0,
                        repetition_penalty=1.2,
                        kv_bits=3.5, 
                        kv_quant_scheme="turboquant", 
                        verbose=False
                    )
            except Exception as e:
                print(f"Error loading file: {e}")
                continue
        else:
            chat_history.append({"role": "user", "content": user_input})
            formatted_prompt = apply_chat_template(processor, config, chat_history, num_images=0)
            print("\nAssistant is thinking...", flush=True)
            output = generate(
                model, processor, formatted_prompt, [], 
                max_tokens=2000, 
                temp=0.0,
                repetition_penalty=1.2,
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
            chat_history.append({"role": "assistant", "content": answer})
        else:
            print(f"\nAssistant: {full_response}\n")
            chat_history.append({"role": "assistant", "content": full_response})
            
    except KeyboardInterrupt:
        break
    except Exception as e:
        print(f"Error: {e}")
        break

print("\nChat ended.")