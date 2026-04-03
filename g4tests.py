from mlx_vlm import load, generate
from mlx_vlm.prompt_utils import apply_chat_template
from mlx_vlm.utils import load_config

# Load model and processor
model_id = "mlx-community/gemma-4-26b-a4b-it-4bit"
model, processor = load(model_id)
config = load_config(model_id)

# Initialize chat history with a system prompt that enables thinking mode
# Thinking mode is triggered by <|think|> at the start of the system instruction
chat_history = [
    {"role": "system", "content": "<|think|> You are a helpful AI assistant that thinks step-by-step before answering."}
]

print("Gemma 4 Chat Agent initialized (Thinking Mode & TurboQuant enabled).")
print("Type 'exit' or 'quit' to end the conversation.\n")

while True:
    try:
        user_input = input("User: ")
        if user_input.lower() in ["exit", "quit"]:
            break
        
        # Add user message to history
        chat_history.append({"role": "user", "content": user_input})
        
        # Format the prompt using the chat template
        formatted_prompt = apply_chat_template(
            processor, 
            config, 
            chat_history, 
            num_images=0
        )
        
        # Generate response using TurboQuant for efficiency and high token limit for thinking
        # kv_bits=3.5 and kv_quant_scheme="turboquant" follow the Gemma 4 blog optimization guide
        print("\nAssistant is thinking...", flush=True)
        
        output = generate(
            model, 
            processor, 
            formatted_prompt, 
            [], # No images for this chat agent
            max_tokens=4000,
            kv_bits=3.5,
            kv_quant_scheme="turboquant",
            verbose=False
        )
        
        # Clean the output if it's not a string (mlx-vlm generate can return an object in some versions)
        if hasattr(output, "text"):
            full_response = output.text
        else:
            full_response = str(output)
            
        # Gemma 4 thinking mode output structure:
        # <|channel>thought
        # [Internal reasoning]
        # <channel|>
        # [Final answer]
        
        if "<channel|>" in full_response:
            parts = full_response.split("<channel|>")
            thought = parts[0].replace("<|channel>thought", "").strip()
            answer = parts[1].strip()
            
            print(f"\n[Thought Process]:\n{thought}\n")
            print(f"Assistant: {answer}\n")
            
            # For multi-turn conversations, only the final answer should be added to history
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