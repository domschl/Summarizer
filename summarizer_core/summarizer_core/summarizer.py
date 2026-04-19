import math
import os
import time
import logging
from typing import Optional
from .engine import BaseEngine
from .cache import WorkCache
from .markdown import get_answer_from_output

logger = logging.getLogger("summarizer_core.summarizer")

def chunked_summarize(engine: BaseEngine, content: str, filepath: str, chunk_size: int, doc_hash: str) -> str:
    num_chunks = math.ceil(len(content) / chunk_size)
    filename = os.path.basename(filepath)
    
    if num_chunks == 0: 
        return ""
        
    if num_chunks == 1:
        print(f"\n--> Document '{filename}' fits in one chunk. Summarizing directly...")
        prompt_text = f"The following is the full text of '{filepath}'. Please provide a detailed summary:\n\n{content}"
        
        if hasattr(engine, 'format_prompt'):
            prompt = engine.format_prompt([{"role": "user", "content": prompt_text}])
        else:
            prompt = prompt_text
            
        output = engine.generate(prompt, max_tokens=1500)
        return get_answer_from_output(output)
        
    cache = WorkCache()
    chunk_summaries, start_index = cache.load_progress(doc_hash, chunk_size)
    
    if start_index > 0:
        if start_index >= num_chunks:
            print(f"\n--> [{filename}] All chunks already processed. Resuming final consolidation...")
        else:
            print(f"\n--> [{filename}] Resuming from chunk {start_index+1}/{num_chunks}...")

    for i in range(start_index, num_chunks):
        start = i * chunk_size
        end = start + chunk_size
        chunk = content[start:end]

        if not chunk.strip():
            logger.info(f"Chunk {i+1} is empty or whitespace only, skipping.")
            cache.save_progress(doc_hash, chunk_size, chunk_summaries, i + 1, filepath)
            continue

        chunk_start = time.time()
        progress_msg = f"--> [{filename}] Summarizing chunk {i+1}/{num_chunks}..."
        print(f"\r{progress_msg}", end="", flush=True)
        
        prompt_text = f"Briefly summarize this part of document '{filepath}':\n\n{chunk}"
        
        if hasattr(engine, 'format_prompt'):
            prompt = engine.format_prompt([{"role": "user", "content": prompt_text}])
        else:
            prompt = prompt_text
            
        output = engine.generate(prompt, max_tokens=500)
        duration = time.time() - chunk_start
        print(f" ({duration:.1f}s)", end="", flush=True)
        
        extracted_output = get_answer_from_output(output)
        chunk_summaries.append(extracted_output)
        cache.save_progress(doc_hash, chunk_size, chunk_summaries, i + 1, filepath)

    print(f"\n--> [{filename}] Consolidating final summary...")
    
    if not chunk_summaries:
        logger.info(f"No valid summaries generated for {filename}, returning empty summary.")
        cache.clear_progress(doc_hash, chunk_size)
        return ""
        
    consolidated_text = "\n\n".join(chunk_summaries)
    
    final_prompt_text = f"The following are summaries of segments from '{filepath}'. Please combine them into a single coherent, detailed summary:\n\n{consolidated_text}"
    
    if hasattr(engine, 'format_prompt'):
        final_prompt = engine.format_prompt([{"role": "user", "content": final_prompt_text}])
    else:
        final_prompt = final_prompt_text
        
    output = engine.generate(final_prompt, max_tokens=1500)
    final_summary = get_answer_from_output(output)
    
    cache.clear_progress(doc_hash, chunk_size)
    return final_summary
