import os
import shutil
import uuid
import sys
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Any, Annotated
# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from summarizer import ChatAgent

app = FastAPI()

# Enable CORS for frontend development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Shared agent instance
agent = ChatAgent()
UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

class ChatRequest(BaseModel):
    message: str

class ChatResponse(BaseModel):
    content: str
    thought: str | None = None
    role: str = "assistant"

@app.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    response = agent.get_response(request.message)
    return response

@app.post("/upload")
async def upload_file(
    file: Annotated[UploadFile, File(...)], 
    mode: Annotated[str, Form()] = "load"
):
    """
    mode can be "load" or "summarize"
    """
    if mode not in ["load", "summarize"]:
        raise HTTPException(status_code=400, detail="Invalid mode. Must be 'load' or 'summarize'.")
    
    filename = file.filename or "uploaded_file"
    file_id = str(uuid.uuid4())
    _, ext = os.path.splitext(filename)
    safe_filename = f"{file_id}{ext}"
    filepath = os.path.join(UPLOAD_DIR, safe_filename)
    
    with open(filepath, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    
    # Absolute path for the agent
    abs_path = os.path.abspath(filepath)
    
    # Prepare the command for the agent
    command = f"/{mode} \"{abs_path}\""
    
    # Process with agent
    response = agent.get_response(command)
    
    # Return both the agent's response and the filename info
    return {
        "agent_response": response,
        "original_filename": file.filename,
        "saved_path": abs_path
    }

@app.get("/history")
async def get_history():
    return agent.chat_history

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
