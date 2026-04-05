import React, { useState, useRef, useEffect } from 'react';
import ReactMarkdown from 'react-markdown';
import remarkMath from 'remark-math';
import rehypeKatex from 'rehype-katex';
import { Send, Upload, FileText, Bot, User, Loader2, Sparkles, X } from 'lucide-react';

interface Message {
  role: 'user' | 'assistant';
  content: string;
  thought?: string;
  id: string;
}

const App: React.FC = () => {
  const [messages, setMessages] = useState<Message[]>([
    {
      role: 'assistant',
      content: 'Hello! I am your Gemma 4 agent with LaTeX and file processing capabilities. How can I help you today?',
      id: 'welcome'
    }
  ]);
  const [input, setInput] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [activeFiles, setActiveFiles] = useState<{name: string, path: string}[]>([]);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  const handleSend = async () => {
    if (!input.trim() || isLoading) return;

    const userMessage: Message = {
      role: 'user',
      content: input,
      id: Date.now().toString()
    };

    setMessages(prev => [...prev, userMessage]);
    setInput('');
    setIsLoading(true);

    try {
      const response = await fetch('http://localhost:8000/chat', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ message: input }),
      });

      const data = await response.json();
      const assistantMessage: Message = {
        role: 'assistant',
        content: data.content,
        thought: data.thought,
        id: (Date.now() + 1).toString()
      };

      setMessages(prev => [...prev, assistantMessage]);
    } catch (error) {
      console.error('Error fetching chat:', error);
      setMessages(prev => [...prev, {
        role: 'assistant',
        content: 'Sorry, I encountered an error. Please make sure the backend is running.',
        id: (Date.now() + 1).toString()
      }]);
    } finally {
      setIsLoading(false);
    }
  };

  const handleFileUpload = async (event: React.ChangeEvent<HTMLInputElement>, mode: 'load' | 'summarize' = 'load') => {
    const file = event.target.files?.[0];
    if (!file) return;

    setIsLoading(true);
    const formData = new FormData();
    formData.append('file', file);
    formData.append('mode', mode);

    try {
      const response = await fetch('http://localhost:8000/upload', {
        method: 'POST',
        body: formData,
      });

      const data = await response.json();
      
      const assistantMessage: Message = {
        role: 'assistant',
        content: data.agent_response.content,
        thought: data.agent_response.thought,
        id: Date.now().toString()
      };

      setActiveFiles(prev => [...prev, { name: data.original_filename, path: data.saved_path }]);
      setMessages(prev => [...prev, assistantMessage]);
    } catch (error) {
       console.error('Error uploading file:', error);
       setMessages(prev => [...prev, {
        role: 'assistant',
        content: 'Error uploading or processing file.',
        id: Date.now().toString()
      }]);
    } finally {
      setIsLoading(false);
      if (fileInputRef.current) fileInputRef.current.value = '';
    }
  };

  return (
    <div className="app-container">
      <header className="header">
        <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
          <Sparkles color="#6366f1" size={28} />
          <h1>Gemma 4 Assistant</h1>
        </div>
        <div style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>
          Thinking Mode & TurboQuant Enabled
        </div>
      </header>

      <main className="chat-window">
        <div className="messages">
          {messages.map((m) => (
            <div key={m.id} className={`message ${m.role}`}>
              <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '8px', fontWeight: 600 }}>
                {m.role === 'user' ? <User size={16} /> : <Bot size={16} />}
                {m.role === 'user' ? 'You' : 'Assistant'}
              </div>
              
              {m.thought && (
                <div className="thought-process">
                  <div style={{ fontWeight: 600, fontSize: '0.75rem', marginBottom: '4px', textTransform: 'uppercase' }}>Thought Process</div>
                  {m.thought}
                </div>
              )}
              
              <div className="markdown">
                <ReactMarkdown 
                  remarkPlugins={[remarkMath]} 
                  rehypePlugins={[rehypeKatex]}
                >
                  {m.content}
                </ReactMarkdown>
              </div>
            </div>
          ))}
          {isLoading && (
            <div className="message assistant">
              <div className="loading-dots">
                <div></div><div></div><div></div>
              </div>
            </div>
          )}
          <div ref={messagesEndRef} />
        </div>

        <div className="input-area">
          {activeFiles.length > 0 && (
            <div className="upload-zone">
              {activeFiles.map((f, i) => (
                <div key={i} className="file-tag">
                  <FileText size={14} />
                  {f.name}
                  <X 
                    size={12} 
                    style={{ cursor: 'pointer' }} 
                    onClick={() => setActiveFiles(prev => prev.filter((_, idx) => idx !== i))}
                  />
                </div>
              ))}
            </div>
          )}
          
          <div className="input-row">
            <button 
              className="icon-button secondary" 
              title="Upload & Summarize"
              onClick={() => fileInputRef.current?.click()}
              disabled={isLoading}
            >
              <Upload size={20} />
            </button>
            <input 
              type="file" 
              ref={fileInputRef} 
              style={{ display: 'none' }} 
              onChange={(e) => handleFileUpload(e, 'summarize')} 
            />
            
            <textarea 
              className="message-input"
              placeholder="Type a message or use /summarize <path>..."
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === 'Enter' && !e.shiftKey) {
                  e.preventDefault();
                  handleSend();
                }
              }}
              rows={1}
            />
            
            <button 
              className="icon-button"
              onClick={handleSend}
              disabled={!input.trim() || isLoading}
            >
              {isLoading ? <Loader2 className="animate-spin" size={20} /> : <Send size={20} />}
            </button>
          </div>
        </div>
      </main>
    </div>
  );
};

export default App;
