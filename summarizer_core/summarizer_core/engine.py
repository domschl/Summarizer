from typing import Protocol

class BaseEngine(Protocol):
    def format_prompt(self, messages: list[dict[str, str]]) -> str:
        """Formats the chat messages into a model-compatible prompt."""
        ...

    def generate(self, prompt: str, max_tokens: int = 1500, temp: float = 0.2, repetition_penalty: float = 1.1) -> str:
        """Generates a response from the model."""
        ...
