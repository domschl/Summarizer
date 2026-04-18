import os
import json
import time

def get_platform_config(config_name: str, default_config: dict = None):
    config_file = os.path.expanduser(f"~/.config/summarizer/{config_name}")
    try:
        if os.path.exists(config_file):
            with open(config_file, 'r') as f:
                return json.load(f)
    except Exception:
        pass
    return default_config or {"chunk_size": 50000}

def watchdog():
    """Exits the process if the parent process dies (PPID becomes 1)."""
    while True:
        if os.getppid() == 1:
            print("Parent process died. Exiting...")
            os._exit(1)
        time.sleep(2)
