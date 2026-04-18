import os
import json
import logging
from datetime import datetime

logger = logging.getLogger("summarizer_core.cache")

class WorkCache:
    def __init__(self, cache_dir: str = "~/.cache/summarizer/work_cache"):
        self.cache_dir = os.path.expanduser(cache_dir)
        os.makedirs(self.cache_dir, exist_ok=True)

    def _get_path(self, doc_hash: str, chunk_size: int) -> str:
        return os.path.join(self.cache_dir, f"{doc_hash}_{chunk_size}.json")

    def load_progress(self, doc_hash: str, chunk_size: int) -> tuple[list[str], int]:
        path = self._get_path(doc_hash, chunk_size)
        if os.path.exists(path):
            try:
                with open(path, 'r') as f:
                    data = json.load(f)
                    last_updated = datetime.fromisoformat(data['last_updated'])
                    if (datetime.now() - last_updated).days < 14:
                        return data.get('chunk_summaries', []), data.get('next_index', 0)
                    else:
                        logger.info(f"Cache entry too old, discarding: {path}")
                        os.remove(path)
            except Exception as e:
                logger.warning(f"Failed to load cache {path}: {e}")
        return [], 0

    def save_progress(self, doc_hash: str, chunk_size: int, chunk_summaries: list[str], next_index: int, filepath: str):
        path = self._get_path(doc_hash, chunk_size)
        data = {
            "doc_hash": doc_hash,
            "chunk_size": chunk_size,
            "filepath": filepath,
            "chunk_summaries": chunk_summaries,
            "next_index": next_index,
            "last_updated": datetime.now().isoformat()
        }
        temp_path = path + ".tmp"
        try:
            with open(temp_path, 'w') as f:
                json.dump(data, f, indent=4)
            os.replace(temp_path, path)
        except Exception as e:
            logger.error(f"Failed to save work cache: {e}")

    def clear_progress(self, doc_hash: str, chunk_size: int):
        path = self._get_path(doc_hash, chunk_size)
        if os.path.exists(path):
            try:
                os.remove(path)
            except Exception as e:
                logger.warning(f"Failed to remove cache entry: {e}")
                
    def clear_by_hash_only(self, doc_hash: str):
        try:
            for filename in os.listdir(self.cache_dir):
                if filename.startswith(f"{doc_hash}_") and filename.endswith(".json"):
                    os.remove(os.path.join(self.cache_dir, filename))
        except Exception as e:
            logger.warning(f"Failed to remove cache entries for hash {doc_hash}: {e}")

    def cleanup_old_entries(self, max_age_days: int = 14):
        now = datetime.now()
        count = 0
        try:
            for filename in os.listdir(self.cache_dir):
                if not filename.endswith(".json"):
                    continue
                path = os.path.join(self.cache_dir, filename)
                try:
                    with open(path, 'r') as f:
                        data = json.load(f)
                        last_updated = datetime.fromisoformat(data['last_updated'])
                        if (now - last_updated).days >= max_age_days:
                            os.remove(path)
                            count += 1
                except Exception:
                    pass
            if count > 0:
                logger.info(f"Cleaned up {count} old work-cache entries.")
        except Exception as e:
            logger.error(f"Error during cache cleanup: {e}")

    def get_all_active_caches(self) -> dict[str, dict]:
        """Returns a mapping of content_hash -> cache data for orchestrator check."""
        active = {}
        try:
            for filename in os.listdir(self.cache_dir):
                if not filename.endswith(".json"):
                    continue
                path = os.path.join(self.cache_dir, filename)
                try:
                    with open(path, 'r') as f:
                        data = json.load(f)
                        active[data['doc_hash']] = data
                except Exception:
                    pass
        except Exception as e:
            logger.error(f"Error reading cache dir: {e}")
        return active
