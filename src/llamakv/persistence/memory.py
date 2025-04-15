"""
Memory backend implementation for LlamaKV.

This module provides a simple in-memory storage backend for the key-value store.
"""

import re
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from llamakv.core.key import Key
from llamakv.core.value import Value


class MemoryBackend:
    """
    In-memory storage backend for the key-value store.

    Stores all data in a Python dictionary in memory. This backend is fast
    but non-persistent; all data is lost when the process exits.
    """

    def __init__(self):
        """Initialize a new memory backend."""
        self._store: Dict[Key, Value] = {}
        self._lock = threading.RLock()
        self._on_set_callbacks: List[Callable[[Key, Value], None]] = []
        self._on_delete_callbacks: List[Callable[[Key], None]] = []

        # Stats
        self._reads = 0
        self._writes = 0
        self._deletes = 0

    def register_on_set(self, callback: Callable[[Key, Value], None]) -> None:
        """
        Register a callback for set operations.

        Args:
            callback: Function to call when a key is set
        """
        self._on_set_callbacks.append(callback)

    def register_on_delete(self, callback: Callable[[Key], None]) -> None:
        """
        Register a callback for delete operations.

        Args:
            callback: Function to call when a key is deleted
        """
        self._on_delete_callbacks.append(callback)

    def set(self, key: Key, value: Value) -> None:
        """
        Set a value for a key.

        Args:
            key: The key
            value: The value
        """
        with self._lock:
            self._store[key] = value
            self._writes += 1

        # Call callbacks
        for callback in self._on_set_callbacks:
            try:
                callback(key, value)
            except Exception as e:
                # Don't let callback exceptions propagate
                pass

    def get(self, key: Key) -> Optional[Value]:
        """
        Get a value for a key.

        Args:
            key: The key

        Returns:
            The value, or None if not found
        """
        with self._lock:
            if key in self._store:
                self._reads += 1
                return self._store[key]
            return None

    def delete(self, key: Key) -> bool:
        """
        Delete a key from the store.

        Args:
            key: The key

        Returns:
            True if the key was deleted, False if it didn't exist
        """
        with self._lock:
            if key in self._store:
                del self._store[key]
                self._deletes += 1
                deleted = True
            else:
                deleted = False

        # Call callbacks (only if the key was deleted)
        if deleted:
            for callback in self._on_delete_callbacks:
                try:
                    callback(key)
                except Exception as e:
                    # Don't let callback exceptions propagate
                    pass

        return deleted

    def keys(
        self, pattern: Optional[str] = None, namespace: Optional[str] = None
    ) -> List[Key]:
        """
        Get all keys in the store.

        Args:
            pattern: Optional pattern to filter keys
            namespace: Optional namespace to filter keys

        Returns:
            List of keys
        """
        with self._lock:
            if pattern is None and namespace is None:
                return list(self._store.keys())

            result = []
            for key in self._store.keys():
                # Filter by namespace if specified
                if namespace is not None and key.namespace != namespace:
                    continue

                # Filter by pattern if specified
                if pattern is not None:
                    key_str = str(key)
                    # Use regex pattern matching
                    if not re.search(pattern, key_str):
                        continue

                result.append(key)

            return result

    def clear(self) -> None:
        """Clear all keys from the store."""
        with self._lock:
            self._store.clear()

    def stats(self) -> Dict[str, Any]:
        """
        Get statistics about the backend.

        Returns:
            Dictionary of statistics
        """
        with self._lock:
            return {
                "reads": self._reads,
                "writes": self._writes,
                "deletes": self._deletes,
                "keys": len(self._store),
                "memory_backend": True,
            }
