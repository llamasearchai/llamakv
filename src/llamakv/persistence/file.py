"""
File backend implementation for LlamaKV.

This module provides a file-based storage backend for the key-value store,
which persists data to disk in a JSON file.
"""

import json
import os
import re
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from llamakv.core.key import Key
from llamakv.core.value import Value, StringValue, IntValue, FloatValue, BytesValue, JsonValue, PickleValue


class FileBackend:
    """
    File-based storage backend for the key-value store.
    
    Stores data in a JSON file on disk, which provides persistence
    across process restarts.
    """
    
    def __init__(self, 
                 file_path: str, 
                 auto_sync: bool = True,
                 sync_interval: int = 5):
        """
        Initialize a file backend.
        
        Args:
            file_path: Path to the storage file
            auto_sync: Whether to automatically sync to disk
            sync_interval: Interval for auto-syncing (in seconds)
        """
        self._file_path = file_path
        self._auto_sync = auto_sync
        self._sync_interval = sync_interval
        self._store: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()
        self._on_set_callbacks: List[Callable[[Key, Value], None]] = []
        self._on_delete_callbacks: List[Callable[[Key], None]] = []
        self._last_sync = time.time()
        self._dirty = False
        
        # Stats
        self._reads = 0
        self._writes = 0
        self._deletes = 0
        self._syncs = 0
        
        # Load data from file if it exists
        self._load()
        
        # Start auto-sync thread if enabled
        if auto_sync:
            self._sync_thread = threading.Thread(target=self._auto_sync_thread, daemon=True)
            self._sync_thread.start()
    
    def _auto_sync_thread(self) -> None:
        """Thread function for auto-syncing."""
        while True:
            time.sleep(1)  # Check every second
            if self._auto_sync and self._dirty and time.time() - self._last_sync >= self._sync_interval:
                try:
                    self.sync()
                except Exception as e:
                    # Log error but don't crash thread
                    pass
    
    def _load(self) -> None:
        """Load data from file."""
        with self._lock:
            if not os.path.exists(self._file_path):
                return
            
            try:
                with open(self._file_path, 'r') as f:
                    data = json.load(f)
                
                for key_str, value_data in data.items():
                    # Create Key object
                    key = Key.from_string(key_str)
                    
                    # Determine value type and create Value object
                    value_type = value_data.get('type')
                    if value_type == 'StringValue':
                        value = StringValue.from_dict(value_data)
                    elif value_type == 'IntValue':
                        value = IntValue.from_dict(value_data)
                    elif value_type == 'FloatValue':
                        value = FloatValue.from_dict(value_data)
                    elif value_type == 'BytesValue':
                        value = BytesValue.from_dict(value_data)
                    elif value_type == 'JsonValue':
                        value = JsonValue.from_dict(value_data)
                    elif value_type == 'PickleValue':
                        value = PickleValue.from_dict(value_data)
                    else:
                        # Skip unknown value types
                        continue
                    
                    # Store in memory
                    self._store[key_str] = value_data
            except Exception as e:
                # If loading fails, start with empty store
                self._store = {}
    
    def sync(self) -> None:
        """Sync data to disk."""
        with self._lock:
            # Ensure directory exists
            os.makedirs(os.path.dirname(os.path.abspath(self._file_path)), exist_ok=True)
            
            # Write to temporary file first to avoid corruption if process is killed
            temp_path = f"{self._file_path}.tmp"
            with open(temp_path, 'w') as f:
                json.dump(self._store, f)
            
            # Rename to actual file (atomic operation on most file systems)
            os.replace(temp_path, self._file_path)
            
            self._last_sync = time.time()
            self._dirty = False
            self._syncs += 1
    
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
        key_str = str(key)
        value_dict = value.to_dict()
        
        with self._lock:
            self._store[key_str] = value_dict
            self._writes += 1
            self._dirty = True
        
        # Call callbacks
        for callback in self._on_set_callbacks:
            try:
                callback(key, value)
            except Exception as e:
                # Don't let callback exceptions propagate
                pass
        
        # Sync immediately if auto_sync is disabled
        if not self._auto_sync:
            self.sync()
    
    def get(self, key: Key) -> Optional[Value]:
        """
        Get a value for a key.
        
        Args:
            key: The key
            
        Returns:
            The value, or None if not found
        """
        key_str = str(key)
        
        with self._lock:
            if key_str in self._store:
                self._reads += 1
                value_data = self._store[key_str]
                
                # Determine value type and create Value object
                value_type = value_data.get('type')
                if value_type == 'StringValue':
                    return StringValue.from_dict(value_data)
                elif value_type == 'IntValue':
                    return IntValue.from_dict(value_data)
                elif value_type == 'FloatValue':
                    return FloatValue.from_dict(value_data)
                elif value_type == 'BytesValue':
                    return BytesValue.from_dict(value_data)
                elif value_type == 'JsonValue':
                    return JsonValue.from_dict(value_data)
                elif value_type == 'PickleValue':
                    return PickleValue.from_dict(value_data)
            
            return None
    
    def delete(self, key: Key) -> bool:
        """
        Delete a key from the store.
        
        Args:
            key: The key
            
        Returns:
            True if the key was deleted, False if it didn't exist
        """
        key_str = str(key)
        
        with self._lock:
            if key_str in self._store:
                del self._store[key_str]
                self._deletes += 1
                self._dirty = True
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
            
            # Sync immediately if auto_sync is disabled
            if not self._auto_sync:
                self.sync()
        
        return deleted
    
    def keys(self, pattern: Optional[str] = None, namespace: Optional[str] = None) -> List[Key]:
        """
        Get all keys in the store.
        
        Args:
            pattern: Optional pattern to filter keys
            namespace: Optional namespace to filter keys
            
        Returns:
            List of keys
        """
        with self._lock:
            result = []
            for key_str in self._store.keys():
                key = Key.from_string(key_str)
                
                # Filter by namespace if specified
                if namespace is not None and key.namespace != namespace:
                    continue
                
                # Filter by pattern if specified
                if pattern is not None:
                    # Use regex pattern matching
                    if not re.search(pattern, key_str):
                        continue
                
                result.append(key)
            
            return result
    
    def clear(self) -> None:
        """Clear all keys from the store."""
        with self._lock:
            self._store.clear()
            self._dirty = True
        
        # Sync immediately if auto_sync is disabled
        if not self._auto_sync:
            self.sync()
    
    def stats(self) -> Dict[str, Any]:
        """
        Get statistics about the backend.
        
        Returns:
            Dictionary of statistics
        """
        with self._lock:
            file_size = 0
            if os.path.exists(self._file_path):
                file_size = os.path.getsize(self._file_path)
            
            return {
                'reads': self._reads,
                'writes': self._writes,
                'deletes': self._deletes,
                'syncs': self._syncs,
                'keys': len(self._store),
                'file_backend': True,
                'file_path': self._file_path,
                'file_size': file_size,
                'last_sync': self._last_sync,
                'auto_sync': self._auto_sync,
                'dirty': self._dirty
            } 