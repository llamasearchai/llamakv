"""
LRU cache implementation for LlamaKV.

This module provides a Least Recently Used (LRU) caching strategy
for the key-value store.
"""

import threading
import time
from collections import OrderedDict
from typing import Any, Dict, List, Optional, Set, Tuple

from llamakv.core.key import Key
from llamakv.core.value import Value
from llamakv.cache.strategy import CacheStrategy


class LRUCache(CacheStrategy):
    """
    Least Recently Used (LRU) cache strategy.
    
    Maintains a fixed-size cache and evicts the least recently used
    items when the cache reaches its capacity.
    """
    
    def __init__(self, capacity: int = 1000, ttl_check: bool = True):
        """
        Initialize an LRU cache.
        
        Args:
            capacity: Maximum number of items to store in the cache
            ttl_check: Whether to check TTL on get operations
        """
        self._capacity = capacity
        self._ttl_check = ttl_check
        self._cache = OrderedDict()  # type: OrderedDict[Key, Value]
        self._lock = threading.RLock()
        
        # Stats
        self._hits = 0
        self._misses = 0
        self._inserts = 0
        self._evictions = 0
        self._expires = 0
    
    def get(self, key: Key) -> Optional[Value]:
        """
        Get a value from the cache.
        
        Args:
            key: The key to look up
            
        Returns:
            The value, or None if not found or expired
        """
        with self._lock:
            if key not in self._cache:
                self._misses += 1
                return None
            
            value = self._cache[key]
            
            # Check if value is expired
            if self._ttl_check and value.is_expired():
                # Remove expired value from cache
                self._cache.pop(key)
                self._expires += 1
                self._misses += 1
                return None
            
            # Move to end (most recently used)
            self._cache.move_to_end(key)
            
            self._hits += 1
            return value
    
    def set(self, key: Key, value: Value) -> None:
        """
        Set a value in the cache.
        
        Args:
            key: The key
            value: The value
        """
        with self._lock:
            # If key already exists, remove it first
            if key in self._cache:
                self._cache.pop(key)
            # If cache is full, remove least recently used item
            elif len(self._cache) >= self._capacity:
                self._cache.popitem(last=False)  # Remove first item (LRU)
                self._evictions += 1
            
            # Add new key-value pair
            self._cache[key] = value
            self._inserts += 1
    
    def delete(self, key: Key) -> bool:
        """
        Delete a key from the cache.
        
        Args:
            key: The key
            
        Returns:
            True if the key was deleted, False if it didn't exist
        """
        with self._lock:
            if key in self._cache:
                self._cache.pop(key)
                return True
            return False
    
    def clear(self) -> None:
        """Clear all keys from the cache."""
        with self._lock:
            self._cache.clear()
    
    def evict_expired(self) -> int:
        """
        Evict expired items from the cache.
        
        Returns:
            Number of items evicted
        """
        with self._lock:
            # Find expired keys
            expired_keys = [
                key for key, value in self._cache.items()
                if value.is_expired()
            ]
            
            # Remove expired keys
            for key in expired_keys:
                self._cache.pop(key)
            
            self._expires += len(expired_keys)
            return len(expired_keys)
    
    def stats(self) -> Dict[str, Any]:
        """
        Get statistics about the cache.
        
        Returns:
            Dictionary of statistics
        """
        with self._lock:
            total = self._hits + self._misses
            hit_rate = (self._hits / total) * 100 if total > 0 else 0
            
            return {
                'type': 'LRUCache',
                'capacity': self._capacity,
                'size': len(self._cache),
                'utilization': (len(self._cache) / self._capacity) * 100,
                'hits': self._hits,
                'misses': self._misses,
                'hit_rate': hit_rate,
                'inserts': self._inserts,
                'evictions': self._evictions,
                'expires': self._expires,
                'ttl_check': self._ttl_check
            } 