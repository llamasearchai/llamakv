"""
TTL cache implementation for LlamaKV.

This module provides a Time-to-Live (TTL) caching strategy
for the key-value store.
"""

import threading
import time
from typing import Any, Dict, List, Optional, Set, Tuple

from llamakv.core.key import Key
from llamakv.core.value import Value
from llamakv.cache.strategy import CacheStrategy


class TTLCache(CacheStrategy):
    """
    Time-to-Live (TTL) cache strategy.
    
    Maintains a cache with automatic expiration of items based on
    their time-to-live values.
    """
    
    def __init__(self, 
                 capacity: int = 1000, 
                 default_ttl: int = 300,  # 5 minutes
                 cleanup_interval: int = 60,  # 1 minute
                 max_size_bytes: Optional[int] = None):
        """
        Initialize a TTL cache.
        
        Args:
            capacity: Maximum number of items to store in the cache
            default_ttl: Default time-to-live in seconds for items without a TTL
            cleanup_interval: Interval for cleaning up expired items (in seconds)
            max_size_bytes: Optional maximum size in bytes for the cache
        """
        self._capacity = capacity
        self._default_ttl = default_ttl
        self._cleanup_interval = cleanup_interval
        self._max_size_bytes = max_size_bytes
        self._cache: Dict[Key, Value] = {}
        self._lock = threading.RLock()
        self._estimated_size_bytes = 0
        self._last_cleanup = time.time()
        
        # Stats
        self._hits = 0
        self._misses = 0
        self._inserts = 0
        self._evictions = 0
        self._expires = 0
        self._cleanups = 0
    
    def _should_cleanup(self) -> bool:
        """Check if it's time to clean up expired items."""
        return time.time() - self._last_cleanup >= self._cleanup_interval
    
    def _estimate_item_size(self, key: Key, value: Value) -> int:
        """
        Estimate the size in bytes of a key-value pair.
        
        Args:
            key: The key
            value: The value
            
        Returns:
            Estimated size in bytes
        """
        # This is a very rough estimate and depends on implementation details
        # of Python objects, but it's better than nothing
        key_size = len(str(key)) * 2  # Rough estimate
        
        # Estimate value size based on its type
        value_dict = value.to_dict()
        value_size = 0
        
        # Add size of value data
        serialized_value = str(value_dict.get('value', ''))
        value_size += len(serialized_value) * 2
        
        # Add metadata size
        metadata = value_dict.get('metadata', {})
        metadata_size = sum(len(str(k)) + len(str(v)) for k, v in metadata.items()) * 2
        
        # Add overhead for Python objects
        overhead = 64  # Rough estimate for object headers and pointers
        
        return key_size + value_size + metadata_size + overhead
    
    def _update_size_bytes(self) -> None:
        """Recalculate the total size of the cache in bytes."""
        if self._max_size_bytes is None:
            return
        
        total = 0
        for key, value in self._cache.items():
            total += self._estimate_item_size(key, value)
        
        self._estimated_size_bytes = total
    
    def _enforce_size_limit(self) -> int:
        """
        Enforce the size limit by evicting items.
        
        Returns:
            Number of items evicted
        """
        if self._max_size_bytes is None:
            return 0
        
        # If we're under the limit, no need to evict
        if self._estimated_size_bytes <= self._max_size_bytes:
            return 0
        
        # Sort items by TTL and evict those expiring soonest
        items = [(k, v) for k, v in self._cache.items()]
        # Sort by expiry time, with items without TTL (None) at the end
        items.sort(key=lambda x: x[1].expiry if x[1].expiry is not None else float('inf'))
        
        evicted = 0
        for key, value in items:
            if self._estimated_size_bytes <= self._max_size_bytes:
                break
                
            size = self._estimate_item_size(key, value)
            del self._cache[key]
            self._estimated_size_bytes -= size
            evicted += 1
            self._evictions += 1
        
        return evicted
    
    def cleanup(self) -> int:
        """
        Clean up expired items.
        
        Returns:
            Number of items cleaned up
        """
        with self._lock:
            now = time.time()
            expired_keys = [
                key for key, value in self._cache.items()
                if value.is_expired()
            ]
            
            # Remove expired keys
            for key in expired_keys:
                if self._max_size_bytes is not None:
                    self._estimated_size_bytes -= self._estimate_item_size(key, self._cache[key])
                del self._cache[key]
            
            self._expires += len(expired_keys)
            self._cleanups += 1
            self._last_cleanup = now
            
            return len(expired_keys)
    
    def get(self, key: Key) -> Optional[Value]:
        """
        Get a value from the cache.
        
        Args:
            key: The key to look up
            
        Returns:
            The value, or None if not found or expired
        """
        with self._lock:
            # Run cleanup if needed
            if self._should_cleanup():
                self.cleanup()
            
            if key not in self._cache:
                self._misses += 1
                return None
            
            value = self._cache[key]
            
            # Check if value is expired
            if value.is_expired():
                # Remove expired value from cache
                if self._max_size_bytes is not None:
                    self._estimated_size_bytes -= self._estimate_item_size(key, value)
                del self._cache[key]
                self._expires += 1
                self._misses += 1
                return None
            
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
            # Set default TTL if not specified
            if value.ttl is None and self._default_ttl > 0:
                # We need to create a new value with the default TTL
                # This is a bit hacky but necessary since Value objects are immutable
                value_dict = value.to_dict()
                value_dict['ttl'] = self._default_ttl
                
                # Create a new Value object with the same class
                value_class = type(value)
                value = value_class.from_dict(value_dict)
            
            # Calculate size of the new item
            if self._max_size_bytes is not None:
                new_size = self._estimate_item_size(key, value)
                
                # If key already exists, remove its size from the total
                if key in self._cache:
                    self._estimated_size_bytes -= self._estimate_item_size(key, self._cache[key])
                
                # Add new size
                self._estimated_size_bytes += new_size
            
            # If cache is full, evict items
            if len(self._cache) >= self._capacity and key not in self._cache:
                # Sort items by TTL and evict those expiring soonest
                items = [(k, v) for k, v in self._cache.items()]
                items.sort(key=lambda x: x[1].expiry if x[1].expiry is not None else float('inf'))
                
                # Remove the item with the smallest TTL
                evict_key, evict_value = items[0]
                if self._max_size_bytes is not None:
                    self._estimated_size_bytes -= self._estimate_item_size(evict_key, evict_value)
                del self._cache[evict_key]
                self._evictions += 1
            
            # Add new key-value pair
            self._cache[key] = value
            self._inserts += 1
            
            # Enforce size limit
            if self._max_size_bytes is not None:
                self._enforce_size_limit()
    
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
                if self._max_size_bytes is not None:
                    self._estimated_size_bytes -= self._estimate_item_size(key, self._cache[key])
                del self._cache[key]
                return True
            return False
    
    def clear(self) -> None:
        """Clear all keys from the cache."""
        with self._lock:
            self._cache.clear()
            if self._max_size_bytes is not None:
                self._estimated_size_bytes = 0
    
    def stats(self) -> Dict[str, Any]:
        """
        Get statistics about the cache.
        
        Returns:
            Dictionary of statistics
        """
        with self._lock:
            total = self._hits + self._misses
            hit_rate = (self._hits / total) * 100 if total > 0 else 0
            
            stats = {
                'type': 'TTLCache',
                'capacity': self._capacity,
                'size': len(self._cache),
                'utilization': (len(self._cache) / self._capacity) * 100,
                'hits': self._hits,
                'misses': self._misses,
                'hit_rate': hit_rate,
                'inserts': self._inserts,
                'evictions': self._evictions,
                'expires': self._expires,
                'cleanups': self._cleanups,
                'default_ttl': self._default_ttl,
                'cleanup_interval': self._cleanup_interval,
                'last_cleanup': self._last_cleanup
            }
            
            if self._max_size_bytes is not None:
                stats.update({
                    'max_size_bytes': self._max_size_bytes,
                    'estimated_size_bytes': self._estimated_size_bytes,
                    'size_utilization': (self._estimated_size_bytes / self._max_size_bytes) * 100
                })
            
            return stats 