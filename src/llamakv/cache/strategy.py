"""
Cache strategy base class for LlamaKV.

This module provides the abstract base class for implementing
caching strategies in the key-value store.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Set, Type, TypeVar, Union

from llamakv.core.key import Key
from llamakv.core.value import Value


class CacheStrategy(ABC):
    """
    Abstract base class for cache strategies.
    
    Cache strategies define how the key-value store caches data
    in memory, including eviction policies and size limits.
    """
    
    @abstractmethod
    def get(self, key: Key) -> Optional[Value]:
        """
        Get a value from the cache.
        
        Args:
            key: The key to look up
            
        Returns:
            The value, or None if not found
        """
        pass
    
    @abstractmethod
    def set(self, key: Key, value: Value) -> None:
        """
        Set a value in the cache.
        
        Args:
            key: The key
            value: The value
        """
        pass
    
    @abstractmethod
    def delete(self, key: Key) -> bool:
        """
        Delete a key from the cache.
        
        Args:
            key: The key
            
        Returns:
            True if the key was deleted, False if it didn't exist
        """
        pass
    
    @abstractmethod
    def clear(self) -> None:
        """Clear all keys from the cache."""
        pass
    
    @abstractmethod
    def stats(self) -> Dict[str, Any]:
        """
        Get statistics about the cache.
        
        Returns:
            Dictionary of statistics
        """
        pass 