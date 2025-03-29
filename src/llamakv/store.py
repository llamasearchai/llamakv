"""
Core KVStore implementation for LlamaKV
"""
import os
import time
import logging
import threading
from typing import Dict, List, Set, Any, Optional, Union, Callable, Tuple, TypeVar

from llamakv.exceptions import KVError, KeyNotFoundError
from llamakv.transaction import Transaction
from llamakv.pubsub import PubSub
from llamakv.backends.memory import MemoryBackend
from llamakv.backends.file import FileBackend
from llamakv.backends.redis import RedisBackend
from llamakv.backends.distributed import DistributedBackend

logger = logging.getLogger(__name__)

T = TypeVar('T')


class KVStore:
    """
    Key-Value Store main class

    Provides a unified interface for different backend implementations.
    """
    def __init__(
        self,
        backend: str = "memory",
        path: Optional[str] = None,
        host: str = "localhost",
        port: int = 6379,
        password: Optional[str] = None,
        nodes: Optional[List[Dict[str, Any]]] = None,
        max_memory: Optional[str] = None,
        eviction_policy: str = "lru",
        max_keys: Optional[int] = None,
        sync_strategy: str = "every_second"
    ):
        """
        Initialize a KVStore with specified backend and options

        Args:
            backend: Backend type ("memory", "file", "redis", "distributed")
            path: Path for file-based storage
            host: Redis host
            port: Redis port
            password: Redis password
            nodes: List of nodes for distributed backend
            max_memory: Maximum memory to use (e.g. "100mb")
            eviction_policy: Eviction policy ("lru", "lfu", "random")
            max_keys: Maximum number of keys to store
            sync_strategy: Sync strategy for persistence ("never", "every_second", "always")
        """
        self.backend_type = backend
        
        # Initialize backend based on type
        if backend == "memory":
            self.backend = MemoryBackend(
                max_memory=max_memory,
                eviction_policy=eviction_policy,
                max_keys=max_keys
            )
        elif backend == "file":
            if not path:
                raise ValueError("Path must be specified for file backend")
            self.backend = FileBackend(
                path=path,
                sync_strategy=sync_strategy,
                max_memory=max_memory,
                eviction_policy=eviction_policy,
                max_keys=max_keys
            )
        elif backend == "redis":
            self.backend = RedisBackend(
                host=host,
                port=port,
                password=password
            )
        elif backend == "distributed":
            if not nodes:
                raise ValueError("Nodes must be specified for distributed backend")
            self.backend = DistributedBackend(nodes=nodes)
        else:
            raise ValueError(f"Unknown backend type: {backend}")
        
        logger.info(f"Initialized KVStore with {backend} backend")
    
    #
    # Basic operations
    #
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """
        Set a key to the specified value

        Args:
            key: Key to set
            value: Value to set (will be serialized if needed)
            ttl: Time-to-live in seconds

        Returns:
            bool: True if successful
        """
        return self.backend.set(key, value, ttl)
    
    def get(self, key: str, default: Optional[T] = None) -> Union[Any, T]:
        """
        Get the value for a key

        Args:
            key: Key to get
            default: Default value to return if key doesn't exist

        Returns:
            The value for the key, or default if key doesn't exist
        """
        try:
            return self.backend.get(key)
        except KeyNotFoundError:
            return default
    
    def delete(self, key: str) -> bool:
        """
        Delete a key

        Args:
            key: Key to delete

        Returns:
            bool: True if key was deleted, False if key didn't exist
        """
        return self.backend.delete(key)
    
    def exists(self, key: str) -> bool:
        """
        Check if a key exists

        Args:
            key: Key to check

        Returns:
            bool: True if key exists, False otherwise
        """
        return self.backend.exists(key)
    
    def expire(self, key: str, ttl: int) -> bool:
        """
        Set a key's time-to-live in seconds

        Args:
            key: Key to set TTL for
            ttl: Time-to-live in seconds

        Returns:
            bool: True if successful, False if key doesn't exist
        """
        return self.backend.expire(key, ttl)
    
    def ttl(self, key: str) -> int:
        """
        Get a key's time-to-live in seconds

        Args:
            key: Key to get TTL for

        Returns:
            int: TTL in seconds, -1 if key has no TTL, -2 if key doesn't exist
        """
        return self.backend.ttl(key)
    
    def keys(self, pattern: str = "*") -> List[str]:
        """
        Get all keys matching a pattern

        Args:
            pattern: Pattern to match

        Returns:
            List of keys matching the pattern
        """
        return self.backend.keys(pattern)
    
    def flush(self) -> bool:
        """
        Delete all keys

        Returns:
            bool: True if successful
        """
        return self.backend.flush()
    
    #
    # Numeric operations
    #
    
    def increment(self, key: str, amount: int = 1) -> int:
        """
        Increment a key by the given amount

        Args:
            key: Key to increment
            amount: Amount to increment by

        Returns:
            int: New value
        """
        return self.backend.increment(key, amount)
    
    def decrement(self, key: str, amount: int = 1) -> int:
        """
        Decrement a key by the given amount

        Args:
            key: Key to decrement
            amount: Amount to decrement by

        Returns:
            int: New value
        """
        return self.backend.decrement(key, amount)
    
    #
    # List operations
    #
    
    def list_push(self, key: str, value: Any, left: bool = False) -> int:
        """
        Push a value onto a list

        Args:
            key: List key
            value: Value to push
            left: Push to the left if True, otherwise to the right

        Returns:
            int: New list length
        """
        return self.backend.list_push(key, value, left)
    
    def list_pop(self, key: str, left: bool = False) -> Any:
        """
        Pop a value from a list

        Args:
            key: List key
            left: Pop from the left if True, otherwise from the right

        Returns:
            Value popped from the list
        """
        return self.backend.list_pop(key, left)
    
    def list_range(self, key: str, start: int = 0, end: int = -1) -> List[Any]:
        """
        Get a range of values from a list

        Args:
            key: List key
            start: Start index
            end: End index

        Returns:
            List of values in the specified range
        """
        return self.backend.list_range(key, start, end)
    
    def list_length(self, key: str) -> int:
        """
        Get the length of a list

        Args:
            key: List key

        Returns:
            int: List length
        """
        return self.backend.list_length(key)
    
    #
    # Set operations
    #
    
    def set_add(self, key: str, *values: Any) -> int:
        """
        Add values to a set

        Args:
            key: Set key
            *values: Values to add

        Returns:
            int: Number of values added
        """
        return self.backend.set_add(key, *values)
    
    def set_remove(self, key: str, *values: Any) -> int:
        """
        Remove values from a set

        Args:
            key: Set key
            *values: Values to remove

        Returns:
            int: Number of values removed
        """
        return self.backend.set_remove(key, *values)
    
    def set_members(self, key: str) -> Set[Any]:
        """
        Get all members of a set

        Args:
            key: Set key

        Returns:
            Set of all members
        """
        return self.backend.set_members(key)
    
    def set_is_member(self, key: str, value: Any) -> bool:
        """
        Check if a value is a member of a set

        Args:
            key: Set key
            value: Value to check

        Returns:
            bool: True if value is a member, False otherwise
        """
        return self.backend.set_is_member(key, value)
    
    #
    # Hash operations
    #
    
    def hash_set(self, key: str, field: str, value: Any) -> bool:
        """
        Set a field in a hash

        Args:
            key: Hash key
            field: Field to set
            value: Value to set

        Returns:
            bool: True if field is new, False if field was updated
        """
        return self.backend.hash_set(key, field, value)
    
    def hash_get(self, key: str, field: str) -> Any:
        """
        Get a field from a hash

        Args:
            key: Hash key
            field: Field to get

        Returns:
            Value of the field
        """
        return self.backend.hash_get(key, field)
    
    def hash_delete(self, key: str, *fields: str) -> int:
        """
        Delete fields from a hash

        Args:
            key: Hash key
            *fields: Fields to delete

        Returns:
            int: Number of fields deleted
        """
        return self.backend.hash_delete(key, *fields)
    
    def hash_exists(self, key: str, field: str) -> bool:
        """
        Check if a field exists in a hash

        Args:
            key: Hash key
            field: Field to check

        Returns:
            bool: True if field exists, False otherwise
        """
        return self.backend.hash_exists(key, field)
    
    def hash_get_all(self, key: str) -> Dict[str, Any]:
        """
        Get all fields and values from a hash

        Args:
            key: Hash key

        Returns:
            Dict of field names to values
        """
        return self.backend.hash_get_all(key)
    
    #
    # Batch operations
    #
    
    def batch_set(self, mapping: Dict[str, Any], ttl: Optional[int] = None) -> bool:
        """
        Set multiple keys to their values

        Args:
            mapping: Dict of keys to values
            ttl: Time-to-live in seconds

        Returns:
            bool: True if successful
        """
        return self.backend.batch_set(mapping, ttl)
    
    def batch_get(self, keys: List[str]) -> Dict[str, Any]:
        """
        Get multiple keys

        Args:
            keys: List of keys to get

        Returns:
            Dict of keys to values
        """
        return self.backend.batch_get(keys)
    
    def batch_delete(self, keys: List[str]) -> int:
        """
        Delete multiple keys

        Args:
            keys: List of keys to delete

        Returns:
            int: Number of keys deleted
        """
        return self.backend.batch_delete(keys)
    
    #
    # Transaction support
    #
    
    def transaction(self) -> Transaction:
        """
        Start a transaction

        Returns:
            Transaction: Transaction object
        """
        return Transaction(self.backend)
    
    #
    # Pub/Sub support
    #
    
    def pubsub(self) -> PubSub:
        """
        Get a pub/sub object

        Returns:
            PubSub: PubSub object
        """
        return PubSub(self.backend)
    
    def publish(self, channel: str, message: Any) -> int:
        """
        Publish a message to a channel

        Args:
            channel: Channel to publish to
            message: Message to publish

        Returns:
            int: Number of clients that received the message
        """
        return self.backend.publish(channel, message)
    
    #
    # Connection management
    #
    
    def close(self) -> None:
        """
        Close the connection to the backend
        """
        self.backend.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close() 