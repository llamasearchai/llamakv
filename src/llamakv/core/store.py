"""
Core KVStore module for LlamaKV.

This module provides the main KVStore class that implements the
key-value storage functionality with support for different backends,
caching strategies, and distributed operations.
"""

import logging
import time
from contextlib import contextmanager
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from llamakv.cache import CacheStrategy, LRUCache
from llamakv.core.key import Key
from llamakv.core.value import (
    BytesValue,
    FloatValue,
    IntValue,
    JsonValue,
    PickleValue,
    StringValue,
    Value,
)
from llamakv.persistence import MemoryBackend

logger = logging.getLogger(__name__)

T = TypeVar("T")
V = TypeVar("V", bound=Value)


class KVStore:
    """
    Main key-value store implementation.

    Provides methods for storing, retrieving, and managing key-value pairs
    with support for different value types, persistence backends, caching,
    and distributed operations.
    """

    def __init__(
        self,
        backend=None,
        cache_strategy=None,
        distributed_client=None,
        default_ttl: Optional[int] = None,
        auto_purge_expired: bool = True,
        purge_interval: int = 60,
    ):
        """
        Initialize a new KV store.

        Args:
            backend: Persistence backend (defaults to MemoryBackend)
            cache_strategy: Cache strategy (defaults to LRUCache)
            distributed_client: Distributed client for multi-node operations
            default_ttl: Default time-to-live for values (in seconds)
            auto_purge_expired: Whether to automatically purge expired values
            purge_interval: Interval for purging expired values (in seconds)
        """
        self._backend = backend or MemoryBackend()
        self._cache = cache_strategy or LRUCache()
        self._distributed = distributed_client
        self._default_ttl = default_ttl
        self._auto_purge = auto_purge_expired
        self._purge_interval = purge_interval
        self._last_purge = time.time()

        # Register hooks for backend events
        self._backend.register_on_set(self._on_backend_set)
        self._backend.register_on_delete(self._on_backend_delete)

        logger.info(
            f"Initialized KVStore with backend: {type(self._backend).__name__}, "
            f"cache: {type(self._cache).__name__}"
        )

    def _maybe_purge_expired(self) -> None:
        """Purge expired values if auto_purge is enabled and interval has passed."""
        if not self._auto_purge:
            return

        now = time.time()
        if now - self._last_purge >= self._purge_interval:
            self.purge_expired()
            self._last_purge = now

    def _on_backend_set(self, key: Key, value: Value) -> None:
        """Callback for backend set operations to update cache."""
        self._cache.set(key, value)

    def _on_backend_delete(self, key: Key) -> None:
        """Callback for backend delete operations to update cache."""
        self._cache.delete(key)

    def _process_key(self, key: Any) -> Key:
        """
        Process a key input into a Key object.

        Args:
            key: Input key (can be Key object, string, or any hashable value)

        Returns:
            Key object
        """
        if isinstance(key, Key):
            return key
        elif isinstance(key, str) and ":" in key:
            # Handle namespace:key format
            namespace, value = key.split(":", 1)
            return Key(value, namespace)
        else:
            return Key(key)

    def set(
        self,
        key: Any,
        value: Any,
        ttl: Optional[int] = None,
        value_type: Optional[Type[Value]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Set a value for a key.

        Args:
            key: The key (can be Key object, string, or any hashable value)
            value: The value to store
            ttl: Time-to-live in seconds (defaults to store default)
            value_type: Value class to use (auto-detected if not specified)
            metadata: Optional metadata to store with the value
        """
        self._maybe_purge_expired()

        # Process the key
        key_obj = self._process_key(key)

        # Use default TTL if not specified
        if ttl is None:
            ttl = self._default_ttl

        # Auto-detect value type if not specified
        if value_type is None:
            if isinstance(value, str):
                value_type = StringValue
            elif isinstance(value, int):
                value_type = IntValue
            elif isinstance(value, float):
                value_type = FloatValue
            elif isinstance(value, bytes):
                value_type = BytesValue
            elif isinstance(value, dict):
                value_type = JsonValue
            else:
                value_type = PickleValue

        # Create the value object
        value_obj = value_type(value, ttl=ttl, metadata=metadata)

        # Set in the backend
        self._backend.set(key_obj, value_obj)

        # Set in the cache
        self._cache.set(key_obj, value_obj)

        # Propagate to distributed nodes if enabled
        if self._distributed:
            self._distributed.propagate_set(key_obj, value_obj)

        logger.debug(f"Set key: {key_obj} with value type: {type(value_obj).__name__}")

    def get(
        self,
        key: Any,
        default: Any = None,
        include_expired: bool = False,
        expected_type: Optional[Type[V]] = None,
    ) -> Optional[Union[Any, V]]:
        """
        Get a value for a key.

        Args:
            key: The key (can be Key object, string, or any hashable value)
            default: Default value to return if key not found
            include_expired: Whether to return expired values
            expected_type: Expected Value type for type checking

        Returns:
            The value, or default if not found
        """
        self._maybe_purge_expired()

        # Process the key
        key_obj = self._process_key(key)

        # Try to get from cache first
        value = self._cache.get(key_obj)
        cache_hit = value is not None

        # If not in cache, try to get from backend
        if not cache_hit:
            value = self._backend.get(key_obj)
            if value:
                # Update cache
                self._cache.set(key_obj, value)

        # Check if value exists and is not expired
        if value is None:
            return default

        if not include_expired and value.is_expired():
            # Value is expired, delete it and return default
            self.delete(key_obj)
            return default

        # Type check if expected_type is specified
        if expected_type and not isinstance(value, expected_type):
            logger.warning(
                f"Type mismatch for key {key_obj}. Expected: {expected_type.__name__}, "
                f"Actual: {type(value).__name__}"
            )
            return default

        logger.debug(f"Get key: {key_obj}, cache hit: {cache_hit}")
        return value.value

    def get_with_metadata(
        self,
        key: Any,
        default: Any = None,
        include_expired: bool = False,
    ) -> Optional[Tuple[Any, Dict[str, Any]]]:
        """
        Get a value with its metadata for a key.

        Args:
            key: The key (can be Key object, string, or any hashable value)
            default: Default value to return if key not found
            include_expired: Whether to return expired values

        Returns:
            Tuple of (value, metadata), or (default, {}) if not found
        """
        self._maybe_purge_expired()

        # Process the key
        key_obj = self._process_key(key)

        # Try to get from cache first
        value = self._cache.get(key_obj)
        if value is None:
            # If not in cache, try to get from backend
            value = self._backend.get(key_obj)
            if value:
                # Update cache
                self._cache.set(key_obj, value)

        # Check if value exists and is not expired
        if value is None:
            return (default, {})

        if not include_expired and value.is_expired():
            # Value is expired, delete it and return default
            self.delete(key_obj)
            return (default, {})

        return (value.value, value.metadata)

    def delete(self, key: Any) -> bool:
        """
        Delete a key from the store.

        Args:
            key: The key to delete

        Returns:
            True if the key was deleted, False if it didn't exist
        """
        # Process the key
        key_obj = self._process_key(key)

        # Delete from backend
        result = self._backend.delete(key_obj)

        # Delete from cache
        self._cache.delete(key_obj)

        # Propagate to distributed nodes if enabled
        if self._distributed:
            self._distributed.propagate_delete(key_obj)

        logger.debug(f"Delete key: {key_obj}, success: {result}")
        return result

    def exists(self, key: Any) -> bool:
        """
        Check if a key exists in the store and is not expired.

        Args:
            key: The key to check

        Returns:
            True if the key exists and is not expired, False otherwise
        """
        self._maybe_purge_expired()

        # Process the key
        key_obj = self._process_key(key)

        # Try to get from cache first
        value = self._cache.get(key_obj)
        if value is None:
            # If not in cache, try to get from backend
            value = self._backend.get(key_obj)
            if value:
                # Update cache
                self._cache.set(key_obj, value)

        # Check if value exists and is not expired
        if value is None:
            return False

        if value.is_expired():
            # Value is expired, delete it
            self.delete(key_obj)
            return False

        return True

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
        self._maybe_purge_expired()

        # Get keys from backend
        keys = self._backend.keys(pattern, namespace)

        # Filter out expired keys
        result = []
        for key in keys:
            value = self._backend.get(key)
            if value and not value.is_expired():
                result.append(key)

        return result

    def count(
        self, pattern: Optional[str] = None, namespace: Optional[str] = None
    ) -> int:
        """
        Count the number of keys in the store.

        Args:
            pattern: Optional pattern to filter keys
            namespace: Optional namespace to filter keys

        Returns:
            Number of keys
        """
        return len(self.keys(pattern, namespace))

    def clear(self) -> None:
        """Clear all keys from the store."""
        # Clear the backend
        self._backend.clear()

        # Clear the cache
        self._cache.clear()

        # Propagate to distributed nodes if enabled
        if self._distributed:
            self._distributed.propagate_clear()

        logger.debug("Cleared all keys from store")

    def purge_expired(self) -> int:
        """
        Purge expired values from the store.

        Returns:
            Number of keys purged
        """
        # Get all keys from backend
        keys = self._backend.keys()

        # Check each key and delete if expired
        purged_count = 0
        for key in keys:
            value = self._backend.get(key)
            if value and value.is_expired():
                self.delete(key)
                purged_count += 1

        self._last_purge = time.time()
        logger.debug(f"Purged {purged_count} expired keys")
        return purged_count

    def get_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the store.

        Returns:
            Dictionary of statistics
        """
        # Get stats from components
        backend_stats = self._backend.stats() if hasattr(self._backend, "stats") else {}
        cache_stats = self._cache.stats() if hasattr(self._cache, "stats") else {}

        # Count keys by type
        keys = self._backend.keys()
        type_counts = {}
        for key in keys:
            value = self._backend.get(key)
            if value:
                value_type = type(value).__name__
                type_counts[value_type] = type_counts.get(value_type, 0) + 1

        # Compute key namespace distribution
        namespace_counts = {}
        for key in keys:
            namespace = key.namespace or "__default__"
            namespace_counts[namespace] = namespace_counts.get(namespace, 0) + 1

        # Build stats dictionary
        stats = {
            "total_keys": len(keys),
            "key_types": type_counts,
            "namespaces": namespace_counts,
            "expired_keys": sum(
                1
                for key in keys
                if self._backend.get(key) and self._backend.get(key).is_expired()
            ),
            "backend": {"type": type(self._backend).__name__, **backend_stats},
            "cache": {"type": type(self._cache).__name__, **cache_stats},
            "distributed": bool(self._distributed),
            "auto_purge": self._auto_purge,
            "purge_interval": self._purge_interval,
            "last_purge": self._last_purge,
        }

        return stats

    @contextmanager
    def transaction(self) -> "KVTransaction":
        """
        Start a transaction for atomic operations.

        Returns:
            Context manager for transaction
        """
        transaction = KVTransaction(self)
        try:
            yield transaction
            transaction.commit()
        except Exception as e:
            transaction.rollback()
            raise e


class KVTransaction:
    """
    Transaction context for atomic operations on KVStore.

    Allows for batching multiple operations and committing or
    rolling them back atomically.
    """

    def __init__(self, store: KVStore):
        """
        Initialize a transaction.

        Args:
            store: KVStore instance
        """
        self._store = store
        self._operations = []
        self._committed = False
        self._rolled_back = False

    def set(
        self,
        key: Any,
        value: Any,
        ttl: Optional[int] = None,
        value_type: Optional[Type[Value]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Add a set operation to the transaction.

        Args:
            key: The key to set
            value: The value to set
            ttl: Optional time-to-live
            value_type: Optional value type
            metadata: Optional metadata
        """
        if self._committed or self._rolled_back:
            raise ValueError("Transaction already completed")

        self._operations.append(("set", key, value, ttl, value_type, metadata))

    def delete(self, key: Any) -> None:
        """
        Add a delete operation to the transaction.

        Args:
            key: The key to delete
        """
        if self._committed or self._rolled_back:
            raise ValueError("Transaction already completed")

        self._operations.append(("delete", key))

    def commit(self) -> None:
        """Commit all operations in the transaction."""
        if self._committed or self._rolled_back:
            raise ValueError("Transaction already completed")

        # Process each operation
        for op in self._operations:
            if op[0] == "set":
                _, key, value, ttl, value_type, metadata = op
                self._store.set(key, value, ttl, value_type, metadata)
            elif op[0] == "delete":
                _, key = op
                self._store.delete(key)

        self._committed = True
        logger.debug(f"Committed transaction with {len(self._operations)} operations")

    def rollback(self) -> None:
        """Roll back all operations in the transaction."""
        if self._committed or self._rolled_back:
            raise ValueError("Transaction already completed")

        self._operations = []
        self._rolled_back = True
        logger.debug("Rolled back transaction")
