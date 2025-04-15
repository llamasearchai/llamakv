"""
Key module for LlamaKV.

This module provides the Key class for representing and manipulating
keys in the key-value store.
"""

import hashlib
from typing import Any, Optional, Union


class Key:
    """
    Represents a key in the key-value store.

    Keys can be created from various types and are normalized for
    consistent lookup and storage. Keys are immutable and hashable.
    """

    def __init__(self, value: Any, namespace: Optional[str] = None):
        """
        Initialize a key with a value and optional namespace.

        Args:
            value: The key value (str, int, bytes, or any hashable object)
            namespace: Optional namespace to scope the key

        Raises:
            TypeError: If the value is not hashable
        """
        # Ensure the value is hashable
        try:
            hash(value)
        except TypeError:
            raise TypeError(f"Key value must be hashable, got {type(value)}")

        self._value = value
        self._namespace = namespace

        # Compute the normalized key string
        if namespace:
            self._key_str = f"{namespace}:{self._normalize_value(value)}"
        else:
            self._key_str = self._normalize_value(value)

        # Compute a hash for the key
        self._key_hash = hashlib.md5(self._key_str.encode("utf-8")).hexdigest()

    @property
    def value(self) -> Any:
        """Get the original key value."""
        return self._value

    @property
    def namespace(self) -> Optional[str]:
        """Get the key namespace."""
        return self._namespace

    @property
    def hash(self) -> str:
        """Get the key hash."""
        return self._key_hash

    def _normalize_value(self, value: Any) -> str:
        """
        Normalize a value to a string representation.

        Args:
            value: The value to normalize

        Returns:
            String representation of the value
        """
        if isinstance(value, bytes):
            # Convert bytes to hex representation
            return value.hex()
        elif isinstance(value, (int, float, bool, str)):
            # Convert simple types directly to string
            return str(value)
        else:
            # For other types, use repr to ensure uniqueness
            return repr(value)

    def __str__(self) -> str:
        """Get the string representation of the key."""
        return self._key_str

    def __repr__(self) -> str:
        """Get the detailed string representation of the key."""
        if self._namespace:
            return f"Key(value={self._value!r}, namespace={self._namespace!r})"
        else:
            return f"Key(value={self._value!r})"

    def __hash__(self) -> int:
        """Get the hash value for the key."""
        return hash(self._key_str)

    def __eq__(self, other: Any) -> bool:
        """
        Check if this key is equal to another.

        Args:
            other: The other key to compare with

        Returns:
            True if the keys are equal, False otherwise
        """
        if not isinstance(other, Key):
            return False
        return self._key_str == other._key_str

    @classmethod
    def from_string(cls, key_str: str) -> "Key":
        """
        Create a key from a string representation.

        Args:
            key_str: String representation of the key

        Returns:
            A new Key instance

        Raises:
            ValueError: If the string is not a valid key representation
        """
        # Check if the key has a namespace
        if ":" in key_str:
            namespace, value = key_str.split(":", 1)
            return cls(value, namespace)
        else:
            return cls(key_str)
