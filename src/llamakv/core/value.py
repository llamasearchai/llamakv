"""
Value module for LlamaKV.

This module provides classes for representing and manipulating
values in the key-value store, with different serialization options.
"""

import json
import pickle
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Generic, Optional, Type, TypeVar, Union

T = TypeVar("T")


class Value(ABC, Generic[T]):
    """
    Base abstract class for values stored in the key-value store.

    Provides common functionality for all value types and defines
    the interface for serialization and deserialization.
    """

    def __init__(
        self,
        value: T,
        ttl: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize a value with optional TTL and metadata.

        Args:
            value: The actual value to store
            ttl: Optional time-to-live in seconds
            metadata: Optional metadata dictionary
        """
        self._value = value
        self._created_at = time.time()
        self._ttl = ttl
        self._metadata = metadata or {}

    @property
    def value(self) -> T:
        """Get the stored value."""
        return self._value

    @property
    def created_at(self) -> float:
        """Get the value creation timestamp."""
        return self._created_at

    @property
    def ttl(self) -> Optional[int]:
        """Get the time-to-live in seconds."""
        return self._ttl

    @property
    def expiry(self) -> Optional[float]:
        """Get the expiry timestamp, if TTL is set."""
        if self._ttl is None:
            return None
        return self._created_at + self._ttl

    @property
    def metadata(self) -> Dict[str, Any]:
        """Get the metadata dictionary."""
        return self._metadata

    def is_expired(self) -> bool:
        """
        Check if the value has expired.

        Returns:
            True if the value has expired, False otherwise
        """
        if self._ttl is None:
            return False
        return time.time() > self._created_at + self._ttl

    def add_metadata(self, key: str, value: Any) -> None:
        """
        Add a metadata key-value pair.

        Args:
            key: Metadata key
            value: Metadata value
        """
        self._metadata[key] = value

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the value to a dictionary for serialization.

        Returns:
            Dictionary representation of the value
        """
        return {
            "value": self._serialize_value(),
            "type": self.__class__.__name__,
            "created_at": self._created_at,
            "ttl": self._ttl,
            "metadata": self._metadata,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Value":
        """
        Create a value from a dictionary representation.

        Args:
            data: Dictionary representation of the value

        Returns:
            A new Value instance

        Raises:
            ValueError: If the dictionary is not a valid value representation
        """
        if "type" not in data or data["type"] != cls.__name__:
            raise ValueError(f"Invalid value type: {data.get('type')}")

        value = cls._deserialize_value(data["value"])
        instance = cls(value)
        instance._created_at = data["created_at"]
        instance._ttl = data.get("ttl")
        instance._metadata = data.get("metadata", {})
        return instance

    @abstractmethod
    def _serialize_value(self) -> Any:
        """Serialize the value for storage."""
        pass

    @classmethod
    @abstractmethod
    def _deserialize_value(cls, serialized: Any) -> T:
        """Deserialize the value from storage."""
        pass


class StringValue(Value[str]):
    """Value implementation for string data."""

    def _serialize_value(self) -> str:
        return self._value

    @classmethod
    def _deserialize_value(cls, serialized: str) -> str:
        return serialized


class IntValue(Value[int]):
    """Value implementation for integer data."""

    def _serialize_value(self) -> int:
        return self._value

    @classmethod
    def _deserialize_value(cls, serialized: int) -> int:
        return serialized


class FloatValue(Value[float]):
    """Value implementation for floating-point data."""

    def _serialize_value(self) -> float:
        return self._value

    @classmethod
    def _deserialize_value(cls, serialized: float) -> float:
        return serialized


class BytesValue(Value[bytes]):
    """Value implementation for binary data."""

    def _serialize_value(self) -> str:
        return self._value.hex()

    @classmethod
    def _deserialize_value(cls, serialized: str) -> bytes:
        return bytes.fromhex(serialized)


class JsonValue(Value[Dict[str, Any]]):
    """Value implementation for JSON data."""

    def _serialize_value(self) -> str:
        return json.dumps(self._value)

    @classmethod
    def _deserialize_value(cls, serialized: str) -> Dict[str, Any]:
        return json.loads(serialized)


class PickleValue(Value[Any]):
    """Value implementation for pickled Python objects."""

    def _serialize_value(self) -> str:
        return pickle.dumps(self._value).hex()

    @classmethod
    def _deserialize_value(cls, serialized: str) -> Any:
        return pickle.loads(bytes.fromhex(serialized))
