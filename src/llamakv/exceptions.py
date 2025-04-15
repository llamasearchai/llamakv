"""
Exception classes for LlamaKV
"""

from typing import Any, Optional


class KVError(Exception):
    """
    Base exception class for LlamaKV errors
    """

    def __init__(self, message: str, key: Optional[str] = None):
        self.message = message
        self.key = key
        super().__init__(message)

    def __str__(self) -> str:
        if self.key:
            return f"{self.message} (key: {self.key})"
        return self.message


class KeyNotFoundError(KVError):
    """
    Raised when a key is not found
    """

    def __init__(self, key: str):
        super().__init__(f"Key not found", key)


class TransactionError(KVError):
    """
    Raised when a transaction fails
    """

    def __init__(self, message: str, operation: Optional[str] = None):
        self.operation = operation
        if operation:
            message = f"{message} (operation: {operation})"
        super().__init__(message)


class ConnectionError(KVError):
    """
    Raised when a connection to a backend fails
    """

    def __init__(self, message: str, backend: Optional[str] = None):
        self.backend = backend
        if backend:
            message = f"{message} (backend: {backend})"
        super().__init__(message)


class PubSubError(KVError):
    """
    Raised when a pub/sub operation fails
    """

    def __init__(self, message: str, channel: Optional[str] = None):
        self.channel = channel
        if channel:
            message = f"{message} (channel: {channel})"
        super().__init__(message)


class ValidationError(KVError):
    """
    Raised when validation fails
    """

    def __init__(self, message: str, value: Optional[Any] = None):
        self.value = value
        if value is not None:
            message = f"{message} (value: {value})"
        super().__init__(message)


class BackendError(KVError):
    """
    Raised when a backend operation fails
    """

    def __init__(self, message: str, backend: Optional[str] = None):
        self.backend = backend
        if backend:
            message = f"{message} (backend: {backend})"
        super().__init__(message)


class SerializationError(KVError):
    """
    Raised when serialization or deserialization fails
    """

    def __init__(self, message: str, data_type: Optional[str] = None):
        self.data_type = data_type
        if data_type:
            message = f"{message} (type: {data_type})"
        super().__init__(message)


class MemoryLimitError(KVError):
    """
    Raised when a memory limit is exceeded
    """

    def __init__(self, message: str, limit: Optional[str] = None):
        self.limit = limit
        if limit:
            message = f"{message} (limit: {limit})"
        super().__init__(message)
