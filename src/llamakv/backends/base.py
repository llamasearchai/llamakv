"""
Base Backend class for LlamaKV
"""

import abc
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

from llamakv.exceptions import KVError


class Backend(abc.ABC):
    """
    Abstract base class for backend implementations

    All backend implementations must implement these methods.
    """

    @abc.abstractmethod
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """
        Set a key to the specified value

        Args:
            key: Key to set
            value: Value to set
            ttl: Time-to-live in seconds

        Returns:
            bool: True if successful
        """
        pass

    @abc.abstractmethod
    def get(self, key: str) -> Any:
        """
        Get the value for a key

        Args:
            key: Key to get

        Returns:
            The value for the key

        Raises:
            KeyNotFoundError: If the key doesn't exist
        """
        pass

    @abc.abstractmethod
    def delete(self, key: str) -> bool:
        """
        Delete a key

        Args:
            key: Key to delete

        Returns:
            bool: True if key was deleted, False if key didn't exist
        """
        pass

    @abc.abstractmethod
    def exists(self, key: str) -> bool:
        """
        Check if a key exists

        Args:
            key: Key to check

        Returns:
            bool: True if key exists, False otherwise
        """
        pass

    @abc.abstractmethod
    def expire(self, key: str, ttl: int) -> bool:
        """
        Set a key's time-to-live in seconds

        Args:
            key: Key to set TTL for
            ttl: Time-to-live in seconds

        Returns:
            bool: True if successful, False if key doesn't exist
        """
        pass

    @abc.abstractmethod
    def ttl(self, key: str) -> int:
        """
        Get a key's time-to-live in seconds

        Args:
            key: Key to get TTL for

        Returns:
            int: TTL in seconds, -1 if key has no TTL, -2 if key doesn't exist
        """
        pass

    @abc.abstractmethod
    def keys(self, pattern: str = "*") -> List[str]:
        """
        Get all keys matching a pattern

        Args:
            pattern: Pattern to match

        Returns:
            List of keys matching the pattern
        """
        pass

    @abc.abstractmethod
    def flush(self) -> bool:
        """
        Delete all keys

        Returns:
            bool: True if successful
        """
        pass

    @abc.abstractmethod
    def increment(self, key: str, amount: int = 1) -> int:
        """
        Increment a key by the given amount

        Args:
            key: Key to increment
            amount: Amount to increment by

        Returns:
            int: New value
        """
        pass

    @abc.abstractmethod
    def decrement(self, key: str, amount: int = 1) -> int:
        """
        Decrement a key by the given amount

        Args:
            key: Key to decrement
            amount: Amount to decrement by

        Returns:
            int: New value
        """
        pass

    @abc.abstractmethod
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
        pass

    @abc.abstractmethod
    def list_pop(self, key: str, left: bool = False) -> Any:
        """
        Pop a value from a list

        Args:
            key: List key
            left: Pop from the left if True, otherwise from the right

        Returns:
            Value popped from the list
        """
        pass

    @abc.abstractmethod
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
        pass

    @abc.abstractmethod
    def list_length(self, key: str) -> int:
        """
        Get the length of a list

        Args:
            key: List key

        Returns:
            int: List length
        """
        pass

    @abc.abstractmethod
    def set_add(self, key: str, *values: Any) -> int:
        """
        Add values to a set

        Args:
            key: Set key
            *values: Values to add

        Returns:
            int: Number of values added
        """
        pass

    @abc.abstractmethod
    def set_remove(self, key: str, *values: Any) -> int:
        """
        Remove values from a set

        Args:
            key: Set key
            *values: Values to remove

        Returns:
            int: Number of values removed
        """
        pass

    @abc.abstractmethod
    def set_members(self, key: str) -> Set[Any]:
        """
        Get all members of a set

        Args:
            key: Set key

        Returns:
            Set of all members
        """
        pass

    @abc.abstractmethod
    def set_is_member(self, key: str, value: Any) -> bool:
        """
        Check if a value is a member of a set

        Args:
            key: Set key
            value: Value to check

        Returns:
            bool: True if value is a member, False otherwise
        """
        pass

    @abc.abstractmethod
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
        pass

    @abc.abstractmethod
    def hash_get(self, key: str, field: str) -> Any:
        """
        Get a field from a hash

        Args:
            key: Hash key
            field: Field to get

        Returns:
            Value of the field
        """
        pass

    @abc.abstractmethod
    def hash_delete(self, key: str, *fields: str) -> int:
        """
        Delete fields from a hash

        Args:
            key: Hash key
            *fields: Fields to delete

        Returns:
            int: Number of fields deleted
        """
        pass

    @abc.abstractmethod
    def hash_exists(self, key: str, field: str) -> bool:
        """
        Check if a field exists in a hash

        Args:
            key: Hash key
            field: Field to check

        Returns:
            bool: True if field exists, False otherwise
        """
        pass

    @abc.abstractmethod
    def hash_get_all(self, key: str) -> Dict[str, Any]:
        """
        Get all fields and values from a hash

        Args:
            key: Hash key

        Returns:
            Dict of field names to values
        """
        pass

    @abc.abstractmethod
    def batch_set(self, mapping: Dict[str, Any], ttl: Optional[int] = None) -> bool:
        """
        Set multiple keys to their values

        Args:
            mapping: Dict of keys to values
            ttl: Time-to-live in seconds

        Returns:
            bool: True if successful
        """
        pass

    @abc.abstractmethod
    def batch_get(self, keys: List[str]) -> Dict[str, Any]:
        """
        Get multiple keys

        Args:
            keys: List of keys to get

        Returns:
            Dict of keys to values
        """
        pass

    @abc.abstractmethod
    def batch_delete(self, keys: List[str]) -> int:
        """
        Delete multiple keys

        Args:
            keys: List of keys to delete

        Returns:
            int: Number of keys deleted
        """
        pass

    @abc.abstractmethod
    def execute_transaction(self, commands: List[tuple]) -> List[Any]:
        """
        Execute a transaction

        Args:
            commands: List of commands to execute

        Returns:
            List of results from each command
        """
        pass

    @abc.abstractmethod
    def subscribe(self, channel: str) -> None:
        """
        Subscribe to a channel

        Args:
            channel: Channel to subscribe to
        """
        pass

    @abc.abstractmethod
    def unsubscribe(self, channel: str) -> None:
        """
        Unsubscribe from a channel

        Args:
            channel: Channel to unsubscribe from
        """
        pass

    @abc.abstractmethod
    def psubscribe(self, pattern: str) -> None:
        """
        Subscribe to a channel pattern

        Args:
            pattern: Pattern to subscribe to
        """
        pass

    @abc.abstractmethod
    def punsubscribe(self, pattern: str) -> None:
        """
        Unsubscribe from a channel pattern

        Args:
            pattern: Pattern to unsubscribe from
        """
        pass

    @abc.abstractmethod
    def publish(self, channel: str, message: Any) -> int:
        """
        Publish a message to a channel

        Args:
            channel: Channel to publish to
            message: Message to publish

        Returns:
            int: Number of clients that received the message
        """
        pass

    @abc.abstractmethod
    def get_message(self) -> Optional[Dict[str, Any]]:
        """
        Get a message from a subscribed channel

        Returns:
            Dict containing message information or None if no message is available
        """
        pass

    @abc.abstractmethod
    def close(self) -> None:
        """
        Close the connection to the backend
        """
        pass
