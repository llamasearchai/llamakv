"""
Transaction implementation for LlamaKV
"""
import logging
from typing import Any, Dict, List, Optional, Set, Callable, Union

from llamakv.exceptions import TransactionError

logger = logging.getLogger(__name__)


class Transaction:
    """
    Transaction class for atomic operations

    Provides a way to perform multiple operations atomically.
    """
    def __init__(self, backend):
        """
        Initialize a transaction with the backend

        Args:
            backend: Backend instance
        """
        self.backend = backend
        self.commands = []
        self.active = True
        self.executed = False
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> 'Transaction':
        """
        Add a SET command to the transaction

        Args:
            key: Key to set
            value: Value to set
            ttl: Time-to-live in seconds

        Returns:
            self for chaining
        """
        self._check_active()
        self.commands.append(("set", key, value, ttl))
        return self
    
    def delete(self, key: str) -> 'Transaction':
        """
        Add a DELETE command to the transaction

        Args:
            key: Key to delete

        Returns:
            self for chaining
        """
        self._check_active()
        self.commands.append(("delete", key))
        return self
    
    def increment(self, key: str, amount: int = 1) -> 'Transaction':
        """
        Add an INCREMENT command to the transaction

        Args:
            key: Key to increment
            amount: Amount to increment by

        Returns:
            self for chaining
        """
        self._check_active()
        self.commands.append(("increment", key, amount))
        return self
    
    def list_push(self, key: str, value: Any, left: bool = False) -> 'Transaction':
        """
        Add a LIST_PUSH command to the transaction

        Args:
            key: List key
            value: Value to push
            left: Push to the left if True, otherwise to the right

        Returns:
            self for chaining
        """
        self._check_active()
        self.commands.append(("list_push", key, value, left))
        return self
    
    def set_add(self, key: str, *values: Any) -> 'Transaction':
        """
        Add a SET_ADD command to the transaction

        Args:
            key: Set key
            *values: Values to add

        Returns:
            self for chaining
        """
        self._check_active()
        self.commands.append(("set_add", key, values))
        return self
    
    def hash_set(self, key: str, field: str, value: Any) -> 'Transaction':
        """
        Add a HASH_SET command to the transaction

        Args:
            key: Hash key
            field: Field to set
            value: Value to set

        Returns:
            self for chaining
        """
        self._check_active()
        self.commands.append(("hash_set", key, field, value))
        return self
    
    def execute(self) -> List[Any]:
        """
        Execute the transaction

        Returns:
            List of results from each command
        """
        self._check_active()
        
        try:
            results = self.backend.execute_transaction(self.commands)
            self.executed = True
            return results
        except Exception as e:
            raise TransactionError(f"Transaction failed: {str(e)}")
        finally:
            self.active = False
    
    def discard(self) -> None:
        """
        Discard the transaction without executing it
        """
        self.active = False
    
    def _check_active(self) -> None:
        """
        Check if the transaction is active

        Raises:
            TransactionError: If the transaction is not active
        """
        if not self.active:
            if self.executed:
                raise TransactionError("Transaction already executed")
            else:
                raise TransactionError("Transaction discarded")
    
    def __enter__(self) -> 'Transaction':
        """
        Enter context manager

        Returns:
            self
        """
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        """
        Exit context manager

        If no exception occurred, execute the transaction.
        If an exception occurred, discard the transaction.

        Returns:
            bool: True if exception was handled, False otherwise
        """
        if exc_type is None:
            try:
                self.execute()
            except Exception:
                # Don't suppress exceptions from execute()
                raise
        else:
            # An exception occurred, discard the transaction
            self.discard()
            # Don't suppress the original exception
            return False 