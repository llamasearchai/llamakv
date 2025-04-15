"""
SQLite backend implementation for LlamaKV.

This module provides a SQLite-based storage backend for the key-value store,
which persists data in a SQLite database file.
"""

import json
import os
import re
import sqlite3
import threading
import time
from contextlib import contextmanager
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

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


class SQLiteBackend:
    """
    SQLite-based storage backend for the key-value store.

    Stores data in a SQLite database, which provides persistence
    and transactions.
    """

    def __init__(
        self,
        db_path: str,
        pragmas: Optional[Dict[str, Any]] = None,
        auto_vacuum: bool = True,
        auto_commit: bool = True,
        auto_commit_interval: int = 5,
    ):
        """
        Initialize a SQLite backend.

        Args:
            db_path: Path to the SQLite database file
            pragmas: Optional SQLite PRAGMA settings
            auto_vacuum: Whether to enable auto-vacuum
            auto_commit: Whether to automatically commit transactions
            auto_commit_interval: Interval for auto-committing (in seconds)
        """
        self._db_path = db_path
        self._pragmas = pragmas or {}
        self._auto_vacuum = auto_vacuum
        self._auto_commit = auto_commit
        self._auto_commit_interval = auto_commit_interval
        self._lock = threading.RLock()
        self._on_set_callbacks: List[Callable[[Key, Value], None]] = []
        self._on_delete_callbacks: List[Callable[[Key], None]] = []
        self._last_commit = time.time()
        self._transaction_active = False

        # Stats
        self._reads = 0
        self._writes = 0
        self._deletes = 0
        self._commits = 0

        # Set up database
        self._setup_db()

        # Start auto-commit thread if enabled
        if auto_commit:
            self._commit_thread = threading.Thread(
                target=self._auto_commit_thread, daemon=True
            )
            self._commit_thread.start()

    def _auto_commit_thread(self) -> None:
        """Thread function for auto-committing."""
        while True:
            time.sleep(1)  # Check every second
            if (
                self._auto_commit
                and self._transaction_active
                and time.time() - self._last_commit >= self._auto_commit_interval
            ):
                try:
                    with self._lock:
                        self._commit()
                except Exception as e:
                    # Log error but don't crash thread
                    pass

    @contextmanager
    def _connection(self) -> sqlite3.Connection:
        """
        Get a SQLite connection.

        Returns:
            Context manager for SQLite connection
        """
        # SQLite connections are not thread-safe, so create a new one for each operation
        conn = sqlite3.connect(self._db_path)

        # Enable foreign keys
        conn.execute("PRAGMA foreign_keys = ON")

        # Apply user pragmas
        for pragma, value in self._pragmas.items():
            conn.execute(f"PRAGMA {pragma} = {value}")

        try:
            yield conn
        finally:
            conn.close()

    def _setup_db(self) -> None:
        """Set up the SQLite database schema."""
        # Ensure directory exists
        os.makedirs(os.path.dirname(os.path.abspath(self._db_path)), exist_ok=True)

        with self._lock, self._connection() as conn:
            # Create tables
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS kv_store (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    type TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    ttl INTEGER,
                    metadata TEXT
                )
            """
            )

            # Create indexes
            conn.execute("CREATE INDEX IF NOT EXISTS idx_type ON kv_store (type)")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_created_at ON kv_store (created_at)"
            )

            # Commit schema changes
            conn.commit()

            # Enable auto-vacuum if requested
            if self._auto_vacuum:
                conn.execute("PRAGMA auto_vacuum = INCREMENTAL")
                conn.commit()

    def _commit(self) -> None:
        """Commit the current transaction."""
        with self._connection() as conn:
            conn.commit()
            self._last_commit = time.time()
            self._transaction_active = False
            self._commits += 1

    def register_on_set(self, callback: Callable[[Key, Value], None]) -> None:
        """
        Register a callback for set operations.

        Args:
            callback: Function to call when a key is set
        """
        self._on_set_callbacks.append(callback)

    def register_on_delete(self, callback: Callable[[Key], None]) -> None:
        """
        Register a callback for delete operations.

        Args:
            callback: Function to call when a key is deleted
        """
        self._on_delete_callbacks.append(callback)

    def set(self, key: Key, value: Value) -> None:
        """
        Set a value for a key.

        Args:
            key: The key
            value: The value
        """
        key_str = str(key)
        value_dict = value.to_dict()

        with self._lock, self._connection() as conn:
            # Convert value_dict to JSON string
            value_json = json.dumps(value_dict["value"])
            metadata_json = json.dumps(value_dict.get("metadata", {}))

            # Insert or replace
            conn.execute(
                """
                INSERT OR REPLACE INTO kv_store
                (key, value, type, created_at, ttl, metadata)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    key_str,
                    value_json,
                    value_dict["type"],
                    value_dict["created_at"],
                    value_dict.get("ttl"),
                    metadata_json,
                ),
            )

            self._writes += 1
            self._transaction_active = True

            # Commit immediately if auto_commit is disabled
            if not self._auto_commit:
                self._commit()

        # Call callbacks
        for callback in self._on_set_callbacks:
            try:
                callback(key, value)
            except Exception as e:
                # Don't let callback exceptions propagate
                pass

    def get(self, key: Key) -> Optional[Value]:
        """
        Get a value for a key.

        Args:
            key: The key

        Returns:
            The value, or None if not found
        """
        key_str = str(key)

        with self._lock, self._connection() as conn:
            # Query database
            cursor = conn.execute(
                """
                SELECT value, type, created_at, ttl, metadata
                FROM kv_store
                WHERE key = ?
                """,
                (key_str,),
            )

            row = cursor.fetchone()
            if row is None:
                return None

            self._reads += 1

            # Parse row
            value_json, value_type, created_at, ttl, metadata_json = row

            # Create value dictionary
            value_dict = {
                "value": json.loads(value_json),
                "type": value_type,
                "created_at": created_at,
                "ttl": ttl,
                "metadata": json.loads(metadata_json) if metadata_json else {},
            }

            # Create Value object based on type
            if value_type == "StringValue":
                return StringValue.from_dict(value_dict)
            elif value_type == "IntValue":
                return IntValue.from_dict(value_dict)
            elif value_type == "FloatValue":
                return FloatValue.from_dict(value_dict)
            elif value_type == "BytesValue":
                return BytesValue.from_dict(value_dict)
            elif value_type == "JsonValue":
                return JsonValue.from_dict(value_dict)
            elif value_type == "PickleValue":
                return PickleValue.from_dict(value_dict)
            else:
                # Unknown value type
                return None

    def delete(self, key: Key) -> bool:
        """
        Delete a key from the store.

        Args:
            key: The key

        Returns:
            True if the key was deleted, False if it didn't exist
        """
        key_str = str(key)

        with self._lock, self._connection() as conn:
            # Check if key exists
            cursor = conn.execute("SELECT 1 FROM kv_store WHERE key = ?", (key_str,))

            exists = cursor.fetchone() is not None

            if exists:
                # Delete key
                conn.execute("DELETE FROM kv_store WHERE key = ?", (key_str,))

                self._deletes += 1
                self._transaction_active = True

                # Commit immediately if auto_commit is disabled
                if not self._auto_commit:
                    self._commit()

        # Call callbacks (only if the key was deleted)
        if exists:
            for callback in self._on_delete_callbacks:
                try:
                    callback(key)
                except Exception as e:
                    # Don't let callback exceptions propagate
                    pass

        return exists

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
        with self._lock, self._connection() as conn:
            # Query all keys
            cursor = conn.execute("SELECT key FROM kv_store")

            # Process results
            result = []
            for row in cursor:
                key_str = row[0]
                key = Key.from_string(key_str)

                # Filter by namespace if specified
                if namespace is not None and key.namespace != namespace:
                    continue

                # Filter by pattern if specified
                if pattern is not None:
                    # Use regex pattern matching
                    if not re.search(pattern, key_str):
                        continue

                result.append(key)

            return result

    def clear(self) -> None:
        """Clear all keys from the store."""
        with self._lock, self._connection() as conn:
            # Delete all rows
            conn.execute("DELETE FROM kv_store")

            self._transaction_active = True

            # Commit immediately if auto_commit is disabled
            if not self._auto_commit:
                self._commit()

    def vacuum(self) -> None:
        """Vacuum the database to reclaim space."""
        with self._lock, self._connection() as conn:
            conn.execute("VACUUM")
            conn.commit()

    def commit(self) -> None:
        """Manually commit the current transaction."""
        with self._lock:
            self._commit()

    def stats(self) -> Dict[str, Any]:
        """
        Get statistics about the backend.

        Returns:
            Dictionary of statistics
        """
        with self._lock, self._connection() as conn:
            # Count total keys
            cursor = conn.execute("SELECT COUNT(*) FROM kv_store")
            total_keys = cursor.fetchone()[0]

            # Count keys by type
            cursor = conn.execute("SELECT type, COUNT(*) FROM kv_store GROUP BY type")
            type_counts = {row[0]: row[1] for row in cursor}

            # Get database size
            cursor = conn.execute("PRAGMA page_count")
            page_count = cursor.fetchone()[0]
            cursor = conn.execute("PRAGMA page_size")
            page_size = cursor.fetchone()[0]
            db_size = page_count * page_size

            # Count expired keys
            now = time.time()
            cursor = conn.execute(
                """
                SELECT COUNT(*) FROM kv_store
                WHERE ttl IS NOT NULL AND created_at + ttl < ?
                """,
                (now,),
            )
            expired_keys = cursor.fetchone()[0]

            return {
                "reads": self._reads,
                "writes": self._writes,
                "deletes": self._deletes,
                "commits": self._commits,
                "keys": total_keys,
                "key_types": type_counts,
                "sqlite_backend": True,
                "db_path": self._db_path,
                "db_size": db_size,
                "last_commit": self._last_commit,
                "auto_commit": self._auto_commit,
                "transaction_active": self._transaction_active,
                "expired_keys": expired_keys,
            }
