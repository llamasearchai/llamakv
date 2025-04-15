#!/usr/bin/env python
"""
Unit tests for the KVStore class.
"""

import os
import tempfile
import time
import unittest
from typing import Any, Dict

from llamakv.cache import LRUCache, TTLCache
from llamakv.core.key import Key
from llamakv.core.store import KVStore
from llamakv.core.value import StringValue
from llamakv.persistence import FileBackend, MemoryBackend, SQLiteBackend


class TestKVStore(unittest.TestCase):
    """Test cases for the KVStore class."""

    def setUp(self):
        """Set up test environment."""
        # Create a store with memory backend
        self.store = KVStore()

    def tearDown(self):
        """Clean up after each test."""
        self.store.clear()

    def test_basic_operations(self):
        """Test basic operations (set, get, delete, exists)."""
        # Set a value
        self.store.set("key1", "value1")

        # Get the value
        value = self.store.get("key1")
        self.assertEqual(value, "value1")

        # Check if key exists
        exists = self.store.exists("key1")
        self.assertTrue(exists)

        # Delete the key
        deleted = self.store.delete("key1")
        self.assertTrue(deleted)

        # Verify key no longer exists
        exists = self.store.exists("key1")
        self.assertFalse(exists)

        # Delete non-existent key
        deleted = self.store.delete("non_existent")
        self.assertFalse(deleted)

    def test_different_value_types(self):
        """Test storing different value types."""
        # String
        self.store.set("string", "test string")
        self.assertEqual(self.store.get("string"), "test string")

        # Integer
        self.store.set("int", 42)
        self.assertEqual(self.store.get("int"), 42)

        # Float
        self.store.set("float", 3.14159)
        self.assertEqual(self.store.get("float"), 3.14159)

        # Dictionary (JSON)
        dict_value = {"name": "Alice", "age": 30}
        self.store.set("dict", dict_value)
        self.assertEqual(self.store.get("dict"), dict_value)

        # List
        list_value = [1, 2, 3, "test"]
        self.store.set("list", list_value)
        self.assertEqual(self.store.get("list"), list_value)

        # Object
        class Person:
            def __init__(self, name, age):
                self.name = name
                self.age = age

            def __eq__(self, other):
                if not isinstance(other, Person):
                    return False
                return self.name == other.name and self.age == other.age

        person = Person("Bob", 25)
        self.store.set("object", person)
        retrieved = self.store.get("object")
        self.assertEqual(retrieved.name, "Bob")
        self.assertEqual(retrieved.age, 25)

    def test_key_types(self):
        """Test using different key types."""
        # String key
        self.store.set("string_key", "value")
        self.assertEqual(self.store.get("string_key"), "value")

        # Integer key
        self.store.set(42, "int_value")
        self.assertEqual(self.store.get(42), "int_value")

        # Key object
        key = Key("test_key", namespace="test")
        self.store.set(key, "key_obj_value")
        self.assertEqual(self.store.get(key), "key_obj_value")

        # Key with namespace via string
        self.store.set("test:another_key", "namespaced_value")
        self.assertEqual(self.store.get("test:another_key"), "namespaced_value")

    def test_ttl(self):
        """Test time-to-live functionality."""
        # Set with TTL
        self.store.set("expires", "value", ttl=1)

        # Get immediately
        self.assertEqual(self.store.get("expires"), "value")

        # Wait for expiration
        time.sleep(1.1)

        # Should be expired now
        self.assertIsNone(self.store.get("expires"))
        self.assertFalse(self.store.exists("expires"))

        # Get with include_expired=True
        self.assertEqual(self.store.get("expires", include_expired=True), "value")

    def test_keys_and_count(self):
        """Test getting keys and counting."""
        # Add some keys
        self.store.set("key1", "value1")
        self.store.set("key2", "value2")
        self.store.set("other", "value3")
        self.store.set("test:key1", "value4")
        self.store.set("test:key2", "value5")

        # Get all keys
        keys = self.store.keys()
        self.assertEqual(len(keys), 5)

        # Get keys with pattern
        keys = self.store.keys(pattern="key.*")
        self.assertEqual(len(keys), 2)

        # Get keys with namespace
        keys = self.store.keys(namespace="test")
        self.assertEqual(len(keys), 2)

        # Count all keys
        count = self.store.count()
        self.assertEqual(count, 5)

        # Count keys with pattern
        count = self.store.count(pattern="key.*")
        self.assertEqual(count, 2)

        # Count keys with namespace
        count = self.store.count(namespace="test")
        self.assertEqual(count, 2)

    def test_metadata(self):
        """Test metadata functionality."""
        # Set with metadata
        metadata = {"created_by": "test", "timestamp": "2023-01-01"}
        self.store.set("meta", "value", metadata=metadata)

        # Get value only
        value = self.store.get("meta")
        self.assertEqual(value, "value")

        # Get with metadata
        value, meta = self.store.get_with_metadata("meta")
        self.assertEqual(value, "value")
        self.assertEqual(meta, metadata)

    def test_file_backend(self):
        """Test using a file backend."""
        # Create a temporary file
        with tempfile.NamedTemporaryFile(delete=False) as temp:
            temp_path = temp.name

        try:
            # Create store with file backend
            backend = FileBackend(temp_path)
            store = KVStore(backend=backend)

            # Add some data
            store.set("file1", "value1")
            store.set("file2", "value2")

            # Create a new store with the same backend to test persistence
            store2 = KVStore(backend=FileBackend(temp_path))

            # Check if data persisted
            self.assertEqual(store2.get("file1"), "value1")
            self.assertEqual(store2.get("file2"), "value2")
        finally:
            # Clean up
            os.unlink(temp_path)

    def test_sqlite_backend(self):
        """Test using a SQLite backend."""
        # Create a temporary file
        with tempfile.NamedTemporaryFile(delete=False) as temp:
            temp_path = temp.name

        try:
            # Create store with SQLite backend
            backend = SQLiteBackend(temp_path)
            store = KVStore(backend=backend)

            # Add some data
            store.set("sql1", "value1")
            store.set("sql2", "value2")

            # Create a new store with the same backend to test persistence
            store2 = KVStore(backend=SQLiteBackend(temp_path))

            # Check if data persisted
            self.assertEqual(store2.get("sql1"), "value1")
            self.assertEqual(store2.get("sql2"), "value2")
        finally:
            # Clean up
            os.unlink(temp_path)

    def test_cache_strategies(self):
        """Test different cache strategies."""
        # LRU cache
        store_lru = KVStore(cache_strategy=LRUCache(capacity=10))
        store_lru.set("lru", "value")
        self.assertEqual(store_lru.get("lru"), "value")

        # TTL cache
        store_ttl = KVStore(cache_strategy=TTLCache(capacity=10, default_ttl=1))
        store_ttl.set("ttl", "value")
        self.assertEqual(store_ttl.get("ttl"), "value")

        # Wait for TTL expiration
        time.sleep(1.1)
        self.assertIsNone(store_ttl.get("ttl"))

    def test_transactions(self):
        """Test transaction functionality."""
        # Set initial values
        self.store.set("tx1", "initial1")
        self.store.set("tx2", "initial2")

        # Perform a transaction
        with self.store.transaction() as tx:
            tx.set("tx1", "updated1")
            tx.set("tx2", "updated2")
            tx.set("tx3", "new3")
            tx.delete("tx2")

        # Check results after transaction
        self.assertEqual(self.store.get("tx1"), "updated1")
        self.assertIsNone(self.store.get("tx2"))
        self.assertEqual(self.store.get("tx3"), "new3")

        # Test transaction rollback
        tx = self.store.transaction()
        tx.set("will_rollback", "value")
        tx.rollback()

        # Should not be set after rollback
        self.assertIsNone(self.store.get("will_rollback"))

    def test_purge_expired(self):
        """Test purging expired values."""
        # Set values with different TTLs
        self.store.set("expires1", "value1", ttl=1)
        self.store.set("expires2", "value2", ttl=10)
        self.store.set("no_expiry", "value3")

        # Wait for the first one to expire
        time.sleep(1.1)

        # Purge expired values
        purged = self.store.purge_expired()
        self.assertEqual(purged, 1)

        # Check which values remain
        self.assertIsNone(self.store.get("expires1"))
        self.assertEqual(self.store.get("expires2"), "value2")
        self.assertEqual(self.store.get("no_expiry"), "value3")

    def test_stats(self):
        """Test getting store statistics."""
        # Perform some operations
        self.store.set("stat1", "value1")
        self.store.get("stat1")
        self.store.get("nonexistent")
        self.store.delete("stat1")

        # Get stats
        stats = self.store.get_stats()

        # Basic checks on stats
        self.assertIsInstance(stats, dict)
        self.assertIn("gets", stats)
        self.assertIn("sets", stats)
        self.assertIn("deletes", stats)
        self.assertIn("hits", stats)
        self.assertIn("misses", stats)


if __name__ == "__main__":
    unittest.main()
