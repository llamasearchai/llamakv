#!/usr/bin/env python
"""
Unit tests for the Key class.
"""

import unittest

from llamakv.core.key import Key


class TestKey(unittest.TestCase):
    """Test cases for the Key class."""
    
    def test_create_simple_key(self):
        """Test creating a key with a simple value."""
        key = Key("test")
        self.assertEqual(key.value, "test")
        self.assertIsNone(key.namespace)
        self.assertEqual(str(key), "test")
    
    def test_create_key_with_namespace(self):
        """Test creating a key with a namespace."""
        key = Key("test", namespace="ns")
        self.assertEqual(key.value, "test")
        self.assertEqual(key.namespace, "ns")
        self.assertEqual(str(key), "ns:test")
    
    def test_key_from_string(self):
        """Test creating a key from a string."""
        key = Key.from_string("test")
        self.assertEqual(key.value, "test")
        self.assertIsNone(key.namespace)
        
        key = Key.from_string("ns:test")
        self.assertEqual(key.value, "test")
        self.assertEqual(key.namespace, "ns")
    
    def test_key_equality(self):
        """Test key equality."""
        key1 = Key("test")
        key2 = Key("test")
        key3 = Key("test", namespace="ns")
        key4 = Key("other")
        
        self.assertEqual(key1, key2)
        self.assertNotEqual(key1, key3)
        self.assertNotEqual(key1, key4)
        self.assertNotEqual(key3, key4)
    
    def test_key_hash(self):
        """Test key hash for dictionary usage."""
        key1 = Key("test")
        key2 = Key("test")
        key3 = Key("test", namespace="ns")
        
        # Same keys should have the same hash
        self.assertEqual(hash(key1), hash(key2))
        
        # Different keys should have different hashes
        self.assertNotEqual(hash(key1), hash(key3))
        
        # Test dictionary usage
        key_dict = {key1: "value1", key3: "value3"}
        self.assertEqual(key_dict[key2], "value1")  # key2 is equivalent to key1
        
    def test_different_value_types(self):
        """Test keys with different value types."""
        key_int = Key(42)
        key_float = Key(3.14)
        key_bytes = Key(b"binary")
        key_bool = Key(True)
        
        self.assertEqual(str(key_int), "42")
        self.assertEqual(str(key_float), "3.14")
        self.assertEqual(key_bytes.value, b"binary")
        self.assertEqual(str(key_bool), "True")
    
    def test_unhashable_key_value(self):
        """Test creating a key with an unhashable value."""
        with self.assertRaises(TypeError):
            Key([1, 2, 3])  # Lists are not hashable


if __name__ == "__main__":
    unittest.main() 