#!/usr/bin/env python
"""
Unit tests for the Value classes.
"""

import json
import time
import unittest
from datetime import datetime

from llamakv.core.value import (
    StringValue, IntValue, FloatValue, BytesValue, JsonValue, PickleValue
)


class TestValue(unittest.TestCase):
    """Test cases for the Value classes."""
    
    def test_string_value(self):
        """Test StringValue class."""
        value = StringValue("test string")
        self.assertEqual(value.value, "test string")
        self.assertIsNone(value.ttl)
        self.assertEqual(value.metadata, {})
        
        # Test serialization/deserialization
        data = value.to_dict()
        self.assertEqual(data['type'], "StringValue")
        self.assertEqual(data['value'], "test string")
        
        # Recreate from dict
        value2 = StringValue.from_dict(data)
        self.assertEqual(value2.value, "test string")
    
    def test_int_value(self):
        """Test IntValue class."""
        value = IntValue(42)
        self.assertEqual(value.value, 42)
        
        # Test serialization/deserialization
        data = value.to_dict()
        self.assertEqual(data['type'], "IntValue")
        self.assertEqual(data['value'], 42)
        
        # Recreate from dict
        value2 = IntValue.from_dict(data)
        self.assertEqual(value2.value, 42)
    
    def test_float_value(self):
        """Test FloatValue class."""
        value = FloatValue(3.14159)
        self.assertEqual(value.value, 3.14159)
        
        # Test serialization/deserialization
        data = value.to_dict()
        self.assertEqual(data['type'], "FloatValue")
        self.assertEqual(data['value'], 3.14159)
        
        # Recreate from dict
        value2 = FloatValue.from_dict(data)
        self.assertEqual(value2.value, 3.14159)
    
    def test_bytes_value(self):
        """Test BytesValue class."""
        binary_data = b"binary data"
        value = BytesValue(binary_data)
        self.assertEqual(value.value, binary_data)
        
        # Test serialization/deserialization
        data = value.to_dict()
        self.assertEqual(data['type'], "BytesValue")
        
        # Recreate from dict
        value2 = BytesValue.from_dict(data)
        self.assertEqual(value2.value, binary_data)
    
    def test_json_value(self):
        """Test JsonValue class."""
        json_data = {"name": "Alice", "age": 30, "active": True}
        value = JsonValue(json_data)
        self.assertEqual(value.value, json_data)
        
        # Test serialization/deserialization
        data = value.to_dict()
        self.assertEqual(data['type'], "JsonValue")
        
        # Recreate from dict
        value2 = JsonValue.from_dict(data)
        self.assertEqual(value2.value, json_data)
    
    def test_pickle_value(self):
        """Test PickleValue class."""
        class Person:
            def __init__(self, name, age):
                self.name = name
                self.age = age
                
            def __eq__(self, other):
                if not isinstance(other, Person):
                    return False
                return self.name == other.name and self.age == other.age
        
        person = Person("Bob", 25)
        value = PickleValue(person)
        self.assertEqual(value.value.name, "Bob")
        self.assertEqual(value.value.age, 25)
        
        # Test serialization/deserialization
        data = value.to_dict()
        self.assertEqual(data['type'], "PickleValue")
        
        # Recreate from dict
        value2 = PickleValue.from_dict(data)
        self.assertEqual(value2.value, person)
    
    def test_ttl_and_expiry(self):
        """Test TTL and expiry functionality."""
        # Create value with TTL
        value = StringValue("expires soon", ttl=1)
        self.assertEqual(value.ttl, 1)
        self.assertIsNotNone(value.expiry)
        self.assertFalse(value.is_expired())  # Shouldn't be expired immediately
        
        # Wait for it to expire
        time.sleep(1.1)
        self.assertTrue(value.is_expired())
        
        # Create value without TTL
        value = StringValue("never expires")
        self.assertIsNone(value.ttl)
        self.assertIsNone(value.expiry)
        self.assertFalse(value.is_expired())
    
    def test_metadata(self):
        """Test metadata functionality."""
        # Create value with metadata
        metadata = {"created_by": "test", "timestamp": "2023-01-01"}
        value = StringValue("with metadata", metadata=metadata)
        self.assertEqual(value.metadata, metadata)
        
        # Add metadata
        value.add_metadata("new_key", "new_value")
        self.assertEqual(value.metadata["new_key"], "new_value")
        
        # Test serialization/deserialization preserves metadata
        data = value.to_dict()
        value2 = StringValue.from_dict(data)
        self.assertEqual(value2.metadata, value.metadata)


if __name__ == "__main__":
    unittest.main() 