#!/usr/bin/env python3
"""
Basic usage example for LlamaKV
"""
import time
from llamakv import KVStore

def main():
    # Create an in-memory KV store
    store = KVStore()
    
    print("LlamaKV Basic Usage Example")
    print("==========================")
    
    # Set and get a key
    print("\n# Basic operations")
    store.set("greeting", "Hello, world!")
    value = store.get("greeting")
    print(f"Set and get: {value}")
    
    # Check if a key exists
    exists = store.exists("greeting")
    print(f"Key 'greeting' exists: {exists}")
    
    # Set with expiration
    print("\n# TTL operations")
    store.set("temporary", "I will expire soon", ttl=2)
    print(f"Set 'temporary' with 2 second TTL: {store.get('temporary')}")
    print(f"TTL for 'temporary': {store.ttl('temporary')} seconds")
    
    # Wait for expiration
    time.sleep(2.5)
    try:
        print(f"'temporary' after TTL: {store.get('temporary')}")
    except Exception as e:
        print(f"'temporary' after TTL: Key not found")
    
    # Numeric operations
    print("\n# Numeric operations")
    store.set("counter", 0)
    store.increment("counter")
    print(f"Counter after increment: {store.get('counter')}")
    store.increment("counter", 5)
    print(f"Counter after increment by 5: {store.get('counter')}")
    store.decrement("counter", 2)
    print(f"Counter after decrement by 2: {store.get('counter')}")
    
    # List operations
    print("\n# List operations")
    store.list_push("users", "user1")
    store.list_push("users", "user2")
    store.list_push("users", "user3", left=True)  # push to the left side
    print(f"List length: {store.list_length('users')}")
    print(f"List range: {store.list_range('users', 0, -1)}")
    print(f"Popped from right: {store.list_pop('users')}")
    print(f"List after pop: {store.list_range('users', 0, -1)}")
    
    # Set operations
    print("\n# Set operations")
    store.set_add("tags", "python", "database", "nosql")
    print(f"Set members: {store.set_members('tags')}")
    print(f"Is 'python' a member: {store.set_is_member('tags', 'python')}")
    print(f"Is 'java' a member: {store.set_is_member('tags', 'java')}")
    store.set_remove("tags", "nosql")
    print(f"Set after remove: {store.set_members('tags')}")
    
    # Hash operations
    print("\n# Hash operations")
    store.hash_set("user:1", "name", "John")
    store.hash_set("user:1", "email", "john@example.com")
    store.hash_set("user:1", "age", 30)
    print(f"Hash field 'name': {store.hash_get('user:1', 'name')}")
    print(f"All hash fields: {store.hash_get_all('user:1')}")
    store.hash_delete("user:1", "age")
    print(f"Hash after delete: {store.hash_get_all('user:1')}")
    
    # Batch operations
    print("\n# Batch operations")
    store.batch_set({
        "batch1": "value1",
        "batch2": "value2",
        "batch3": "value3"
    })
    print(f"Batch get: {store.batch_get(['batch1', 'batch2', 'batch3'])}")
    deleted = store.batch_delete(["batch1", "batch2"])
    print(f"Deleted {deleted} keys")
    print(f"Remaining keys: {store.keys('batch*')}")
    
    # Clean up
    store.flush()
    print("\nAll keys have been flushed")
    
    # Transaction example
    print("\n# Transaction example")
    with store.transaction() as txn:
        txn.set("tx1", "value1")
        txn.set("tx2", "value2")
        txn.increment("tx_counter", 10)
    
    print(f"Transaction results: {store.batch_get(['tx1', 'tx2', 'tx_counter'])}")
    
    # Close the store
    store.close()
    print("\nStore closed")

if __name__ == "__main__":
    main() 