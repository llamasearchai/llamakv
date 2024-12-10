# LlamaKV

A flexible and powerful key-value storage system for LlamaSearch.ai applications.

## Features

- **Core KV Store**: Fast and reliable key-value storage
- **Multiple Backends**: Support for in-memory, file-based, and Redis backends
- **Data Types**: Support for strings, integers, lists, sets, and hashes
- **Persistence**: Options for snapshotting and append-only file persistence
- **TTL Support**: Automatic expiration of keys
- **Transactions**: Atomic operations and optimistic locking
- **Pub/Sub**: Publish/subscribe messaging pattern
- **Caching**: LRU/LFU caching policies with configurable size limits
- **Distributed Mode**: Clustering and sharding for horizontal scaling
- **Monitoring**: Metrics and statistics for monitoring performance

## Installation

### Using pip

```bash
pip install llamakv
```

### From source

```bash
git clone https://llamasearch.ai
cd llamakv
pip install -e .
```

## Quick Start

```python
from llamakv import KVStore

# Create an in-memory KV store
store = KVStore()

# Set a key
store.set("greeting", "Hello, world!")

# Get a key
value = store.get("greeting")
print(value)  # Outputs: Hello, world!

# Set with expiration (TTL in seconds)
store.set("temporary", "I will disappear", ttl=60)

# Check if a key exists
exists = store.exists("greeting")
print(exists)  # Outputs: True

# Delete a key
store.delete("greeting")

# Use integer values
store.set("counter", 0)
store.increment("counter")
store.increment("counter", 5)
print(store.get("counter"))  # Outputs: 6

# Work with lists
store.list_push("users", "user1")
store.list_push("users", "user2")
print(store.list_range("users", 0, -1))  # Outputs: ["user1", "user2"]

# Work with sets
store.set_add("tags", "python")
store.set_add("tags", "database")
print(store.set_members("tags"))  # Outputs: {"python", "database"}

# Work with hashes
store.hash_set("user:1", "name", "John")
store.hash_set("user:1", "email", "john@example.com")
print(store.hash_get("user:1", "name"))  # Outputs: John
print(store.hash_get_all("user:1"))  # Outputs: {"name": "John", "email": "john@example.com"}
```

## File-based Storage

```python
from llamakv import KVStore

# Create a file-based KV store
store = KVStore(backend="file", path="./data.kv")

# Operations are the same as with in-memory storage
store.set("key", "value")
print(store.get("key"))  # Outputs: value

# Close the store when done (automatically saves data)
store.close()
```

## Redis Backend

```python
from llamakv import KVStore

# Create a Redis-backed KV store
store = KVStore(
    backend="redis",
    host="localhost",
    port=6379,
    password="optional_password"
)

# Operations are the same as with other backends
store.set("key", "value")
print(store.get("key"))  # Outputs: value
```

## Distributed Mode

```python
from llamakv import KVStore

# Create a distributed KV store (cluster mode)
store = KVStore(
    backend="distributed",
    nodes=[
        {"host": "node1.example.com", "port": 6379},
        {"host": "node2.example.com", "port": 6379},
        {"host": "node3.example.com", "port": 6379}
    ]
)

# Operations are the same as with other backends
store.set("key", "value")
print(store.get("key"))  # Outputs: value
```

## Advanced Usage

### Transactions

```python
from llamakv import KVStore

store = KVStore()

# Start a transaction
with store.transaction() as txn:
    txn.set("key1", "value1")
    txn.set("key2", "value2")
    # If any operation fails, the entire transaction is rolled back
```

### Pub/Sub

```python
from llamakv import KVStore
import threading

store = KVStore()

# In one thread (subscriber)
def listen_for_messages():
    pubsub = store.pubsub()
    pubsub.subscribe("channel1")
    for message in pubsub.listen():
        print(f"Received: {message}")

# Start listening in a background thread
t = threading.Thread(target=listen_for_messages)
t.daemon = True
t.start()

# In main thread (publisher)
store.publish("channel1", "Hello subscribers!")
```

### Batch Operations

```python
from llamakv import KVStore

store = KVStore()

# Batch set
store.batch_set({
    "key1": "value1",
    "key2": "value2",
    "key3": "value3"
})

# Batch get
values = store.batch_get(["key1", "key2", "key3"])
print(values)  # Outputs: {"key1": "value1", "key2": "value2", "key3": "value3"}

# Batch delete
store.batch_delete(["key1", "key2"])
```

## Performance Tuning

```python
from llamakv import KVStore

# Configure performance settings
store = KVStore(
    max_memory="100mb",
    eviction_policy="lru",
    max_keys=10000,
    sync_strategy="every_second"
)
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT 
# Updated in commit 1 - 2025-04-04 17:46:38

# Updated in commit 9 - 2025-04-04 17:46:38

# Updated in commit 17 - 2025-04-04 17:46:39

# Updated in commit 25 - 2025-04-04 17:46:39

# Updated in commit 1 - 2025-04-05 14:44:32

# Updated in commit 9 - 2025-04-05 14:44:32

# Updated in commit 17 - 2025-04-05 14:44:32

# Updated in commit 25 - 2025-04-05 14:44:32

# Updated in commit 1 - 2025-04-05 15:30:42

# Updated in commit 9 - 2025-04-05 15:30:43

# Updated in commit 17 - 2025-04-05 15:30:43

# Updated in commit 25 - 2025-04-05 15:30:43

# Updated in commit 1 - 2025-04-05 16:10:58

# Updated in commit 9 - 2025-04-05 16:10:59

# Updated in commit 17 - 2025-04-05 16:10:59

# Updated in commit 25 - 2025-04-05 16:10:59

# Updated in commit 1 - 2025-04-05 17:17:28

# Updated in commit 9 - 2025-04-05 17:17:28

# Updated in commit 17 - 2025-04-05 17:17:29

# Updated in commit 25 - 2025-04-05 17:17:29

# Updated in commit 1 - 2025-04-05 17:49:01

# Updated in commit 9 - 2025-04-05 17:49:01

# Updated in commit 17 - 2025-04-05 17:49:02

# Updated in commit 25 - 2025-04-05 17:49:02

# Updated in commit 1 - 2025-04-05 18:38:45
