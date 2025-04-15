#!/usr/bin/env python
"""
Distributed operation example for LlamaKV.

This example demonstrates how to set up a distributed key-value store
with multiple nodes, showing how to use the client and server components
to create a distributed architecture.

Note: This example uses multiple processes on the same machine for demonstration
purposes. In a real-world scenario, these would be separate machines.
"""

import json
import logging
import os
import signal
import sys
import time
from multiprocessing import Process

from llamakv.core.key import Key
from llamakv.core.store import KVStore
from llamakv.distributed import DistributedClient, DistributedServer
from llamakv.persistence import FileBackend, MemoryBackend

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def start_server(port, node_id, db_path=None):
    """Start a distributed server node."""
    # Use a file backend if db_path is provided, otherwise use memory
    if db_path:
        backend = FileBackend(db_path)
        logger.info(f"Node {node_id} using file backend at {db_path}")
    else:
        backend = MemoryBackend()
        logger.info(f"Node {node_id} using memory backend")

    # Create KVStore
    store = KVStore(backend=backend)

    # Create server
    server = DistributedServer(
        store=store, host="localhost", port=port, node_id=node_id, log_requests=True
    )

    # Start server
    server.start()

    logger.info(f"Node {node_id} started on port {port}")

    # Keep process alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info(f"Node {node_id} shutting down")
        server.shutdown()


def client_example(server_ports):
    """Run client operations against distributed nodes."""
    logger.info("Starting distributed client example")

    # Define node URLs
    nodes = [f"http://localhost:{port}" for port in server_ports]
    logger.info(f"Using nodes: {nodes}")

    # Create distributed client
    client = DistributedClient(
        nodes=nodes, retry_interval=1, retry_attempts=2, async_updates=True
    )

    # Create KVStore with distributed client
    store = KVStore(backend=MemoryBackend(), distributed_client=client)

    try:
        # Set some values (will be propagated to all nodes)
        logger.info("Setting values...")
        store.set("distributed:1", "Value 1")
        store.set("distributed:2", "Value 2")
        store.set("distributed:3", {"complex": "value", "with": ["array", "items"]})

        # Wait for async operations to complete
        time.sleep(1)
        client.wait_for_queue_empty(2)

        # Get values locally
        logger.info("Getting values locally...")
        value1 = store.get("distributed:1")
        value2 = store.get("distributed:2")
        value3 = store.get("distributed:3")

        logger.info(f"Local get - distributed:1 = {value1}")
        logger.info(f"Local get - distributed:2 = {value2}")
        logger.info(f"Local get - distributed:3 = {value3}")

        # Get a value from remote nodes
        logger.info("Getting value from remote nodes...")
        remote_value = client.get_remote(Key("distributed:1"))

        if remote_value:
            logger.info(f"Remote get - distributed:1 = {remote_value.get('value')}")
        else:
            logger.info("Remote get - distributed:1 = Not found")

        # Delete a value (will be propagated to all nodes)
        logger.info("Deleting distributed:2...")
        store.delete("distributed:2")

        # Wait for async operations to complete
        time.sleep(1)
        client.wait_for_queue_empty(2)

        # Verify deletion
        exists_locally = store.exists("distributed:2")
        logger.info(f"After delete - distributed:2 exists locally: {exists_locally}")

        remote_value = client.get_remote(Key("distributed:2"))
        logger.info(
            f"After delete - distributed:2 exists remotely: {remote_value is not None}"
        )

        # Get client stats
        stats = client.stats()
        logger.info(f"Client stats: {json.dumps(stats, indent=2)}")

    except Exception as e:
        logger.error(f"Error in client example: {e}")
    finally:
        # Shutdown client
        client.shutdown()
        logger.info("Client shutdown complete")


def main():
    """Run the distributed example."""
    # Create temporary directory for persistence
    os.makedirs("./tmp", exist_ok=True)

    # Define server ports
    server_ports = [8081, 8082, 8083]

    # Start server processes
    processes = []

    for i, port in enumerate(server_ports):
        node_id = f"node-{i+1}"
        db_path = f"./tmp/llamakv_node_{i+1}.json"

        p = Process(target=start_server, args=(port, node_id, db_path))
        p.start()
        processes.append(p)

    # Wait for servers to start
    logger.info("Waiting for servers to start...")
    time.sleep(3)

    try:
        # Run client example
        client_example(server_ports)
    finally:
        # Terminate server processes
        for p in processes:
            p.terminate()

        # Wait for processes to exit
        for p in processes:
            p.join()

    logger.info("Distributed example completed")


if __name__ == "__main__":
    main()
