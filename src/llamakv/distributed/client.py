"""
Distributed client implementation for LlamaKV.

This module provides the client-side functionality for distributed
operations in the key-value store.
"""

import json
import logging
import queue
import socket
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import requests
from requests.exceptions import RequestException

from llamakv.core.key import Key
from llamakv.core.value import Value

logger = logging.getLogger(__name__)


class DistributedClient:
    """
    Client for distributed key-value store operations.

    Provides functionality for coordinating with remote nodes
    in a distributed key-value store cluster.
    """

    def __init__(
        self,
        nodes: List[str],
        retry_interval: int = 5,
        retry_attempts: int = 3,
        connect_timeout: int = 5,
        read_timeout: int = 10,
        async_updates: bool = True,
        max_queue_size: int = 1000,
    ):
        """
        Initialize a distributed client.

        Args:
            nodes: List of node URLs (e.g., ["http://node1:8080", "http://node2:8080"])
            retry_interval: Interval between retry attempts (in seconds)
            retry_attempts: Number of retry attempts for operations
            connect_timeout: Connection timeout (in seconds)
            read_timeout: Read timeout (in seconds)
            async_updates: Whether to send updates asynchronously
            max_queue_size: Maximum size of the async update queue
        """
        self._nodes = nodes
        self._retry_interval = retry_interval
        self._retry_attempts = retry_attempts
        self._connect_timeout = connect_timeout
        self._read_timeout = read_timeout
        self._async_updates = async_updates
        self._max_queue_size = max_queue_size

        # Set up update queue for async operations
        self._update_queue = queue.Queue(maxsize=max_queue_size)
        self._running = True

        # Start update thread if async updates are enabled
        if async_updates:
            self._update_thread = threading.Thread(
                target=self._process_updates, daemon=True
            )
            self._update_thread.start()

        # Statistics
        self._stats = {
            "propagations_sent": 0,
            "propagations_failed": 0,
            "reads_sent": 0,
            "reads_failed": 0,
            "nodes_down": set(),
            "last_sync": time.time(),
        }

        logger.info(f"Initialized distributed client with {len(nodes)} nodes")

    def _process_updates(self) -> None:
        """Worker thread for processing asynchronous updates."""
        while self._running:
            try:
                # Get next update from queue
                update = self._update_queue.get(timeout=1)

                # Process update
                operation, key, value = update

                if operation == "set":
                    self._propagate_set_sync(key, value)
                elif operation == "delete":
                    self._propagate_delete_sync(key)
                elif operation == "clear":
                    self._propagate_clear_sync()

                # Mark task as done
                self._update_queue.task_done()
            except queue.Empty:
                # Queue is empty, wait for more updates
                continue
            except Exception as e:
                logger.error(f"Error processing update: {e}")

    def _propagate_set_sync(self, key: Key, value: Value) -> bool:
        """
        Synchronously propagate a set operation to all nodes.

        Args:
            key: The key
            value: The value

        Returns:
            True if successful on at least one node, False otherwise
        """
        key_str = str(key)
        value_dict = value.to_dict()

        # Convert to JSON
        data = {"key": key_str, "value": value_dict, "operation": "set"}

        # Success tracker
        success = False

        # Send to all nodes
        for node in self._nodes:
            if node in self._stats["nodes_down"]:
                # Skip nodes that are known to be down
                continue

            url = f"{node}/api/v1/propagate"

            # Try to send with retries
            for attempt in range(self._retry_attempts):
                try:
                    response = requests.post(
                        url,
                        json=data,
                        timeout=(self._connect_timeout, self._read_timeout),
                    )

                    if response.status_code == 200:
                        # Success
                        if node in self._stats["nodes_down"]:
                            self._stats["nodes_down"].remove(node)
                        success = True
                        break
                    else:
                        # Error
                        logger.warning(
                            f"Error propagating set to {node}: {response.status_code}"
                        )
                        if attempt == self._retry_attempts - 1:
                            self._stats["propagations_failed"] += 1
                            self._stats["nodes_down"].add(node)
                except RequestException as e:
                    # Connection error
                    logger.warning(f"Connection error propagating set to {node}: {e}")
                    if attempt == self._retry_attempts - 1:
                        self._stats["propagations_failed"] += 1
                        self._stats["nodes_down"].add(node)

                # Wait before retry
                if attempt < self._retry_attempts - 1:
                    time.sleep(self._retry_interval)

        if success:
            self._stats["propagations_sent"] += 1
        return success

    def _propagate_delete_sync(self, key: Key) -> bool:
        """
        Synchronously propagate a delete operation to all nodes.

        Args:
            key: The key

        Returns:
            True if successful on at least one node, False otherwise
        """
        key_str = str(key)

        # Convert to JSON
        data = {"key": key_str, "operation": "delete"}

        # Success tracker
        success = False

        # Send to all nodes
        for node in self._nodes:
            if node in self._stats["nodes_down"]:
                # Skip nodes that are known to be down
                continue

            url = f"{node}/api/v1/propagate"

            # Try to send with retries
            for attempt in range(self._retry_attempts):
                try:
                    response = requests.post(
                        url,
                        json=data,
                        timeout=(self._connect_timeout, self._read_timeout),
                    )

                    if response.status_code == 200:
                        # Success
                        if node in self._stats["nodes_down"]:
                            self._stats["nodes_down"].remove(node)
                        success = True
                        break
                    else:
                        # Error
                        logger.warning(
                            f"Error propagating delete to {node}: {response.status_code}"
                        )
                        if attempt == self._retry_attempts - 1:
                            self._stats["propagations_failed"] += 1
                            self._stats["nodes_down"].add(node)
                except RequestException as e:
                    # Connection error
                    logger.warning(
                        f"Connection error propagating delete to {node}: {e}"
                    )
                    if attempt == self._retry_attempts - 1:
                        self._stats["propagations_failed"] += 1
                        self._stats["nodes_down"].add(node)

                # Wait before retry
                if attempt < self._retry_attempts - 1:
                    time.sleep(self._retry_interval)

        if success:
            self._stats["propagations_sent"] += 1
        return success

    def _propagate_clear_sync(self) -> bool:
        """
        Synchronously propagate a clear operation to all nodes.

        Returns:
            True if successful on at least one node, False otherwise
        """
        # Convert to JSON
        data = {"operation": "clear"}

        # Success tracker
        success = False

        # Send to all nodes
        for node in self._nodes:
            if node in self._stats["nodes_down"]:
                # Skip nodes that are known to be down
                continue

            url = f"{node}/api/v1/propagate"

            # Try to send with retries
            for attempt in range(self._retry_attempts):
                try:
                    response = requests.post(
                        url,
                        json=data,
                        timeout=(self._connect_timeout, self._read_timeout),
                    )

                    if response.status_code == 200:
                        # Success
                        if node in self._stats["nodes_down"]:
                            self._stats["nodes_down"].remove(node)
                        success = True
                        break
                    else:
                        # Error
                        logger.warning(
                            f"Error propagating clear to {node}: {response.status_code}"
                        )
                        if attempt == self._retry_attempts - 1:
                            self._stats["propagations_failed"] += 1
                            self._stats["nodes_down"].add(node)
                except RequestException as e:
                    # Connection error
                    logger.warning(f"Connection error propagating clear to {node}: {e}")
                    if attempt == self._retry_attempts - 1:
                        self._stats["propagations_failed"] += 1
                        self._stats["nodes_down"].add(node)

                # Wait before retry
                if attempt < self._retry_attempts - 1:
                    time.sleep(self._retry_interval)

        if success:
            self._stats["propagations_sent"] += 1
        return success

    def propagate_set(self, key: Key, value: Value) -> bool:
        """
        Propagate a set operation to all nodes.

        Args:
            key: The key
            value: The value

        Returns:
            True if successful (or queued), False otherwise
        """
        if self._async_updates:
            try:
                # Add to queue for async processing
                self._update_queue.put(("set", key, value), block=False)
                return True
            except queue.Full:
                logger.error("Update queue is full, dropping set operation")
                return False
        else:
            # Process synchronously
            return self._propagate_set_sync(key, value)

    def propagate_delete(self, key: Key) -> bool:
        """
        Propagate a delete operation to all nodes.

        Args:
            key: The key

        Returns:
            True if successful (or queued), False otherwise
        """
        if self._async_updates:
            try:
                # Add to queue for async processing
                self._update_queue.put(("delete", key, None), block=False)
                return True
            except queue.Full:
                logger.error("Update queue is full, dropping delete operation")
                return False
        else:
            # Process synchronously
            return self._propagate_delete_sync(key)

    def propagate_clear(self) -> bool:
        """
        Propagate a clear operation to all nodes.

        Returns:
            True if successful (or queued), False otherwise
        """
        if self._async_updates:
            try:
                # Add to queue for async processing
                self._update_queue.put(("clear", None, None), block=False)
                return True
            except queue.Full:
                logger.error("Update queue is full, dropping clear operation")
                return False
        else:
            # Process synchronously
            return self._propagate_clear_sync()

    def get_remote(self, key: Key) -> Optional[Dict[str, Any]]:
        """
        Get a value from a remote node.

        Args:
            key: The key

        Returns:
            Value dictionary, or None if not found
        """
        key_str = str(key)

        # Try each node until success or all fail
        for node in self._nodes:
            if node in self._stats["nodes_down"]:
                # Skip nodes that are known to be down
                continue

            url = f"{node}/api/v1/key/{key_str}"

            # Try to get with retries
            for attempt in range(self._retry_attempts):
                try:
                    response = requests.get(
                        url, timeout=(self._connect_timeout, self._read_timeout)
                    )

                    if response.status_code == 200:
                        # Success
                        if node in self._stats["nodes_down"]:
                            self._stats["nodes_down"].remove(node)
                        self._stats["reads_sent"] += 1
                        return response.json()
                    elif response.status_code == 404:
                        # Not found
                        if node in self._stats["nodes_down"]:
                            self._stats["nodes_down"].remove(node)
                        self._stats["reads_sent"] += 1
                        return None
                    else:
                        # Error
                        logger.warning(
                            f"Error getting key from {node}: {response.status_code}"
                        )
                        if attempt == self._retry_attempts - 1:
                            self._stats["reads_failed"] += 1
                            self._stats["nodes_down"].add(node)
                except RequestException as e:
                    # Connection error
                    logger.warning(f"Connection error getting key from {node}: {e}")
                    if attempt == self._retry_attempts - 1:
                        self._stats["reads_failed"] += 1
                        self._stats["nodes_down"].add(node)

                # Wait before retry
                if attempt < self._retry_attempts - 1:
                    time.sleep(self._retry_interval)

        # All nodes failed
        logger.warning(f"All nodes failed while getting key {key_str}")
        return None

    def shutdown(self) -> None:
        """Shutdown the client and stop background threads."""
        self._running = False

        if self._async_updates:
            # Wait for update thread to finish
            self._update_thread.join(timeout=5)

    def wait_for_queue_empty(self, timeout: Optional[float] = None) -> bool:
        """
        Wait for the update queue to empty.

        Args:
            timeout: Timeout in seconds, or None to wait indefinitely

        Returns:
            True if queue is empty, False if timeout occurred
        """
        if not self._async_updates:
            return True

        try:
            self._update_queue.join(timeout=timeout)
            return True
        except:
            return False

    def stats(self) -> Dict[str, Any]:
        """
        Get statistics about the client.

        Returns:
            Dictionary of statistics
        """
        queue_size = 0
        if self._async_updates:
            queue_size = self._update_queue.qsize()

        return {
            "nodes": len(self._nodes),
            "nodes_down": len(self._stats["nodes_down"]),
            "propagations_sent": self._stats["propagations_sent"],
            "propagations_failed": self._stats["propagations_failed"],
            "reads_sent": self._stats["reads_sent"],
            "reads_failed": self._stats["reads_failed"],
            "async_updates": self._async_updates,
            "queue_size": queue_size,
            "max_queue_size": self._max_queue_size,
            "queue_utilization": (
                (queue_size / self._max_queue_size) * 100
                if self._max_queue_size > 0
                else 0
            ),
            "retry_attempts": self._retry_attempts,
            "retry_interval": self._retry_interval,
            "last_sync": self._stats["last_sync"],
        }
