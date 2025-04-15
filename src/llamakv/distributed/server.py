"""
Distributed server implementation for LlamaKV.

This module provides the server-side functionality for distributed
operations in the key-value store.
"""

import json
import logging
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

from flask import Flask, jsonify, request

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

logger = logging.getLogger(__name__)


class DistributedServer:
    """
    Server for distributed key-value store operations.

    Provides a REST API for remote nodes to interact with the local
    key-value store in a distributed cluster.
    """

    def __init__(
        self,
        store=None,
        host: str = "0.0.0.0",
        port: int = 8080,
        api_prefix: str = "/api/v1",
        node_id: Optional[str] = None,
        auth_token: Optional[str] = None,
        allow_propagation: bool = True,
        propagation_source_header: str = "X-Propagation-Source",
        log_requests: bool = True,
    ):
        """
        Initialize a distributed server.

        Args:
            store: Key-value store instance
            host: Host to bind the server to
            port: Port to bind the server to
            api_prefix: Prefix for API endpoints
            node_id: Unique ID for this node
            auth_token: Optional authentication token for API requests
            allow_propagation: Whether to allow propagation from other nodes
            propagation_source_header: Header for propagation source
            log_requests: Whether to log all API requests
        """
        self._store = store
        self._host = host
        self._port = port
        self._api_prefix = api_prefix.rstrip("/")
        self._node_id = node_id or f"node-{id(self)}"
        self._auth_token = auth_token
        self._allow_propagation = allow_propagation
        self._propagation_source_header = propagation_source_header
        self._log_requests = log_requests

        # Flask app
        self._app = Flask(f"LlamaKV-{self._node_id}")

        # Set up routes
        self._setup_routes()

        # Server thread
        self._server_thread = None
        self._running = False

        # Statistics
        self._stats = {
            "requests": 0,
            "propagations_received": 0,
            "gets": 0,
            "sets": 0,
            "deletes": 0,
            "clears": 0,
            "errors": 0,
            "unauthorized": 0,
            "start_time": time.time(),
        }

        logger.info(
            f"Initialized distributed server on {host}:{port} with node ID {self._node_id}"
        )

    def _setup_routes(self) -> None:
        """Set up API routes."""

        # Get value
        @self._app.route(f"{self._api_prefix}/key/<key>", methods=["GET"])
        def get_key(key):
            self._stats["requests"] += 1
            self._stats["gets"] += 1

            if self._log_requests:
                logger.debug(f"GET {self._api_prefix}/key/{key}")

            # Authenticate
            if not self._authenticate(request):
                self._stats["unauthorized"] += 1
                return jsonify({"error": "Unauthorized"}), 401

            # Process key
            try:
                key_obj = Key.from_string(key)

                # Get value
                value = self._store.get(key_obj)

                if value is None:
                    return jsonify({"error": "Key not found"}), 404

                # Convert value to dictionary
                value_dict = value.to_dict()

                return jsonify(value_dict), 200
            except Exception as e:
                logger.error(f"Error getting key {key}: {e}")
                self._stats["errors"] += 1
                return jsonify({"error": str(e)}), 500

        # Set value
        @self._app.route(f"{self._api_prefix}/key", methods=["POST"])
        def set_key():
            self._stats["requests"] += 1
            self._stats["sets"] += 1

            if self._log_requests:
                logger.debug(f"POST {self._api_prefix}/key")

            # Authenticate
            if not self._authenticate(request):
                self._stats["unauthorized"] += 1
                return jsonify({"error": "Unauthorized"}), 401

            # Process request
            try:
                data = request.json

                if not data or "key" not in data or "value" not in data:
                    return jsonify({"error": "Invalid request"}), 400

                # Extract key and value
                key_str = data["key"]
                value_data = data["value"]
                ttl = data.get("ttl")
                metadata = data.get("metadata", {})

                # Create key object
                key_obj = Key.from_string(key_str)

                # Determine value type
                value_type = data.get("type", "StringValue")
                if value_type == "StringValue":
                    value_class = StringValue
                elif value_type == "IntValue":
                    value_class = IntValue
                elif value_type == "FloatValue":
                    value_class = FloatValue
                elif value_type == "BytesValue":
                    value_class = BytesValue
                elif value_type == "JsonValue":
                    value_class = JsonValue
                elif value_type == "PickleValue":
                    value_class = PickleValue
                else:
                    return jsonify({"error": f"Invalid value type: {value_type}"}), 400

                # Create value object
                value_obj = value_class(value_data, ttl=ttl, metadata=metadata)

                # Set in store
                self._store.set(key_obj, value_obj)

                return jsonify({"success": True}), 200
            except Exception as e:
                logger.error(f"Error setting key: {e}")
                self._stats["errors"] += 1
                return jsonify({"error": str(e)}), 500

        # Delete value
        @self._app.route(f"{self._api_prefix}/key/<key>", methods=["DELETE"])
        def delete_key(key):
            self._stats["requests"] += 1
            self._stats["deletes"] += 1

            if self._log_requests:
                logger.debug(f"DELETE {self._api_prefix}/key/{key}")

            # Authenticate
            if not self._authenticate(request):
                self._stats["unauthorized"] += 1
                return jsonify({"error": "Unauthorized"}), 401

            # Process key
            try:
                key_obj = Key.from_string(key)

                # Delete key
                deleted = self._store.delete(key_obj)

                if not deleted:
                    return jsonify({"error": "Key not found"}), 404

                return jsonify({"success": True}), 200
            except Exception as e:
                logger.error(f"Error deleting key {key}: {e}")
                self._stats["errors"] += 1
                return jsonify({"error": str(e)}), 500

        # Propagation endpoint
        @self._app.route(f"{self._api_prefix}/propagate", methods=["POST"])
        def propagate():
            self._stats["requests"] += 1
            self._stats["propagations_received"] += 1

            if self._log_requests:
                logger.debug(f"POST {self._api_prefix}/propagate")

            # Authenticate
            if not self._authenticate(request):
                self._stats["unauthorized"] += 1
                return jsonify({"error": "Unauthorized"}), 401

            # Check if propagation is allowed
            if not self._allow_propagation:
                return jsonify({"error": "Propagation not allowed"}), 403

            # Check for propagation loop
            source = request.headers.get(self._propagation_source_header)
            if source and source == self._node_id:
                return jsonify({"success": True, "skipped": True}), 200

            # Process request
            try:
                data = request.json

                if not data or "operation" not in data:
                    return jsonify({"error": "Invalid request"}), 400

                operation = data["operation"]

                if operation == "set":
                    # Extract key and value
                    key_str = data["key"]
                    value_dict = data["value"]

                    # Create key object
                    key_obj = Key.from_string(key_str)

                    # Determine value type
                    value_type = value_dict["type"]
                    if value_type == "StringValue":
                        value_obj = StringValue.from_dict(value_dict)
                    elif value_type == "IntValue":
                        value_obj = IntValue.from_dict(value_dict)
                    elif value_type == "FloatValue":
                        value_obj = FloatValue.from_dict(value_dict)
                    elif value_type == "BytesValue":
                        value_obj = BytesValue.from_dict(value_dict)
                    elif value_type == "JsonValue":
                        value_obj = JsonValue.from_dict(value_dict)
                    elif value_type == "PickleValue":
                        value_obj = PickleValue.from_dict(value_dict)
                    else:
                        return (
                            jsonify({"error": f"Invalid value type: {value_type}"}),
                            400,
                        )

                    # Set in store
                    self._store.set(key_obj, value_obj)
                    self._stats["sets"] += 1

                elif operation == "delete":
                    # Extract key
                    key_str = data["key"]

                    # Create key object
                    key_obj = Key.from_string(key_str)

                    # Delete key
                    self._store.delete(key_obj)
                    self._stats["deletes"] += 1

                elif operation == "clear":
                    # Clear store
                    self._store.clear()
                    self._stats["clears"] += 1

                else:
                    return jsonify({"error": f"Invalid operation: {operation}"}), 400

                return jsonify({"success": True}), 200
            except Exception as e:
                logger.error(f"Error processing propagation: {e}")
                self._stats["errors"] += 1
                return jsonify({"error": str(e)}), 500

        # Health check
        @self._app.route(f"{self._api_prefix}/health", methods=["GET"])
        def health():
            return (
                jsonify(
                    {
                        "status": "ok",
                        "node_id": self._node_id,
                        "uptime": time.time() - self._stats["start_time"],
                    }
                ),
                200,
            )

        # Stats
        @self._app.route(f"{self._api_prefix}/stats", methods=["GET"])
        def stats():
            # Authenticate
            if not self._authenticate(request):
                self._stats["unauthorized"] += 1
                return jsonify({"error": "Unauthorized"}), 401

            store_stats = (
                self._store.get_stats() if hasattr(self._store, "get_stats") else {}
            )

            return (
                jsonify(
                    {
                        "node_id": self._node_id,
                        "requests": self._stats["requests"],
                        "propagations_received": self._stats["propagations_received"],
                        "gets": self._stats["gets"],
                        "sets": self._stats["sets"],
                        "deletes": self._stats["deletes"],
                        "clears": self._stats["clears"],
                        "errors": self._stats["errors"],
                        "unauthorized": self._stats["unauthorized"],
                        "uptime": time.time() - self._stats["start_time"],
                        "store": store_stats,
                    }
                ),
                200,
            )

    def _authenticate(self, req) -> bool:
        """
        Authenticate a request.

        Args:
            req: Flask request object

        Returns:
            True if authenticated, False otherwise
        """
        if self._auth_token is None:
            return True

        auth_header = req.headers.get("Authorization")
        if auth_header and auth_header.startswith("Bearer "):
            token = auth_header.split(" ")[1]
            return token == self._auth_token

        return False

    def start(self) -> None:
        """Start the server in a background thread."""
        if self._running:
            logger.warning("Server already running")
            return

        self._running = True
        self._server_thread = threading.Thread(
            target=self._app.run,
            kwargs={
                "host": self._host,
                "port": self._port,
                "debug": False,
                "use_reloader": False,
            },
            daemon=True,
        )
        self._server_thread.start()

        logger.info(f"Started server on {self._host}:{self._port}")

    def shutdown(self) -> None:
        """Shutdown the server."""
        self._running = False

        # Flask doesn't have a clean shutdown mechanism when running in a thread
        # So we'll just let the daemon thread terminate when the process exits

        logger.info("Server shutdown initiated")

    def get_url(self) -> str:
        """
        Get the server URL.

        Returns:
            URL of the server
        """
        return f"http://{self._host}:{self._port}"

    def stats(self) -> Dict[str, Any]:
        """
        Get statistics about the server.

        Returns:
            Dictionary of statistics
        """
        return {
            "node_id": self._node_id,
            "host": self._host,
            "port": self._port,
            "api_prefix": self._api_prefix,
            "running": self._running,
            "allow_propagation": self._allow_propagation,
            "requests": self._stats["requests"],
            "propagations_received": self._stats["propagations_received"],
            "gets": self._stats["gets"],
            "sets": self._stats["sets"],
            "deletes": self._stats["deletes"],
            "clears": self._stats["clears"],
            "errors": self._stats["errors"],
            "unauthorized": self._stats["unauthorized"],
            "uptime": time.time() - self._stats["start_time"],
        }
