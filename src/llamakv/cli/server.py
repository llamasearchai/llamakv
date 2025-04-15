#!/usr/bin/env python
"""
Command-line interface for LlamaKV server.

This module provides a command-line interface for starting a LlamaKV server.
"""

import argparse
import logging
import os
import signal
import sys
import time

from llamakv.core.store import KVStore
from llamakv.distributed import DistributedServer
from llamakv.persistence import FileBackend, MemoryBackend, SQLiteBackend


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Start a LlamaKV server.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument("--host", default="0.0.0.0", help="Host to bind the server to")

    parser.add_argument(
        "--port", type=int, default=8080, help="Port to bind the server to"
    )

    parser.add_argument(
        "--node-id",
        default=None,
        help="Unique ID for this node (defaults to auto-generated)",
    )

    parser.add_argument(
        "--api-prefix", default="/api/v1", help="Prefix for API endpoints"
    )

    parser.add_argument(
        "--auth-token", default=None, help="Authentication token for API requests"
    )

    parser.add_argument(
        "--no-propagation",
        action="store_true",
        help="Disable propagation from other nodes",
    )

    # Storage options
    storage_group = parser.add_argument_group("Storage Options")

    storage_type = storage_group.add_mutually_exclusive_group(required=True)
    storage_type.add_argument(
        "--memory",
        action="store_true",
        help="Use in-memory storage (data will be lost when server stops)",
    )

    storage_type.add_argument(
        "--file", metavar="PATH", help="Use file-based storage (JSON file)"
    )

    storage_type.add_argument(
        "--sqlite", metavar="PATH", help="Use SQLite-based storage"
    )

    # File backend options
    file_group = parser.add_argument_group("File Backend Options")
    file_group.add_argument(
        "--sync-interval",
        type=int,
        default=5,
        help="Interval for syncing to disk (seconds) for file backend",
    )

    # SQLite backend options
    sqlite_group = parser.add_argument_group("SQLite Backend Options")
    sqlite_group.add_argument(
        "--auto-vacuum",
        action="store_true",
        help="Enable auto-vacuuming for SQLite backend",
    )
    sqlite_group.add_argument(
        "--commit-interval",
        type=int,
        default=5,
        help="Interval for committing to SQLite (seconds)",
    )

    # Logging options
    logging_group = parser.add_argument_group("Logging Options")
    logging_group.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Set the logging level",
    )

    logging_group.add_argument("--log-file", help="Log to a file instead of stdout")

    logging_group.add_argument(
        "--log-requests", action="store_true", help="Log all API requests"
    )

    return parser.parse_args()


def setup_logging(args):
    """Set up logging configuration."""
    log_level = getattr(logging, args.log_level)

    logging_config = {
        "level": log_level,
        "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    }

    if args.log_file:
        logging_config["filename"] = args.log_file

    logging.basicConfig(**logging_config)


def create_backend(args):
    """Create a persistence backend based on command-line arguments."""
    if args.memory:
        return MemoryBackend()
    elif args.file:
        # Ensure directory exists
        os.makedirs(os.path.dirname(os.path.abspath(args.file)), exist_ok=True)
        return FileBackend(args.file, sync_interval=args.sync_interval)
    elif args.sqlite:
        # Ensure directory exists
        os.makedirs(os.path.dirname(os.path.abspath(args.sqlite)), exist_ok=True)
        return SQLiteBackend(
            args.sqlite,
            auto_vacuum=args.auto_vacuum,
            auto_commit_interval=args.commit_interval,
        )


def handle_signals(server):
    """Set up signal handlers for graceful shutdown."""

    def signal_handler(signum, frame):
        logging.info(f"Received signal {signum}, shutting down...")
        server.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


def main():
    """Run the LlamaKV server CLI."""
    args = parse_args()
    setup_logging(args)

    logging.info("Starting LlamaKV server...")

    try:
        # Create backend
        backend = create_backend(args)
        logging.info(f"Using {backend.__class__.__name__}")

        # Create store
        store = KVStore(backend=backend)

        # Create server
        server = DistributedServer(
            store=store,
            host=args.host,
            port=args.port,
            api_prefix=args.api_prefix,
            node_id=args.node_id,
            auth_token=args.auth_token,
            allow_propagation=not args.no_propagation,
            log_requests=args.log_requests,
        )

        # Handle signals
        handle_signals(server)

        # Start server
        server.start()
        logging.info(f"Server started at http://{args.host}:{args.port}")

        # Keep main thread alive
        while True:
            time.sleep(1)

    except Exception as e:
        logging.error(f"Error starting server: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
