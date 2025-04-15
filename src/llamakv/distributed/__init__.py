"""
Distributed module for LlamaKV.

This module provides distributed operation capabilities:
- DistributedClient: Client for connecting to a distributed key-value store
- DistributedServer: Server for hosting a distributed key-value store
"""

from llamakv.distributed.client import DistributedClient
from llamakv.distributed.server import DistributedServer

__all__ = ["DistributedClient", "DistributedServer"]
