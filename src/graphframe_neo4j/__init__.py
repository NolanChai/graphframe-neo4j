"""
GraphFrame-Neo4j: A pandas-like interface for Neo4j with DataFrame-style querying and idempotent upserts.

Main exports for the library.
"""

from .graph import Graph
from .frames.nodeframe import NodeFrame
from .frames.edgeframe import EdgeFrame
from .frames.pathframe import PathFrame
from .write.writeplan import WritePlan
from .schema.manager import SchemaManager

__all__ = ["Graph", "NodeFrame", "EdgeFrame", "PathFrame", "WritePlan", "SchemaManager"]
