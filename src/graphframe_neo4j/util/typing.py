"""
Type definitions and utilities for GraphFrame-Neo4j.
"""

from typing import Any, Dict, List, TypeVar, Tuple
from enum import Enum

# Basic types
NodeData = Dict[str, Any]
EdgeData = Dict[str, Any]
PathData = Dict[str, Any]
QueryParams = Dict[str, Any]

# For DataFrame-like structures
Record = Dict[str, Any]
Records = List[Record]

# For filtering
FilterOp = str  # "eq", "ne", "lt", "lte", "gt", "gte", "in", "not_in", etc.
FilterCondition = Tuple[str, FilterOp, Any]

# For traversal
Direction = str  # "out", "in", "both"
TraversalAlias = Tuple[str, str, str]  # (from_alias, rel_alias, to_alias)

# For write operations
WriteStats = Dict[str, int]

class WriteOperationType(Enum):
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"
    MERGE = "merge"

T = TypeVar('T')
