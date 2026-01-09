"""
Custom exceptions for GraphFrame-Neo4j.
"""

class GraphFrameError(Exception):
    """Base exception for all GraphFrame-Neo4j errors."""
    pass

class ConnectionError(GraphFrameError):
    """Error related to Neo4j connection."""
    pass

class QueryError(GraphFrameError):
    """Error related to query compilation or execution."""
    pass

class WriteError(GraphFrameError):
    """Error related to write operations."""
    pass

class SchemaError(GraphFrameError):
    """Error related to schema operations."""
    pass

class IdempotencyError(GraphFrameError):
    """Error related to idempotency guarantees."""
    pass

class ValidationError(GraphFrameError):
    """Error related to input validation."""
    pass
