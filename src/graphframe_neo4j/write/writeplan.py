"""
WritePlan: Represents a write operation that can be previewed and committed.
"""

from typing import Any, Dict, Optional, Union, Tuple
from ..graph import Graph
from ..util.typing import WriteStats
from ..util.errors import WriteError
from .upsert import UpsertCompiler

class WritePlan:
    """
    Represents a write operation that can be previewed and committed.
    
    Args:
        graph: Graph instance
        operation_type: Type of write operation
        target: Target label/type
        data: Data to write
        *args: Additional arguments for the operation
        **kwargs: Additional keyword arguments
    """
    
    def __init__(self, graph: Graph, operation_type: str, target: str, *args: Any, **kwargs: Any):
        self._graph = graph
        self._operation_type = operation_type
        self._target = target
        self._args = args
        self._kwargs = kwargs
        self._compiled: Optional[Dict[str, Any]] = None
        self._stats: Optional[WriteStats] = None
        self._upsert_compiler = UpsertCompiler(graph)
    
    def __repr__(self) -> str:
        return f"<WritePlan {self._operation_type} {self._target}>"
    
    def compile(self) -> Dict[str, Any]:
        """Compile the write operation to Cypher and parameters."""
        if self._compiled is None:
            operation_type = self._operation_type
            target = self._target
            
            if operation_type == "upsert":
                # Node upsert: compile_node_upsert(label, data, key, **kwargs)
                if len(self._args) >= 2:
                    data = self._args[0]
                    key = self._args[1]
                    kwargs = self._kwargs
                    patch = kwargs.get("patch", False)
                    null_policy = kwargs.get("null_policy", "set_nulls" if not patch else "ignore_nulls")
                    
                    self._compiled = self._upsert_compiler.compile_node_upsert(
                        label=target,
                        data=data,
                        key=key,
                        patch=patch,
                        null_policy=null_policy
                    )
                else:
                    self._compiled = {
                        "cypher": f"// Upsert {target} - insufficient arguments",
                        "params": {}
                    }
            
            elif operation_type == "relationship_upsert":
                # Relationship upsert: compile_relationship_upsert(rel_type, data, src, dst, rel_key, **kwargs)
                if len(self._args) >= 4:
                    data = self._args[0]
                    src = self._args[1]
                    dst = self._args[2]
                    rel_key = self._args[3] if len(self._args) > 3 else None
                    kwargs = self._kwargs
                    patch = kwargs.get("patch", False)
                    null_policy = kwargs.get("null_policy", "set_nulls" if not patch else "ignore_nulls")
                    
                    self._compiled = self._upsert_compiler.compile_relationship_upsert(
                        rel_type=target,
                        data=data,
                        src=src,
                        dst=dst,
                        rel_key=rel_key,
                        patch=patch,
                        null_policy=null_policy
                    )
                else:
                    self._compiled = {
                        "cypher": f"// Relationship upsert {target} - insufficient arguments",
                        "params": {}
                    }
            
            elif operation_type == "patch":
                # Node patch: compile_node_update with patch semantics
                if len(self._args) >= 1:
                    updates = self._args[0]
                    where_conditions = self._kwargs.get("where_conditions")
                    null_policy = self._kwargs.get("null_policy", "ignore_nulls")
                    
                    # Parse where conditions if they're in kwargs format (only if where_kwargs is not empty)
                    if "where_kwargs" in self._kwargs and self._kwargs["where_kwargs"]:
                        where_kwargs = self._kwargs["where_kwargs"]
                        where_conditions = self._upsert_compiler._compiler.parse_filter_kwargs(where_kwargs)
                    
                    # Ensure where_conditions is a list even if it's None
                    if where_conditions is None:
                        where_conditions = []
                    
                    self._compiled = self._upsert_compiler.compile_node_update(
                        label=target,
                        updates=updates,
                        where_conditions=where_conditions,
                        null_policy=null_policy
                    )
                else:
                    self._compiled = {
                        "cypher": f"// patch {target} - insufficient arguments",
                        "params": {}
                    }
            elif operation_type == "update":
                # Node update: compile_node_update(label, updates, where_conditions)
                if len(self._args) >= 1:
                    updates = self._args[0]
                    where_conditions = self._kwargs.get("where_conditions")
                    null_policy = self._kwargs.get("null_policy", "ignore_nulls")
                    
                    # Parse where conditions if they're in kwargs format (only if where_kwargs is not empty)
                    if "where_kwargs" in self._kwargs and self._kwargs["where_kwargs"]:
                        where_kwargs = self._kwargs["where_kwargs"]
                        where_conditions = self._upsert_compiler._compiler.parse_filter_kwargs(where_kwargs)
                    
                    # Ensure where_conditions is a list even if it's None
                    if where_conditions is None:
                        where_conditions = []
                    
                    self._compiled = self._upsert_compiler.compile_node_update(
                        label=target,
                        updates=updates,
                        where_conditions=where_conditions,
                        null_policy=null_policy
                    )
                else:
                    self._compiled = {
                        "cypher": f"// Update {target} - insufficient arguments",
                        "params": {}
                    }
            
            elif operation_type == "relationship_update":
                # Relationship update: compile_relationship_update(rel_type, updates, where_conditions)
                if len(self._args) >= 1:
                    updates = self._args[0]
                    where_conditions = self._kwargs.get("where_conditions")
                    null_policy = self._kwargs.get("null_policy", "ignore_nulls")
                    
                    # Parse where conditions if they're in kwargs format (only if where_kwargs is not empty)
                    if "where_kwargs" in self._kwargs and self._kwargs["where_kwargs"]:
                        where_kwargs = self._kwargs["where_kwargs"]
                        where_conditions = self._upsert_compiler._compiler.parse_filter_kwargs(where_kwargs)
                    
                    self._compiled = self._upsert_compiler.compile_relationship_update(
                        rel_type=target,
                        updates=updates,
                        where_conditions=where_conditions,
                        null_policy=null_policy
                    )
                else:
                    self._compiled = {
                        "cypher": f"// Relationship update {target} - insufficient arguments",
                        "params": {}
                    }
            elif operation_type == "relationship_delete":
                # Relationship delete: compile_relationship_delete(rel_type, where_conditions)
                where_conditions = self._kwargs.get("where_conditions")
                
                # Parse where conditions if they're in kwargs format (only if where_kwargs is not empty)
                if "where_kwargs" in self._kwargs and self._kwargs["where_kwargs"]:
                    where_kwargs = self._kwargs["where_kwargs"]
                    where_conditions = self._upsert_compiler._compiler.parse_filter_kwargs(where_kwargs)
                
                self._compiled = self._upsert_compiler.compile_relationship_delete(
                    rel_type=target,
                    where_conditions=where_conditions
                )
            elif operation_type == "delete":
                # Node delete: compile_node_delete(label, where_conditions, detach)
                where_conditions = self._kwargs.get("where_conditions")
                detach = self._kwargs.get("detach", False)
                
                # Parse where conditions if they're in kwargs format (only if where_kwargs is not empty)
                if "where_kwargs" in self._kwargs and self._kwargs["where_kwargs"]:
                    where_kwargs = self._kwargs["where_kwargs"]
                    where_conditions = self._upsert_compiler._compiler.parse_filter_kwargs(where_kwargs)
                
                self._compiled = self._upsert_compiler.compile_node_delete(
                    label=target,
                    where_conditions=where_conditions,
                    detach=detach
                )
            
            elif operation_type == "ensure_unique":
                # Ensure unique constraint: CREATE CONSTRAINT IF NOT EXISTS
                if len(self._args) >= 1:
                    property = self._args[0]
                    label = target  # target is the label
                    
                    cypher = f"""
                    CREATE CONSTRAINT IF NOT EXISTS constraint_{label}_{property} 
                    FOR (n:{label}) REQUIRE n.{property} IS UNIQUE
                    """
                    
                    self._compiled = {
                        "cypher": cypher.strip(),
                        "params": {}
                    }
                else:
                    self._compiled = {
                        "cypher": f"// ensure_unique {target} - insufficient arguments",
                        "params": {}
                    }
            elif operation_type == "ensure_node_key":
                # Ensure node key constraint: CREATE CONSTRAINT IF NOT EXISTS
                if len(self._args) >= 1:
                    properties = self._args[0]
                    label = target  # target is the label
                    
                    # Normalize properties to list
                    if isinstance(properties, str):
                        properties = [properties]
                    
                    # Create constraint name
                    prop_list = "_".join(properties)
                    constraint_name = f"constraint_{label}_{prop_list}"
                    
                    # Build Cypher
                    prop_list_cypher = ", ".join([f"n.{prop}" for prop in properties])
                    cypher = f"""
                    CREATE CONSTRAINT IF NOT EXISTS {constraint_name} 
                    FOR (n:{label}) REQUIRE ({prop_list_cypher}) IS NODE KEY
                    """
                    
                    self._compiled = {
                        "cypher": cypher.strip(),
                        "params": {}
                    }
                else:
                    self._compiled = {
                        "cypher": f"// ensure_node_key {target} - insufficient arguments",
                        "params": {}
                    }
            elif operation_type == "ensure_index":
                # Ensure index: CREATE INDEX IF NOT EXISTS
                if len(self._args) >= 1:
                    property = self._args[0]
                    kwargs = self._args[1] if len(self._args) > 1 else {}
                    label = target  # target is the label
                    
                    # Build index name
                    index_name = f"index_{label}_{property}"
                    
                    cypher = f"""
                    CREATE INDEX IF NOT EXISTS {index_name} 
                    FOR (n:{label}) ON (n.{property})
                    """
                    
                    self._compiled = {
                        "cypher": cypher.strip(),
                        "params": {}
                    }
                else:
                    self._compiled = {
                        "cypher": f"// ensure_index {target} - insufficient arguments",
                        "params": {}
                    }
            elif operation_type == "drop_unique":
                # Drop unique constraint: DROP CONSTRAINT IF EXISTS
                if len(self._args) >= 1:
                    property = self._args[0]
                    label = target  # target is the label
                    
                    constraint_name = f"constraint_{label}_{property}"
                    
                    cypher = f"""
                    DROP CONSTRAINT IF EXISTS {constraint_name}
                    """
                    
                    self._compiled = {
                        "cypher": cypher.strip(),
                        "params": {}
                    }
                else:
                    self._compiled = {
                        "cypher": f"// drop_unique {target} - insufficient arguments",
                        "params": {}
                    }
            elif operation_type == "drop_index":
                # Drop index: DROP INDEX IF EXISTS
                if len(self._args) >= 1:
                    property = self._args[0]
                    label = target  # target is the label
                    
                    index_name = f"index_{label}_{property}"
                    
                    cypher = f"""
                    DROP INDEX IF EXISTS {index_name}
                    """
                    
                    self._compiled = {
                        "cypher": cypher.strip(),
                        "params": {}
                    }
                else:
                    self._compiled = {
                        "cypher": f"// drop_index {target} - insufficient arguments",
                        "params": {}
                    }
            else:
                # Fallback for other operation types
                self._compiled = {
                    "cypher": f"// {operation_type} {target}",
                    "params": {}
                }
        
        return self._compiled
    
    def preview(self) -> Dict[str, Any]:
        """Preview what would be executed (same as compile for now)."""
        return self.compile()
    
    def commit(self) -> WriteStats:
        """Execute the write operation and return statistics."""
        self.compile()  # Ensure compilation happens
        
        if self._compiled is None:
            raise WriteError("WritePlan has not been compiled successfully")
        
        if not self._graph:
            raise WriteError("WritePlan is not associated with a Graph instance")
        
        try:
            # Execute the compiled Cypher query
            cypher = self._compiled["cypher"]
            params = self._compiled["params"]
            
            # Skip execution for comment-like queries (test/unsupported operations)
            if cypher.strip().startswith("//"):
                self._stats = {
                    "nodes_created": 0,
                    "nodes_updated": 0,
                    "relationships_created": 0,
                    "rows_affected": 0,
                    "operation_type": self._operation_type,
                    "target": self._target,
                    "status": "skipped"  # Mark as skipped for test/unsupported operations
                }
                return self._stats
            
            # Use the graph's cypher method to execute the write operation
            result = self._graph.cypher(cypher, **params)
            
            # Parse statistics from the result
            # For now, return basic statistics - this will be enhanced with actual Neo4j summary counters
            self._stats = {
                "nodes_created": len(result) if result else 0,
                "nodes_updated": 0,  # Will be enhanced with actual counters
                "relationships_created": 0,  # Will be enhanced with actual counters
                "rows_affected": len(result) if result else 0,
                "operation_type": self._operation_type,
                "target": self._target
            }
            
            return self._stats
            
        except Exception as e:
            # Wrap any execution errors with our custom exception
            raise WriteError(f"Failed to execute write operation: {str(e)}") from e
    
    def explain(self) -> Dict[str, Any]:
        """Get execution plan (best effort)."""
        # TODO: Implement explain functionality
        return {"plan": "Not implemented yet"}
    
    def profile(self) -> Dict[str, Any]:
        """Get profiling information (best effort)."""
        # TODO: Implement profile functionality
        return {"profile": "Not implemented yet"}
