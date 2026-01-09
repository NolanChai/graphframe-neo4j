"""
Upsert functionality for nodes and relationships.
"""

from typing import Any, Dict, List, Optional, Union, Tuple
from ..util.errors import WriteError
from ..frames.compiler import QueryCompiler
from ..graph import Graph

class UpsertCompiler:
    """
    Compiles upsert operations into Cypher queries.
    """
    
    def __init__(self, graph: Optional[Graph] = None):
        self._compiler = QueryCompiler()
        self._graph = graph
    
    def compile_node_upsert(
        self,
        label: str,
        data: Union[List[Dict[str, Any]], Dict[str, Any]],
        key: Union[str, List[str]],
        patch: bool = False,
        null_policy: str = "ignore_nulls",
        batch_size: int = 1000
    ) -> Dict[str, Any]:
        """
        Compile a node upsert operation.
        
        Args:
            label: Node label
            data: Data to upsert (list of dicts or single dict)
            key: Key field(s) for uniqueness
            patch: Whether to patch existing nodes
            null_policy: How to handle null values
            batch_size: Batch size for UNWIND
        """
        # Normalize data to list
        if isinstance(data, dict):
            data = [data]
        
        if not data:
            return {"cypher": "// No data to upsert", "params": {}}
        
        # Normalize key to list
        if isinstance(key, str):
            key = [key]
        
        # Validate key fields exist in data
        for item in data:
            for key_field in key:
                if key_field not in item:
                    raise WriteError(f"Key field '{key_field}' not found in data item: {item}")
        
        # Prepare parameters
        params = {"batch": data}
        
        # Build key properties access (use map projection syntax)
        key_props = ", ".join([f"{field}: item.{field}" for field in key])
        
        # Build property assignments
        all_properties = set()
        for item in data:
            all_properties.update(item.keys())
        
        # Filter out key properties for SET clause and sort for consistent ordering
        set_properties = sorted([prop for prop in all_properties if prop not in key])
        
        # Build SET clause based on null policy (use direct property access)
        if patch:
            # For patch operations, use CASE WHEN
            set_clauses = []
            for prop in set_properties:
                if null_policy == "ignore_nulls":
                    set_clauses.append(f"SET n.{prop} = case when item.{prop} IS NOT NULL then item.{prop} else n.{prop} end")
                else:  # set_nulls
                    set_clauses.append(f"SET n.{prop} = item.{prop}")
            set_clause = "\n        ".join(set_clauses)
        elif null_policy == "ignore_nulls":
            # When ignoring nulls (but not patching), use CASE WHEN
            set_clauses = []
            for prop in set_properties:
                set_clauses.append(f"SET n.{prop} = case when item.{prop} IS NOT NULL then item.{prop} else n.{prop} end")
            set_clause = "\n        ".join(set_clauses)
        else:
            # For full upsert, set all non-key properties
            set_clauses = [f"SET n.{prop} = item.{prop}" for prop in set_properties]
            set_clause = "\n        ".join(set_clauses)
        
        # Build Cypher query
        cypher = f"""
        UNWIND $batch AS item
        MERGE (n:{label} {{{key_props}}})
        {set_clause}
        """
        
        return {
            "cypher": cypher.strip(),
            "params": params,
            "stats": {
                "nodes_processed": len(data),
                "batches": (len(data) + batch_size - 1) // batch_size
            }
        }
    
    def compile_relationship_upsert(
        self,
        rel_type: str,
        data: Union[List[Dict[str, Any]], Dict[str, Any]],
        src: Tuple[str, Union[str, List[str]]],
        dst: Tuple[str, Union[str, List[str]]],
        rel_key: Optional[Union[str, List[str]]] = None,
        patch: bool = False,
        null_policy: str = "ignore_nulls",
        batch_size: int = 1000
    ) -> Dict[str, Any]:
        """
        Compile a relationship upsert operation.
        
        Args:
            rel_type: Relationship type
            data: Relationship data to upsert
            src: (label, key) for source nodes
            dst: (label, key) for destination nodes
            rel_key: Optional key for relationship uniqueness
            patch: Whether to patch existing relationships
            null_policy: How to handle null values
            batch_size: Batch size for UNWIND
        """
        # Normalize data to list
        if isinstance(data, dict):
            data = [data]
        
        if not data:
            return {"cypher": "// No data to upsert", "params": {}}
        
        # Apply relationship uniqueness policy
        if not rel_key and self._graph:
            policy = self._graph.rel_uniqueness_policy
            if policy == "require_rel_key":
                raise WriteError(
                    "Relationship upsert requires rel_key for uniqueness when rel_uniqueness_policy='require_rel_key'. "
                    "Either provide rel_key or change the policy to 'single_edge_per_pair'."
                )
            # For "single_edge_per_pair" policy (default), we'll enforce single edge per pair
            # For "allow_multiple" policy, we would allow multiple edges but that's not recommended
        
        # Normalize keys
        src_label, src_key = src
        dst_label, dst_key = dst
        
        if isinstance(src_key, str):
            src_key = [src_key]
        if isinstance(dst_key, str):
            dst_key = [dst_key]
        if rel_key and isinstance(rel_key, str):
            rel_key = [rel_key]
        
        # Validate required fields
        for item in data:
            # Check source key fields
            for key_field in src_key:
                if key_field not in item:
                    raise WriteError(f"Source key field '{key_field}' not found in data item: {item}")
            
            # Check destination key fields
            for key_field in dst_key:
                if key_field not in item:
                    raise WriteError(f"Destination key field '{key_field}' not found in data item: {item}")
        
        # Prepare parameters
        params = {"batch": data}
        
        # Build source and destination key properties (use map projection syntax)
        src_key_props = ", ".join([f"{field}: item.{field}" for field in src_key])
        dst_key_props = ", ".join([f"{field}: item.{field}" for field in dst_key])
        
        # Build relationship key if provided (use map projection syntax)
        if rel_key:
            rel_key_props = ", ".join([f"{field}: item.{field}" for field in rel_key])
            rel_match = f"{{{rel_key_props}}}"
            rel_identifiers = ", ".join([f"item.{field}" for field in rel_key])
        else:
            rel_match = ""
            rel_identifiers = ""
        
        # Get all properties for SET clause
        all_properties = set()
        for item in data:
            all_properties.update(item.keys())
        
        # Filter out key properties and sort for consistent ordering
        key_properties = set(src_key + dst_key)
        if rel_key:
            key_properties.update(rel_key)
        
        set_properties = sorted([prop for prop in all_properties if prop not in key_properties])
        
        # Build SET clause
        if patch:
            set_clauses = []
            for prop in set_properties:
                if null_policy == "ignore_nulls":
                    set_clauses.append(f"SET r.{prop} = case when item.{prop} IS NOT NULL then item.{prop} else r.{prop} end")
                else:  # set_nulls
                    set_clauses.append(f"SET r.{prop} = item.{prop}")
            set_clause = "\n        ".join(set_clauses)
        elif null_policy == "ignore_nulls":
            # When ignoring nulls (but not patching), use CASE WHEN
            set_clauses = []
            for prop in set_properties:
                set_clauses.append(f"SET r.{prop} = case when item.{prop} IS NOT NULL then item.{prop} else r.{prop} end")
            set_clause = "\n        ".join(set_clauses)
        else:
            set_clauses = [f"SET r.{prop} = item.{prop}" for prop in set_properties]
            set_clause = "\n        ".join(set_clauses)
        
        # Build Cypher query
        if rel_key:
            cypher = f"""
            UNWIND $batch AS item
            MERGE (a:{src_label} {{{src_key_props}}})
            MERGE (b:{dst_label} {{{dst_key_props}}})
            MERGE (a)-[r:{rel_type} {rel_match}]->(b)
            {set_clause}
            """
        else:
            cypher = f"""
            UNWIND $batch AS item
            MERGE (a:{src_label} {{{src_key_props}}})
            MERGE (b:{dst_label} {{{dst_key_props}}})
            MERGE (a)-[r:{rel_type}]->(b)
            {set_clause}
            """
        
        return {
            "cypher": cypher.strip(),
            "params": params,
            "stats": {
                "relationships_processed": len(data),
                "batches": (len(data) + batch_size - 1) // batch_size
            }
        }
    
    def compile_node_update(
        self,
        label: str,
        updates: Dict[str, Any],
        where_conditions: Optional[List[Dict[str, Any]]] = None,
        null_policy: str = "ignore_nulls"
    ) -> Dict[str, Any]:
        """
        Compile a node update operation.
        
        Args:
            label: Node label
            updates: Properties to update
            where_conditions: Conditions for which nodes to update
            null_policy: How to handle null values
        """
        # Build SET clause
        params = {}
        if null_policy == "ignore_nulls":
            set_clauses = []
            for prop, value in updates.items():
                if value is not None:
                    param_name = f"param_{len(set_clauses)}"
                    set_clauses.append(f"n.{prop} = ${param_name}")
                    params[param_name] = value
        else:
            set_clauses = []
            for prop, value in updates.items():
                param_name = f"param_{len(set_clauses)}"
                set_clauses.append(f"n.{prop} = ${param_name}")
                params[param_name] = value
        
        set_clause = "SET " + ", ".join(set_clauses)
        
        # Build WHERE clause if conditions provided
        where_clause = ""
        if where_conditions:
            where_parts = []
            for condition in where_conditions:
                field = condition.get("field", "")
                op = condition.get("op", "eq")
                value = condition.get("value")
                
                if field:
                    # Map operation
                    op_mapping = {
                        "eq": "=",
                        "ne": "<>",
                        "lt": "<",
                        "lte": "<=",
                        "gt": ">",
                        "gte": ">=",
                        "in": "IN",
                        "not_in": "NOT IN",
                        "contains": "CONTAINS",
                        "startswith": "STARTS WITH",
                        "endswith": "ENDS WITH",
                        "regex": "=~"
                    }
                    cypher_op = op_mapping.get(op, "=")
                    
                    if op in ("in", "not_in"):
                        param_name = f"where_{len(where_parts)}"
                        params[param_name] = value
                        where_parts.append(f"n.{field} {cypher_op} ${param_name}")
                    else:
                        param_name = f"where_{len(where_parts)}"
                        params[param_name] = value
                        where_parts.append(f"n.{field} {cypher_op} ${param_name}")
            
            if where_parts:
                where_clause = "WHERE " + " AND ".join(where_parts)
        
        # Build Cypher query
        cypher = f"""
        MATCH (n:{label})
        {where_clause}
        {set_clause}
        """
        
        return {
            "cypher": cypher.strip(),
            "params": params
        }
    
    def compile_relationship_update(
        self,
        rel_type: str,
        updates: Dict[str, Any],
        where_conditions: Optional[List[Dict[str, Any]]] = None,
        null_policy: str = "ignore_nulls"
    ) -> Dict[str, Any]:
        """
        Compile a relationship update operation.
        
        Args:
            rel_type: Relationship type
            updates: Properties to update
            where_conditions: Conditions for which relationships to update
            null_policy: How to handle null values
        """
        # Build SET clause
        params = {}
        if null_policy == "ignore_nulls":
            set_clauses = []
            for prop, value in updates.items():
                if value is not None:
                    param_name = f"param_{len(set_clauses)}"
                    set_clauses.append(f"r.{prop} = ${param_name}")
                    params[param_name] = value
        else:
            set_clauses = []
            for prop, value in updates.items():
                param_name = f"param_{len(set_clauses)}"
                set_clauses.append(f"r.{prop} = ${param_name}")
                params[param_name] = value
        
        set_clause = "SET " + ", ".join(set_clauses)
        
        # Build WHERE clause if conditions provided
        where_clause = ""
        if where_conditions:
            where_parts = []
            for condition in where_conditions:
                field = condition.get("field", "")
                op = condition.get("op", "eq")
                value = condition.get("value")
                
                if field:
                    # Map operation
                    op_mapping = {
                        "eq": "=",
                        "ne": "<>",
                        "lt": "<",
                        "lte": "<=",
                        "gt": ">",
                        "gte": ">=",
                        "in": "IN",
                        "not_in": "NOT IN",
                        "contains": "CONTAINS",
                        "startswith": "STARTS WITH",
                        "endswith": "ENDS WITH",
                        "regex": "=~",
                        "exists": "IS NOT NULL",
                        "is_null": "IS NULL",
                        "not_null": "IS NOT NULL"
                    }
                    cypher_op = op_mapping.get(op, "=")
                    
                    if op in ("exists", "is_null", "not_null"):
                        where_parts.append(f"r.{field} {cypher_op}")
                    elif op in ("in", "not_in"):
                        param_name = f"where_{len(where_parts)}"
                        params[param_name] = value
                        where_parts.append(f"r.{field} {cypher_op} ${param_name}")
                    else:
                        param_name = f"where_{len(where_parts)}"
                        params[param_name] = value
                        where_parts.append(f"r.{field} {cypher_op} ${param_name}")
            
            if where_parts:
                where_clause = "WHERE " + " AND ".join(where_parts)
        
        # Build Cypher query
        cypher = f"""
        MATCH ()-[r:{rel_type}]->()
        {where_clause}
        {set_clause}
        """
        
        return {
            "cypher": cypher.strip(),
            "params": params
        }
    
    def compile_relationship_delete(
        self,
        rel_type: str,
        where_conditions: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Compile a relationship delete operation.
        
        Args:
            rel_type: Relationship type
            where_conditions: Conditions for which relationships to delete
        """
        # Build WHERE clause
        where_clause = ""
        params = {}
        
        if where_conditions:
            where_parts = []
            for condition in where_conditions:
                field = condition.get("field", "")
                op = condition.get("op", "eq")
                value = condition.get("value")
                
                if field:
                    # Map operation
                    op_mapping = {
                        "eq": "=",
                        "ne": "<>",
                        "lt": "<",
                        "lte": "<=",
                        "gt": ">",
                        "gte": ">=",
                        "in": "IN",
                        "not_in": "NOT IN",
                        "contains": "CONTAINS",
                        "startswith": "STARTS WITH",
                        "endswith": "ENDS WITH",
                        "regex": "=~",
                        "exists": "IS NOT NULL",
                        "is_null": "IS NULL",
                        "not_null": "IS NOT NULL"
                    }
                    cypher_op = op_mapping.get(op, "=")
                    
                    if op in ("exists", "is_null", "not_null"):
                        where_parts.append(f"r.{field} {cypher_op}")
                    elif op in ("in", "not_in"):
                        param_name = f"where_{len(where_parts)}"
                        params[param_name] = value
                        where_parts.append(f"r.{field} {cypher_op} ${param_name}")
                    else:
                        param_name = f"where_{len(where_parts)}"
                        params[param_name] = value
                        where_parts.append(f"r.{field} {cypher_op} ${param_name}")
            
            if where_parts:
                where_clause = "WHERE " + " AND ".join(where_parts)
        
        # Build Cypher query
        cypher = f"""
        MATCH ()-[r:{rel_type}]->()
        {where_clause}
        DELETE r
        """
        
        return {
            "cypher": cypher.strip(),
            "params": params
        }
    
    def compile_node_delete(
        self,
        label: str,
        where_conditions: Optional[List[Dict[str, Any]]] = None,
        detach: bool = False
    ) -> Dict[str, Any]:
        """
        Compile a node delete operation.
        
        Args:
            label: Node label
            where_conditions: Conditions for which nodes to delete
            detach: Whether to detach delete
        """
        # Build WHERE clause
        where_clause = ""
        params = {}
        
        if where_conditions:
            where_parts = []
            for condition in where_conditions:
                field = condition.get("field", "")
                op = condition.get("op", "eq")
                value = condition.get("value")
                
                if field:
                    # Map operation
                    op_mapping = {
                        "eq": "=",
                        "ne": "<>",
                        "lt": "<",
                        "lte": "<=",
                        "gt": ">",
                        "gte": ">=",
                        "in": "IN",
                        "not_in": "NOT IN",
                        "contains": "CONTAINS",
                        "startswith": "STARTS WITH",
                        "endswith": "ENDS WITH",
                        "regex": "=~",
                        "exists": "IS NOT NULL",
                        "is_null": "IS NULL",
                        "not_null": "IS NOT NULL"
                    }
                    cypher_op = op_mapping.get(op, "=")
                    
                    if op in ("exists", "is_null", "not_null"):
                        where_parts.append(f"n.{field} {cypher_op}")
                    elif op in ("in", "not_in"):
                        param_name = f"where_{len(where_parts)}"
                        params[param_name] = value
                        where_parts.append(f"n.{field} {cypher_op} ${param_name}")
                    else:
                        param_name = f"where_{len(where_parts)}"
                        params[param_name] = value
                        where_parts.append(f"n.{field} {cypher_op} ${param_name}")
            
            if where_parts:
                where_clause = "WHERE " + " AND ".join(where_parts)
        
        # Build DELETE clause
        delete_clause = "DETACH DELETE n" if detach else "DELETE n"
        
        # Build Cypher query
        cypher = f"""
        MATCH (n:{label})
        {where_clause}
        {delete_clause}
        """
        
        return {
            "cypher": cypher.strip(),
            "params": params
        }
