"""
Advanced update operations for GraphFrame-Neo4j.
"""

from typing import Any, Dict, List, Optional, Union, Tuple
from .upsert import UpsertCompiler
from ..util.errors import WriteError
from ..frames.compiler import QueryCompiler


class AdvancedUpdateCompiler:
    """
    Compiles advanced update operations into Cypher queries.
    """
    
    def __init__(self, graph: Optional[Any] = None):
        self._compiler = QueryCompiler()
        self._graph = graph
    
    def compile_inc_update(
        self,
        label: str,
        field: str,
        value: Union[int, float],
        where_conditions: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Compile an increment update operation.
        
        Args:
            label: Node label
            field: Field to increment
            value: Amount to increment by
            where_conditions: Conditions for which nodes to update
        """
        # Build WHERE clause
        where_clause = ""
        params = {}
        
        if where_conditions:
            where_parts = []
            for condition in where_conditions:
                field_name = condition.get("field", "")
                op = condition.get("op", "eq")
                cond_value = condition.get("value")
                
                if field_name:
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
                        where_parts.append(f"n.{field_name} {cypher_op}")
                    elif op in ("in", "not_in"):
                        param_name = f"where_{len(where_parts)}"
                        params[param_name] = cond_value
                        where_parts.append(f"n.{field_name} {cypher_op} ${param_name}")
                    else:
                        param_name = f"where_{len(where_parts)}"
                        params[param_name] = cond_value
                        where_parts.append(f"n.{field_name} {cypher_op} ${param_name}")
            
            if where_parts:
                where_clause = "WHERE " + " AND ".join(where_parts)
        
        # Build SET clause with coalesce for increment
        inc_param = f"inc_{len(params)}"
        params[inc_param] = value
        set_clause = f"SET n.{field} = coalesce(n.{field}, 0) + ${inc_param}"
        
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
    
    def compile_unset_update(
        self,
        label: str,
        field: str,
        where_conditions: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Compile an unset (remove property) update operation.
        
        Args:
            label: Node label
            field: Field to unset/remove
            where_conditions: Conditions for which nodes to update
        """
        # Build WHERE clause
        where_clause = ""
        params = {}
        
        if where_conditions:
            where_parts = []
            for condition in where_conditions:
                field_name = condition.get("field", "")
                op = condition.get("op", "eq")
                cond_value = condition.get("value")
                
                if field_name:
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
                        where_parts.append(f"n.{field_name} {cypher_op}")
                    elif op in ("in", "not_in"):
                        param_name = f"where_{len(where_parts)}"
                        params[param_name] = cond_value
                        where_parts.append(f"n.{field_name} {cypher_op} ${param_name}")
                    else:
                        param_name = f"where_{len(where_parts)}"
                        params[param_name] = cond_value
                        where_parts.append(f"n.{field_name} {cypher_op} ${param_name}")
            
            if where_parts:
                where_clause = "WHERE " + " AND ".join(where_parts)
        
        # Build REMOVE clause
        remove_clause = f"REMOVE n.{field}"
        
        # Build Cypher query
        cypher = f"""
        MATCH (n:{label})
        {where_clause}
        {remove_clause}
        """
        
        return {
            "cypher": cypher.strip(),
            "params": params
        }
    
    def compile_list_append(
        self,
        label: str,
        field: str,
        value: Any,
        where_conditions: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Compile a list append operation.
        
        Args:
            label: Node label
            field: List field to append to
            value: Value to append
            where_conditions: Conditions for which nodes to update
        """
        # Build WHERE clause
        where_clause = ""
        params = {}
        
        if where_conditions:
            where_parts = []
            for condition in where_conditions:
                field_name = condition.get("field", "")
                op = condition.get("op", "eq")
                cond_value = condition.get("value")
                
                if field_name:
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
                        where_parts.append(f"n.{field_name} {cypher_op}")
                    elif op in ("in", "not_in"):
                        param_name = f"where_{len(where_parts)}"
                        params[param_name] = cond_value
                        where_parts.append(f"n.{field_name} {cypher_op} ${param_name}")
                    else:
                        param_name = f"where_{len(where_parts)}"
                        params[param_name] = cond_value
                        where_parts.append(f"n.{field_name} {cypher_op} ${param_name}")
            
            if where_parts:
                where_clause = "WHERE " + " AND ".join(where_parts)
        
        # Build SET clause with list append
        list_param = f"list_{len(params)}"
        params[list_param] = value
        set_clause = f"SET n.{field} = coalesce(n.{field}, []) + ${list_param}"
        
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
    
    def compile_list_remove(
        self,
        label: str,
        field: str,
        value: Any,
        where_conditions: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Compile a list remove operation.
        
        Args:
            label: Node label
            field: List field to remove from
            value: Value to remove
            where_conditions: Conditions for which nodes to update
        """
        # Build WHERE clause
        where_clause = ""
        params = {}
        
        if where_conditions:
            where_parts = []
            for condition in where_conditions:
                field_name = condition.get("field", "")
                op = condition.get("op", "eq")
                cond_value = condition.get("value")
                
                if field_name:
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
                        where_parts.append(f"n.{field_name} {cypher_op}")
                    elif op in ("in", "not_in"):
                        param_name = f"where_{len(where_parts)}"
                        params[param_name] = cond_value
                        where_parts.append(f"n.{field_name} {cypher_op} ${param_name}")
                    else:
                        param_name = f"where_{len(where_parts)}"
                        params[param_name] = cond_value
                        where_parts.append(f"n.{field_name} {cypher_op} ${param_name}")
            
            if where_parts:
                where_clause = "WHERE " + " AND ".join(where_parts)
        
        # Build SET clause with list remove
        list_param = f"list_{len(params)}"
        params[list_param] = value
        set_clause = f"SET n.{field} = [x IN coalesce(n.{field}, []) WHERE x <> ${list_param}]"
        
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
    
    def compile_map_merge(
        self,
        label: str,
        field: str,
        map_data: Dict[str, Any],
        where_conditions: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Compile a map merge operation.
        
        Args:
            label: Node label
            field: Map field to merge into
            map_data: Dictionary to merge
            where_conditions: Conditions for which nodes to update
        """
        # Build WHERE clause
        where_clause = ""
        params = {}
        
        if where_conditions:
            where_parts = []
            for condition in where_conditions:
                field_name = condition.get("field", "")
                op = condition.get("op", "eq")
                cond_value = condition.get("value")
                
                if field_name:
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
                        where_parts.append(f"n.{field_name} {cypher_op}")
                    elif op in ("in", "not_in"):
                        param_name = f"where_{len(where_parts)}"
                        params[param_name] = cond_value
                        where_parts.append(f"n.{field_name} {cypher_op} ${param_name}")
                    else:
                        param_name = f"where_{len(where_parts)}"
                        params[param_name] = cond_value
                        where_parts.append(f"n.{field_name} {cypher_op} ${param_name}")
            
            if where_parts:
                where_clause = "WHERE " + " AND ".join(where_parts)
        
        # Build SET clause with map merge
        map_param = f"map_{len(params)}"
        params[map_param] = map_data
        set_clause = f"SET n.{field} += ${map_param}"
        
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
