"""
Query compiler for converting frame operations to Cypher queries.
"""

from typing import Any, Dict, List, Optional, Tuple

class QueryCompiler:
    """
    Compiles frame operations into Cypher queries and parameters.
    """
    
    def __init__(self):
        self._clauses: List[str] = []
        self._params: Dict[str, Any] = {}
        self._param_counter = 0
    
    def _add_clause(self, clause: str) -> None:
        """Add a clause to the query."""
        if clause:
            self._clauses.append(clause)
    
    def _add_param(self, value: Any) -> str:
        """Add a parameter and return its parameter name."""
        param_name = f"param_{self._param_counter}"
        self._param_counter += 1
        self._params[param_name] = value
        return param_name
    
    def _compile_where_clause(self, conditions: List[Dict[str, Any]]) -> str:
        """Compile WHERE conditions into Cypher."""
        if not conditions:
            return ""
        
        where_parts = []
        for condition in conditions:
            field = condition.get("field", "")
            op = condition.get("op", "eq")
            value = condition.get("value")
            
            if not field:
                continue
            
            # Handle parameterized values (skip for NULL operations)
            if op not in ("exists", "is_null", "not_null"):
                if isinstance(value, (str, int, float, bool)):
                    param_name = self._add_param(value)
                    cypher_value = f"${param_name}"
                elif isinstance(value, list):
                    # For IN operations
                    param_name = self._add_param(value)
                    cypher_value = f"${param_name}"
                else:
                    cypher_value = f"${self._add_param(value)}"
            else:
                cypher_value = ""  # Not used for NULL ops
            
            # Map Python-style ops to Cypher
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
                where_parts.append(f"n.{field} {cypher_op} {cypher_value}")
            elif op == "contains":
                where_parts.append(f"n.{field} {cypher_op} {cypher_value}")
            elif op == "regex":
                where_parts.append(f"n.{field} {cypher_op} {cypher_value}")
            else:
                where_parts.append(f"n.{field} {cypher_op} {cypher_value}")
        
        if where_parts:
            return f"WHERE {' AND '.join(where_parts)}"
        return ""
    
    def _compile_select_clause(self, fields: List[str], alias: str = "n") -> str:
        """Compile SELECT clause."""
        if not fields:
            return f"{alias}"
        
        # Handle special cases and field mapping
        compiled_fields = []
        for field in fields:
            if field.startswith(alias + "__"):
                # Already properly prefixed
                compiled_fields.append(field)
            elif field == "*":
                compiled_fields.append(f"{alias}.*")
            else:
                compiled_fields.append(f"{alias}.{field}")
        
        return ", ".join(compiled_fields)
    
    def _compile_order_by_clause(self, order_by: List[Tuple[str, str]], alias: str = "n") -> str:
        """Compile ORDER BY clause."""
        if not order_by:
            return ""
        
        order_parts = []
        for field, direction in order_by:
            direction = direction.upper() if direction else "ASC"
            if direction not in ("ASC", "DESC"):
                direction = "ASC"
            
            if field.startswith(alias + "."):
                order_parts.append(f"{field} {direction}")
            else:
                order_parts.append(f"{alias}.{field} {direction}")
        
        if order_parts:
            return f"ORDER BY {' , '.join(order_parts)}"
        return ""
    
    def _compile_limit_clause(self, limit: Optional[int]) -> str:
        """Compile LIMIT clause."""
        if limit is not None and limit >= 0:
            return f"LIMIT {limit}"
        return ""
    
    def _compile_offset_clause(self, offset: Optional[int]) -> str:
        """Compile OFFSET clause."""
        if offset is not None and offset > 0:
            return f"SKIP {offset}"
        return ""
    
    def compile_node_query(
        self, 
        label: str, 
        alias: str = "n",
        conditions: Optional[List[Dict[str, Any]]] = None,
        fields: Optional[List[str]] = None,
        order_by: Optional[List[Tuple[str, str]]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None
    ) -> Dict[str, Any]:
        """Compile a complete node query."""
        self._clauses = []
        self._params = {}
        self._param_counter = 0
        
        # MATCH clause
        match_clause = f"MATCH ({alias}:{label})"
        self._add_clause(match_clause)
        
        # WHERE clause
        where_clause = self._compile_where_clause(conditions or [])
        if where_clause:
            self._add_clause(where_clause)
        
        # RETURN clause
        select_clause = self._compile_select_clause(fields or [], alias)
        return_clause = f"RETURN {select_clause}"
        self._add_clause(return_clause)
        
        # ORDER BY clause
        order_by_clause = self._compile_order_by_clause(order_by or [], alias)
        if order_by_clause:
            self._add_clause(order_by_clause)
        
        # OFFSET clause (SKIP in Cypher)
        offset_clause = self._compile_offset_clause(offset)
        if offset_clause:
            self._add_clause(offset_clause)
        
        # LIMIT clause
        limit_clause = self._compile_limit_clause(limit)
        if limit_clause:
            self._add_clause(limit_clause)
        
        # Combine all clauses
        cypher_query = "\n".join(self._clauses)
        
        return {
            "cypher": cypher_query,
            "params": self._params
        }
    
    def compile_edge_query(
        self,
        rel_type: str,
        alias: str = "r",
        conditions: Optional[List[Dict[str, Any]]] = None,
        fields: Optional[List[str]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None
    ) -> Dict[str, Any]:
        """Compile a relationship query."""
        self._clauses = []
        self._params = {}
        self._param_counter = 0
        
        # MATCH clause for relationships
        match_clause = f"MATCH ()-[{alias}:{rel_type}]-()"
        self._add_clause(match_clause)
        
        # WHERE clause
        where_clause = self._compile_where_clause(conditions or [])
        if where_clause:
            self._add_clause(f"WHERE {where_clause}")
        
        # RETURN clause
        if fields and len(fields) > 0:
            # Select specific fields
            select_fields = []
            for field in fields:
                # Handle namespaced fields for relationships
                if field.startswith("rel__"):
                    prop_name = field[6:]  # Remove "rel__" prefix
                    select_fields.append(f"{alias}.{prop_name} AS {field}")
                else:
                    select_fields.append(f"{alias}.{field} AS {field}")
            return_clause = f"RETURN {', '.join(select_fields)}"
        else:
            # Default: return all relationship properties
            return_clause = f"RETURN {alias} AS {alias}"
        
        self._add_clause(return_clause)
        
        # LIMIT and OFFSET
        if limit is not None:
            self._add_clause(f"LIMIT {limit}")
        if offset is not None:
            self._add_clause(f"SKIP {offset}")
        
        cypher_query = "\n".join(self._clauses)
        return {
            "cypher": cypher_query,
            "params": self._params
        }
    
    def compile_traversal_query(
        self,
        from_label: str,
        from_alias: str = "from",
        rel_type: str = "",
        rel_alias: str = "rel",
        to_label: str = "",
        to_alias: str = "to",
        direction: str = "out",
        conditions: Optional[List[Dict[str, Any]]] = None,
        fields: Optional[List[str]] = None,
        order_by: Optional[List[Tuple[str, str]]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None
    ) -> Dict[str, Any]:
        """Compile a traversal query with relationship patterns."""
        self._clauses = []
        self._params = {}
        self._param_counter = 0
        
        # Determine relationship direction pattern
        direction_patterns = {
            "out": f"-{rel_alias}:{rel_type}->",
            "in": f"<-{rel_alias}:{rel_type}-",
            "both": f"-{rel_alias}:{rel_type}-"
        }
        
        dir_pattern = direction_patterns.get(direction, "->")
        
        # MATCH clause with relationship pattern
        if to_label:
            match_clause = f"MATCH ({from_alias}:{from_label}){dir_pattern}({to_alias}:{to_label})"
        else:
            match_clause = f"MATCH ({from_alias}:{from_label}){dir_pattern}({to_alias})"
        
        self._add_clause(match_clause)
        
        # WHERE clause
        where_clause = self._compile_traversal_where_clause(conditions or [], from_alias, rel_alias, to_alias)
        if where_clause:
            self._add_clause(where_clause)
        
        # RETURN clause
        select_clause = self._compile_traversal_select_clause(fields or [], from_alias, rel_alias, to_alias)
        return_clause = f"RETURN {select_clause}"
        self._add_clause(return_clause)
        
        # ORDER BY clause
        order_by_clause = self._compile_traversal_order_by_clause(order_by or [], from_alias, rel_alias, to_alias)
        if order_by_clause:
            self._add_clause(order_by_clause)
        
        # OFFSET clause (SKIP in Cypher)
        offset_clause = self._compile_offset_clause(offset)
        if offset_clause:
            self._add_clause(offset_clause)
        
        # LIMIT clause
        limit_clause = self._compile_limit_clause(limit)
        if limit_clause:
            self._add_clause(limit_clause)
        
        # Combine all clauses
        cypher_query = "\n".join(self._clauses)
        
        return {
            "cypher": cypher_query,
            "params": self._params
        }
    
    def _compile_traversal_where_clause(
        self, 
        conditions: List[Dict[str, Any]], 
        from_alias: str = "from",
        rel_alias: str = "rel", 
        to_alias: str = "to"
    ) -> str:
        """Compile WHERE conditions for traversal queries with proper namespacing."""
        if not conditions:
            return ""
        
        where_parts = []
        for condition in conditions:
            field = condition.get("field", "")
            op = condition.get("op", "eq")
            value = condition.get("value")
            
            if not field:
                continue
            
            # Handle namespaced fields (from__prop, rel__prop, to__prop)
            alias = from_alias  # Default to from alias
            
            # Check for standard namespace prefixes first
            if field.startswith("from__"):
                prop = field[len("from__"):]
                alias = from_alias
                field = prop
            elif field.startswith("rel__"):
                prop = field[len("rel__"):]
                alias = rel_alias
                field = prop
            elif field.startswith("to__"):
                prop = field[len("to__"):]
                alias = to_alias
                field = prop
            # Then check for custom alias prefixes
            elif field.startswith(f"{from_alias}__"):
                prop = field[len(f"{from_alias}__"):]
                alias = from_alias
                field = prop
            elif field.startswith(f"{rel_alias}__"):
                prop = field[len(f"{rel_alias}__"):]
                alias = rel_alias
                field = prop
            elif field.startswith(f"{to_alias}__"):
                prop = field[len(f"{to_alias}__"):]
                alias = to_alias
                field = prop
            
            # Handle parameterized values (skip for NULL operations)
            if op not in ("exists", "is_null", "not_null"):
                if isinstance(value, (str, int, float, bool)):
                    param_name = self._add_param(value)
                    cypher_value = f"${param_name}"
                elif isinstance(value, list):
                    param_name = self._add_param(value)
                    cypher_value = f"${param_name}"
                else:
                    cypher_value = f"${self._add_param(value)}"
            else:
                cypher_value = ""
            
            # Map Python-style ops to Cypher
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
                where_parts.append(f"{alias}.{field} {cypher_op}")
            elif op in ("in", "not_in"):
                where_parts.append(f"{alias}.{field} {cypher_op} {cypher_value}")
            elif op == "contains":
                where_parts.append(f"{alias}.{field} {cypher_op} {cypher_value}")
            elif op == "regex":
                where_parts.append(f"{alias}.{field} {cypher_op} {cypher_value}")
            else:
                where_parts.append(f"{alias}.{field} {cypher_op} {cypher_value}")
        
        if where_parts:
            return f"WHERE {' AND '.join(where_parts)}"
        return ""
    
    def _compile_traversal_select_clause(
        self, 
        fields: List[str], 
        from_alias: str = "from",
        rel_alias: str = "rel",
        to_alias: str = "to"
    ) -> str:
        """Compile SELECT clause for traversal queries with namespacing."""
        if not fields:
            # Return all aliases by default
            return f"{from_alias}, {rel_alias}, {to_alias}"
        
        # Handle special cases and field mapping
        compiled_fields = []
        for field in fields:
            if field == "*":
                compiled_fields.append(f"{from_alias}, {rel_alias}, {to_alias}")
            elif field.startswith(f"{from_alias}__"):
                prop = field[len(f"{from_alias}__"):]
                compiled_fields.append(f"{from_alias}.{prop}")
            elif field.startswith(f"{rel_alias}__"):
                prop = field[len(f"{rel_alias}__"):]
                compiled_fields.append(f"{rel_alias}.{prop}")
            elif field.startswith(f"{to_alias}__"):
                prop = field[len(f"{to_alias}__"):]
                compiled_fields.append(f"{to_alias}.{prop}")
            elif field.startswith("from__"):
                prop = field[len("from__"):]
                compiled_fields.append(f"{from_alias}.{prop}")
            elif field.startswith("rel__"):
                prop = field[len("rel__"):]
                compiled_fields.append(f"{rel_alias}.{prop}")
            elif field.startswith("to__"):
                prop = field[len("to__"):]
                compiled_fields.append(f"{to_alias}.{prop}")
            else:
                # Default to 'from' alias if no prefix
                compiled_fields.append(f"{from_alias}.{field}")
        
        return ", ".join(compiled_fields)
    
    def _compile_traversal_order_by_clause(
        self, 
        order_by: List[Tuple[str, str]], 
        from_alias: str = "from",
        rel_alias: str = "rel",
        to_alias: str = "to"
    ) -> str:
        """Compile ORDER BY clause for traversal queries."""
        if not order_by:
            return ""
        
        order_parts = []
        for field, direction in order_by:
            direction = direction.upper() if direction else "ASC"
            if direction not in ("ASC", "DESC"):
                direction = "ASC"
            
            # Handle namespaced fields
            alias = from_alias
            if field.startswith(f"{from_alias}__"):
                prop = field[len(f"{from_alias}__"):]
                alias = from_alias
                field = prop
            elif field.startswith(f"{rel_alias}__"):
                prop = field[len(f"{rel_alias}__"):]
                alias = rel_alias
                field = prop
            elif field.startswith(f"{to_alias}__"):
                prop = field[len(f"{to_alias}__"):]
                alias = to_alias
                field = prop
            elif field.startswith("from__"):
                prop = field[len("from__"):]
                alias = from_alias
                field = prop
            elif field.startswith("rel__"):
                prop = field[len("rel__"):]
                alias = rel_alias
                field = prop
            elif field.startswith("to__"):
                prop = field[len("to__"):]
                alias = to_alias
                field = prop
            
            order_parts.append(f"{alias}.{field} {direction}")
        
        if order_parts:
            return f"ORDER BY {' , '.join(order_parts)}"
        return ""
    
    def compile_back_query(
        self,
        from_label: str,
        from_alias: str = "from",
        rel_type: str = "",
        rel_alias: str = "rel",
        to_label: str = "",
        to_alias: str = "to",
        direction: str = "out",
        back_conditions: Optional[List[Dict[str, Any]]] = None,
        back_fields: Optional[List[str]] = None,
        back_order_by: Optional[List[Tuple[str, str]]] = None,
        back_limit: Optional[int] = None,
        back_offset: Optional[int] = None
    ) -> Dict[str, Any]:
        """Compile a back() query that returns to originating nodes after filtering."""
        self._clauses = []
        self._params = {}
        self._param_counter = 0
        
        # Determine relationship direction pattern
        direction_patterns = {
            "out": f"-{rel_alias}:{rel_type}->",
            "in": f"<-{rel_alias}:{rel_type}-",
            "both": f"-{rel_alias}:{rel_type}-"
        }
        
        dir_pattern = direction_patterns.get(direction, "->")
        
        # MATCH clause with relationship pattern and path
        if to_label:
            match_clause = f"MATCH path = ({from_alias}:{from_label}){dir_pattern}({to_alias}:{to_label})"
        else:
            match_clause = f"MATCH path = ({from_alias}:{from_label}){dir_pattern}({to_alias})"
        
        self._add_clause(match_clause)
        
        # WHERE clause for traversal conditions
        where_clause = self._compile_traversal_where_clause(back_conditions or [], from_alias, rel_alias, to_alias)
        if where_clause:
            self._add_clause(where_clause)
        
        # WITH clause to filter the path
        self._add_clause("WITH from")
        
        # RETURN clause for originating nodes - handle namespaced fields from traversal
        if back_fields:
            # Convert namespaced fields back to proper field references
            compiled_fields = []
            for field in back_fields:
                if field.startswith("from__"):
                    prop = field[len("from__"):]
                    compiled_fields.append(f"from.{prop}")
                elif field.startswith("rel__"):
                    prop = field[len("rel__"):]
                    compiled_fields.append(f"rel.{prop}")
                elif field.startswith("to__"):
                    prop = field[len("to__"):]
                    compiled_fields.append(f"to.{prop}")
                else:
                    # Default to from alias
                    compiled_fields.append(f"from.{field}")
            select_clause = ", ".join(compiled_fields)
        else:
            # Return the from node by default
            select_clause = "from"
        
        return_clause = f"RETURN {select_clause}"
        self._add_clause(return_clause)
        
        # ORDER BY clause - handle namespaced fields
        if back_order_by:
            order_parts = []
            for field, direction in back_order_by:
                direction = direction.upper() if direction else "ASC"
                if direction not in ("ASC", "DESC"):
                    direction = "ASC"
                
                # Handle namespaced fields in order by
                if field.startswith("from__"):
                    prop = field[len("from__"):]
                    order_parts.append(f"from.{prop} {direction}")
                elif field.startswith("rel__"):
                    prop = field[len("rel__"):]
                    order_parts.append(f"rel.{prop} {direction}")
                elif field.startswith("to__"):
                    prop = field[len("to__"):]
                    order_parts.append(f"to.{prop} {direction}")
                else:
                    # Default to from alias
                    order_parts.append(f"from.{field} {direction}")
            
            if order_parts:
                order_by_clause = f"ORDER BY {' , '.join(order_parts)}"
                self._add_clause(order_by_clause)
        
        # OFFSET clause (SKIP in Cypher)
        offset_clause = self._compile_offset_clause(back_offset)
        if offset_clause:
            self._add_clause(offset_clause)
        
        # LIMIT clause
        limit_clause = self._compile_limit_clause(back_limit)
        if limit_clause:
            self._add_clause(limit_clause)
        
        # Combine all clauses
        cypher_query = "\n".join(self._clauses)
        
        return {
            "cypher": cypher_query,
            "params": self._params
        }
    
    def parse_filter_kwargs(self, kwargs: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parse filter kwargs into conditions."""
        conditions = []
        
        for key, value in kwargs.items():
            if "__" in key:
                # Handle field__op syntax
                field_parts = key.split("__")
                
                # Check if this is a namespaced field with operation
                # Namespaced fields can be:
                # - Standard: from__prop, rel__prop, to__prop
                # - With operation: from__prop__op, rel__prop__op, to__prop__op
                # - Custom aliases: p__prop, r__prop, c__prop (when using custom aliases)
                
                if len(field_parts) >= 2:
                    # Check if this looks like a namespaced field
                    # A namespaced field has at least 2 parts and the operation (if present) is a known operation
                    potential_op = field_parts[-1] if len(field_parts) > 2 else None
                    known_ops = ["eq", "ne", "lt", "lte", "gt", "gte", "in", "not_in", "contains", "startswith", "endswith", "regex", "exists", "is_null", "not_null"]
                    
                    if len(field_parts) == 2:
                        # This could be either namespaced field (namespace__prop) or field__op
                        # If the second part is a known operation, it's field__op
                        if field_parts[1] in known_ops:
                            field = field_parts[0]
                            op = field_parts[1]
                        else:
                            # This is namespaced field
                            field = key
                            op = "eq"
                    elif len(field_parts) >= 3:
                        # Check if the last part is a known operation
                        if potential_op in known_ops:
                            # This is namespaced field with operation: namespace__prop__op
                            namespace_parts = field_parts[:-1]
                            field = "__".join(namespace_parts)
                            op = potential_op
                        else:
                            # This is field__op syntax where op contains underscores
                            field = field_parts[0]
                            op = "__".join(field_parts[1:])
                    else:
                        # This is just the field itself
                        field = field_parts[0]
                        op = "eq"
                else:
                    # Simple field
                    field = field_parts[0]
                    op = "eq"
                
                conditions.append({
                    "field": field,
                    "op": op,
                    "value": value
                })
            else:
                # Simple equality
                conditions.append({
                    "field": key,
                    "op": "eq",
                    "value": value
                })
        
        return conditions
