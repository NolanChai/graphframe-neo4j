"""
NodeFrame: DataFrame-like interface for querying Neo4j nodes.
"""

from typing import Any, Dict, List, Optional, Union, Tuple
from .baseframe import BaseFrame
from .compiler import QueryCompiler

class NodeFrame(BaseFrame):
    """
    DataFrame-like interface for querying Neo4j nodes.
    
    Args:
        graph: Graph instance
        label: Node label to query
    """
    
    def __init__(self, graph: 'Graph', label: str):
        super().__init__(graph, label)
        self._label = label
        self._alias = "n"  # Default alias for nodes
        self._compiler = QueryCompiler()
        self._traversal_info: Optional[Dict[str, Any]] = None
    
    def where(self, **kwargs: Any) -> 'NodeFrame':
        """Filter nodes by conditions."""
        conditions = self._compiler.parse_filter_kwargs(kwargs)
        self._filters.extend(conditions)
        return self
    
    def select(self, *fields: str) -> 'NodeFrame':
        """Select specific fields from nodes."""
        if fields:
            self._selected_fields = list(fields)
        return self
    
    def limit(self, limit: int) -> 'NodeFrame':
        """Limit the number of results."""
        self._limit = limit
        return self
    
    def offset(self, offset: int) -> 'NodeFrame':
        """Skip a number of results."""
        self._offset = offset
        return self
    
    def order_by(self, *fields: str) -> 'NodeFrame':
        """Order results by fields."""
        for field in fields:
            # Parse field[__desc] or field[__asc] syntax
            direction = "ASC"
            if field.endswith("__desc"):
                field = field[:-6]
                direction = "DESC"
            elif field.endswith("__asc"):
                field = field[:-5]
            
            self._order_by.append((field, direction))
        return self
    
    def to_records(self) -> List[Dict[str, Any]]:
        """Execute query and return results as records."""
        compiled = self.compile()
        cypher = compiled["cypher"]
        params = compiled["params"]
        
        def work(tx):  # type: ignore
            result = tx.run(cypher, **params)
            return [dict(record) for record in result]
        
        with self._graph.session() as session:
            return session.execute_read(work)
    
    def compile(self) -> Dict[str, Any]:
        """Compile to Cypher query and parameters."""
        # Check if this is a back() query from traversal
        if self._traversal_info and self._traversal_info.get('is_back_query'):
            traversal_info = self._traversal_info
            return self._compiler.compile_back_query(
                from_label=self._label,
                from_alias=traversal_info['from_alias'],
                rel_type=traversal_info['rel_type'],
                rel_alias=traversal_info['rel_alias'],
                to_label=traversal_info['to_label'],
                to_alias=traversal_info['to_alias'],
                direction=traversal_info['direction'],
                back_conditions=self._filters,
                back_fields=self._selected_fields,
                back_order_by=self._order_by,
                back_limit=self._limit,
                back_offset=self._offset
            )
        else:
            # Regular node query
            return self._compiler.compile_node_query(
                label=self._label,
                alias=self._alias,
                conditions=self._filters,
                fields=self._selected_fields,
                order_by=self._order_by,
                limit=self._limit,
                offset=self._offset
            )
    
    def traverse(self, rel_type: str, to: str, direction: str = "out", alias: Optional[Tuple[str, str, str]] = None) -> 'PathFrame':
        """Traverse relationships from these nodes."""
        from .pathframe import PathFrame
        # TODO: Implement traversal
        return PathFrame(self._graph, rel_type, to, direction, alias, from_label=self._label)
    
    def upsert(self, data: Union[List[Dict[str, Any]], Dict[str, Any]], key: Union[str, List[str]], **kwargs: Any) -> 'WritePlan':
        """Upsert nodes (create or update)."""
        from ..write.writeplan import WritePlan
        # Upsert logic implemented in WritePlan
        return WritePlan(self._graph, "upsert", self._label, data, key, **kwargs)
    
    def patch(self, **updates: Any) -> 'WritePlan':
        """Patch/update existing nodes."""
        from ..write.writeplan import WritePlan
        # Pass where conditions for the update
        return WritePlan(
            self._graph, "patch", self._label, updates,
            where_kwargs={},
            where_conditions=self._filters
        )
    
    def inc(self, field: str, value: Union[int, float]) -> 'WritePlan':
        """Increment a numeric field."""
        from ..write.advanced import AdvancedUpdateCompiler
        from ..write.writeplan import WritePlan
        
        compiler = AdvancedUpdateCompiler(self._graph)
        compiled = compiler.compile_inc_update(
            label=self._label,
            field=field,
            value=value,
            where_conditions=self._filters
        )
        
        # Create a WritePlan that uses the pre-compiled advanced operation
        class AdvancedWritePlan(WritePlan):
            def __init__(self, graph, compiled_result):
                self._graph = graph
                self._operation_type = "advanced"
                self._target = "unknown"
                self._args = ()
                self._kwargs = {}
                self._upsert_compiler = None
                self._compiled = compiled_result
                self._stats = None
                self._operation_type = "advanced"
                self._target = "unknown"
                
            def compile(self):
                return self._compiled
                
            def preview(self):
                return self.compile()
        
        return AdvancedWritePlan(self._graph, compiled)
    
    def unset(self, field: str) -> 'WritePlan':
        """Remove/unset a property."""
        from ..write.advanced import AdvancedUpdateCompiler
        from ..write.writeplan import WritePlan
        
        compiler = AdvancedUpdateCompiler(self._graph)
        compiled = compiler.compile_unset_update(
            label=self._label,
            field=field,
            where_conditions=self._filters
        )
        
        # Create a WritePlan that uses the pre-compiled advanced operation
        class AdvancedWritePlan(WritePlan):
            def __init__(self, graph, compiled_result):
                self._graph = graph
                self._operation_type = "advanced"
                self._target = "unknown"
                self._args = ()
                self._kwargs = {}
                self._upsert_compiler = None
                self._compiled = compiled_result
                self._stats = None
                self._operation_type = "advanced"
                self._target = "unknown"
                self._args = ()
                self._kwargs = {}
                self._upsert_compiler = None
                
            def compile(self):
                return self._compiled
                
            def preview(self):
                return self.compile()
        
        return AdvancedWritePlan(self._graph, compiled)
    
    def list_append(self, field: str, value: Any) -> 'WritePlan':
        """Append a value to a list property."""
        from ..write.advanced import AdvancedUpdateCompiler
        from ..write.writeplan import WritePlan
        
        compiler = AdvancedUpdateCompiler(self._graph)
        compiled = compiler.compile_list_append(
            label=self._label,
            field=field,
            value=value,
            where_conditions=self._filters
        )
        
        # Create a WritePlan that uses the pre-compiled advanced operation
        class AdvancedWritePlan(WritePlan):
            def __init__(self, graph, compiled_result):
                self._graph = graph
                self._operation_type = "advanced"
                self._target = "unknown"
                self._args = ()
                self._kwargs = {}
                self._upsert_compiler = None
                self._compiled = compiled_result
                self._stats = None
                
            def compile(self):
                return self._compiled
                
            def preview(self):
                return self.compile()
        
        return AdvancedWritePlan(self._graph, compiled)
    
    def list_remove(self, field: str, value: Any) -> 'WritePlan':
        """Remove a value from a list property."""
        from ..write.advanced import AdvancedUpdateCompiler
        from ..write.writeplan import WritePlan
        
        compiler = AdvancedUpdateCompiler(self._graph)
        compiled = compiler.compile_list_remove(
            label=self._label,
            field=field,
            value=value,
            where_conditions=self._filters
        )
        
        # Create a WritePlan that uses the pre-compiled advanced operation
        class AdvancedWritePlan(WritePlan):
            def __init__(self, graph, compiled_result):
                self._graph = graph
                self._operation_type = "advanced"
                self._target = "unknown"
                self._args = ()
                self._kwargs = {}
                self._upsert_compiler = None
                self._compiled = compiled_result
                self._stats = None
                
            def compile(self):
                return self._compiled
                
            def preview(self):
                return self.compile()
        
        return AdvancedWritePlan(self._graph, compiled)
    
    def map_merge(self, field: str, map_data: Dict[str, Any]) -> 'WritePlan':
        """Merge a dictionary into a map property."""
        from ..write.advanced import AdvancedUpdateCompiler
        from ..write.writeplan import WritePlan
        
        compiler = AdvancedUpdateCompiler(self._graph)
        compiled = compiler.compile_map_merge(
            label=self._label,
            field=field,
            map_data=map_data,
            where_conditions=self._filters
        )
        
        # Create a WritePlan that uses the pre-compiled advanced operation
        class AdvancedWritePlan(WritePlan):
            def __init__(self, graph, compiled_result):
                self._graph = graph
                self._operation_type = "advanced"
                self._target = "unknown"
                self._args = ()
                self._kwargs = {}
                self._upsert_compiler = None
                self._compiled = compiled_result
                self._stats = None
                
            def compile(self):
                return self._compiled
                
            def preview(self):
                return self.compile()
        
        return AdvancedWritePlan(self._graph, compiled)
    
    def delete(self, detach: bool = False) -> 'WritePlan':
        """Delete nodes."""
        from ..write.writeplan import WritePlan
        # Pass where conditions for the delete
        return WritePlan(
            self._graph, "delete", self._label,
            where_kwargs={},
            where_conditions=self._filters,
            detach=detach
        )
