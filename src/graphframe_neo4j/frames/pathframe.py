"""
PathFrame: DataFrame-like interface for querying Neo4j paths.
"""

from typing import Any, Dict, List, Optional, Tuple
from .baseframe import BaseFrame
from .compiler import QueryCompiler

class PathFrame(BaseFrame):
    """
    DataFrame-like interface for querying Neo4j paths.
    
    Args:
        graph: Graph instance
        rel_type: Relationship type
        to_label: Target node label
        direction: Traversal direction ("out", "in", "both")
        alias: Optional aliases for from/rel/to nodes
    """
    
    def __init__(self, graph: 'Graph', rel_type: str, to_label: str, direction: str = "out", alias: Optional[Tuple[str, str, str]] = None, from_label: str = ""):
        super().__init__(graph, rel_type)
        self._rel_type = rel_type
        self._to_label = to_label
        self._direction = direction
        self._from_label = from_label or rel_type  # Store the original node label
        self._from_alias, self._rel_alias, self._to_alias = alias or ("from", "rel", "to")
        self._compiler = QueryCompiler()
    
    def where(self, **kwargs: Any) -> 'PathFrame':
        """Filter paths by conditions."""
        conditions = self._compiler.parse_filter_kwargs(kwargs)
        self._filters.extend(conditions)
        return self
    
    def select(self, *fields: str) -> 'PathFrame':
        """Select specific fields from paths."""
        if fields:
            self._selected_fields = list(fields)
        return self
    
    def order_by(self, *fields: str) -> 'PathFrame':
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
    
    def limit(self, limit: int) -> 'PathFrame':
        """Limit the number of results."""
        self._limit = limit
        return self
    
    def offset(self, offset: int) -> 'PathFrame':
        """Skip a number of results."""
        self._offset = offset
        return self
    
    def back(self) -> 'NodeFrame':
        """Return to the originating nodes after traversal."""
        from .nodeframe import NodeFrame
        
        # Create a NodeFrame that will use the back() query compilation
        node_frame = NodeFrame(self._graph, self._from_label)
        
        # Copy over the filters that were applied during traversal
        # These will be used as the back_conditions in the back query
        node_frame._filters = self._filters.copy()
        node_frame._selected_fields = self._selected_fields.copy()
        node_frame._order_by = self._order_by.copy()
        node_frame._limit = self._limit
        node_frame._offset = self._offset
        
        # Store traversal info for back query compilation
        node_frame._traversal_info = {
            'rel_type': self._rel_type,
            'to_label': self._to_label,
            'direction': self._direction,
            'from_alias': self._from_alias,
            'rel_alias': self._rel_alias,
            'to_alias': self._to_alias,
            'is_back_query': True
        }
        
        return node_frame
    
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
        return self._compiler.compile_traversal_query(
            from_label=self._from_label,
            from_alias=self._from_alias,
            rel_type=self._rel_type,
            rel_alias=self._rel_alias,
            to_label=self._to_label,
            to_alias=self._to_alias,
            direction=self._direction,
            conditions=self._filters,
            fields=self._selected_fields,
            order_by=self._order_by,
            limit=self._limit,
            offset=self._offset
        )
