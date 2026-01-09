"""
EdgeFrame: DataFrame-like interface for querying Neo4j relationships.
"""

from typing import Any, Dict, List, Optional, Union
from .baseframe import BaseFrame

class EdgeFrame(BaseFrame):
    """
    DataFrame-like interface for querying Neo4j relationships.
    
    Args:
        graph: Graph instance
        rel_type: Relationship type to query
    """
    
    def __init__(self, graph: 'Graph', rel_type: str):
        super().__init__(graph, rel_type)
        self._rel_type = rel_type
        self._alias = "r"  # Default alias for relationships
    
    def where(self, **kwargs: Any) -> 'EdgeFrame':
        """Filter relationships by conditions."""
        from ..frames.compiler import QueryCompiler
        if not hasattr(self, '_compiler'):
            self._compiler = QueryCompiler()
        conditions = self._compiler.parse_filter_kwargs(kwargs)
        self._filters.extend(conditions)
        return self
    
    def select(self, *fields: str) -> 'EdgeFrame':
        """Select specific fields from relationships."""
        # TODO: Implement field selection
        return self
    
    def limit(self, limit: int) -> 'EdgeFrame':
        """Limit the number of results."""
        # TODO: Implement limit
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
        if not hasattr(self, '_compiler'):
            from .compiler import QueryCompiler
            self._compiler = QueryCompiler()
        
        return self._compiler.compile_edge_query(
            rel_type=self._rel_type,
            alias=self._alias,
            conditions=self._filters,
            fields=self._selected_fields,
            limit=self._limit,
            offset=self._offset
        )
    
    def upsert(self, data: Union[List[Dict[str, Any]], Dict[str, Any]], src: tuple, dst: tuple, rel_key: Optional[Union[str, List[str]]] = None, **kwargs: Any) -> 'WritePlan':
        """Upsert relationships (create or update)."""
        from ..write.writeplan import WritePlan
        # Use relationship_upsert operation type
        return WritePlan(self._graph, "relationship_upsert", self._rel_type, data, src, dst, rel_key, kwargs)
    
    def patch(self, **updates: Any) -> 'WritePlan':
        """Patch/update existing relationships."""
        from ..write.writeplan import WritePlan
        # Pass where conditions for the update
        return WritePlan(
            self._graph, "relationship_update", self._rel_type, updates,
            where_kwargs={},
            where_conditions=self._filters
        )
    
    def delete(self) -> 'WritePlan':
        """Delete relationships."""
        from ..write.writeplan import WritePlan
        # Pass where conditions for the delete
        return WritePlan(
            self._graph, "relationship_delete", self._rel_type,
            where_kwargs={},
            where_conditions=self._filters
        )
