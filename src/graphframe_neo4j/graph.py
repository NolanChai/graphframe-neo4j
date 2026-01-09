"""
Core Graph class for connecting to Neo4j and managing sessions.
"""

from typing import Optional, Tuple, Any, Dict, List
from neo4j import GraphDatabase, Driver, Session
from .util.errors import ConnectionError

# Optional networkx integration
try:
    import networkx as nx
    HAS_NETWORKX = True
except ImportError:
    HAS_NETWORKX = False
    nx = None

class Graph:
    """
    Main entry point for connecting to Neo4j and performing operations.
    
    Args:
        uri: Neo4j connection URI (e.g., "bolt://localhost:7687")
        auth: Tuple of (username, password)
        database: Database name (default: "neo4j")
        **kwargs: Additional driver configuration
    """
    
    def __init__(self, uri: str, auth: Tuple[str, str], database: str = "neo4j", **kwargs: Any):
        self.uri = uri
        self.auth = auth
        self.database = database
        self.driver_kwargs = kwargs
        self._driver: Optional[Driver] = None
        # Relationship uniqueness policy: "require_rel_key", "single_edge_per_pair", or "allow_multiple"
        self.rel_uniqueness_policy = kwargs.get("rel_uniqueness_policy", "single_edge_per_pair")
    
    @classmethod
    def connect(cls, uri: str, auth: Tuple[str, str], database: str = "neo4j", **kwargs: Any) -> 'Graph':
        """Create a new Graph instance and connect to Neo4j."""
        return cls(uri, auth, database, **kwargs)
    
    def __enter__(self):
        """Context manager entry."""
        self._ensure_driver()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - close the driver."""
        self.close()
    
    def _ensure_driver(self) -> Driver:
        """Ensure we have a driver instance, creating one if needed."""
        if self._driver is None:
            try:
                self._driver = GraphDatabase.driver(
                    self.uri, 
                    auth=self.auth, 
                    **self.driver_kwargs
                )
            except Exception as e:
                raise ConnectionError(f"Failed to connect to Neo4j at {self.uri}: {e}") from e
        return self._driver
    
    def session(self) -> Session:
        """Create a new session."""
        driver = self._ensure_driver()
        return driver.session(database=self.database)
    
    def close(self):
        """Close the driver connection."""
        if self._driver is not None:
            self._driver.close()
            self._driver = None
    
    def nodes(self, label: str) -> 'NodeFrame':
        """Get a NodeFrame for querying nodes with the given label."""
        from .frames.nodeframe import NodeFrame
        return NodeFrame(self, label)
    
    def rels(self, rel_type: str) -> 'EdgeFrame':
        """Get an EdgeFrame for querying relationships with the given type."""
        from .frames.edgeframe import EdgeFrame
        return EdgeFrame(self, rel_type)
    
    def schema(self) -> 'SchemaManager':
        """Get the SchemaManager for this graph."""
        from .schema.manager import SchemaManager
        return SchemaManager(self)
    
    def cypher(self, query: str, **params: Any) -> Any:
        """Execute raw Cypher query with parameters."""
        def work(tx):  # type: ignore
            result = tx.run(query, **params)
            return result.data()
        
        with self.session() as session:
            return session.execute_write(work)

    def to_networkx(
        self, 
        node_labels: Optional[List[str]] = None, 
        rel_types: Optional[List[str]] = None,
        limit: Optional[int] = None
    ) -> "nx.Graph":
        """
        Export a subgraph to a NetworkX graph.
        
        Requires networkx to be installed (install with `uv add networkx`).
        
        Args:
            node_labels: List of node labels to include (None for all)
            rel_types: List of relationship types to include (None for all)
            limit: Maximum number of nodes to fetch
            
        Returns:
            nx.Graph: NetworkX graph representation
            
        Raises:
            ImportError: If networkx is not installed
        """
        if not HAS_NETWORKX:
            raise ImportError(
                "networkx is required for to_networkx(). Install with: uv add networkx"
            )
        
        # Create a new NetworkX graph
        graph = nx.Graph()
        
        # Query nodes
        node_query = "MATCH (n)"
        if node_labels:
            node_query += " WHERE " + " OR ".join([f"'{label}' IN labels(n)" for label in node_labels])
        node_query += " RETURN id(n) AS node_id, labels(n) AS labels, properties(n) AS props"
        if limit:
            node_query += f" LIMIT {limit}"
        
        def fetch_nodes(tx):  # type: ignore
            result = tx.run(node_query)
            return [dict(record) for record in result]
        
        with self.session() as session:
            nodes_data = session.execute_read(fetch_nodes)
        
        # Add nodes to NetworkX graph
        for node in nodes_data:
            node_id = node["node_id"]
            labels = node["labels"]
            props = node["props"]
            
            # Use the first label as node type, or "Node" if no labels
            node_type = labels[0] if labels else "Node"
            
            # Add node with properties
            graph.add_node(node_id, **props)
            # Store labels as a separate attribute
            graph.nodes[node_id]["labels"] = labels
            graph.nodes[node_id]["type"] = node_type
        
        # Query relationships
        rel_query = "MATCH ()-[r]->()"
        if rel_types:
            rel_query += " WHERE type(r) IN $rel_types"
        rel_query += " RETURN id(startNode(r)) AS source, id(endNode(r)) AS target, type(r) AS rel_type, properties(r) AS props"
        
        def fetch_rels(tx):  # type: ignore
            result = tx.run(rel_query, rel_types=rel_types or [])
            return [dict(record) for record in result]
        
        with self.session() as session:
            rels_data = session.execute_read(fetch_rels)
        
        # Add relationships to NetworkX graph
        for rel in rels_data:
            source = rel["source"]
            target = rel["target"]
            rel_type = rel["rel_type"]
            props = rel["props"]
            
            # Add edge with relationship properties
            graph.add_edge(source, target, **props)
            # Store relationship type as a separate attribute
            graph.edges[source, target]["type"] = rel_type
        
        return graph
