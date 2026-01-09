"""
Integration test configuration for GraphFrame-Neo4j.
"""

import pytest
from graphframe_neo4j import Graph
import os


@pytest.fixture
def neo4j_graph():
    """Provide a connected Graph instance for integration tests."""
    uri = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
    user = os.environ.get("NEO4J_USER", "neo4j")
    password = os.environ.get("NEO4J_PASSWORD", "testpassword")
    database = os.environ.get("NEO4J_DATABASE", "neo4j")
    
    return Graph.connect(uri, (user, password), database=database)


@pytest.fixture
def cleanup_graph(neo4j_graph):
    """Clean up test data after tests."""
    yield neo4j_graph
    # Add cleanup logic here if needed
    # For now, we'll just ensure the connection is closed
    neo4j_graph.close()