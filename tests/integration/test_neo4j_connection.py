"""
Integration tests for Neo4j connection.
"""

import pytest
import os
from graphframe_neo4j import Graph

# Mark these as integration tests
pytestmark = pytest.mark.integration

def test_neo4j_connection():
    """Test actual connection to Neo4j."""
    # Get connection details from environment variables
    uri = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
    user = os.environ.get("NEO4J_USER", "neo4j")
    password = os.environ.get("NEO4J_PASSWORD", "password")
    database = os.environ.get("NEO4J_DATABASE", "neo4j")
    
    # Create and test connection
    g = Graph.connect(uri, auth=(user, password), database=database)
    
    # Test that we can get a session
    with g.session() as session:
        # Test a simple query
        result = session.run("RETURN 1 as test")
        record = result.single()
        assert record["test"] == 1
    
    # Test context manager
    with Graph.connect(uri, auth=(user, password), database=database) as g2:
        with g2.session() as session:
            result = session.run("RETURN 'hello' as greeting")
            record = result.single()
            assert record["greeting"] == "hello"

def test_raw_cypher():
    """Test raw Cypher execution."""
    uri = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
    user = os.environ.get("NEO4J_USER", "neo4j")
    password = os.environ.get("NEO4J_PASSWORD", "password")
    database = os.environ.get("NEO4J_DATABASE", "neo4j")
    
    g = Graph.connect(uri, auth=(user, password), database=database)
    
    # Test raw Cypher
    result = g.cypher("RETURN $param as value", param="test_value")
    assert len(result) == 1
    assert result[0]["value"] == "test_value"
