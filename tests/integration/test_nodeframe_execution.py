"""
Integration tests for NodeFrame execution.
"""

import pytest
import os
from graphframe_neo4j import Graph

# Mark these as integration tests
pytestmark = pytest.mark.integration

def test_nodeframe_execution():
    """Test actual execution of NodeFrame queries."""
    # Get connection details from environment variables
    uri = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
    user = os.environ.get("NEO4J_USER", "neo4j")
    password = os.environ.get("NEO4J_PASSWORD", "password")
    database = os.environ.get("NEO4J_DATABASE", "neo4j")
    
    g = Graph.connect(uri, auth=(user, password), database=database)
    
    # Create some test data
    setup_cypher = """
    CREATE (p1:Person {name: 'Alice', age: 30, country: 'US', email: 'alice@example.com'})
    CREATE (p2:Person {name: 'Bob', age: 25, country: 'CA', email: 'bob@example.com'})
    CREATE (p3:Person {name: 'Charlie', age: 35, country: 'US', email: 'charlie@example.com'})
    """
    
    try:
        with g.session() as session:
            session.run(setup_cypher)
        
        # Test simple query
        query = g.nodes("Person").where(country="US")
        results = query.to_records()
        
        assert len(results) == 2
        names = [r["n"]["name"] for r in results]
        assert "Alice" in names
        assert "Charlie" in names
        
        # Test with selection
        query = g.nodes("Person").where(country="US").select("name", "age")
        results = query.to_records()
        
        assert len(results) == 2
        for record in results:
            assert "name" in record
            assert "age" in record
            assert "email" not in record  # Should not be selected
        
        # Test with filtering
        query = g.nodes("Person").where(age__gte=30, country="US")
        results = query.to_records()
        
        assert len(results) == 1
        assert results[0]["n"]["name"] == "Charlie"
        
        # Test with ordering
        query = g.nodes("Person").order_by("age__desc").limit(2)
        results = query.to_records()
        
        assert len(results) == 2
        ages = [r["n"]["age"] for r in results]
        assert ages == sorted(ages, reverse=True)
        
    finally:
        # Clean up
        cleanup_cypher = "MATCH (p:Person) WHERE p.email IN ['alice@example.com', 'bob@example.com', 'charlie@example.com'] DELETE p"
        with g.session() as session:
            session.run(cleanup_cypher)

def test_compilation_and_execution_consistency():
    """Test that compilation and execution are consistent."""
    uri = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
    user = os.environ.get("NEO4J_USER", "neo4j")
    password = os.environ.get("NEO4J_PASSWORD", "password")
    database = os.environ.get("NEO4J_DATABASE", "neo4j")
    
    g = Graph.connect(uri, auth=(user, password), database=database)
    
    # Create test data
    setup_cypher = """
    CREATE (p:Person {name: 'Test', age: 25})
    """
    
    try:
        with g.session() as session:
            session.run(setup_cypher)
        
        # Build query
        query = g.nodes("Person").where(name="Test").select("name", "age")
        
        # Get compiled query
        compiled = query.compile()
        
        # Execute via to_records
        results1 = query.to_records()
        
        # Execute via raw Cypher
        results2 = g.cypher(compiled["cypher"], **compiled["params"])
        
        # Should get same results
        assert len(results1) == len(results2)
        if results1 and results2:
            assert results1[0]["name"] == results2[0]["name"]
            assert results1[0]["age"] == results2[0]["age"]
        
    finally:
        # Clean up
        cleanup_cypher = "MATCH (p:Person {name: 'Test'}) DELETE p"
        with g.session() as session:
            session.run(cleanup_cypher)
