"""
Test NodeFrame compilation functionality.
"""

import pytest
from graphframe_neo4j import Graph

def test_simple_match_compilation():
    """Test basic MATCH compilation."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    # Simple node query
    query = g.nodes("Person")
    compiled = query.compile()
    
    assert "MATCH (n:Person)" in compiled["cypher"]
    assert "RETURN n" in compiled["cypher"]
    assert compiled["params"] == {}

def test_where_compilation():
    """Test WHERE clause compilation."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    # Simple equality
    query = g.nodes("Person").where(name="John")
    compiled = query.compile()
    
    assert "WHERE n.name = $param_0" in compiled["cypher"]
    assert compiled["params"]["param_0"] == "John"
    
    # Multiple conditions
    query = g.nodes("Person").where(name="John", age=30)
    compiled = query.compile()
    
    assert "WHERE n.name = $param_0 AND n.age = $param_1" in compiled["cypher"]
    assert compiled["params"]["param_0"] == "John"
    assert compiled["params"]["param_1"] == 30

def test_filter_operations():
    """Test various filter operations."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    # Greater than
    query = g.nodes("Person").where(age__gte=21)
    compiled = query.compile()
    assert "WHERE n.age >= $param_0" in compiled["cypher"]
    assert compiled["params"]["param_0"] == 21
    
    # Less than
    query = g.nodes("Person").where(age__lt=30)
    compiled = query.compile()
    assert "WHERE n.age < $param_0" in compiled["cypher"]
    
    # IN operation
    query = g.nodes("Person").where(country__in=["US", "CA", "UK"])
    compiled = query.compile()
    assert "WHERE n.country IN $param_0" in compiled["cypher"]
    assert compiled["params"]["param_0"] == ["US", "CA", "UK"]
    
    # Contains
    query = g.nodes("Person").where(name__contains="ohn")
    compiled = query.compile()
    assert "WHERE n.name CONTAINS $param_0" in compiled["cypher"]

def test_select_compilation():
    """Test SELECT clause compilation."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    # Select specific fields
    query = g.nodes("Person").select("name", "email", "age")
    compiled = query.compile()
    
    assert "RETURN n.name, n.email, n.age" in compiled["cypher"]
    
    # Select with where
    query = g.nodes("Person").where(age__gte=21).select("name", "email")
    compiled = query.compile()
    
    assert "WHERE n.age >= $param_0" in compiled["cypher"]
    assert "RETURN n.name, n.email" in compiled["cypher"]

def test_limit_offset_compilation():
    """Test LIMIT and OFFSET compilation."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    # Limit only
    query = g.nodes("Person").limit(10)
    compiled = query.compile()
    assert "LIMIT 10" in compiled["cypher"]
    
    # Offset only
    query = g.nodes("Person").offset(20)
    compiled = query.compile()
    assert "SKIP 20" in compiled["cypher"]
    
    # Both limit and offset
    query = g.nodes("Person").offset(20).limit(10)
    compiled = query.compile()
    assert "SKIP 20" in compiled["cypher"]
    assert "LIMIT 10" in compiled["cypher"]

def test_order_by_compilation():
    """Test ORDER BY compilation."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    # Simple order by
    query = g.nodes("Person").order_by("name")
    compiled = query.compile()
    assert "ORDER BY n.name ASC" in compiled["cypher"]
    
    # Descending order
    query = g.nodes("Person").order_by("age__desc")
    compiled = query.compile()
    assert "ORDER BY n.age DESC" in compiled["cypher"]
    
    # Multiple fields
    query = g.nodes("Person").order_by("country", "name__desc")
    compiled = query.compile()
    assert "ORDER BY n.country ASC , n.name DESC" in compiled["cypher"]

def test_complete_query_compilation():
    """Test complete query with all components."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    # Complete query
    query = (g.nodes("Person")
            .where(age__gte=21, country="US")
            .select("name", "email", "age")
            .order_by("name")
            .limit(10)
            .offset(0))
    
    compiled = query.compile()
    
    # Check all components are present
    assert "MATCH (n:Person)" in compiled["cypher"]
    assert "WHERE n.age >= $param_0 AND n.country = $param_1" in compiled["cypher"]
    assert "RETURN n.name, n.email, n.age" in compiled["cypher"]
    assert "ORDER BY n.name ASC" in compiled["cypher"]
    assert "LIMIT 10" in compiled["cypher"]
    
    # Check parameters
    assert compiled["params"]["param_0"] == 21
    assert compiled["params"]["param_1"] == "US"

def test_parameter_safety():
    """Test that parameters are properly escaped."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    # Test with potentially dangerous input
    dangerous_name = "Robert'); DROP TABLE Students;--"
    query = g.nodes("Person").where(name=dangerous_name)
    compiled = query.compile()
    
    # Should use parameter, not string interpolation
    assert "WHERE n.name = $param_0" in compiled["cypher"]
    assert dangerous_name not in compiled["cypher"]
    assert compiled["params"]["param_0"] == dangerous_name
