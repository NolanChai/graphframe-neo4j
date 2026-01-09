"""
Test traversal compilation functionality.
"""

import pytest
from graphframe_neo4j import Graph

def test_basic_traversal_compilation():
    """Test basic traversal query compilation."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    # Basic outbound traversal
    query = g.nodes("Person").traverse("WORKS_AT", to="Company")
    compiled = query.compile()
    
    assert "MATCH (from:Person)-rel:WORKS_AT->(to:Company)" in compiled["cypher"]
    assert "RETURN from, rel, to" in compiled["cypher"]
    assert compiled["params"] == {}

def test_inbound_traversal():
    """Test inbound traversal."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    query = g.nodes("Company").traverse("WORKS_AT", to="Person", direction="in")
    compiled = query.compile()
    
    assert "MATCH (from:Company)<-rel:WORKS_AT-(to:Person)" in compiled["cypher"]

def test_both_direction_traversal():
    """Test both direction traversal."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    query = g.nodes("Person").traverse("KNOWS", to="Person", direction="both")
    compiled = query.compile()
    
    assert "MATCH (from:Person)-rel:KNOWS-(to:Person)" in compiled["cypher"]

def test_custom_aliases():
    """Test traversal with custom aliases."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    query = g.nodes("Person").traverse("WORKS_AT", to="Company", alias=("p", "r", "c"))
    compiled = query.compile()
    
    assert "MATCH (p:Person)-r:WORKS_AT->(c:Company)" in compiled["cypher"]
    assert "RETURN p, r, c" in compiled["cypher"]

def test_traversal_with_where():
    """Test traversal with WHERE conditions."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    query = (g.nodes("Person")
            .traverse("WORKS_AT", to="Company")
            .where(to__city="San Francisco", rel__since__gte=2020))
    
    compiled = query.compile()
    
    assert "WHERE to.city = $param_0 AND rel.since >= $param_1" in compiled["cypher"]
    assert compiled["params"]["param_0"] == "San Francisco"
    assert compiled["params"]["param_1"] == 2020

def test_traversal_field_selection():
    """Test traversal with field selection."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    query = (g.nodes("Person")
            .traverse("WORKS_AT", to="Company")
            .select("from__name", "rel__role", "to__name"))
    
    compiled = query.compile()
    
    assert "RETURN from.name, rel.role, to.name" in compiled["cypher"]

def test_traversal_with_order_by():
    """Test traversal with ORDER BY."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    query = (g.nodes("Person")
            .traverse("WORKS_AT", to="Company")
            .order_by("to__name", "rel__since__desc"))
    
    compiled = query.compile()
    
    assert "ORDER BY to.name ASC , rel.since DESC" in compiled["cypher"]

def test_traversal_with_limit_offset():
    """Test traversal with LIMIT and OFFSET."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    query = (g.nodes("Person")
            .traverse("WORKS_AT", to="Company")
            .limit(10)
            .offset(20))
    
    compiled = query.compile()
    
    assert "SKIP 20" in compiled["cypher"]
    assert "LIMIT 10" in compiled["cypher"]

def test_back_query_compilation():
    """Test back() query compilation."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    # Create traversal and then back()
    traversal = (g.nodes("Person")
                .traverse("WORKS_AT", to="Company")
                .where(to__city="San Francisco")
                .limit(10))
    
    back_query = traversal.back()
    compiled = back_query.compile()
    
    # Should compile to a back query that filters and returns originating nodes
    assert "MATCH path = (from:Person)-rel:WORKS_AT->(to:Company)" in compiled["cypher"]
    assert "WHERE to.city = $param_0" in compiled["cypher"]
    assert "WITH from" in compiled["cypher"]
    assert "RETURN from" in compiled["cypher"]
    assert "LIMIT 10" in compiled["cypher"]

def test_back_query_with_selection():
    """Test back() query with field selection."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    traversal = (g.nodes("Person")
                .traverse("WORKS_AT", to="Company")
                .where(to__city="San Francisco")
                .select("name", "age"))
    
    back_query = traversal.back()
    compiled = back_query.compile()
    
    assert "RETURN from.name, from.age" in compiled["cypher"]

def test_back_query_with_filtering():
    """Test back() query with additional filtering."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    traversal = (g.nodes("Person")
                .traverse("WORKS_AT", to="Company")
                .where(to__city="San Francisco"))
    
    back_query = traversal.back().where(age__gte=21)
    compiled = back_query.compile()
    
    # Should include both traversal conditions and back conditions
    assert "WHERE to.city = $param_0 AND from.age >= $param_1" in compiled["cypher"]

def test_complex_traversal_pattern():
    """Test complex traversal with multiple conditions."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    query = (g.nodes("Person")
            .where(age__gte=21)
            .traverse("WORKS_AT", to="Company")
            .where(
                to__city="San Francisco",
                rel__since__gte=2020,
                rel__role__in=["Engineer", "Manager"]
            )
            .select("from__name", "to__name", "rel__role")
            .order_by("rel__since__desc")
            .limit(5))
    
    compiled = query.compile()
    
    # Check all components (note: from.age condition is not carried over to traversal)
    assert "MATCH (from:Person)-rel:WORKS_AT->(to:Company)" in compiled["cypher"]
    assert "WHERE to.city = $param_0 AND rel.since >= $param_1 AND rel.role IN $param_2" in compiled["cypher"]
    assert "RETURN from.name, to.name, rel.role" in compiled["cypher"]
    assert "ORDER BY rel.since DESC" in compiled["cypher"]
    assert "LIMIT 5" in compiled["cypher"]

def test_traversal_namespacing_variations():
    """Test different namespacing patterns."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    # Test default namespacing
    query1 = g.nodes("Person").traverse("WORKS_AT", to="Company").where(from__age__gte=21)
    compiled1 = query1.compile()
    assert "WHERE from.age >= $param_0" in compiled1["cypher"]
    
    # Test custom alias namespacing
    query2 = g.nodes("Person").traverse("WORKS_AT", to="Company", alias=("p", "r", "c")).where(p__age__gte=21)
    compiled2 = query2.compile()
    assert "WHERE p.age >= $param_0" in compiled2["cypher"]

def test_traversal_parameter_safety():
    """Test parameter safety in traversal queries."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    dangerous_input = "'); DROP TABLE Users;--"
    query = g.nodes("Person").traverse("WORKS_AT", to="Company").where(to__name=dangerous_input)
    compiled = query.compile()
    
    # Should use parameters, not string interpolation
    assert dangerous_input not in compiled["cypher"]
    assert "WHERE to.name = $param_0" in compiled["cypher"]
    assert compiled["params"]["param_0"] == dangerous_input
