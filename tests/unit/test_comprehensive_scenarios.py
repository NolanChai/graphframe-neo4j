"""
Comprehensive scenario tests for NodeFrame functionality.
"""

import pytest
from graphframe_neo4j import Graph

def test_chaining_order_independence():
    """Test that method chaining order doesn't affect results."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    # Different chaining orders should produce equivalent queries
    query1 = g.nodes("Person").where(age__gte=21).select("name").limit(10).order_by("name")
    query2 = g.nodes("Person").limit(10).where(age__gte=21).order_by("name").select("name")
    query3 = g.nodes("Person").order_by("name").select("name").where(age__gte=21).limit(10)
    
    compiled1 = query1.compile()
    compiled2 = query2.compile()
    compiled3 = query3.compile()
    
    # All should have the same components (order may vary)
    for compiled in [compiled1, compiled2, compiled3]:
        assert "MATCH (n:Person)" in compiled["cypher"]
        assert "WHERE n.age >= $param_0" in compiled["cypher"]
        assert "RETURN n.name" in compiled["cypher"]
        assert "LIMIT 10" in compiled["cypher"]
        assert "ORDER BY n.name ASC" in compiled["cypher"]
        assert compiled["params"]["param_0"] == 21

def test_multiple_where_calls():
    """Test multiple where() calls accumulate conditions."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    query = (g.nodes("Person")
            .where(age__gte=21)
            .where(country="US")
            .where(name__contains="J"))
    
    compiled = query.compile()
    
    # Should have all conditions
    assert "WHERE n.age >= $param_0 AND n.country = $param_1 AND n.name CONTAINS $param_2" in compiled["cypher"]
    assert compiled["params"]["param_0"] == 21
    assert compiled["params"]["param_1"] == "US"
    assert compiled["params"]["param_2"] == "J"

def test_select_overwrites():
    """Test that multiple select() calls overwrite previous selection."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    query = g.nodes("Person").select("name", "age").select("email")
    compiled = query.compile()
    
    # Should only have the last selection
    assert "RETURN n.email" in compiled["cypher"]
    assert "n.name" not in compiled["cypher"]
    assert "n.age" not in compiled["cypher"]

def test_empty_string_values():
    """Test handling of empty string values."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    query = g.nodes("Person").where(name="", email="")
    compiled = query.compile()
    
    assert "WHERE n.name = $param_0 AND n.email = $param_1" in compiled["cypher"]
    assert compiled["params"]["param_0"] == ""
    assert compiled["params"]["param_1"] == ""

def test_large_parameter_values():
    """Test handling of large parameter values."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    large_string = "A" * 10000
    large_list = list(range(1000))
    
    query = g.nodes("Person").where(description=large_string, ids__in=large_list)
    compiled = query.compile()
    
    # Should handle large values properly
    assert compiled["params"]["param_0"] == large_string
    assert compiled["params"]["param_1"] == large_list
    assert large_string not in compiled["cypher"]

def test_unicode_values():
    """Test handling of Unicode values."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    unicode_name = "José María"
    emoji_description = "User with 👍 and 🎉"
    chinese_name = "李明"
    
    query = g.nodes("Person").where(
        name=unicode_name,
        description=emoji_description,
        full_name=chinese_name
    )
    compiled = query.compile()
    
    # Should handle Unicode properly
    assert compiled["params"]["param_0"] == unicode_name
    assert compiled["params"]["param_1"] == emoji_description
    assert compiled["params"]["param_2"] == chinese_name
    
    # Should not have Unicode in the Cypher query itself
    assert unicode_name not in compiled["cypher"]
    assert emoji_description not in compiled["cypher"]
    assert chinese_name not in compiled["cypher"]

def test_mixed_filter_types():
    """Test mixing different filter types in one query."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    query = g.nodes("Person").where(
        age__gte=18,
        age__lte=65,
        country__in=["US", "CA", "UK"],
        status__ne="deleted",
        email__contains="@",
        verified__not_null=True
    )
    compiled = query.compile()
    
    # Should have all filter types
    assert "WHERE" in compiled["cypher"]
    assert "n.age >= $param_0" in compiled["cypher"]
    assert "n.age <= $param_1" in compiled["cypher"]
    assert "n.country IN $param_2" in compiled["cypher"]
    assert "n.status <> $param_3" in compiled["cypher"]
    assert "n.email CONTAINS $param_4" in compiled["cypher"]
    assert "n.verified IS NOT NULL" in compiled["cypher"]

def test_compiler_reusability():
    """Test that compiler can be reused for multiple queries."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    # Create multiple queries and ensure they don't interfere
    query1 = g.nodes("Person").where(name="Alice")
    query2 = g.nodes("Company").where(name="Acme")
    query3 = g.nodes("Product").where(price__gte=100)
    
    compiled1 = query1.compile()
    compiled2 = query2.compile()
    compiled3 = query3.compile()
    
    # Each should have independent parameters
    assert compiled1["params"]["param_0"] == "Alice"
    assert compiled2["params"]["param_0"] == "Acme"
    assert compiled3["params"]["param_0"] == 100
    
    # Each should have different labels
    assert "MATCH (n:Person)" in compiled1["cypher"]
    assert "MATCH (n:Company)" in compiled2["cypher"]
    assert "MATCH (n:Product)" in compiled3["cypher"]

def test_query_with_no_results():
    """Test query that should return no results."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    # Impossible condition
    query = g.nodes("Person").where(age__lt=0)
    compiled = query.compile()
    
    assert "WHERE n.age < $param_0" in compiled["cypher"]
    assert compiled["params"]["param_0"] == 0
    
    # Should still execute without error
    results = query.to_records()
    assert isinstance(results, list)  # May be empty list

def test_very_large_limit():
    """Test with very large limit values."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    query = g.nodes("Person").limit(1000000)
    compiled = query.compile()
    
    assert "LIMIT 1000000" in compiled["cypher"]

def test_compound_conditions_same_field():
    """Test multiple conditions on the same field."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    # This should work - multiple conditions on same field
    query = g.nodes("Person").where(age__gte=18, age__lte=65)
    compiled = query.compile()
    
    assert "WHERE n.age >= $param_0 AND n.age <= $param_1" in compiled["cypher"]
    assert compiled["params"]["param_0"] == 18
    assert compiled["params"]["param_1"] == 65
