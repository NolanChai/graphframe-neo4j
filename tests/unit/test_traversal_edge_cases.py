"""
Edge case tests for traversal functionality.
"""

import pytest
from graphframe_neo4j import Graph

def test_empty_traversal():
    """Test traversal with no conditions or selections."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    query = g.nodes("Person").traverse("WORKS_AT", to="Company")
    compiled = query.compile()
    
    assert "MATCH (from:Person)-rel:WORKS_AT->(to:Company)" in compiled["cypher"]
    assert "RETURN from, rel, to" in compiled["cypher"]
    assert compiled["params"] == {}

def test_traversal_with_no_to_label():
    """Test traversal without specifying to label."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    query = g.nodes("Person").traverse("WORKS_AT", to="")
    compiled = query.compile()
    
    assert "MATCH (from:Person)-rel:WORKS_AT->(to)" in compiled["cypher"]

def test_complex_namespacing_with_operations():
    """Test complex namespacing with various operations."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    query = g.nodes("Person").traverse("WORKS_AT", to="Company").where(
        from__age__gte=21,
        from__age__lte=65,
        to__city__in=["SF", "NYC"],
        to__country__ne="US",
        rel__since__lt=2020,
        rel__role__contains="Eng"
    )
    
    compiled = query.compile()
    
    assert "WHERE from.age >= $param_0 AND from.age <= $param_1 AND to.city IN $param_2" in compiled["cypher"]
    assert "to.country <> $param_3" in compiled["cypher"]
    assert "rel.since < $param_4" in compiled["cypher"]
    assert "rel.role CONTAINS $param_5" in compiled["cypher"]

def test_mixed_namespacing_and_regular_filters():
    """Test mixing namespaced and regular filter syntax."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    # This should work - namespaced fields take precedence
    query = g.nodes("Person").traverse("WORKS_AT", to="Company").where(
        from__age__gte=21,  # namespaced
        city="SF",         # regular (should default to 'from' alias)
        since__gte=2020     # regular (should default to 'from' alias)
    )
    
    compiled = query.compile()
    
    # Should have all conditions
    assert "WHERE from.age >= $param_0 AND from.city = $param_1 AND from.since >= $param_2" in compiled["cypher"]

def test_traversal_with_null_operations():
    """Test traversal with NULL operations."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    query = g.nodes("Person").traverse("WORKS_AT", to="Company").where(
        to__website__is_null=True,
        rel__end_date__not_null=True
    )
    
    compiled = query.compile()
    
    assert "WHERE to.website IS NULL AND rel.end_date IS NOT NULL" in compiled["cypher"]
    assert compiled["params"] == {}  # No parameters for NULL ops

def test_back_query_with_complex_filtering():
    """Test back() query with complex filtering."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    traversal = (g.nodes("Person")
                .traverse("WORKS_AT", to="Company")
                .where(
                    to__city="San Francisco",
                    to__industry__in=["Tech", "Finance"],
                    rel__since__gte=2020
                )
                .select("from__name", "from__age")
                .order_by("from__age__desc")
                .limit(10))
    
    back_query = traversal.back().where(
        age__gte=21,
        status__ne="inactive"
    )
    
    compiled = back_query.compile()
    
    # Should have both traversal and back conditions
    assert "WHERE to.city = $param_0 AND to.industry IN $param_1 AND rel.since >= $param_2" in compiled["cypher"]
    assert "from.age >= $param_3 AND from.status <> $param_4" in compiled["cypher"]
    assert "WITH from" in compiled["cypher"]
    assert "RETURN from.name, from.age" in compiled["cypher"]
    assert "ORDER BY from.age DESC" in compiled["cypher"]
    assert "LIMIT 10" in compiled["cypher"]

def test_traversal_with_special_characters():
    """Test traversal with special characters in values."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    special_values = {
        "to__name": "O'Reilly Media",
        "to__email": "test@example.com",
        "rel__notes": "It's a great company!"
    }
    
    query = g.nodes("Person").traverse("WORKS_AT", to="Company").where(**special_values)
    compiled = query.compile()
    
    # Should use parameters, not string interpolation
    for value in special_values.values():
        assert value not in compiled["cypher"]
    
    # Should have proper parameters
    assert compiled["params"]["param_0"] == "O'Reilly Media"
    assert compiled["params"]["param_1"] == "test@example.com"
    assert compiled["params"]["param_2"] == "It's a great company!"

def test_traversal_with_unicode():
    """Test traversal with Unicode values."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    unicode_values = {
        "to__name": "José's Company",
        "to__description": "Company with 👍 and 🎉",
        "rel__role": "Developer 💻"
    }
    
    query = g.nodes("Person").traverse("WORKS_AT", to="Company").where(**unicode_values)
    compiled = query.compile()
    
    # Should handle Unicode properly in parameters
    assert compiled["params"]["param_0"] == "José's Company"
    assert compiled["params"]["param_1"] == "Company with 👍 and 🎉"
    assert compiled["params"]["param_2"] == "Developer 💻"
    
    # Should not have Unicode in Cypher query
    assert "José's Company" not in compiled["cypher"]
    assert "👍" not in compiled["cypher"]

def test_traversal_with_large_parameter_values():
    """Test traversal with large parameter values."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    large_string = "A" * 10000
    large_list = list(range(1000))
    
    query = g.nodes("Person").traverse("WORKS_AT", to="Company").where(
        to__description=large_string,
        to__tags__in=large_list
    )
    
    compiled = query.compile()
    
    # Should handle large values
    assert compiled["params"]["param_0"] == large_string
    assert compiled["params"]["param_1"] == large_list
    assert large_string not in compiled["cypher"]

def test_traversal_chaining_complexity():
    """Test complex method chaining order."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    # Complex chaining in different orders should work
    query1 = (g.nodes("Person")
             .traverse("WORKS_AT", to="Company")
             .where(to__city="SF")
             .select("from__name", "to__name")
             .order_by("to__name")
             .limit(10)
             .offset(0))
    
    query2 = (g.nodes("Person")
             .traverse("WORKS_AT", to="Company")
             .limit(10)
             .offset(0)
             .order_by("to__name")
             .select("from__name", "to__name")
             .where(to__city="SF"))
    
    # Both should compile successfully
    compiled1 = query1.compile()
    compiled2 = query2.compile()
    
    # Both should have the same components
    for compiled in [compiled1, compiled2]:
        assert "WHERE to.city = $param_0" in compiled["cypher"]
        assert "RETURN from.name, to.name" in compiled["cypher"]
        assert "ORDER BY to.name ASC" in compiled["cypher"]
        assert "LIMIT 10" in compiled["cypher"]

def test_back_query_with_no_traversal_filters():
    """Test back() query when no traversal filters were applied."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    traversal = g.nodes("Person").traverse("WORKS_AT", to="Company")
    back_query = traversal.back().where(age__gte=21)
    
    compiled = back_query.compile()
    
    # Should only have back conditions
    assert "WHERE from.age >= $param_0" in compiled["cypher"]
    assert "WITH from" in compiled["cypher"]
    assert "RETURN from" in compiled["cypher"]

def test_traversal_with_zero_and_negative_limits():
    """Test traversal with edge case limit values."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    # Zero limit
    query1 = g.nodes("Person").traverse("WORKS_AT", to="Company").limit(0)
    compiled1 = query1.compile()
    assert "LIMIT 0" in compiled1["cypher"]
    
    # Negative limit (should be ignored)
    query2 = g.nodes("Person").traverse("WORKS_AT", to="Company").limit(-1)
    compiled2 = query2.compile()
    assert "LIMIT" not in compiled2["cypher"]
    
    # Large limit
    query3 = g.nodes("Person").traverse("WORKS_AT", to="Company").limit(1000000)
    compiled3 = query3.compile()
    assert "LIMIT 1000000" in compiled3["cypher"]

def test_traversal_compiler_reusability():
    """Test that traversal compiler can handle multiple queries."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    # Create multiple traversal queries
    queries = [
        g.nodes("Person").traverse("WORKS_AT", to="Company").where(to__city="SF"),
        g.nodes("Company").traverse("HAS_OFFICE", to="City").where(to__country="US"),
        g.nodes("Person").traverse("KNOWS", to="Person").where(rel__since__gte=2020)
    ]
    
    # Compile all
    compiled_queries = [q.compile() for q in queries]
    
    # Each should have independent parameters
    assert compiled_queries[0]["params"]["param_0"] == "SF"
    assert compiled_queries[1]["params"]["param_0"] == "US"
    assert compiled_queries[2]["params"]["param_0"] == 2020
    
    # Each should have different patterns
    assert "WORKS_AT" in compiled_queries[0]["cypher"]
    assert "HAS_OFFICE" in compiled_queries[1]["cypher"]
    assert "KNOWS" in compiled_queries[2]["cypher"]

def test_empty_selection_in_traversal():
    """Test traversal with empty field selection."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    query = g.nodes("Person").traverse("WORKS_AT", to="Company").select()
    compiled = query.compile()
    
    # Should return all aliases by default
    assert "RETURN from, rel, to" in compiled["cypher"]

def test_traversal_with_wildcard_selection():
    """Test traversal with wildcard selection."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    query = g.nodes("Person").traverse("WORKS_AT", to="Company").select("*")
    compiled = query.compile()
    
    # Should return all aliases
    assert "RETURN from, rel, to" in compiled["cypher"]
