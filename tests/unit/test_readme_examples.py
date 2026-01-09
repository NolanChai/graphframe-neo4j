"""
Test that the examples from the README work.
"""

import pytest
from graphframe_neo4j import Graph

def test_readme_basic_examples():
    """Test the basic examples from the README."""
    # Create a Graph instance (won't actually connect in tests)
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"), database="neo4j")
    
    # Test nodes frame creation
    People = g.nodes("Person")
    assert People is not None
    assert str(People) == "<NodeFrame label=Person>"
    
    # Test method chaining (methods return self)
    query = People.where(age__gte=21, country="US").select("name", "email").limit(10)
    assert query is not None
    assert str(query) == "<NodeFrame label=Person>"
    
    # Test that to_records returns a list (even if empty)
    results = query.to_records()
    assert isinstance(results, list)
    
    # Test compile returns expected structure
    compiled = query.compile()
    assert isinstance(compiled, dict)
    assert "cypher" in compiled
    assert "params" in compiled
    
    # Test traversal
    traversal = People.traverse("WORKS_AT", to="Company", direction="out", alias=("p", "r", "c"))
    assert traversal is not None
    
    # Test back() from traversal
    back_result = traversal.back()
    assert back_result is not None
    assert str(back_result) == "<NodeFrame label=Person>"
