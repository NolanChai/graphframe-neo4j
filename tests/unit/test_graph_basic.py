"""
Test basic Graph functionality.
"""

import pytest
from graphframe_neo4j import Graph

def test_graph_creation():
    """Test that we can create a Graph instance."""
    # Note: We're not actually connecting, just testing instantiation
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"), database="neo4j")
    assert g is not None
    assert g.uri == "bolt://localhost:7687"
    assert g.auth == ("neo4j", "password")
    assert g.database == "neo4j"

def test_graph_context_manager():
    """Test that Graph works as a context manager."""
    with Graph.connect("bolt://localhost:7687", auth=("neo4j", "password")) as g:
        assert g is not None
        # The driver should be created when entering context
        assert g._driver is not None
        assert hasattr(g._driver, 'session')

def test_graph_frames():
    """Test that we can get frame instances from Graph."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    # Test nodes frame
    people = g.nodes("Person")
    assert people is not None
    assert str(people) == "<NodeFrame label=Person>"
    
    # Test relationships frame
    works_at = g.rels("WORKS_AT")
    assert works_at is not None
    assert str(works_at) == "<EdgeFrame label=WORKS_AT>"
    
    # Test schema manager
    schema = g.schema()
    assert schema is not None
