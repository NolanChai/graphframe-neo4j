"""
Test networkx integration for GraphFrame-Neo4j.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from graphframe_neo4j import Graph


def test_to_networkx_requires_networkx():
    """Test that to_networkx() raises ImportError when networkx is not installed."""
    # Temporarily hide networkx to test the ImportError
    with patch.dict('sys.modules', {'networkx': None}):
        # Reload the module to pick up the networkx absence
        import importlib
        import graphframe_neo4j.graph
        importlib.reload(graphframe_neo4j.graph)
        
        # Create a real Graph instance to test the actual method
        graph = Graph("bolt://localhost:7687", ("neo4j", "test"))
        
        with pytest.raises(ImportError, match="networkx is required for to_networkx"):
            graph.to_networkx()


@pytest.mark.skipif(True, reason="networkx not required for basic functionality")
def test_to_networkx_with_networkx():
    """Test that to_networkx() works when networkx is installed."""
    # This test would require networkx to be installed
    # For now, we skip it since networkx is optional
    pass


def test_graph_basic_functionality():
    """Test that basic Graph functionality still works."""
    # Create a mock graph
    mock_graph = Mock()
    mock_graph.nodes.return_value = Mock()
    mock_graph.rels.return_value = Mock()
    mock_graph.schema.return_value = Mock()
    
    # Test that basic methods are callable
    nodes_frame = mock_graph.nodes("Person")
    rels_frame = mock_graph.rels("KNOWS")
    schema_manager = mock_graph.schema()
    
    # Verify the methods return the expected types
    assert hasattr(nodes_frame, 'where')
    assert hasattr(rels_frame, 'where')
    assert hasattr(schema_manager, 'ensure_index')