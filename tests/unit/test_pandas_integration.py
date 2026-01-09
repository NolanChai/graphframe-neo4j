"""
Test pandas integration for GraphFrame-Neo4j.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from graphframe_neo4j import Graph
from graphframe_neo4j.frames.nodeframe import NodeFrame


def test_to_df_requires_pandas():
    """Test that to_df() raises ImportError when pandas is not installed."""
    # Create a mock graph and node frame
    mock_graph = Mock()
    node_frame = NodeFrame(mock_graph, "Person")
    
    # Mock the to_records method to return some data
    node_frame.to_records = Mock(return_value=[
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25}
    ])
    
    # Temporarily hide pandas to test the ImportError
    with patch.dict('sys.modules', {'pandas': None}):
        # Reload the module to pick up the pandas absence
        import importlib
        import graphframe_neo4j.frames.baseframe
        importlib.reload(graphframe_neo4j.frames.baseframe)
        
        with pytest.raises(ImportError, match="pandas is required for to_df"):
            node_frame.to_df()


@pytest.mark.skipif(True, reason="pandas not required for basic functionality")
def test_to_df_with_pandas():
    """Test that to_df() works when pandas is installed."""
    # This test would require pandas to be installed
    # For now, we skip it since pandas is optional
    pass


def test_to_records_basic():
    """Test that to_records() works for basic node queries."""
    # Create a mock graph
    mock_graph = Mock()
    
    # Mock the session context manager using MagicMock
    mock_session = MagicMock()
    mock_session.execute_read.return_value = [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25}
    ]
    
    # Mock the session method to return a context manager
    mock_context = MagicMock()
    mock_context.__enter__.return_value = mock_session
    mock_context.__exit__.return_value = None
    mock_graph.session.return_value = mock_context
    
    # Create node frame and test to_records
    node_frame = NodeFrame(mock_graph, "Person")
    
    # Mock the compile method to return a simple query
    node_frame.compile = Mock(return_value={
        "cypher": "MATCH (n:Person) RETURN n",
        "params": {}
    })
    
    records = node_frame.to_records()
    
    # Verify the results
    assert len(records) == 2
    assert records[0]["name"] == "Alice"
    assert records[1]["age"] == 25


@pytest.mark.skipif(True, reason="EdgeFrame implementation pending")
def test_edgeframe_to_records():
    """Test that EdgeFrame to_records() works."""
    pass


@pytest.mark.skipif(True, reason="PathFrame implementation pending")
def test_pathframe_to_records():
    """Test that PathFrame to_records() works."""
    pass