"""
Test basic imports and module structure.
"""

def test_basic_imports():
    """Test that all main modules can be imported."""
    from graphframe_neo4j import Graph, NodeFrame, EdgeFrame, PathFrame, WritePlan, SchemaManager
    
    # Just verify they're importable
    assert Graph is not None
    assert NodeFrame is not None
    assert EdgeFrame is not None
    assert PathFrame is not None
    assert WritePlan is not None
    assert SchemaManager is not None
