"""
Test basic WritePlan functionality.
"""

from graphframe_neo4j import Graph, WritePlan

def test_writeplan_creation():
    """Test that we can create a WritePlan."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    # Create a write plan directly
    plan = WritePlan(g, "test_operation", "Person")
    assert plan is not None
    assert str(plan) == "<WritePlan test_operation Person>"
    
    # Test basic methods
    compiled = plan.compile()
    assert "cypher" in compiled
    assert "params" in compiled
    assert compiled["cypher"].startswith("// test_operation Person")
    
    preview = plan.preview()
    assert preview == compiled
    
    # Test commit (should return dummy stats for now)
    stats = plan.commit()
    assert stats["nodes_created"] == 0
    assert stats["nodes_updated"] == 0
    assert stats["relationships_created"] == 0
    assert stats["status"] == "skipped"  # Should be skipped for test operations
    
    # Test explain and profile
    explain_result = plan.explain()
    assert "plan" in explain_result
    
    profile_result = plan.profile()
    assert "profile" in profile_result
