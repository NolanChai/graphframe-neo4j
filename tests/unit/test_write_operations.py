"""
Test basic write operations.
"""

from graphframe_neo4j import Graph

def test_node_upsert():
    """Test node upsert operation."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    # Test upsert with single record
    data = [{"email": "john@example.com", "name": "John", "age": 30}]
    plan = g.nodes("Person").upsert(data, key="email")
    
    assert plan is not None
    assert str(plan) == "<WritePlan upsert Person>"
    
    # Test compilation
    compiled = plan.compile()
    assert "cypher" in compiled
    assert "params" in compiled
    
    # Test commit
    stats = plan.commit()
    assert isinstance(stats, dict)

def test_node_patch():
    """Test node patch operation."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    plan = g.nodes("Person").where(country="US").patch(active=True)
    
    assert plan is not None
    assert str(plan) == "<WritePlan patch Person>"
    
    stats = plan.commit()
    assert isinstance(stats, dict)

def test_node_delete():
    """Test node delete operation."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    plan = g.nodes("Temp").where(obsolete=True).delete(detach=True)
    
    assert plan is not None
    assert str(plan) == "<WritePlan delete Temp>"
    
    stats = plan.commit()
    assert isinstance(stats, dict)

def test_relationship_upsert():
    """Test relationship upsert operation."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    data = [{"email": "john@example.com", "domain": "company.com", "since": "2020-01-01", "role": "Engineer"}]
    plan = g.rels("WORKS_AT").upsert(
        data, 
        src=("Person", "email"), 
        dst=("Company", "domain"),
        rel_key=["since", "role"]
    )
    
    assert plan is not None
    assert str(plan) == "<WritePlan relationship_upsert WORKS_AT>"
    
    stats = plan.commit()
    assert isinstance(stats, dict)

def test_schema_operations():
    """Test schema operations."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    schema = g.schema()
    
    # Test ensure_unique
    plan = schema.ensure_unique("Person", "email")
    assert plan is not None
    
    # Test ensure_index
    plan = schema.ensure_index("Person", "name")
    assert plan is not None
    
    # Test describe
    description = schema.describe()
    assert isinstance(description, dict)
    assert "constraints" in description
    assert "indexes" in description
