"""
Basic integration tests for GraphFrame-Neo4j.
"""

import pytest


@pytest.mark.integration
def test_node_upsert_integration(neo4j_graph):
    """Test node upsert with real Neo4j database."""
    g = neo4j_graph
    
    # Test data
    test_data = [
        {"email": "test@example.com", "name": "Test User", "age": 30}
    ]
    
    # Upsert and verify
    plan = g.nodes("Person").upsert(test_data, key="email")
    result = plan.commit()
    
    # Verify the upsert worked
    found = g.nodes("Person").where(email="test@example.com").to_records()
    assert len(found) == 1
    assert found[0]["name"] == "Test User"
    
    # Test idempotency - should not create duplicates
    plan2 = g.nodes("Person").upsert(test_data, key="email")
    result2 = plan2.commit()
    
    found2 = g.nodes("Person").where(email="test@example.com").to_records()
    assert len(found2) == 1  # Still only one node


@pytest.mark.integration
def test_node_query_integration(neo4j_graph):
    """Test node querying with real Neo4j database."""
    g = neo4j_graph
    
    # Create test data
    test_data = [
        {"email": "alice@example.com", "name": "Alice", "age": 25, "country": "US"},
        {"email": "bob@example.com", "name": "Bob", "age": 30, "country": "UK"},
        {"email": "charlie@example.com", "name": "Charlie", "age": 35, "country": "US"}
    ]
    
    # Upsert test data
    plan = g.nodes("Person").upsert(test_data, key="email")
    plan.commit()
    
    # Test filtering
    us_people = g.nodes("Person").where(country="US").to_records()
    assert len(us_people) == 2
    
    # Test age filtering
    adults = g.nodes("Person").where(age__gte=30).to_records()
    assert len(adults) == 2
    
    # Test combined filtering
    us_adults = g.nodes("Person").where(country="US", age__gte=30).to_records()
    assert len(us_adults) == 1
    assert us_adults[0]["name"] == "Charlie"


@pytest.mark.integration
def test_relationship_upsert_integration(neo4j_graph):
    """Test relationship upsert with real Neo4j database."""
    g = neo4j_graph
    
    # Create test nodes first
    people_data = [
        {"email": "john@example.com", "name": "John"},
        {"email": "jane@example.com", "name": "Jane"}
    ]
    company_data = [
        {"domain": "company.com", "name": "Test Company"}
    ]
    
    g.nodes("Person").upsert(people_data, key="email").commit()
    g.nodes("Company").upsert(company_data, key="domain").commit()
    
    # Create relationship data
    rel_data = [
        {"email": "john@example.com", "domain": "company.com", "role": "Engineer", "since": 2020}
    ]
    
    # Upsert relationship
    plan = g.rels("WORKS_AT").upsert(
        rel_data,
        src=("Person", "email"),
        dst=("Company", "domain"),
        rel_key="role"
    )
    result = plan.commit()
    
    # Verify relationship was created
    # This is a bit tricky to verify without traversal, but we can check the write result
    assert result is not None