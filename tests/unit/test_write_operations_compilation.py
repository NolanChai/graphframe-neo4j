"""
Test write operations compilation.
"""

import pytest
from graphframe_neo4j import Graph
from graphframe_neo4j.util.errors import WriteError

def test_node_upsert_compilation():
    """Test node upsert compilation."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    # Simple upsert
    data = [{"email": "john@example.com", "name": "John", "age": 30}]
    plan = g.nodes("Person").upsert(data, key="email")
    compiled = plan.compile()
    
    assert "UNWIND $batch AS item" in compiled["cypher"]
    assert "MERGE (n:Person {email: item.email})" in compiled["cypher"]
    assert "SET n.name = item.name" in compiled["cypher"]
    assert "SET n.age = item.age" in compiled["cypher"]
    assert compiled["params"]["batch"] == data

def test_node_upsert_with_patch():
    """Test node upsert with patch mode."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    data = [{"email": "john@example.com", "name": "John", "age": 30}]
    plan = g.nodes("Person").upsert(data, key="email", patch=True)
    compiled = plan.compile()
    
    assert "case when item.name IS NOT NULL" in compiled["cypher"]
    assert "case when item.age IS NOT NULL" in compiled["cypher"]
    assert "SET n.age = case when item.age IS NOT NULL then item.age else n.age end" in compiled["cypher"]

def test_node_upsert_composite_key():
    """Test node upsert with composite key."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    data = [{"email": "john@example.com", "username": "john", "name": "John"}]
    plan = g.nodes("Person").upsert(data, key=["email", "username"])
    compiled = plan.compile()
    
    assert "MERGE (n:Person {email: item.email, username: item.username})" in compiled["cypher"]

def test_node_upsert_multiple_records():
    """Test node upsert with multiple records."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    data = [
        {"email": "john@example.com", "name": "John", "age": 30},
        {"email": "jane@example.com", "name": "Jane", "age": 25}
    ]
    plan = g.nodes("Person").upsert(data, key="email")
    compiled = plan.compile()
    
    assert compiled["params"]["batch"] == data
    assert len(compiled["params"]["batch"]) == 2

def test_node_upsert_with_null_policy():
    """Test node upsert with different null policies."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    data = [{"email": "john@example.com", "name": "John", "age": None}]
    
    # ignore_nulls (default)
    plan1 = g.nodes("Person").upsert(data, key="email", null_policy="ignore_nulls")
    compiled1 = plan1.compile()
    assert "case when item.age IS NOT NULL" in compiled1["cypher"]
    
    # set_nulls
    plan2 = g.nodes("Person").upsert(data, key="email", null_policy="set_nulls")
    compiled2 = plan2.compile()
    assert "n.age = item.age" in compiled2["cypher"]
    assert "case when" not in compiled2["cypher"]

def test_relationship_upsert_compilation():
    """Test relationship upsert compilation."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    data = [{"email": "john@example.com", "domain": "company.com", "since": 2020, "role": "Engineer"}]
    plan = g.rels("WORKS_AT").upsert(
        data, 
        src=("Person", "email"), 
        dst=("Company", "domain")
    )
    compiled = plan.compile()
    
    assert "UNWIND $batch AS item" in compiled["cypher"]
    assert "MERGE (a:Person {email: item.email})" in compiled["cypher"]
    assert "MERGE (b:Company {domain: item.domain})" in compiled["cypher"]
    assert "MERGE (a)-[r:WORKS_AT]->(b)" in compiled["cypher"]
    assert "SET r.since = item.since" in compiled["cypher"]
    assert "SET r.role = item.role" in compiled["cypher"]

def test_relationship_upsert_with_rel_key():
    """Test relationship upsert with relationship key."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    data = [{"email": "john@example.com", "domain": "company.com", "since": 2020, "role": "Engineer"}]
    plan = g.rels("WORKS_AT").upsert(
        data, 
        src=("Person", "email"), 
        dst=("Company", "domain"),
        rel_key=["since", "role"]
    )
    compiled = plan.compile()
    
    assert "MERGE (a)-[r:WORKS_AT {since: item.since, role: item.role}]->(b)" in compiled["cypher"]


def test_relationship_update_compilation():
    """Test relationship update compilation."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    # Update with where conditions
    query = g.rels("WORKS_AT").where(role="Engineer")
    plan = query.patch(active=True, salary=100000)
    compiled = plan.compile()
    
    assert "MATCH ()-[r:WORKS_AT]->()" in compiled["cypher"]
    assert "WHERE r.role = $where_0" in compiled["cypher"]
    assert "SET r.active = $param_0, r.salary = $param_1" in compiled["cypher"]
    assert compiled["params"]["where_0"] == "Engineer"
    assert compiled["params"]["param_0"] == True
    assert compiled["params"]["param_1"] == 100000


def test_relationship_delete_compilation():
    """Test relationship delete compilation."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    # Delete with where conditions
    query = g.rels("WORKS_AT").where(status="inactive")
    plan = query.delete()
    compiled = plan.compile()
    
    assert "MATCH ()-[r:WORKS_AT]->()" in compiled["cypher"]
    assert "WHERE r.status = $where_0" in compiled["cypher"]
    assert "DELETE r" in compiled["cypher"]
    assert compiled["params"]["where_0"] == "inactive"


def test_relationship_update_with_null_policy():
    """Test relationship update with null policy."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    # ignore_nulls (default) - should ignore None values
    query = g.rels("WORKS_AT").where(role="Engineer")
    plan = query.patch(active=True, last_updated=None)
    compiled = plan.compile()
    
    # Should only set active, not last_updated
    assert "SET r.active = $param_0" in compiled["cypher"]
    assert "last_updated" not in compiled["cypher"]


def test_advanced_inc_update():
    """Test increment update operation."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    query = g.nodes("Person").where(name="John")
    plan = query.inc("score", 10)
    compiled = plan.compile()
    
    assert "MATCH (n:Person)" in compiled["cypher"]
    assert "WHERE n.name = $where_0" in compiled["cypher"]
    assert "SET n.score = coalesce(n.score, 0) + $inc_1" in compiled["cypher"]
    assert compiled["params"]["where_0"] == "John"
    assert compiled["params"]["inc_1"] == 10


def test_advanced_unset_update():
    """Test unset (remove property) operation."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    query = g.nodes("Person").where(status="inactive")
    plan = query.unset("temporary_field")
    compiled = plan.compile()
    
    assert "MATCH (n:Person)" in compiled["cypher"]
    assert "WHERE n.status = $where_0" in compiled["cypher"]
    assert "REMOVE n.temporary_field" in compiled["cypher"]
    assert compiled["params"]["where_0"] == "inactive"


def test_advanced_list_append():
    """Test list append operation."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    query = g.nodes("Person").where(email="john@example.com")
    plan = query.list_append("tags", "premium")
    compiled = plan.compile()
    
    assert "MATCH (n:Person)" in compiled["cypher"]
    assert "WHERE n.email = $where_0" in compiled["cypher"]
    assert "SET n.tags = coalesce(n.tags, []) + $list_1" in compiled["cypher"]
    assert compiled["params"]["where_0"] == "john@example.com"
    assert compiled["params"]["list_1"] == "premium"


def test_advanced_list_remove():
    """Test list remove operation."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    query = g.nodes("Person").where(role="Engineer")
    plan = query.list_remove("tags", "temp")
    compiled = plan.compile()
    
    assert "MATCH (n:Person)" in compiled["cypher"]
    assert "WHERE n.role = $where_0" in compiled["cypher"]
    assert "SET n.tags = [x IN coalesce(n.tags, []) WHERE x <> $list_1]" in compiled["cypher"]
    assert compiled["params"]["where_0"] == "Engineer"
    assert compiled["params"]["list_1"] == "temp"


def test_advanced_map_merge():
    """Test map merge operation."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    query = g.nodes("Person").where(country="US")
    metadata = {"last_updated": "2024-01-01", "source": "import"}
    plan = query.map_merge("metadata", metadata)
    compiled = plan.compile()
    
    assert "MATCH (n:Person)" in compiled["cypher"]
    assert "WHERE n.country = $where_0" in compiled["cypher"]
    assert "SET n.metadata += $map_1" in compiled["cypher"]
    assert compiled["params"]["where_0"] == "US"
    assert compiled["params"]["map_1"] == metadata


def test_advanced_operations_without_where():
    """Test advanced operations without where conditions."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    # Test inc without where
    plan = g.nodes("Person").inc("views", 1)
    compiled = plan.compile()
    assert "MATCH (n:Person)" in compiled["cypher"]
    assert "WHERE" not in compiled["cypher"]  # No WHERE clause
    assert "SET n.views = coalesce(n.views, 0) + $inc_0" in compiled["cypher"]


def test_schema_ensure_unique():
    """Test schema ensure_unique operation."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    plan = g.schema().ensure_unique("Person", "email")
    compiled = plan.compile()
    
    assert "CREATE CONSTRAINT IF NOT EXISTS constraint_Person_email" in compiled["cypher"]
    assert "FOR (n:Person) REQUIRE n.email IS UNIQUE" in compiled["cypher"]


def test_schema_ensure_index():
    """Test schema ensure_index operation."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    plan = g.schema().ensure_index("Person", "name")
    compiled = plan.compile()
    
    assert "CREATE INDEX IF NOT EXISTS index_Person_name" in compiled["cypher"]
    assert "FOR (n:Person) ON (n.name)" in compiled["cypher"]


def test_schema_ensure_node_key():
    """Test schema ensure_node_key operation."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    plan = g.schema().ensure_node_key("Person", ["email", "username"])
    compiled = plan.compile()
    
    assert "CREATE CONSTRAINT IF NOT EXISTS constraint_Person_email_username" in compiled["cypher"]
    assert "FOR (n:Person) REQUIRE (n.email, n.username) IS NODE KEY" in compiled["cypher"]


def test_schema_drop_unique():
    """Test schema drop_unique operation."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    plan = g.schema().drop_unique("Person", "email")
    compiled = plan.compile()
    
    assert "DROP CONSTRAINT IF EXISTS constraint_Person_email" in compiled["cypher"]


def test_schema_drop_index():
    """Test schema drop_index operation."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    plan = g.schema().drop_index("Person", "name")
    compiled = plan.compile()
    
    assert "DROP INDEX IF EXISTS index_Person_name" in compiled["cypher"]


def test_schema_ensure_unique():
    """Test schema ensure_unique operation."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    plan = g.schema().ensure_unique("Person", "email")
    compiled = plan.compile()
    
    assert "CREATE CONSTRAINT IF NOT EXISTS constraint_Person_email" in compiled["cypher"]
    assert "FOR (n:Person) REQUIRE n.email IS UNIQUE" in compiled["cypher"]


def test_schema_ensure_index():
    """Test schema ensure_index operation."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    plan = g.schema().ensure_index("Person", "name")
    compiled = plan.compile()
    
    assert "CREATE INDEX IF NOT EXISTS index_Person_name" in compiled["cypher"]
    assert "FOR (n:Person) ON (n.name)" in compiled["cypher"]


def test_schema_ensure_node_key():
    """Test schema ensure_node_key operation."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    plan = g.schema().ensure_node_key("Person", ["email", "username"])
    compiled = plan.compile()
    
    assert "CREATE CONSTRAINT IF NOT EXISTS constraint_Person_email_username" in compiled["cypher"]
    assert "FOR (n:Person) REQUIRE (n.email, n.username) IS NODE KEY" in compiled["cypher"]


def test_schema_drop_unique():
    """Test schema drop_unique operation."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    plan = g.schema().drop_unique("Person", "email")
    compiled = plan.compile()
    
    assert "DROP CONSTRAINT IF EXISTS constraint_Person_email" in compiled["cypher"]


def test_schema_drop_index():
    """Test schema drop_index operation."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    plan = g.schema().drop_index("Person", "name")
    compiled = plan.compile()
    
    assert "DROP INDEX IF EXISTS index_Person_name" in compiled["cypher"]


def test_schema_ensure_unique():
    """Test schema ensure_unique operation."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    plan = g.schema().ensure_unique("Person", "email")
    compiled = plan.compile()
    
    assert "CREATE CONSTRAINT IF NOT EXISTS constraint_Person_email" in compiled["cypher"]
    assert "FOR (n:Person) REQUIRE n.email IS UNIQUE" in compiled["cypher"]


def test_schema_ensure_index():
    """Test schema ensure_index operation."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    plan = g.schema().ensure_index("Person", "name")
    compiled = plan.compile()
    
    assert "CREATE INDEX IF NOT EXISTS index_Person_name" in compiled["cypher"]
    assert "FOR (n:Person) ON (n.name)" in compiled["cypher"]


def test_schema_ensure_node_key():
    """Test schema ensure_node_key operation."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    plan = g.schema().ensure_node_key("Person", ["email", "username"])
    compiled = plan.compile()
    
    assert "CREATE CONSTRAINT IF NOT EXISTS constraint_Person_email_username" in compiled["cypher"]
    assert "FOR (n:Person) REQUIRE (n.email, n.username) IS NODE KEY" in compiled["cypher"]


def test_schema_drop_unique():
    """Test schema drop_unique operation."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    plan = g.schema().drop_unique("Person", "email")
    compiled = plan.compile()
    
    assert "DROP CONSTRAINT IF EXISTS constraint_Person_email" in compiled["cypher"]


def test_schema_drop_index():
    """Test schema drop_index operation."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    plan = g.schema().drop_index("Person", "name")
    compiled = plan.compile()
    
    assert "DROP INDEX IF EXISTS index_Person_name" in compiled["cypher"]
    assert compiled["params"] == {}

def test_relationship_upsert_composite_keys():
    """Test relationship upsert with composite keys."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    data = [{"email": "john@example.com", "username": "john", "domain": "company.com", "name": "Acme"}]
    plan = g.rels("WORKS_AT").upsert(
        data, 
        src=("Person", ["email", "username"]), 
        dst=("Company", ["domain", "name"])
    )
    compiled = plan.compile()
    
    assert "MERGE (a:Person {email: item.email, username: item.username})" in compiled["cypher"]
    assert "MERGE (b:Company {domain: item.domain, name: item.name})" in compiled["cypher"]

def test_node_update_compilation():
    """Test node update compilation."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    # Update with where conditions
    query = g.nodes("Person").where(country="US")
    plan = query.patch(active=True, last_updated="2024-01-01")
    compiled = plan.compile()
    
    assert "MATCH (n:Person)" in compiled["cypher"]
    assert "WHERE n.country = $where_0" in compiled["cypher"]
    assert "SET n.active = $param_0, n.last_updated = $param_1" in compiled["cypher"]
    assert compiled["params"]["where_0"] == "US"
    assert compiled["params"]["param_0"] == True
    assert compiled["params"]["param_1"] == "2024-01-01"

def test_node_update_with_null_policy():
    """Test node update with null policy."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    # ignore_nulls (default) - should ignore None values
    query = g.nodes("Person").where(country="US")
    plan = query.patch(active=True, last_updated=None)
    compiled = plan.compile()
    
    # Should only set active, not last_updated
    assert "SET n.active = $param_0" in compiled["cypher"]
    assert "last_updated" not in compiled["cypher"]
    assert "param_1" not in compiled["params"]

def test_node_delete_compilation():
    """Test node delete compilation."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    # Delete with where conditions
    query = g.nodes("Person").where(country="US", status="inactive")
    plan = query.delete()
    compiled = plan.compile()
    
    assert "MATCH (n:Person)" in compiled["cypher"]
    assert "WHERE n.country = $where_0 AND n.status = $where_1" in compiled["cypher"]
    assert "DELETE n" in compiled["cypher"]
    assert compiled["params"]["where_0"] == "US"
    assert compiled["params"]["where_1"] == "inactive"

def test_node_delete_with_detach():
    """Test node delete with detach."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    query = g.nodes("Person").where(status="inactive")
    plan = query.delete(detach=True)
    compiled = plan.compile()
    
    assert "DETACH DELETE n" in compiled["cypher"]

def test_node_delete_with_null_operations():
    """Test node delete with NULL operations in where clause."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    query = g.nodes("Person").where(email__is_null=True)
    plan = query.delete()
    compiled = plan.compile()
    
    assert "WHERE n.email IS NULL" in compiled["cypher"]
    assert compiled["params"] == {}

def test_write_plan_stats():
    """Test that write plans include statistics."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    data = [
        {"email": "john@example.com", "name": "John"},
        {"email": "jane@example.com", "name": "Jane"}
    ]
    plan = g.nodes("Person").upsert(data, key="email")
    compiled = plan.compile()
    
    assert "stats" in compiled
    assert compiled["stats"]["nodes_processed"] == 2
    assert compiled["stats"]["batches"] == 1

def test_empty_upsert():
    """Test upsert with empty data."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    plan = g.nodes("Person").upsert([], key="email")
    compiled = plan.compile()
    
    assert "// No data to upsert" in compiled["cypher"]
    assert compiled["params"] == {}

def test_single_record_upsert():
    """Test upsert with single record (not list)."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    plan = g.nodes("Person").upsert({"email": "john@example.com", "name": "John"}, key="email")
    compiled = plan.compile()
    
    assert compiled["params"]["batch"] == [{"email": "john@example.com", "name": "John"}]

def test_relationship_upsert_missing_key_fields():
    """Test that relationship upsert validates key fields."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    data = [{"domain": "company.com"}]  # Missing email field
    
    with pytest.raises(Exception) as exc_info:
        plan = g.rels("WORKS_AT").upsert(
            data, 
            src=("Person", "email"), 
            dst=("Company", "domain")
        )
        plan.compile()
    
    assert "Source key field 'email' not found" in str(exc_info.value)

def test_node_upsert_missing_key_fields():
    """Test that node upsert validates key fields."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    data = [{"name": "John"}]  # Missing email field
    
    with pytest.raises(Exception) as exc_info:
        plan = g.nodes("Person").upsert(data, key="email")
        plan.compile()
    
    assert "Key field 'email' not found" in str(exc_info.value)

def test_complex_update_with_multiple_conditions():
    """Test update with multiple where conditions."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    query = g.nodes("Person").where(age__gte=18, country="US", status="active")
    plan = query.patch(verified=True)
    compiled = plan.compile()
    
    assert "WHERE n.age >= $where_0 AND n.country = $where_1 AND n.status = $where_2" in compiled["cypher"]
    assert compiled["params"]["where_0"] == 18
    assert compiled["params"]["where_1"] == "US"
    assert compiled["params"]["where_2"] == "active"


def test_relationship_uniqueness_policy_default():
    """Test relationship uniqueness policy with default (single_edge_per_pair)."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    data = [{"email": "john@example.com", "domain": "company.com", "role": "Engineer"}]
    
    # Should work without rel_key (default policy)
    plan = g.rels("WORKS_AT").upsert(
        data,
        src=("Person", "email"),
        dst=("Company", "domain")
    )
    compiled = plan.compile()
    
    assert "UNWIND $batch AS item" in compiled["cypher"]
    assert "MERGE (a:Person {email: item.email})" in compiled["cypher"]
    assert "MERGE (b:Company {domain: item.domain})" in compiled["cypher"]
    assert "MERGE (a)-[r:WORKS_AT]->(b)" in compiled["cypher"]


def test_relationship_uniqueness_policy_require_rel_key():
    """Test relationship uniqueness policy with require_rel_key."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"), rel_uniqueness_policy="require_rel_key")
    
    data = [{"email": "john@example.com", "domain": "company.com", "role": "Engineer"}]
    
    # Should fail without rel_key
    plan = g.rels("WORKS_AT").upsert(
        data,
        src=("Person", "email"),
        dst=("Company", "domain")
    )
    
    with pytest.raises(WriteError, match="requires rel_key for uniqueness"):
        plan.compile()


def test_relationship_uniqueness_policy_with_rel_key():
    """Test relationship uniqueness policy works with rel_key."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"), rel_uniqueness_policy="require_rel_key")
    
    data = [{"email": "john@example.com", "domain": "company.com", "role": "Engineer", "since": 2020}]
    
    # Should work with rel_key
    plan = g.rels("WORKS_AT").upsert(
        data,
        src=("Person", "email"),
        dst=("Company", "domain"),
        rel_key="role"
    )
    compiled = plan.compile()
    
    assert "MERGE (a)-[r:WORKS_AT {role: item.role}]->(b)" in compiled["cypher"]


def test_advanced_inc_update():
    """Test increment update operation."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    query = g.nodes("Person").where(name="John")
    plan = query.inc("score", 10)
    compiled = plan.compile()
    
    assert "MATCH (n:Person)" in compiled["cypher"]
    assert "WHERE n.name = $where_0" in compiled["cypher"]
    assert "SET n.score = coalesce(n.score, 0) + $inc_1" in compiled["cypher"]
    assert compiled["params"]["where_0"] == "John"
    assert compiled["params"]["inc_1"] == 10


def test_advanced_unset_update():
    """Test unset (remove property) operation."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    query = g.nodes("Person").where(status="inactive")
    plan = query.unset("temporary_field")
    compiled = plan.compile()
    
    assert "MATCH (n:Person)" in compiled["cypher"]
    assert "WHERE n.status = $where_0" in compiled["cypher"]
    assert "REMOVE n.temporary_field" in compiled["cypher"]
    assert compiled["params"]["where_0"] == "inactive"


def test_advanced_list_append():
    """Test list append operation."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    query = g.nodes("Person").where(email="john@example.com")
    plan = query.list_append("tags", "premium")
    compiled = plan.compile()
    
    assert "MATCH (n:Person)" in compiled["cypher"]
    assert "WHERE n.email = $where_0" in compiled["cypher"]
    assert "SET n.tags = coalesce(n.tags, []) + $list_1" in compiled["cypher"]
    assert compiled["params"]["where_0"] == "john@example.com"
    assert compiled["params"]["list_1"] == "premium"


def test_advanced_list_remove():
    """Test list remove operation."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    query = g.nodes("Person").where(role="Engineer")
    plan = query.list_remove("tags", "temp")
    compiled = plan.compile()
    
    assert "MATCH (n:Person)" in compiled["cypher"]
    assert "WHERE n.role = $where_0" in compiled["cypher"]
    assert "SET n.tags = [x IN coalesce(n.tags, []) WHERE x <> $list_1]" in compiled["cypher"]
    assert compiled["params"]["where_0"] == "Engineer"
    assert compiled["params"]["list_1"] == "temp"


def test_advanced_map_merge():
    """Test map merge operation."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    query = g.nodes("Person").where(country="US")
    metadata = {"last_updated": "2024-01-01", "source": "import"}
    plan = query.map_merge("metadata", metadata)
    compiled = plan.compile()
    
    assert "MATCH (n:Person)" in compiled["cypher"]
    assert "WHERE n.country = $where_0" in compiled["cypher"]
    assert "SET n.metadata += $map_1" in compiled["cypher"]
    assert compiled["params"]["where_0"] == "US"
    assert compiled["params"]["map_1"] == metadata


def test_advanced_operations_without_where():
    """Test advanced operations without where conditions."""
    g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    
    # Test inc without where
    plan = g.nodes("Person").inc("views", 1)
    compiled = plan.compile()
    assert "MATCH (n:Person)" in compiled["cypher"]
    assert "WHERE" not in compiled["cypher"]  # No WHERE clause
    assert "SET n.views = coalesce(n.views, 0) + $inc_0" in compiled["cypher"]
