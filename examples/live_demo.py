"""
Live Demo with Mock Data for GraphFrame-Neo4j

This example demonstrates the library's functionality by showing the actual
Cypher queries that would be generated for various operations.
"""

from graphframe_neo4j import Graph
from graphframe_neo4j.frames.nodeframe import NodeFrame
from graphframe_neo4j.frames.edgeframe import EdgeFrame
from graphframe_neo4j.write.writeplan import WritePlan
import json


def print_compiled_query(description, frame_or_plan):
    """Helper to print compiled Cypher queries."""
    print(f"\n{description}")
    print("-" * 60)
    compiled = frame_or_plan.compile()
    print(f"Cypher Query:\n{compiled['cypher']}")
    if compiled['params']:
        print(f"Parameters:\n{json.dumps(compiled['params'], indent=2)}")
    else:
        print("Parameters: None")
    print()


def demo_node_operations():
    """Demonstrate node operations with actual query compilation."""
    print("📊 NODE OPERATIONS DEMO")
    print("=" * 80)
    
    g = Graph("bolt://localhost:7687", ("neo4j", "password"))
    
    # 1. Basic node query
    people = g.nodes("Person")
    print_compiled_query("1. Basic node query", people)
    
    # 2. Query with filters
    adults = people.where(age__gte=18, country="US")
    print_compiled_query("2. Query with multiple filters", adults)
    
    # 3. String operations
    johns = people.where(name__contains="John", email__endswith="@example.com")
    print_compiled_query("3. String operations (contains, endswith)", johns)
    
    # 4. NULL operations
    active_users = people.where(last_login__not_null=True, deleted_at__is_null=True)
    print_compiled_query("4. NULL operations (not_null, is_null)", active_users)
    
    # 5. IN operations
    countries = people.where(country__in=["US", "UK", "Canada"])
    print_compiled_query("5. IN operation", countries)
    
    # 6. Regex operations
    company_emails = people.where(email__regex=r"^.*@company\\.com$")
    print_compiled_query("6. Regex operation", company_emails)
    
    # 7. Complex query with ordering and limits
    top_10 = people.where(status="active").order_by("age__desc", "name__asc").limit(10).offset(0)
    print_compiled_query("7. Complex query with ordering and limits", top_10)
    
    # 8. Field selection
    names_only = people.select("name", "email")
    print_compiled_query("8. Field selection", names_only)


def demo_relationship_operations():
    """Demonstrate relationship operations with actual query compilation."""
    print("🔗 RELATIONSHIP OPERATIONS DEMO")
    print("=" * 80)
    
    g = Graph("bolt://localhost:7687", ("neo4j", "password"))
    
    # 1. Basic relationship query
    works_at = g.rels("WORKS_AT")
    print_compiled_query("1. Basic relationship query", works_at)
    
    # 2. Filtered relationship query
    recent_works = works_at.where(since__gte=2020, role="Engineer")
    print_compiled_query("2. Filtered relationship query", recent_works)
    
    # 3. Relationship with limit
    limited_rels = works_at.limit(50)
    print_compiled_query("3. Relationship query with limit", limited_rels)


def demo_traversal_operations():
    """Demonstrate traversal operations with actual query compilation."""
    print("🧭 TRAVERSAL OPERATIONS DEMO")
    print("=" * 80)
    
    g = Graph("bolt://localhost:7687", ("neo4j", "password"))
    
    # 1. Basic traversal
    people = g.nodes("Person")
    path = people.traverse("WORKS_AT", to="Company", direction="out")
    print_compiled_query("1. Basic traversal (outgoing)", path)
    
    # 2. Incoming traversal
    incoming = people.traverse("FOLLOWS", to="Person", direction="in")
    print_compiled_query("2. Incoming traversal", incoming)
    
    # 3. Bidirectional traversal
    bidirectional = people.traverse("KNOWS", to="Person", direction="both")
    print_compiled_query("3. Bidirectional traversal", bidirectional)
    
    # 4. Traversal with custom aliases
    path_with_aliases = people.traverse(
        "WORKS_AT", 
        to="Company", 
        direction="out", 
        alias=("person", "works", "company")
    )
    print_compiled_query("4. Traversal with custom aliases", path_with_aliases)
    
    # 5. Filtered traversal
    filtered_path = path.where(
        rel__since__gte=2020,
        company__city="San Francisco"
    )
    print_compiled_query("5. Filtered traversal", filtered_path)
    
    # 6. Back to originating nodes
    back_query = filtered_path.back()
    print_compiled_query("6. Back to originating nodes", back_query)


def demo_write_operations():
    """Demonstrate write operations with actual query compilation."""
    print("✍️ WRITE OPERATIONS DEMO")
    print("=" * 80)
    
    g = Graph("bolt://localhost:7687", ("neo4j", "password"))
    
    # 1. Node upsert
    person_data = [
        {"email": "john@example.com", "name": "John Doe", "age": 30, "country": "US"},
        {"email": "jane@example.com", "name": "Jane Smith", "age": 25, "country": "UK"}
    ]
    
    upsert_plan = g.nodes("Person").upsert(person_data, key="email")
    print_compiled_query("1. Node upsert (batch)", upsert_plan)
    
    # 2. Node patch/update
    patch_plan = g.nodes("Person").where(country="US").patch(status="active")
    print_compiled_query("2. Node patch/update", patch_plan)
    
    # 3. Node delete
    delete_plan = g.nodes("Person").where(status="inactive").delete()
    print_compiled_query("3. Node delete", delete_plan)
    
    # 4. Advanced operations
    inc_plan = g.nodes("Product").where(category="Electronics").inc("views", 1)
    print_compiled_query("4. Increment operation", inc_plan)
    
    list_append_plan = g.nodes("User").where(email="john@example.com").list_append("tags", "premium")
    print_compiled_query("5. List append operation", list_append_plan)
    
    list_remove_plan = g.nodes("User").where(role="Engineer").list_remove("tags", "temp")
    print_compiled_query("6. List remove operation", list_remove_plan)
    
    metadata = {"last_updated": "2024-01-01", "source": "import"}
    map_merge_plan = g.nodes("Document").where(status="active").map_merge("metadata", metadata)
    print_compiled_query("7. Map merge operation", map_merge_plan)
    
    unset_plan = g.nodes("User").where(status="inactive").unset("temp_data")
    print_compiled_query("8. Unset operation", unset_plan)


def demo_relationship_write_operations():
    """Demonstrate relationship write operations with actual query compilation."""
    print("🔄 RELATIONSHIP WRITE OPERATIONS DEMO")
    print("=" * 80)
    
    g = Graph("bolt://localhost:7687", ("neo4j", "password"))
    
    # 1. Relationship upsert
    rel_data = [
        {
            "email": "john@example.com", 
            "domain": "company.com", 
            "role": "Engineer", 
            "since": 2020
        }
    ]
    
    rel_upsert_plan = g.rels("WORKS_AT").upsert(
        rel_data,
        src=("Person", "email"),
        dst=("Company", "domain"),
        rel_key="role"
    )
    print_compiled_query("1. Relationship upsert", rel_upsert_plan)
    
    # 2. Relationship update
    rel_update_plan = g.rels("WORKS_AT").where(role="Engineer").patch(salary=100000)
    print_compiled_query("2. Relationship update", rel_update_plan)
    
    # 3. Relationship delete
    rel_delete_plan = g.rels("WORKS_AT").where(since__lt=2010).delete()
    print_compiled_query("3. Relationship delete", rel_delete_plan)


def demo_schema_operations():
    """Demonstrate schema operations with actual query compilation."""
    print("📋 SCHEMA OPERATIONS DEMO")
    print("=" * 80)
    
    g = Graph("bolt://localhost:7687", ("neo4j", "password"))
    schema = g.schema()
    
    # 1. Create index
    index_plan = schema.ensure_index("Person", "email")
    print_compiled_query("1. Create index", index_plan)
    
    # 2. Create unique constraint
    unique_plan = schema.ensure_unique("Person", "email")
    print_compiled_query("2. Create unique constraint", unique_plan)
    
    # 3. Create node key (composite)
    node_key_plan = schema.ensure_node_key("Person", ["email", "country"])
    print_compiled_query("3. Create composite node key", node_key_plan)
    
    # 4. Drop index
    drop_index_plan = schema.drop_index("Person", "country")
    print_compiled_query("4. Drop index", drop_index_plan)
    
    # 5. Drop unique constraint
    drop_unique_plan = schema.drop_unique("Product", "sku")
    print_compiled_query("5. Drop unique constraint", drop_unique_plan)


def demo_complex_scenarios():
    """Demonstrate complex real-world scenarios."""
    print("🎯 COMPLEX SCENARIOS DEMO")
    print("=" * 80)
    
    g = Graph("bolt://localhost:7687", ("neo4j", "password"))
    
    # Scenario 1: User recommendation system
    print("\n🔍 Scenario 1: User Recommendation System")
    print("-" * 60)
    
    # Find active users in US who are engineers
    engineers = g.nodes("Person").where(
        country="US",
        role="Engineer",
        status="active"
    )
    
    # Find companies they work at
    companies = engineers.traverse("WORKS_AT", to="Company", direction="out")
    
    # Find other engineers at the same companies (using the company nodes)
    # We need to start from the original graph for the second traversal
    colleagues = g.nodes("Company").traverse("WORKS_AT", to="Person", direction="in").where(
        rel__role="Engineer",
        to__email__ne="john@example.com"  # Exclude current user
    )
    
    print_compiled_query("Recommend colleagues", colleagues)
    
    # Scenario 2: Product catalog management
    print("\n🛒 Scenario 2: Product Catalog Management")
    print("-" * 60)
    
    # Find electronics products with low inventory
    low_inventory = g.nodes("Product").where(
        category="Electronics",
        inventory__lt=10,
        status="active"
    )
    
    # Update them to be featured
    featured_plan = low_inventory.patch(featured=True, last_updated="2024-01-01")
    print_compiled_query("Feature low inventory products", featured_plan)
    
    # Scenario 3: User activity analysis
    print("\n📊 Scenario 3: User Activity Analysis")
    print("-" * 60)
    
    # Find active users with recent activity
    active_users = g.nodes("User").where(
        last_login__gte="2024-01-01",
        status="active"
    )
    
    # Get their recent purchases
    purchases = active_users.traverse("PURCHASED", to="Product", direction="out").where(
        rel__date__gte="2024-01-01"
    )
    
    print_compiled_query("Recent purchases by active users", purchases)


def main():
    """Run the live demo."""
    print("🚀 GRAPHFRAME-NEO4J LIVE DEMO")
    print("=" * 80)
    print("This demo shows the actual Cypher queries generated by GraphFrame-Neo4j")
    print("for various operations. No database connection is required.")
    print()
    
    demo_node_operations()
    demo_relationship_operations()
    demo_traversal_operations()
    demo_write_operations()
    demo_relationship_write_operations()
    demo_schema_operations()
    demo_complex_scenarios()
    
    print("🎉 DEMO COMPLETED!")
    print("=" * 80)
    print()
    print("Key Takeaways:")
    print("• GraphFrame-Neo4j provides a pandas-like API for Neo4j")
    print("• All queries are parameterized for security")
    print("• Complex graph operations are simplified")
    print("• Write operations are explicit and require commit()")
    print("• Schema management is built-in")
    print()
    print("To try these with a real database:")
    print("1. Start Neo4j: docker-compose up -d")
    print("2. Run integration tests: uv run pytest -m integration -q")
    print("3. Or use the examples in your own code with real connections")


if __name__ == "__main__":
    main()