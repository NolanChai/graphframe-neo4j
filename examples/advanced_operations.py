"""
Advanced Operations Examples for GraphFrame-Neo4j

This example demonstrates advanced query patterns and operations.
"""

from graphframe_neo4j import Graph


def demo_complex_filters():
    """Demonstrate complex filtering operations."""
    print("=== Complex Filter Examples ===")
    
    g = Graph("bolt://localhost:7687", ("neo4j", "password"))
    people = g.nodes("Person")
    
    # Example 1: Multiple conditions
    query1 = people.where(
        age__gte=18,
        age__lte=65,
        country__in=["US", "UK", "Canada"],
        status="active"
    )
    print(f"Complex AND query: {query1}")
    
    # Example 2: String operations
    query2 = people.where(
        name__contains="John",
        email__endswith="@example.com"
    )
    print(f"String operations: {query2}")
    
    # Example 3: NULL operations
    query3 = people.where(
        last_login__not_null=True,
        deleted_at__is_null=True
    )
    print(f"NULL operations: {query3}")
    
    # Example 4: Regex operations
    query4 = people.where(
        email__regex=r"^.*@company\\.com$"
    )
    print(f"Regex operation: {query4}")
    
    print()


def demo_advanced_updates():
    """Demonstrate advanced update operations."""
    print("=== Advanced Update Examples ===")
    
    g = Graph("bolt://localhost:7687", ("neo4j", "password"))
    
    # Example 1: Increment with conditions
    inc_query = g.nodes("Product").where(category="Electronics").inc("views", 1)
    print(f"Increment views: {inc_query}")
    
    # Example 2: List operations
    list_query = g.nodes("User").where(email="john@example.com")
    append_plan = list_query.list_append("purchase_history", "product_123")
    remove_plan = list_query.list_remove("wishlist", "product_456")
    print(f"List append: {append_plan}")
    print(f"List remove: {remove_plan}")
    
    # Example 3: Map merge
    metadata = {"last_updated": "2024-01-01", "source": "import"}
    merge_plan = g.nodes("Document").where(status="active").map_merge("metadata", metadata)
    print(f"Map merge: {merge_plan}")
    
    # Example 4: Unset properties
    unset_plan = g.nodes("User").where(status="inactive").unset("temp_data")
    print(f"Unset property: {unset_plan}")
    
    print()


def demo_traversal_patterns():
    """Demonstrate various traversal patterns."""
    print("=== Traversal Pattern Examples ===")
    
    g = Graph("bolt://localhost:7687", ("neo4j", "password"))
    
    # Example 1: Outgoing relationships
    outgoing = g.nodes("Person").traverse("FOLLOWS", to="Person", direction="out")
    print(f"Outgoing traversal: {outgoing}")
    
    # Example 2: Incoming relationships
    incoming = g.nodes("Person").traverse("FOLLOWS", to="Person", direction="in")
    print(f"Incoming traversal: {incoming}")
    
    # Example 3: Bidirectional relationships
    bidirectional = g.nodes("Person").traverse("KNOWS", to="Person", direction="both")
    print(f"Bidirectional traversal: {bidirectional}")
    
    # Example 4: Traversal with filtering
    filtered_traversal = outgoing.where(
        rel__since__gte=2020,
        to__status="active"
    )
    print(f"Filtered traversal: {filtered_traversal}")
    
    # Example 5: Back to origin with filtering
    back_query = filtered_traversal.back()
    print(f"Back query: {back_query}")
    
    print()


def demo_batch_operations():
    """Demonstrate batch operations and write plans."""
    print("=== Batch Operation Examples ===")
    
    g = Graph("bolt://localhost:7687", ("neo4j", "password"))
    
    # Example 1: Large batch upsert
    large_batch = [
        {"email": f"user{i}@example.com", "name": f"User {i}", "age": 20 + (i % 50)}
        for i in range(1000)
    ]
    
    upsert_plan = g.nodes("User").upsert(large_batch, key="email", batch_size=500)
    print(f"Large batch upsert plan: {upsert_plan}")
    
    # Example 2: Write plan operations
    plan = g.nodes("Product").where(category="Electronics").patch(status="featured")
    
    # Show all write plan methods
    compiled = plan.compile()
    print(f"Compiled write plan: {compiled['cypher']}")
    
    # Preview (would show sample data in real usage)
    preview = plan.preview()
    print(f"Write plan preview: {preview}")
    
    print()


def demo_schema_management():
    """Demonstrate comprehensive schema management."""
    print("=== Schema Management Examples ===")
    
    g = Graph("bolt://localhost:7687", ("neo4j", "password"))
    schema = g.schema()
    
    # Example 1: Create multiple indexes
    index1 = schema.ensure_index("Person", "email")
    index2 = schema.ensure_index("Person", "country")
    print(f"Index 1: {index1}")
    print(f"Index 2: {index2}")
    
    # Example 2: Create unique constraints
    unique1 = schema.ensure_unique("Product", "sku")
    unique2 = schema.ensure_unique("User", "username")
    print(f"Unique 1: {unique1}")
    print(f"Unique 2: {unique2}")
    
    # Example 3: Composite node keys
    composite_key = schema.ensure_node_key("Order", ["order_id", "customer_id"])
    print(f"Composite key: {composite_key}")
    
    # Example 4: Drop constraints
    drop_unique = schema.drop_unique("Product", "sku")
    drop_index = schema.drop_index("Person", "country")
    print(f"Drop unique: {drop_unique}")
    print(f"Drop index: {drop_index}")
    
    print()


def demo_error_handling():
    """Demonstrate error handling patterns."""
    print("=== Error Handling Examples ===")
    
    g = Graph("bolt://localhost:7687", ("neo4j", "password"))
    
    # Example 1: Missing key in upsert (would raise error)
    try:
        # This would fail because 'key' is required
        bad_data = [{"name": "John"}]  # No key field
        # g.nodes("Person").upsert(bad_data, key="email")  # Would raise error
        print("Missing key error: Would raise error if uncommented")
    except Exception as e:
        print(f"Caught error: {e}")
    
    # Example 2: Invalid filter operations
    try:
        # This would fail because 'invalid_op' is not a valid operator
        # people.where(name__invalid_op="John")
        print("Invalid operator error: Would raise error if uncommented")
    except Exception as e:
        print(f"Caught error: {e}")
    
    print()


def demo_performance_patterns():
    """Demonstrate performance optimization patterns."""
    print("=== Performance Pattern Examples ===")
    
    g = Graph("bolt://localhost:7687", ("neo4j", "password"))
    
    # Example 1: Efficient filtering with indexes
    # First ensure index exists, then query
    schema = g.schema()
    schema.ensure_index("Person", "email")
    
    efficient_query = g.nodes("Person").where(email="john@example.com")
    print(f"Indexed query: {efficient_query}")
    
    # Example 2: Select only needed fields
    minimal_query = g.nodes("Person").select("email", "name")
    print(f"Minimal field selection: {minimal_query}")
    
    # Example 3: Use limits for large datasets
    limited_query = g.nodes("Person").limit(100)
    print(f"Limited query: {limited_query}")
    
    # Example 4: Batch processing
    batch_data = [{"email": f"user{i}@example.com"} for i in range(10000)]
    batch_plan = g.nodes("User").upsert(batch_data, key="email", batch_size=1000)
    print(f"Batch processing plan: {batch_plan}")
    
    print()


def main():
    """Run all advanced examples."""
    print("GraphFrame-Neo4j Advanced Examples")
    print("=" * 50)
    
    demo_complex_filters()
    demo_advanced_updates()
    demo_traversal_patterns()
    demo_batch_operations()
    demo_schema_management()
    demo_error_handling()
    demo_performance_patterns()
    
    print("All advanced examples completed!")
    print("\nThese examples demonstrate the full power of GraphFrame-Neo4j's API.")
    print("The library provides a pandas-like interface with Neo4j's graph capabilities.")


if __name__ == "__main__":
    main()