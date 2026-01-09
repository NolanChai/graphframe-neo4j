"""
Basic Usage Examples for GraphFrame-Neo4j

This example demonstrates the core functionality of GraphFrame-Neo4j
using mock data to show how the API works.
"""

from graphframe_neo4j import Graph
from graphframe_neo4j.frames.nodeframe import NodeFrame
from graphframe_neo4j.frames.edgeframe import EdgeFrame
from graphframe_neo4j.write.writeplan import WritePlan


def demo_node_queries():
    """Demonstrate node querying capabilities."""
    print("=== Node Query Examples ===")
    
    # Create a mock graph (won't actually connect)
    g = Graph("bolt://localhost:7687", ("neo4j", "password"))
    
    # Example 1: Basic node query
    people = g.nodes("Person")
    print(f"Created NodeFrame: {people}")
    
    # Example 2: Query with filters
    adults = people.where(age__gte=18, country="US")
    print(f"Filtered query: {adults}")
    
    # Example 3: Select specific fields
    names_only = people.select("name", "email")
    print(f"Selected fields: {names_only}")
    
    # Example 4: Add ordering and limits
    top_10 = people.order_by("age__desc").limit(10)
    print(f"Ordered and limited: {top_10}")
    
    # Example 5: Compile to see the Cypher that would be generated
    compiled = people.where(name__contains="John").compile()
    print(f"Compiled Cypher: {compiled['cypher']}")
    print(f"Parameters: {compiled['params']}")
    
    print()


def demo_relationship_queries():
    """Demonstrate relationship querying capabilities."""
    print("=== Relationship Query Examples ===")
    
    g = Graph("bolt://localhost:7687", ("neo4j", "password"))
    
    # Example 1: Basic relationship query
    works_at = g.rels("WORKS_AT")
    print(f"Created EdgeFrame: {works_at}")
    
    # Example 2: Filter relationships
    recent_works = works_at.where(since__gte=2020)
    print(f"Filtered relationships: {recent_works}")
    
    # Example 3: Compile relationship query
    compiled = works_at.where(role="Engineer").compile()
    print(f"Compiled relationship Cypher: {compiled['cypher']}")
    
    print()


def demo_write_operations():
    """Demonstrate write operations with mock data."""
    print("=== Write Operation Examples ===")
    
    g = Graph("bolt://localhost:7687", ("neo4j", "password"))
    
    # Example 1: Node upsert
    person_data = [
        {"email": "john@example.com", "name": "John Doe", "age": 30, "country": "US"},
        {"email": "jane@example.com", "name": "Jane Smith", "age": 25, "country": "UK"}
    ]
    
    upsert_plan = g.nodes("Person").upsert(person_data, key="email")
    print(f"Created upsert plan: {upsert_plan}")
    
    # Show what the compiled Cypher would look like
    compiled_upsert = upsert_plan.compile()
    print(f"Upsert Cypher: {compiled_upsert['cypher']}")
    
    # Example 2: Node patch/update
    patch_plan = g.nodes("Person").where(country="US").patch(status="active")
    print(f"Created patch plan: {patch_plan}")
    
    # Example 3: Advanced operations
    inc_plan = g.nodes("Person").where(role="Engineer").inc("score", 10)
    print(f"Increment plan: {inc_plan}")
    
    list_append_plan = g.nodes("Person").where(email="john@example.com").list_append("tags", "premium")
    print(f"List append plan: {list_append_plan}")
    
    print()


def demo_traversal_operations():
    """Demonstrate traversal capabilities."""
    print("=== Traversal Examples ===")
    
    g = Graph("bolt://localhost:7687", ("neo4j", "password"))
    
    # Example 1: Basic traversal
    people = g.nodes("Person")
    path = people.traverse("WORKS_AT", to="Company", direction="out")
    print(f"Created traversal path: {path}")
    
    # Example 2: Traversal with custom aliases
    path_with_aliases = people.traverse(
        "WORKS_AT", 
        to="Company", 
        direction="out", 
        alias=("person", "works", "company")
    )
    print(f"Traversal with aliases: {path_with_aliases}")
    
    # Example 3: Filter on traversal
    filtered_path = path.where(company__city="San Francisco")
    print(f"Filtered traversal: {filtered_path}")
    
    # Example 4: Back to originating nodes
    back_query = path.back()
    print(f"Back query: {back_query}")
    
    print()


def demo_schema_operations():
    """Demonstrate schema management operations."""
    print("=== Schema Operation Examples ===")
    
    g = Graph("bolt://localhost:7687", ("neo4j", "password"))
    schema = g.schema()
    
    # Example 1: Create index
    index_plan = schema.ensure_index("Person", "email")
    print(f"Index creation plan: {index_plan}")
    
    # Example 2: Create unique constraint
    unique_plan = schema.ensure_unique("Person", "email")
    print(f"Unique constraint plan: {unique_plan}")
    
    # Example 3: Create node key (composite key)
    node_key_plan = schema.ensure_node_key("Person", ["email", "country"])
    print(f"Node key plan: {node_key_plan}")
    
    print()


def demo_optional_extras():
    """Demonstrate optional extras (pandas, networkx)."""
    print("=== Optional Extras Examples ===")
    
    g = Graph("bolt://localhost:7687", ("neo4j", "password"))
    
    # Example 1: Pandas integration (would require pandas installed)
    print("Pandas integration example:")
    print("# people_df = g.nodes('Person').where(age__gte=21).to_df()")
    print("# print(people_df.head())")
    
    # Example 2: NetworkX integration (would require networkx installed)
    print("\nNetworkX integration example:")
    print("# nx_graph = g.to_networkx(node_labels=['Person', 'Company'], rel_types=['WORKS_AT'])")
    print("# print(f'Nodes: {nx_graph.number_of_nodes()}, Edges: {nx_graph.number_of_edges()}')")
    
    print()


def main():
    """Run all examples."""
    print("GraphFrame-Neo4j Examples")
    print("=" * 50)
    
    demo_node_queries()
    demo_relationship_queries()
    demo_write_operations()
    demo_traversal_operations()
    demo_schema_operations()
    demo_optional_extras()
    
    print("All examples completed!")
    print("\nNote: These examples show the API usage but don't execute against a real database.")
    print("To run with a real Neo4j database, use the integration test setup.")


if __name__ == "__main__":
    main()