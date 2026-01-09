# GraphFrame-Neo4j Examples

This directory contains examples demonstrating GraphFrame-Neo4j functionality.

## Available Examples

### 1. Basic Usage (`basic_usage.py`)

Demonstrates core functionality:
- Node queries with filtering, ordering, and limits
- Relationship queries
- Write operations (upsert, patch, delete)
- Traversal operations
- Schema management

```bash
uv run python examples/basic_usage.py
```

### 2. Advanced Operations (`advanced_operations.py`)

Shows advanced patterns:
- Complex filtering with multiple conditions
- Advanced update operations (increment, list operations, map merge)
- Various traversal patterns
- Batch operations and write plans
- Schema management operations

```bash
uv run python examples/advanced_operations.py
```

### 3. Live Demo (`live_demo.py`)

Comprehensive demonstration showing actual Cypher queries generated:
- Node operations with all filter types
- Relationship operations
- Traversal operations
- Write operations
- Relationship write operations
- Schema operations
- Complex real-world scenarios

```bash
uv run python examples/live_demo.py
```

## Key Features Demonstrated

### Node Operations
- Basic queries: `g.nodes("Person")`
- Filtering: `.where(age__gte=18, country="US")`
- String operations: `name__contains`, `email__endswith`
- NULL operations: `last_login__not_null`, `deleted_at__is_null`
- IN operations: `country__in=["US", "UK"]`
- Regex operations: `email__regex=r"^.*@company\\.com$"`
- Ordering: `.order_by("age__desc", "name__asc")`
- Limits: `.limit(10).offset(0)`
- Field selection: `.select("name", "email")`

### Relationship Operations
- Basic queries: `g.rels("WORKS_AT")`
- Filtering: `.where(since__gte=2020)`
- Limits: `.limit(50)`

### Traversal Operations
- Outgoing: `.traverse("FOLLOWS", to="Person", direction="out")`
- Incoming: `.traverse("FOLLOWS", to="Person", direction="in")`
- Bidirectional: `.traverse("KNOWS", to="Person", direction="both")`
- Custom aliases: `alias=("person", "works", "company")`
- Filtered traversal: `.where(rel__since__gte=2020)`
- Back to origin: `.back()`

### Write Operations
- Node upsert: `.upsert(data, key="email")`
- Patch/update: `.patch(status="active")`
- Delete: `.delete()`
- Advanced operations:
  - Increment: `.inc("views", 1)`
  - List append: `.list_append("tags", "premium")`
  - List remove: `.list_remove("tags", "temp")`
  - Map merge: `.map_merge("metadata", {...})`
  - Unset: `.unset("temp_data")`

### Relationship Write Operations
- Upsert: `.upsert(data, src=("Person", "email"), dst=("Company", "domain"))`
- Update: `.patch(salary=100000)`
- Delete: `.delete()`

### Schema Operations
- Create index: `.ensure_index("Person", "email")`
- Create unique: `.ensure_unique("Person", "email")`
- Composite key: `.ensure_node_key("Person", ["email", "country"])`
- Drop index: `.drop_index("Person", "country")`
- Drop unique: `.drop_unique("Product", "sku")`

## Running Examples with Real Database

```bash
# Start Neo4j
docker-compose up -d

# Modify connection
g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "testpassword"), database="neo4j")

# Run examples
uv run python examples/live_demo.py
```

## Key Takeaways

- Pandas-like API for Neo4j
- Parameterized queries for security
- Lazy evaluation for efficiency
- Explicit writes for safety
- Graph traversal for complex queries
- Schema management for data integrity

## Next Steps

- Try examples with your own Neo4j database
- Explore integration tests for complex scenarios
- Check API documentation in main README
- Contribute your own examples and use cases