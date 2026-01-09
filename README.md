# GraphFrame-Neo4j

[Currently in Pre-release for testing, will upload package later to Pypi]
A pandas-like interface for Neo4j with DataFrame-style querying and idempotent upserts. Compile with uv :)

## Quick Start

```python
from graphframe_neo4j import Graph

# Connect to Neo4j
g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"), database="neo4j")

# Query nodes
people = g.nodes("Person").where(age__gte=21, country="US").select("name", "email").limit(10)
results = people.to_records()

# Upsert nodes (idempotent)
data = [{"email": "john@example.com", "name": "John", "age": 30}]
plan = g.nodes("Person").upsert(data, key="email")
plan.commit()

# Traverse relationships
colleagues = g.nodes("Person") \
    .traverse("WORKS_AT", to="Company", direction="out") \
    .where(company__city="San Francisco") \
    .back()
```

## Installation

### For Users

```bash
pip install graphframe-neo4j
pip install graphframe-neo4j[pandas,networkx,tqdm]  # Optional extras
```

### For Developers

```bash
git clone https://github.com/your-repo/graphframe-neo4j.git
cd graphframe-neo4j
uv sync --all-groups
```

## Key Features

### Pandas-like API

```python
query = g.nodes("Person") \
    .where(age__gte=18, country="US") \
    .select("name", "email", "age") \
    .order_by("age__desc") \
    .limit(100)
results = query.to_records()
```

### Idempotent Upserts

```python
data = [{"email": "john@example.com", "name": "John"}]
plan = g.nodes("Person").upsert(data, key="email")
plan.commit()  # Safe to run multiple times
```

### Graph Traversal

```python
colleagues = g.nodes("Person") \
    .traverse("WORKS_AT", to="Company", direction="out") \
    .traverse("WORKS_AT", to="Person", direction="in") \
    .where(to__email__ne="john@example.com")
```

### Parameterized Queries

```python
# All values are automatically parameterized
safe_query = g.nodes("Person").where(
    name__contains=user_input,  # Parameterized
    age__gte=user_age           # Parameterized
)
```

## Core Concepts

### Frames

```python
# NodeFrame - for querying nodes
people = g.nodes("Person")

# EdgeFrame - for querying relationships
relationships = g.rels("WORKS_AT")

# PathFrame - for graph traversal
path = people.traverse("WORKS_AT", to="Company")
```

### Write Operations

```python
# All writes return a WritePlan
plan = g.nodes("Person").upsert(data, key="email")

# Preview and execute
plan.compile()  # See Cypher
plan.preview()  # See sample data
plan.commit()   # Execute
```

### Filter Operations

| Operator | Example | Cypher |
|----------|---------|--------|
| `eq` | `.where(age=30)` | `WHERE n.age = $param` |
| `ne` | `.where(age__ne=30)` | `WHERE n.age <> $param` |
| `gt` | `.where(age__gt=30)` | `WHERE n.age > $param` |
| `gte` | `.where(age__gte=30)` | `WHERE n.age >= $param` |
| `lt` | `.where(age__lt=30)` | `WHERE n.age < $param` |
| `lte` | `.where(age__lte=30)` | `WHERE n.age <= $param` |
| `in` | `.where(country__in=["US", "UK"])` | `WHERE n.country IN $param` |
| `contains` | `.where(name__contains="John")` | `WHERE n.name CONTAINS $param` |
| `startswith` | `.where(name__startswith="John")` | `WHERE n.name STARTS WITH $param` |
| `endswith` | `.where(name__endswith="@example.com")` | `WHERE n.name ENDS WITH $param` |
| `regex` | `.where(email__regex=r"^.*@company\\.com$")` | `WHERE n.email =~ $param` |
| `is_null` | `.where(deleted_at__is_null=True)` | `WHERE n.deleted_at IS NULL` |
| `not_null` | `.where(last_login__not_null=True)` | `WHERE n.last_login IS NOT NULL` |

## Examples

### Basic Querying

```python
from graphframe_neo4j import Graph

g = Graph.connect("bolt://localhost:7687", auth=("neo4j", "password"))

# Query with multiple conditions
people = g.nodes("Person").where(
    age__gte=18,
    country__in=["US", "UK"],
    status="active"
).select("name", "email", "age").limit(100)

results = people.to_records()
```

### Relationship Queries

```python
# Basic relationship query
works_at = g.rels("WORKS_AT").where(
    since__gte=2020,
    role="Engineer"
).limit(50)
```

### Traversal Operations

```python
# Basic traversal
people = g.nodes("Person")
companies = people.traverse("WORKS_AT", to="Company", direction="out")

# Traversal with custom aliases
path = people.traverse(
    "WORKS_AT", 
    to="Company", 
    direction="out", 
    alias=("person", "works", "company")
)

# Filtered traversal
filtered = path.where(
    rel__since__gte=2020,
    company__city="San Francisco"
)

# Back to originating nodes
back_query = filtered.back()
```

### Write Operations

```python
# Node upsert
data = [
    {"email": "john@example.com", "name": "John Doe", "age": 30},
    {"email": "jane@example.com", "name": "Jane Smith", "age": 25}
]
plan = g.nodes("Person").upsert(data, key="email")
plan.commit()

# Patch/update
update_plan = g.nodes("Person").where(country="US").patch(status="active")
update_plan.commit()

# Advanced operations
inc_plan = g.nodes("Product").where(category="Electronics").inc("views", 1)
list_plan = g.nodes("User").where(email="john@example.com").list_append("tags", "premium")
map_plan = g.nodes("Document").where(status="active").map_merge("metadata", {"updated": "2024-01-01"})
```

### Relationship Write Operations

```python
# Relationship upsert
rel_data = [
    {
        "email": "john@example.com", 
        "domain": "company.com", 
        "role": "Engineer", 
        "since": 2020
    }
]

rel_plan = g.rels("WORKS_AT").upsert(
    rel_data,
    src=("Person", "email"),
    dst=("Company", "domain"),
    rel_key="role"
)
rel_plan.commit()

# Relationship update and delete
update_plan = g.rels("WORKS_AT").where(role="Engineer").patch(salary=100000)
delete_plan = g.rels("WORKS_AT").where(since__lt=2010).delete()
```

### Schema Management

```python
schema = g.schema()

# Create indexes and constraints
schema.ensure_index("Person", "email")
schema.ensure_unique("Person", "email")
schema.ensure_node_key("Person", ["email", "country"])

# Drop indexes and constraints
schema.drop_index("Person", "country")
schema.drop_unique("Product", "sku")
```

## Testing

### Unit Tests

```bash
uv run pytest -q
uv run pytest tests/unit/test_nodeframe_compilation.py -v
uv run pytest --cov=src --cov-report=term-missing -q
```

### Integration Tests

```bash
docker-compose up -d
export NEO4J_URI="bolt://localhost:7687"
export NEO4J_USER="neo4j"
export NEO4J_PASSWORD="testpassword"
export NEO4J_DATABASE="neo4j"
uv run pytest -m integration -q
```

## Project Structure

```
graphframe-neo4j/
├── src/
│   └── graphframe_neo4j/
│       ├── __init__.py          # Public API exports
│       ├── graph.py             # Connection management
│       ├── frames/              # Frame implementations
│       ├── write/               # Write operations
│       ├── schema/              # Schema management
│       └── util/                # Utilities
├── tests/
│   ├── unit/                   # Unit tests
│   └── integration/            # Integration tests
├── examples/                   # Example scripts
├── docker-compose.yml          # Neo4j test environment
├── pyproject.toml              # Project configuration
└── README.md                   # Documentation
```

## Development

```bash
uv sync --all-groups
uv run pytest -q
uv run ruff check .
uv run mypy src/
```

## API Reference

### Graph

```python
Graph.connect(uri, auth, database="neo4j", **kwargs)
g.nodes(label) -> NodeFrame
g.rels(rel_type) -> EdgeFrame
g.schema() -> SchemaManager
g.cypher(query, **params) -> Any
g.to_networkx(node_labels=None, rel_types=None, limit=None) -> nx.Graph
```

### NodeFrame

```python
NodeFrame.where(**kwargs) -> NodeFrame
NodeFrame.select(*fields) -> NodeFrame
NodeFrame.order_by(*fields) -> NodeFrame
NodeFrame.limit(limit) -> NodeFrame
NodeFrame.offset(offset) -> NodeFrame
NodeFrame.compile() -> dict
NodeFrame.to_records() -> list[dict]
NodeFrame.to_df() -> pd.DataFrame
NodeFrame.upsert(data, key, **kwargs) -> WritePlan
NodeFrame.patch(**updates) -> WritePlan
NodeFrame.inc(field, value) -> WritePlan
NodeFrame.unset(field) -> WritePlan
NodeFrame.list_append(field, value) -> WritePlan
NodeFrame.list_remove(field, value) -> WritePlan
NodeFrame.map_merge(field, data) -> WritePlan
NodeFrame.delete(detach=False) -> WritePlan
NodeFrame.traverse(rel_type, to, direction="out", alias=None) -> PathFrame
```

### WritePlan

```python
WritePlan.compile() -> dict
WritePlan.preview() -> dict
WritePlan.commit() -> Any
WritePlan.explain() -> str
WritePlan.profile() -> str
```

### SchemaManager

```python
SchemaManager.ensure_index(label, prop) -> WritePlan
SchemaManager.ensure_unique(label, prop) -> WritePlan
SchemaManager.ensure_node_key(label, props) -> WritePlan
SchemaManager.drop_index(label, prop) -> WritePlan
SchemaManager.drop_unique(label, prop) -> WritePlan
```

## License

MIT License. See the LICENSE file for details.