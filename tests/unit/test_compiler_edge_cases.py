"""
Test edge cases for the query compiler.
"""

from graphframe_neo4j.frames.compiler import QueryCompiler

def test_empty_conditions():
    """Test compilation with no conditions."""
    compiler = QueryCompiler()
    
    result = compiler.compile_node_query("Person")
    
    assert "MATCH (n:Person)" in result["cypher"]
    assert "RETURN n" in result["cypher"]
    assert result["params"] == {}

def test_empty_fields():
    """Test compilation with no selected fields."""
    compiler = QueryCompiler()
    
    result = compiler.compile_node_query("Person", fields=[])
    
    assert "RETURN n" in result["cypher"]

def test_special_characters_in_values():
    """Test handling of special characters in parameter values."""
    compiler = QueryCompiler()
    
    conditions = [
        {"field": "name", "op": "eq", "value": "O'Brien"},
        {"field": "email", "op": "eq", "value": "test@example.com"},
        {"field": "description", "op": "contains", "value": "it's a test"}
    ]
    
    result = compiler.compile_node_query("Person", conditions=conditions)
    
    # Should use parameters, not string interpolation
    assert "O'Brien" not in result["cypher"]
    assert "test@example.com" not in result["cypher"]
    assert "it's a test" not in result["cypher"]
    
    # Should have proper parameters
    assert result["params"]["param_0"] == "O'Brien"
    assert result["params"]["param_1"] == "test@example.com"
    assert result["params"]["param_2"] == "it's a test"

def test_boolean_values():
    """Test handling of boolean values."""
    compiler = QueryCompiler()
    
    conditions = [
        {"field": "active", "op": "eq", "value": True},
        {"field": "deleted", "op": "eq", "value": False}
    ]
    
    result = compiler.compile_node_query("Person", conditions=conditions)
    
    assert "WHERE n.active = $param_0 AND n.deleted = $param_1" in result["cypher"]
    assert result["params"]["param_0"] is True
    assert result["params"]["param_1"] is False

def test_numeric_values():
    """Test handling of various numeric values."""
    compiler = QueryCompiler()
    
    conditions = [
        {"field": "age", "op": "eq", "value": 25},
        {"field": "score", "op": "gte", "value": 95.5},
        {"field": "count", "op": "lt", "value": 0}
    ]
    
    result = compiler.compile_node_query("Person", conditions=conditions)
    
    assert result["params"]["param_0"] == 25
    assert result["params"]["param_1"] == 95.5
    assert result["params"]["param_2"] == 0

def test_null_operations():
    """Test NULL-related operations."""
    compiler = QueryCompiler()
    
    conditions = [
        {"field": "email", "op": "is_null"},
        {"field": "phone", "op": "not_null"}
    ]
    
    result = compiler.compile_node_query("Person", conditions=conditions)
    
    assert "WHERE n.email IS NULL AND n.phone IS NOT NULL" in result["cypher"]
    assert result["params"] == {}  # No parameters for NULL ops

def test_parse_filter_kwargs():
    """Test parsing of filter kwargs."""
    compiler = QueryCompiler()
    
    kwargs = {
        "name": "John",
        "age__gte": 21,
        "country__in": ["US", "CA"],
        "active__ne": False
    }
    
    conditions = compiler.parse_filter_kwargs(kwargs)
    
    expected = [
        {"field": "name", "op": "eq", "value": "John"},
        {"field": "age", "op": "gte", "value": 21},
        {"field": "country", "op": "in", "value": ["US", "CA"]},
        {"field": "active", "op": "ne", "value": False}
    ]
    
    assert conditions == expected

def test_complex_label_names():
    """Test handling of complex label names."""
    compiler = QueryCompiler()
    
    result = compiler.compile_node_query("Person_With_Complex_Name")
    
    assert "MATCH (n:Person_With_Complex_Name)" in result["cypher"]

def test_zero_limit():
    """Test handling of zero limit."""
    compiler = QueryCompiler()
    
    result = compiler.compile_node_query("Person", limit=0)
    
    assert "LIMIT 0" in result["cypher"]

def test_negative_limit():
    """Test handling of negative limit (should be ignored)."""
    compiler = QueryCompiler()
    
    result = compiler.compile_node_query("Person", limit=-1)
    
    assert "LIMIT" not in result["cypher"]
