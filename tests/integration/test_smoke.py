import pytest


@pytest.mark.integration
def test_neo4j_is_reachable(neo4j_session):
    result = neo4j_session.run("RETURN 1 AS x").single()
    assert result["x"] == 1
