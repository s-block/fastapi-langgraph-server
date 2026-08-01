"""Public package smoke tests."""

from importlib.metadata import metadata, version

import fastapi_langgraph_server


def test_package_imports() -> None:
    assert fastapi_langgraph_server.__name__ == "fastapi_langgraph_server"
    assert fastapi_langgraph_server.create_app is not None
    assert fastapi_langgraph_server.create_router is not None
    assert fastapi_langgraph_server.InMemorySaver is not None
    assert fastapi_langgraph_server.BoundedInMemorySaver is not None
    assert fastapi_langgraph_server.InMemoryCheckpointConfig is not None
    assert fastapi_langgraph_server.RequestBodyLimitMiddleware is not None


def test_distribution_version() -> None:
    assert version("fastapi-langgraph-server") == "0.1.0"


def test_redis_checkpointer_is_an_optional_dependency() -> None:
    requirements = metadata("fastapi-langgraph-server").get_all("Requires-Dist") or []

    redis_requirements = [
        requirement
        for requirement in requirements
        if requirement.startswith("langgraph-checkpoint-redis")
    ]
    assert len(redis_requirements) == 1
    assert "extra ==" in redis_requirements[0]
    assert "redis" in redis_requirements[0]
