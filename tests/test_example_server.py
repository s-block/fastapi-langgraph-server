"""Smoke tests for the runnable example server."""

import runpy
from collections.abc import Callable
from pathlib import Path
from typing import cast

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from langgraph.checkpoint.base import BaseCheckpointSaver

from fastapi_langgraph_server import (
    StandaloneAppConfig,
    create_app,
)


class CloseTrackingSaver(BaseCheckpointSaver[str]):
    """Checkpointer exposing async resource cleanup for lifespan coverage."""

    def __init__(self) -> None:
        self.closed = False

    async def aclose(self) -> None:
        self.closed = True


class ContextTrackingSaver(BaseCheckpointSaver[str]):
    """Checkpointer with Redis-style asynchronous context management."""

    def __init__(self) -> None:
        self.entered = False
        self.exited = False

    async def __aenter__(self) -> "ContextTrackingSaver":
        self.entered = True
        return self

    async def __aexit__(
        self,
        _exc_type: object,
        _exc_value: object,
        _traceback: object,
    ) -> None:
        self.exited = True


def test_standalone_app_closes_checkpointer_resources() -> None:
    saver = CloseTrackingSaver()
    app = create_app(StandaloneAppConfig(checkpointer=saver))

    with TestClient(app) as client:
        assert client.get("/health").status_code == 200

    assert saver.closed


def test_standalone_app_enters_async_checkpointer_context() -> None:
    saver = ContextTrackingSaver()
    app = create_app(StandaloneAppConfig(checkpointer=saver))

    with TestClient(app) as client:
        assert saver.entered
        assert client.get("/health").status_code == 200

    assert saver.exited


def test_standalone_app_disables_cross_origin_access_by_default() -> None:
    app = create_app(StandaloneAppConfig())

    with TestClient(app) as client:
        response = client.options(
            "/health",
            headers={
                "Origin": "https://untrusted.example",
                "Access-Control-Request-Method": "GET",
            },
        )

    assert "access-control-allow-origin" not in response.headers


@pytest.mark.parametrize(
    "config_factory",
    [
        lambda: StandaloneAppConfig(max_request_body_bytes=0),
        lambda: StandaloneAppConfig(max_concurrent_runs=0),
        lambda: StandaloneAppConfig(run_timeout_seconds=float("inf")),
    ],
)
def test_standalone_resource_limits_are_validated(
    config_factory: Callable[[], StandaloneAppConfig],
) -> None:
    with pytest.raises(ValueError):
        create_app(config_factory())


def _load_example_app() -> FastAPI:
    example_path = Path(__file__).parents[1] / "examples" / "basic.py"
    namespace = runpy.run_path(str(example_path))
    return cast("FastAPI", namespace["app"])


def test_basic_example_serves_and_runs_langgraph() -> None:
    app = _load_example_app()
    with TestClient(app) as client:
        health = client.get("/health")
        assistant = client.get("/assistants/greeter")
        run = client.post(
            "/runs/stream",
            json={
                "assistant_id": "greeter",
                "input": {"name": "LangGraph"},
            },
        )

    assert health.status_code == 200
    assert health.json() == {"status": "ok"}
    assert assistant.status_code == 200
    assert assistant.json()["graph_id"] == "greeter"
    assert run.status_code == 200
    assert "Hello, LangGraph!" in run.text
