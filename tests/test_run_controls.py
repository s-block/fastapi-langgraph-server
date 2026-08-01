"""Integration coverage for execution and standalone resource controls."""

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import TypedDict

import httpx
import pytest
from langchain_core.runnables import RunnableLambda
from langgraph.graph import END, START, StateGraph

from fastapi_langgraph_server import AssistantConfig, StandaloneAppConfig, create_app


class ControlState(TypedDict, total=False):
    value: str
    result: str


def _assistant(
    node: Callable[[ControlState], ControlState | Awaitable[ControlState]],
    assistant_id: str = "controlled",
) -> AssistantConfig:
    builder = StateGraph(ControlState)
    builder.add_node("work", RunnableLambda(node))
    builder.add_edge(START, "work")
    builder.add_edge("work", END)
    return AssistantConfig(
        assistant_id=assistant_id,
        graph_id=assistant_id,
        name="Controlled graph",
        checkpointed_graph_factory=lambda saver: builder.compile(checkpointer=saver),
    )


@pytest.mark.asyncio
async def test_same_thread_and_process_capacity_are_rejected() -> None:
    started = asyncio.Event()
    finish = asyncio.Event()

    async def wait_for_release(state: ControlState) -> ControlState:
        started.set()
        await finish.wait()
        return {"result": state.get("value", "")}

    assistant = _assistant(wait_for_release)
    app = create_app(
        StandaloneAppConfig(
            assistants={assistant.assistant_id: assistant},
            max_concurrent_runs=1,
        )
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        first_task = asyncio.create_task(
            client.post(
                "/threads/one/runs",
                json={"assistant_id": assistant.assistant_id, "input": {"value": "1"}},
            )
        )
        await asyncio.wait_for(started.wait(), timeout=1)
        same_thread = await client.post(
            "/threads/one/runs",
            json={
                "assistant_id": assistant.assistant_id,
                "input": {"value": "same"},
                "multitask_strategy": "reject",
            },
        )
        process_full = await client.post(
            "/threads/two/runs",
            json={"assistant_id": assistant.assistant_id, "input": {"value": "2"}},
        )
        finish.set()
        first = await first_task

    assert first.status_code == 200
    assert same_thread.status_code == 409
    assert "already has an active run" in same_thread.json()["detail"]
    assert process_full.status_code == 429


@pytest.mark.asyncio
async def test_run_timeouts_release_leases_for_later_requests() -> None:
    async def slow(state: ControlState) -> ControlState:
        await asyncio.sleep(0.05)
        return {"result": state.get("value", "")}

    assistant = _assistant(slow)
    app = create_app(
        StandaloneAppConfig(
            assistants={assistant.assistant_id: assistant},
            max_concurrent_runs=1,
            run_timeout_seconds=0.01,
        )
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        non_streaming = await client.post(
            "/threads/timeout/runs",
            json={"assistant_id": assistant.assistant_id, "input": {}},
        )
        streaming = await client.post(
            "/threads/timeout/runs/stream",
            json={"assistant_id": assistant.assistant_id, "input": {}},
        )

    assert non_streaming.status_code == 504
    assert streaming.status_code == 200
    assert "RUN_TIMEOUT" in streaming.text


@pytest.mark.asyncio
async def test_stream_failures_are_generic_and_release_capacity(
    caplog: pytest.LogCaptureFixture,
) -> None:
    def fail(_state: ControlState) -> ControlState:
        raise RuntimeError("provider response contained sensitive text")

    assistant = _assistant(fail)
    app = create_app(
        StandaloneAppConfig(
            assistants={assistant.assistant_id: assistant},
            max_concurrent_runs=1,
        )
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        first = await client.post(
            "/threads/failure/runs/stream",
            json={"assistant_id": assistant.assistant_id, "input": {}},
        )
        second = await client.post(
            "/threads/failure/runs/stream",
            json={"assistant_id": assistant.assistant_id, "input": {}},
        )

    assert first.status_code == 200
    assert "INTERNAL_ERROR" in first.text
    assert "sensitive text" not in first.text
    assert "sensitive text" not in caplog.text
    assert "RuntimeError" in caplog.text
    assert second.status_code == 200
    assert "INTERNAL_ERROR" in second.text


@pytest.mark.asyncio
async def test_standalone_body_limit_rejects_chunked_requests() -> None:
    def echo(state: ControlState) -> ControlState:
        return {"result": state.get("value", "")}

    async def oversized_body() -> AsyncIterator[bytes]:
        yield b"{" + b" " * 40
        yield b" " * 40 + b"}"

    assistant = _assistant(echo)
    app = create_app(
        StandaloneAppConfig(
            assistants={assistant.assistant_id: assistant},
            max_request_body_bytes=64,
        )
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.post(
            "/runs/stream",
            content=oversized_body(),
            headers={"content-type": "application/json"},
        )

    assert response.status_code == 413
    assert response.json()["detail"] == "Request body exceeds the configured limit"
