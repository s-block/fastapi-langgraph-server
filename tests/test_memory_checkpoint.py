"""Production guardrail tests for the optional in-memory checkpoint saver."""

import asyncio
import concurrent.futures
from dataclasses import dataclass
from typing import Any, TypedDict, cast

import pytest
from fastapi.testclient import TestClient
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    BaseCheckpointSaver,
    CheckpointMetadata,
    empty_checkpoint,
)
from langgraph.graph import END, START, StateGraph

from fastapi_langgraph_server import (
    AssistantConfig,
    BoundedInMemorySaver,
    CheckpointCapacityError,
    InMemoryCheckpointConfig,
    InMemorySaver,
    StandaloneAppConfig,
    create_app,
)


@dataclass(slots=True)
class FakeClock:
    now: float = 0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class CounterState(TypedDict, total=False):
    count: int


def increment(state: CounterState) -> CounterState:
    return {"count": state.get("count", 0) + 1}


def checkpoint_config(thread_id: str) -> RunnableConfig:
    return cast(
        "RunnableConfig",
        {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
    )


async def put_value(
    saver: BoundedInMemorySaver,
    thread_id: str,
    value: Any,
) -> RunnableConfig:
    checkpoint = empty_checkpoint()
    checkpoint["channel_values"] = {"value": value}
    checkpoint["channel_versions"] = {"value": "1"}
    metadata = cast(
        "CheckpointMetadata",
        {"source": "input", "step": 1, "parents": {}},
    )
    return await saver.aput(
        checkpoint_config(thread_id),
        checkpoint,
        metadata,
        {"value": "1"},
    )


@pytest.mark.asyncio
async def test_default_alias_runs_a_graph_with_checkpoint_history() -> None:
    saver = InMemorySaver(
        config=InMemoryCheckpointConfig(max_checkpoints_per_thread=100)
    )
    builder = StateGraph(CounterState)
    builder.add_node("increment", increment)
    builder.add_edge(START, "increment")
    builder.add_edge("increment", END)
    graph = builder.compile(checkpointer=saver)
    config = checkpoint_config("bounded-history")

    for _ in range(5):
        result = await graph.ainvoke({}, config)

    history = [item async for item in saver.alist(config)]
    assert result == {"count": 5}
    assert history
    assert history[0].checkpoint["channel_values"] == {"count": 5}


@pytest.mark.asyncio
async def test_checkpoint_limit_fails_without_pruning_parent_history() -> None:
    saver = BoundedInMemorySaver(
        config=InMemoryCheckpointConfig(max_checkpoints_per_thread=3)
    )

    stored = [await put_value(saver, "limited", value) for value in range(3)]
    with pytest.raises(
        CheckpointCapacityError,
        match="max_checkpoints_per_thread",
    ):
        await put_value(saver, "limited", 4)

    assert saver.stats().checkpoints == 3
    assert await saver.aget_tuple(stored[-1]) is not None


@pytest.mark.asyncio
async def test_idle_ttl_and_lru_thread_limit_evict_complete_threads() -> None:
    clock = FakeClock()
    saver = BoundedInMemorySaver(
        config=InMemoryCheckpointConfig(
            idle_ttl_seconds=10,
            max_threads=2,
        ),
        clock=clock,
    )
    first = await put_value(saver, "first", 1)
    clock.advance(1)
    second = await put_value(saver, "second", 2)
    clock.advance(1)
    assert await saver.aget_tuple(first) is not None
    clock.advance(1)
    await put_value(saver, "third", 3)

    assert await saver.aget_tuple(second) is None
    assert await saver.aget_tuple(first) is not None
    assert saver.stats().threads == 2

    clock.advance(11)
    assert saver.purge_expired() == 2
    assert saver.stats().threads == 0
    assert saver.stats().serialized_bytes == 0


@pytest.mark.asyncio
async def test_oversized_checkpoint_is_rejected_without_storing_state() -> None:
    saver = BoundedInMemorySaver(
        config=InMemoryCheckpointConfig(
            max_checkpoint_bytes=1_024,
            max_pending_write_bytes=512,
            max_total_bytes=4_096,
        )
    )

    with pytest.raises(CheckpointCapacityError, match="max_checkpoint_bytes"):
        await put_value(saver, "oversized", "x" * 4_096)

    assert await saver.aget_tuple(checkpoint_config("oversized")) is None
    assert saver.stats().serialized_bytes == 0


@pytest.mark.asyncio
async def test_total_byte_budget_evicts_lru_threads_before_writing() -> None:
    saver = BoundedInMemorySaver(
        config=InMemoryCheckpointConfig(
            max_checkpoint_bytes=1_024,
            max_pending_write_bytes=512,
            max_total_bytes=1_500,
        )
    )
    first = await put_value(saver, "first-budget", "x" * 600)
    second = await put_value(saver, "second-budget", "y" * 600)

    assert await saver.aget_tuple(first) is None
    assert await saver.aget_tuple(second) is not None
    assert saver.stats().serialized_bytes <= 1_500


@pytest.mark.asyncio
async def test_concurrent_async_writes_remain_isolated() -> None:
    saver = BoundedInMemorySaver(config=InMemoryCheckpointConfig(max_threads=50))

    stored_configs = await asyncio.gather(
        *(put_value(saver, f"thread-{index}", index) for index in range(25))
    )
    loaded = await asyncio.gather(
        *(saver.aget_tuple(config) for config in stored_configs)
    )

    assert [
        item.checkpoint["channel_values"]["value"] for item in loaded if item
    ] == list(range(25))
    assert saver.stats().threads == 25


def test_concurrent_threaded_writes_remain_isolated() -> None:
    saver = BoundedInMemorySaver(config=InMemoryCheckpointConfig(max_threads=50))

    def write(index: int) -> RunnableConfig:
        return asyncio.run(put_value(saver, f"sync-thread-{index}", index))

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        configs = list(executor.map(write, range(25)))

    loaded = [saver.get_tuple(config) for config in configs]
    assert [
        item.checkpoint["channel_values"]["value"]
        for item in loaded
        if item is not None
    ] == list(range(25))


def test_untrusted_identifiers_and_invalid_limits_are_rejected() -> None:
    saver = BoundedInMemorySaver()

    with pytest.raises(ValueError, match="control characters"):
        saver.get_tuple(checkpoint_config("unsafe\nthread"))
    with pytest.raises(ValueError, match="max_checkpoint_bytes"):
        InMemoryCheckpointConfig(
            max_checkpoint_bytes=2_048,
            max_total_bytes=1_024,
        )
    with pytest.raises(ValueError, match="finite number"):
        InMemoryCheckpointConfig(idle_ttl_seconds=float("nan"))
    with pytest.raises(ValueError, match="integer"):
        InMemoryCheckpointConfig(max_threads=True)


def test_explicit_in_memory_saver_is_shared_with_graph_factory() -> None:
    received_savers: list[BaseCheckpointSaver[str]] = []
    checkpointer = BoundedInMemorySaver()

    def compile_graph(saver: BaseCheckpointSaver[str] | None) -> object:
        assert saver is not None
        received_savers.append(saver)
        builder = StateGraph(CounterState)
        builder.add_node("increment", increment)
        builder.add_edge(START, "increment")
        builder.add_edge("increment", END)
        return builder.compile(checkpointer=saver)

    assistant = AssistantConfig(
        assistant_id="counter",
        graph_id="counter",
        name="Counter",
        checkpointed_graph_factory=compile_graph,
    )
    app = create_app(
        StandaloneAppConfig(
            assistants={"counter": assistant},
            checkpointer=checkpointer,
            cors_origins=(),
        )
    )

    with TestClient(app) as client:
        first = client.post(
            "/threads/default-memory/runs",
            json={"assistant_id": "counter", "input": {}},
        )
        second = client.post(
            "/threads/default-memory/runs",
            json={"assistant_id": "counter", "input": {}},
        )
        state = client.get("/threads/default-memory/state")

    assert first.status_code == 200
    assert second.status_code == 200
    assert state.json()["values"]["count"] == 2
    assert len(received_savers) == 1
    assert isinstance(received_savers[0], BoundedInMemorySaver)


def test_server_returns_controlled_error_when_checkpoint_capacity_is_full() -> None:
    checkpointer = BoundedInMemorySaver(
        config=InMemoryCheckpointConfig(max_checkpoints_per_thread=1)
    )

    def compile_graph(
        saver: BaseCheckpointSaver[str] | None,
    ) -> object:
        assert saver is not None
        builder = StateGraph(CounterState)
        builder.add_node("increment", increment)
        builder.add_edge(START, "increment")
        builder.add_edge("increment", END)
        return builder.compile(checkpointer=saver)

    assistant = AssistantConfig(
        assistant_id="limited",
        graph_id="limited",
        name="Limited",
        checkpointed_graph_factory=compile_graph,
    )
    app = create_app(
        StandaloneAppConfig(
            assistants={"limited": assistant},
            checkpointer=checkpointer,
            cors_origins=(),
        )
    )

    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.post(
            "/threads/limited/runs",
            json={"assistant_id": "limited", "input": {}},
        )

    assert response.status_code == 507
    assert response.json()["detail"] == "Checkpoint capacity exceeded"
