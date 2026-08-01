"""Exercise the package with a real compiled LangGraph."""

from __future__ import annotations

from typing import TYPE_CHECKING, TypedDict

import httpx
import pytest
from fastapi.testclient import TestClient
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.pregel.remote import RemoteGraph
from langgraph_sdk.client import LangGraphClient
from langgraph_sdk.errors import NotFoundError

from fastapi_langgraph_server import AssistantConfig, StandaloneAppConfig, create_app

if TYPE_CHECKING:
    from langchain_core.runnables import RunnableConfig


class AgentState(TypedDict, total=False):
    question: str
    response: str


def answer(state: AgentState) -> AgentState:
    return {"response": f"Answered: {state['question']}"}


def test_native_compiled_graph_streams_and_checkpoints_state() -> None:
    checkpointer = InMemorySaver()
    builder = StateGraph(AgentState)
    builder.add_node("answer", answer)
    builder.add_edge(START, "answer")
    builder.add_edge("answer", END)
    assistant = AssistantConfig(
        assistant_id="native",
        graph_id="native",
        name="Native graph",
        checkpointed_graph_factory=lambda saver: builder.compile(checkpointer=saver),
    )
    app = create_app(
        StandaloneAppConfig(
            assistants={"native": assistant},
            checkpointer=checkpointer,
            cors_origins=(),
        )
    )

    with TestClient(app) as client:
        response = client.post(
            "/threads/native-thread/runs/stream",
            json={
                "assistant_id": "native",
                "input": {"question": "What is reusable?"},
                "stream_mode": ["values"],
            },
        )
        state = client.get("/threads/native-thread/state")
        updated = client.post(
            "/threads/native-thread/state",
            json={"values": {"response": "Manually updated"}},
        )
        updated_state = client.get("/threads/native-thread/state")

    assert response.status_code == 200
    assert "Answered: What is reusable?" in response.text
    assert state.status_code == 200
    assert state.json()["values"]["response"] == "Answered: What is reusable?"
    assert updated.status_code == 200
    assert updated_state.json()["values"]["response"] == "Manually updated"


def test_updates_stream_persists_the_latest_native_checkpoint() -> None:
    checkpointer = InMemorySaver()
    builder = StateGraph(AgentState)
    builder.add_node("answer", answer)
    builder.add_edge(START, "answer")
    builder.add_edge("answer", END)
    assistant = AssistantConfig(
        assistant_id="native",
        graph_id="native",
        name="Native graph",
        checkpointed_graph_factory=lambda saver: builder.compile(checkpointer=saver),
    )
    app = create_app(
        StandaloneAppConfig(
            assistants={"native": assistant},
            checkpointer=checkpointer,
            cors_origins=(),
        )
    )

    with TestClient(app) as client:
        first = client.post(
            "/threads/current/runs",
            json={"assistant_id": "native", "input": {"question": "First"}},
        )
        second = client.post(
            "/threads/current/runs/stream",
            json={
                "assistant_id": "native",
                "input": {"question": "Second"},
                "stream_mode": ["updates"],
            },
        )
        state = client.get("/threads/current/state")
        graph_response = client.get("/assistants/native/graph")

    assert first.status_code == 200
    assert second.status_code == 200
    assert state.json()["values"]["response"] == "Answered: Second"
    assert any(node["id"] == "answer" for node in graph_response.json()["nodes"])


def test_checkpoint_metadata_recovers_thread_assistant_ownership() -> None:
    checkpointer = InMemorySaver()
    builder = StateGraph(AgentState)
    builder.add_node("answer", answer)
    builder.add_edge(START, "answer")
    builder.add_edge("answer", END)
    assistant = AssistantConfig(
        assistant_id="durable-owner",
        graph_id="durable-owner",
        name="Durable owner",
        checkpointed_graph_factory=lambda saver: builder.compile(checkpointer=saver),
    )
    first_app = create_app(
        StandaloneAppConfig(
            assistants={assistant.assistant_id: assistant},
            checkpointer=checkpointer,
        )
    )
    with TestClient(first_app) as client:
        run = client.post(
            "/threads/persisted/runs",
            json={
                "assistant_id": assistant.assistant_id,
                "input": {"question": "Before restart"},
            },
        )

    second_app = create_app(
        StandaloneAppConfig(
            assistants={assistant.assistant_id: assistant},
            checkpointer=checkpointer,
        )
    )
    with TestClient(second_app) as client:
        update = client.post(
            "/threads/persisted/state",
            json={"values": {"response": "After restart"}},
        )
        state = client.get("/threads/persisted/state")

    assert run.status_code == 200
    assert update.status_code == 200
    assert state.json()["values"]["response"] == "After restart"
    assert "fastapi_langgraph_server_assistant_id" not in state.text


@pytest.mark.asyncio
async def test_remote_graph_client_contract() -> None:
    checkpointer = InMemorySaver()
    builder = StateGraph(AgentState)
    builder.add_node("answer", answer)
    builder.add_edge(START, "answer")
    builder.add_edge("answer", END)
    assistant = AssistantConfig(
        assistant_id="remote",
        graph_id="remote",
        name="Remote graph",
        checkpointed_graph_factory=lambda saver: builder.compile(checkpointer=saver),
    )
    app = create_app(
        StandaloneAppConfig(
            assistants={"remote": assistant},
            checkpointer=checkpointer,
            cors_origins=(),
        )
    )
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://test",
    ) as http_client:
        sdk_client = LangGraphClient(http_client)
        remote = RemoteGraph("remote", client=sdk_client)
        stateless_result = await remote.ainvoke({"question": "Stateless invocation"})
        config: RunnableConfig = {"configurable": {"thread_id": "remote-thread"}}
        result = await remote.ainvoke(
            {"question": "Can RemoteGraph connect?"},
            config=config,
        )
        state = await remote.aget_state(config)
        checkpoint_state = await remote.aget_state(state.config)
        history = [item async for item in remote.aget_state_history(config)]
        earlier = [
            item
            async for item in remote.aget_state_history(
                config,
                before=state.config,
            )
        ]
        graph = await remote.aget_graph()
        stream_config: RunnableConfig = {"configurable": {"thread_id": "remote-stream"}}
        streamed = [
            item
            async for item in remote.astream(
                {"question": "Stream remotely"},
                config=stream_config,
            )
        ]
        updated_config = await remote.aupdate_state(
            state.config,
            {"response": "Updated remotely"},
        )
        updated_state = await remote.aget_state(updated_config)
        await sdk_client.threads.delete("remote-thread")
        with pytest.raises(NotFoundError):
            await remote.aget_state(config)

    assert result["response"] == "Answered: Can RemoteGraph connect?"
    assert stateless_result["response"] == "Answered: Stateless invocation"
    assert state.values["response"] == "Answered: Can RemoteGraph connect?"
    assert checkpoint_state.values == state.values
    assert history[0].config == state.config
    assert state.config not in [item.config for item in earlier]
    assert "answer" in graph.nodes
    assert streamed
    assert "Answered: Stream remotely" in str(streamed)
    assert updated_state.values["response"] == "Updated remotely"
