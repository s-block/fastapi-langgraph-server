"""Integration coverage for the embeddable FastAPI route set."""

from collections.abc import Callable
from typing import TypedDict, cast

from fastapi import FastAPI, HTTPException, Request
from fastapi.testclient import TestClient
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import BaseCheckpointSaver, CheckpointTuple
from langgraph.graph import END, START, StateGraph

from fastapi_langgraph_server import (
    AssistantConfig,
    BoundedInMemorySaver,
    InMemoryThreadStore,
    LangGraphServerConfig,
    StandaloneAppConfig,
    install_routes,
)


class AgentState(TypedDict, total=False):
    message: str
    response: str


def respond(state: AgentState) -> AgentState:
    return {"response": f"Echo: {state.get('message', '')}"}


def graph_factory(
    checkpointer: BaseCheckpointSaver[str] | None,
) -> object:
    builder = StateGraph(AgentState)
    builder.add_node("respond", respond)
    builder.add_edge(START, "respond")
    builder.add_edge("respond", END)
    return builder.compile(checkpointer=checkpointer)


class TrackingSaver(BoundedInMemorySaver):
    """Record which thread IDs reach the configured saver."""

    def __init__(self) -> None:
        super().__init__()
        self.read_thread_ids: list[str] = []
        self.deleted_thread_ids: list[str] = []

    async def aget_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        configurable = config.get("configurable", {})
        thread_id = configurable.get("thread_id")
        if isinstance(thread_id, str):
            self.read_thread_ids.append(thread_id)
        return await super().aget_tuple(config)

    async def adelete_thread(self, thread_id: str) -> None:
        self.deleted_thread_ids.append(thread_id)
        await super().adelete_thread(thread_id)


def _assistant(assistant_id: str = "echo") -> AssistantConfig:
    return AssistantConfig(
        assistant_id=assistant_id,
        graph_id=f"{assistant_id}-graph",
        name=f"{assistant_id.title()} graph",
        checkpointed_graph_factory=graph_factory,
        metadata={"team": assistant_id},
        input_schema={"type": "object", "properties": {}},
        output_schema={"type": "object", "properties": {}},
    )


def _config(
    assistant_id: str = "echo",
    *,
    request_authorizer: Callable[[Request], None] | None = None,
    checkpointer: BaseCheckpointSaver[str] | None = None,
) -> LangGraphServerConfig:
    assistant = _assistant(assistant_id)
    return LangGraphServerConfig(
        assistants={assistant_id: assistant},
        checkpointer=checkpointer,
        request_authorizer=request_authorizer,
    )


def test_default_is_stateless_and_graph_factory_receives_none() -> None:
    received: list[BaseCheckpointSaver[str] | None] = []

    def create_graph(checkpointer: BaseCheckpointSaver[str] | None) -> object:
        received.append(checkpointer)
        return graph_factory(checkpointer)

    assistant = AssistantConfig(
        assistant_id="echo",
        graph_id="echo-graph",
        name="Echo graph",
        checkpointed_graph_factory=create_graph,
    )
    config = LangGraphServerConfig(assistants={"echo": assistant})

    assert config.checkpointer is None
    assert received == [None]
    assert StandaloneAppConfig().to_server_config().checkpointer is None


def test_configured_checkpointer_is_used_for_all_run_thread_ids() -> None:
    saver = TrackingSaver()
    received: list[BaseCheckpointSaver[str]] = []

    def create_graph(checkpointer: BaseCheckpointSaver[str] | None) -> object:
        assert checkpointer is not None
        received.append(checkpointer)
        return graph_factory(checkpointer)

    assistant = AssistantConfig(
        assistant_id="echo",
        graph_id="echo-graph",
        name="Echo graph",
        checkpointed_graph_factory=create_graph,
    )
    config = LangGraphServerConfig(
        assistants={"echo": assistant},
        checkpointer=saver,
    )
    app = FastAPI()
    install_routes(app, config)

    with TestClient(app) as client:
        threaded = client.post(
            "/threads/conversation-1/runs/stream",
            json={"assistant_id": "echo", "input": {"message": "hello"}},
        )
        stateless = client.post(
            "/runs/stream",
            json={"assistant_id": "echo", "input": {"message": "hello"}},
        )

    assert threaded.status_code == 200
    assert stateless.status_code == 200
    assert received == [saver]
    assert "conversation-1" in saver.read_thread_ids
    assert len(saver.deleted_thread_ids) == 1
    assert saver.deleted_thread_ids[0] in saver.read_thread_ids


def test_all_routes_work_when_installed_in_an_existing_app() -> None:
    app = FastAPI()
    install_routes(
        app,
        _config(checkpointer=BoundedInMemorySaver()),
        prefix="/langgraph",
    )

    with TestClient(app) as client:
        assert client.get("/langgraph/health").json() == {"status": "ok"}
        assert client.get("/langgraph/info").json()["type"] == "langgraph-server"

        assistant = client.get("/langgraph/assistants/echo")
        graph = client.get("/langgraph/assistants/echo/graph")
        schemas = client.get("/langgraph/assistants/echo/schemas")
        search = client.post("/langgraph/assistants/search", json={})
        filtered_search = client.post(
            "/langgraph/assistants/search",
            json={"metadata": {"team": "other"}},
        )
        created = client.post(
            "/langgraph/threads",
            json={"thread_id": "thread-1", "metadata": {"source": "test"}},
        )
        initial_state = client.get("/langgraph/threads/thread-1/state")
        run = client.post(
            "/langgraph/threads/thread-1/runs",
            json={"assistant_id": "echo", "input": {"message": "hello"}},
        )
        state = client.get("/langgraph/threads/thread-1/state")
        state_at_checkpoint = client.post(
            "/langgraph/threads/thread-1/state/checkpoint",
            json={"checkpoint": state.json()["checkpoint"], "subgraphs": False},
        )
        thread = client.get("/langgraph/threads/thread-1")
        history = client.post(
            "/langgraph/threads/thread-1/history",
            json={"limit": 10},
        )
        update = client.post(
            "/langgraph/threads/thread-1/state",
            json={"values": {"response": "manual"}},
        )
        updated_state = client.get("/langgraph/threads/thread-1/state")
        streamed = client.post(
            "/langgraph/threads/thread-1/runs/stream",
            json={
                "assistant_id": "echo",
                "input": {"message": "streamed"},
                "stream_mode": ["values"],
            },
        )
        stateless = client.post(
            "/langgraph/runs/stream",
            json={"assistant_id": "echo", "input": {"message": "stateless"}},
        )

    assert assistant.status_code == 200
    assert assistant.json()["graph_id"] == "echo-graph"
    assert any(node["id"] == "respond" for node in graph.json()["nodes"])
    assert schemas.json()["input_schema"]["type"] == "object"
    assert [item["assistant_id"] for item in search.json()] == ["echo"]
    assert filtered_search.json() == []
    assert created.status_code == 200
    assert created.json()["metadata"] == {"source": "test"}
    assert initial_state.json()["values"] == {}
    assert run.status_code == 200
    assert state.json()["values"]["response"] == "Echo: hello"
    assert state_at_checkpoint.json()["values"]["response"] == "Echo: hello"
    assert thread.json()["values"]["response"] == "Echo: hello"
    assert history.json()[0]["values"]["response"] == "Echo: hello"
    assert update.status_code == 200
    assert update.json()["checkpoint"]["thread_id"] == "thread-1"
    assert updated_state.json()["values"]["response"] == "manual"
    assert "event: values" in streamed.text
    assert "event: end" in streamed.text
    assert "event: end" in stateless.text


def test_router_bound_configs_are_isolated_by_prefix() -> None:
    app = FastAPI()
    install_routes(app, _config("alpha"), prefix="/alpha")
    install_routes(app, _config("beta"), prefix="/beta")

    with TestClient(app) as client:
        alpha = client.post("/alpha/assistants/search", json={}).json()
        beta = client.post("/beta/assistants/search", json={}).json()

    assert [item["assistant_id"] for item in alpha] == ["alpha"]
    assert [item["assistant_id"] for item in beta] == ["beta"]


def test_thread_is_owned_by_its_first_assistant_and_can_be_deleted() -> None:
    app = FastAPI()
    config = LangGraphServerConfig(
        assistants={"alpha": _assistant("alpha"), "beta": _assistant("beta")},
        checkpointer=BoundedInMemorySaver(),
    )
    install_routes(app, config)

    with TestClient(app) as client:
        first = client.post(
            "/threads/shared/runs",
            json={"assistant_id": "alpha", "input": {"message": "first"}},
        )
        conflict = client.post(
            "/threads/shared/runs",
            json={"assistant_id": "beta", "input": {"message": "second"}},
        )
        deleted = client.delete("/threads/shared")
        missing = client.get("/threads/shared")

    assert first.status_code == 200
    assert conflict.status_code == 409
    assert "belongs to assistant alpha" in conflict.json()["detail"]
    assert deleted.status_code == 204
    assert missing.status_code == 404


def test_request_authorizer_protects_every_data_endpoint() -> None:
    def deny(_request: Request) -> None:
        raise HTTPException(status_code=401, detail="Authentication required")

    app = FastAPI()
    install_routes(app, _config(request_authorizer=deny))

    with TestClient(app) as client:
        health = client.get("/health")
        assistant = client.get("/assistants/echo")
        thread = client.get("/threads/private-thread/state")
        deleted = client.delete("/threads/private-thread")
        run = client.post(
            "/runs/stream",
            json={"assistant_id": "echo", "input": {}},
        )

    assert health.status_code == 200
    assert assistant.status_code == 401
    assert thread.status_code == 401
    assert deleted.status_code == 401
    assert run.status_code == 401


def test_oversized_thread_identifiers_are_rejected_at_http_boundary() -> None:
    app = FastAPI()
    install_routes(app, _config())
    oversized = "x" * 257

    with TestClient(app) as client:
        path_response = client.get(f"/threads/{oversized}/state")
        body_response = client.post("/threads", json={"thread_id": oversized})

    assert path_response.status_code == 422
    assert body_response.status_code == 422


def test_reserved_checkpoint_config_is_validated_at_http_boundary() -> None:
    app = FastAPI()
    install_routes(app, _config())

    with TestClient(app) as client:
        oversized_namespace = client.post(
            "/runs/stream",
            json={
                "assistant_id": "echo",
                "input": {},
                "config": {
                    "configurable": {"checkpoint_ns": "x" * 257},
                },
            },
        )
        invalid_map = client.post(
            "/runs/stream",
            json={
                "assistant_id": "echo",
                "input": {},
                "config": {"configurable": {"checkpoint_map": "invalid"}},
            },
        )

    assert oversized_namespace.status_code == 422
    assert invalid_map.status_code == 422


def test_oversized_thread_metadata_is_not_retained() -> None:
    app = FastAPI()
    config = _config()
    install_routes(app, config)

    with TestClient(app) as client:
        response = client.post(
            "/threads",
            json={"thread_id": "oversized", "metadata": {"value": "x" * 140_000}},
        )

    store = cast("InMemoryThreadStore", config.thread_store)
    assert response.status_code == 413
    assert len(store._threads) == 0


def test_unsupported_run_options_are_rejected() -> None:
    app = FastAPI()
    install_routes(app, _config())

    with TestClient(app) as client:
        missing = client.post(
            "/threads/missing/runs/stream",
            json={"assistant_id": "echo", "input": {}, "if_not_exists": "reject"},
        )
        unsupported = client.post(
            "/threads/new/runs/stream",
            json={"assistant_id": "echo", "input": {}, "webhook": "https://x"},
        )
        threaded_completion = client.post(
            "/threads/new/runs/stream",
            json={"assistant_id": "echo", "input": {}, "on_completion": "delete"},
        )

    assert missing.status_code == 404
    assert unsupported.status_code == 422
    assert threaded_completion.status_code == 422


def test_stateless_runs_do_not_retain_shared_persistence() -> None:
    config = _config()
    app = FastAPI()
    install_routes(app, config)

    with TestClient(app) as client:
        response = client.post(
            "/runs/stream",
            json={"assistant_id": "echo", "input": {"message": "hello"}},
        )

    store = cast("InMemoryThreadStore", config.thread_store)
    assert response.status_code == 200
    assert config.checkpointer is None
    assert len(store._threads) == 0


def test_thread_id_does_not_enable_checkpointing_when_none_is_configured() -> None:
    config = _config()
    app = FastAPI()
    install_routes(app, config)

    with TestClient(app) as client:
        first = client.post(
            "/threads/conversation-1/runs/stream",
            json={"assistant_id": "echo", "input": {"message": "first"}},
        )
        second = client.post(
            "/threads/conversation-1/runs/stream",
            json={"assistant_id": "echo", "input": {"message": "second"}},
        )
        state = client.get("/threads/conversation-1/state")

    assert "Echo: first" in first.text
    assert "Echo: second" in second.text
    assert state.status_code == 501
    assert state.json()["detail"] == "Checkpoint persistence is not configured"
