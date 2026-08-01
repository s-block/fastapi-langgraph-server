"""Configuration objects for the reusable LangGraph server."""

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from importlib.metadata import version as package_version
from math import isfinite
from typing import Any

from fastapi import Request
from langgraph.checkpoint.base import BaseCheckpointSaver

from fastapi_langgraph_server.coordination import RunCoordinator
from fastapi_langgraph_server.storage import InMemoryThreadStore, ThreadStore

type CheckpointedGraphFactory = Callable[[BaseCheckpointSaver[str] | None], object]
type PayloadTransformer = Callable[[dict[str, Any]], dict[str, Any]]
type RequestAuthorizer = Callable[[Request], Awaitable[None] | None]


@dataclass(frozen=True, slots=True)
class AssistantConfig:
    """Describe one graph exposed through the LangGraph HTTP protocol.

    ``checkpointed_graph_factory`` receives the configured saver, or ``None``
    when persistence is disabled, and must return an asynchronous compiled
    LangGraph.
    """

    assistant_id: str
    graph_id: str
    name: str
    checkpointed_graph_factory: CheckpointedGraphFactory
    description: str = ""
    metadata: Mapping[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    input_transformer: PayloadTransformer | None = None
    output_transformer: PayloadTransformer | None = None
    default_stream_mode: tuple[str, ...] | None = None
    input_schema: Mapping[str, Any] | None = None
    output_schema: Mapping[str, Any] | None = None
    state_schema: Mapping[str, Any] | None = None
    config_schema: Mapping[str, Any] | None = None
    graph: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        for field_name, value in (
            ("assistant_id", self.assistant_id),
            ("graph_id", self.graph_id),
        ):
            if not value:
                raise ValueError(f"{field_name} must not be empty")
            if len(value) > 256:
                raise ValueError(f"{field_name} exceeds its maximum length")
            if not value.isprintable():
                raise ValueError(f"{field_name} must contain only printable characters")

    def create_graph(self, checkpointer: BaseCheckpointSaver[str] | None) -> object:
        """Compile and validate an asynchronous graph with the server saver."""
        graph = self.checkpointed_graph_factory(checkpointer)
        if not callable(getattr(graph, "ainvoke", None)):
            raise TypeError("checkpointed_graph_factory must return an async graph")
        if not callable(getattr(graph, "astream", None)):
            raise TypeError("checkpointed_graph_factory must return a streaming graph")
        return graph


@dataclass(slots=True)
class LangGraphServerConfig:
    """Configure assistants, persistence, and request authorization."""

    assistants: dict[str, AssistantConfig] = field(default_factory=dict)
    checkpointer: BaseCheckpointSaver[str] | None = None
    request_authorizer: RequestAuthorizer | None = None
    thread_store: ThreadStore = field(default_factory=InMemoryThreadStore)
    max_concurrent_runs: int = 100
    run_timeout_seconds: float | None = None
    _graphs: dict[str, object] = field(init=False, repr=False)
    run_coordinator: RunCoordinator = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if (
            isinstance(self.max_concurrent_runs, bool)
            or not isinstance(self.max_concurrent_runs, int)
            or self.max_concurrent_runs <= 0
        ):
            raise ValueError("max_concurrent_runs must be an integer greater than zero")
        if self.run_timeout_seconds is not None and (
            isinstance(self.run_timeout_seconds, bool)
            or not isinstance(self.run_timeout_seconds, (int, float))
            or not isfinite(self.run_timeout_seconds)
            or self.run_timeout_seconds <= 0
        ):
            raise ValueError("run_timeout_seconds must be a finite number above zero")
        self.assistants = dict(self.assistants)
        for assistant_id, assistant in self.assistants.items():
            if assistant_id != assistant.assistant_id:
                raise ValueError(
                    "assistant registry key must match AssistantConfig.assistant_id"
                )
        self._graphs = {
            assistant_id: assistant.create_graph(self.checkpointer)
            for assistant_id, assistant in self.assistants.items()
        }
        self.run_coordinator = RunCoordinator(self.max_concurrent_runs)

    def get_assistant(self, assistant_id: str) -> AssistantConfig | None:
        """Return the configured assistant with ``assistant_id``."""
        return self.assistants.get(assistant_id)

    def get_graph_runtime(self, assistant_id: str) -> object:
        """Return the reusable graph built during server configuration."""
        try:
            return self._graphs[assistant_id]
        except KeyError as exc:  # pragma: no cover - guarded by route lookup
            raise LookupError(f"Assistant {assistant_id} is not configured") from exc


@dataclass(slots=True)
class StandaloneAppConfig:
    """Configure a standalone FastAPI application containing the routes."""

    title: str = "LangGraph API"
    description: str = "LangGraph-compatible API server"
    version: str = field(
        default_factory=lambda: package_version("fastapi-langgraph-server")
    )
    debug: bool = False
    cors_origins: tuple[str, ...] = ()
    assistants: dict[str, AssistantConfig] = field(default_factory=dict)
    checkpointer: BaseCheckpointSaver[str] | None = None
    request_authorizer: RequestAuthorizer | None = None
    thread_store: ThreadStore = field(default_factory=InMemoryThreadStore)
    max_concurrent_runs: int = 100
    run_timeout_seconds: float | None = None
    max_request_body_bytes: int | None = 1_048_576

    def __post_init__(self) -> None:
        if self.max_request_body_bytes is not None and (
            isinstance(self.max_request_body_bytes, bool)
            or not isinstance(self.max_request_body_bytes, int)
            or self.max_request_body_bytes <= 0
        ):
            raise ValueError(
                "max_request_body_bytes must be an integer greater than zero"
            )

    def to_server_config(self) -> LangGraphServerConfig:
        """Create the route-level configuration used by the application."""
        return LangGraphServerConfig(
            assistants=self.assistants,
            checkpointer=self.checkpointer,
            request_authorizer=self.request_authorizer,
            thread_store=self.thread_store,
            max_concurrent_runs=self.max_concurrent_runs,
            run_timeout_seconds=self.run_timeout_seconds,
        )
