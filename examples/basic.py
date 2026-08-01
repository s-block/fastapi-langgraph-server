"""Minimal checkpointed LangGraph served through FastAPI."""

from typing import TypedDict

from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.graph import END, START, StateGraph

from fastapi_langgraph_server import AssistantConfig, StandaloneAppConfig, create_app


class GreetingState(TypedDict, total=False):
    """Input and output state for the example graph."""

    name: str
    greeting: str


def greet(state: GreetingState) -> GreetingState:
    """Return a deterministic greeting so the server needs no external service."""
    name = state.get("name", "world")
    return {"greeting": f"Hello, {name}!"}


def build_graph(checkpointer: BaseCheckpointSaver[str] | None) -> object:
    """Compile statelessly or with the explicitly configured checkpointer."""
    builder = StateGraph(GreetingState)
    builder.add_node("greet", greet)
    builder.add_edge(START, "greet")
    builder.add_edge("greet", END)
    return builder.compile(checkpointer=checkpointer)


assistant = AssistantConfig(
    assistant_id="greeter",
    graph_id="greeter",
    name="Example greeter",
    description="A minimal deterministic LangGraph for local smoke testing.",
    checkpointed_graph_factory=build_graph,
    input_schema={
        "type": "object",
        "properties": {"name": {"type": "string"}},
    },
    output_schema={
        "type": "object",
        "properties": {"greeting": {"type": "string"}},
    },
)

app = create_app(
    StandaloneAppConfig(
        title="fastapi-langgraph-server example",
        description="A local test server backed by a basic LangGraph.",
        assistants={assistant.assistant_id: assistant},
        cors_origins=(),
    )
)
