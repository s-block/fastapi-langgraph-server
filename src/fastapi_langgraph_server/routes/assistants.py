"""Assistant discovery endpoints."""

from typing import Any, Protocol, cast

from fastapi import APIRouter, HTTPException, Request

from fastapi_langgraph_server.dependencies import (
    RequestAuthorization,
    ServerConfig,
    get_assistant_config,
)
from fastapi_langgraph_server.models import (
    Assistant,
    AssistantSearch,
    Config,
    GraphSchema,
)

router = APIRouter()


class _SerializableGraph(Protocol):
    def to_json(self) -> dict[str, Any]: ...


def _assistant_response(assistant_id: str, request: Request) -> Assistant:
    assistant = get_assistant_config(assistant_id, request)
    return Assistant(
        assistant_id=assistant.assistant_id,
        graph_id=assistant.graph_id,
        config=Config(),
        created_at=assistant.created_at,
        updated_at=assistant.created_at,
        metadata=dict(assistant.metadata),
        version=1,
        name=assistant.name,
        description=assistant.description,
    )


@router.get("/assistants/{assistant_id}", name="assistants:get")
async def get_assistant(
    assistant_id: str,
    request: Request,
    _authorization: RequestAuthorization,
) -> Assistant:
    """Get assistant metadata by ID."""
    return _assistant_response(assistant_id, request)


@router.get("/assistants/{assistant_id}/graph", name="assistants:get-graph")
async def get_assistant_graph(
    assistant_id: str,
    request: Request,
    config: ServerConfig,
    _authorization: RequestAuthorization,
) -> dict[str, Any]:
    """Return configured or native graph topology."""
    assistant = get_assistant_config(assistant_id, request)
    if assistant.graph is not None:
        return dict(assistant.graph)
    graph_provider = config.get_graph_runtime(assistant_id)
    get_graph = getattr(graph_provider, "get_graph", None)
    if not callable(get_graph):
        raise HTTPException(
            status_code=501,
            detail="Graph topology is not available for this assistant",
        )
    graph = get_graph()
    if not callable(getattr(graph, "to_json", None)):
        raise HTTPException(
            status_code=501,
            detail="Graph topology is not serializable for this assistant",
        )
    return cast("_SerializableGraph", graph).to_json()


def _graph_schema(graph: object, method_name: str) -> dict[str, Any] | None:
    method = getattr(graph, method_name, None)
    if not callable(method):
        return None
    schema = method()
    return dict(schema) if isinstance(schema, dict) else None


@router.get("/assistants/{assistant_id}/schemas", name="assistants:get-schemas")
async def get_assistant_schemas(
    assistant_id: str,
    request: Request,
    config: ServerConfig,
    _authorization: RequestAuthorization,
) -> GraphSchema:
    """Return configured schemas or schemas exposed by a native graph."""
    assistant = get_assistant_config(assistant_id, request)
    graph = config.get_graph_runtime(assistant_id)
    input_schema = (
        dict(assistant.input_schema)
        if assistant.input_schema is not None
        else _graph_schema(graph, "get_input_jsonschema")
    )
    output_schema = (
        dict(assistant.output_schema)
        if assistant.output_schema is not None
        else _graph_schema(graph, "get_output_jsonschema")
    )
    return GraphSchema(
        graph_id=assistant.graph_id,
        input_schema=input_schema,
        output_schema=output_schema,
        state_schema=(
            dict(assistant.state_schema)
            if assistant.state_schema is not None
            else input_schema
        ),
        config_schema=(
            dict(assistant.config_schema)
            if assistant.config_schema is not None
            else None
        ),
    )


@router.post("/assistants/search", name="assistants:search")
async def search_assistants(
    request_body: AssistantSearch,
    config: ServerConfig,
    _authorization: RequestAuthorization,
) -> list[Assistant]:
    """Search configured assistants by graph ID."""
    results = [
        Assistant(
            assistant_id=assistant.assistant_id,
            graph_id=assistant.graph_id,
            config=Config(),
            created_at=assistant.created_at,
            updated_at=assistant.created_at,
            metadata=dict(assistant.metadata),
            version=1,
            name=assistant.name,
            description=assistant.description,
        )
        for assistant in config.assistants.values()
        if request_body.graph_id is None or assistant.graph_id == request_body.graph_id
        if request_body.metadata is None
        or all(
            assistant.metadata.get(key) == value
            for key, value in request_body.metadata.items()
        )
    ]
    start = request_body.offset
    return results[start : start + request_body.limit]
