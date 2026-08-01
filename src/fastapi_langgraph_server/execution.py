"""Graph invocation helpers shared by run endpoints."""

import inspect
from collections.abc import Mapping
from typing import Any, cast

from langchain_core.runnables import RunnableConfig

from fastapi_langgraph_server.config import AssistantConfig
from fastapi_langgraph_server.models import Config
from fastapi_langgraph_server.state import ASSISTANT_ID_METADATA_KEY


def prepare_input(
    assistant: AssistantConfig,
    input_data: dict[str, Any],
) -> dict[str, Any]:
    """Apply the configured input transformation without adding hidden state."""
    source = dict(input_data)
    transformed = (
        assistant.input_transformer(source) if assistant.input_transformer else source
    )
    return dict(transformed)


def build_runnable_config(
    thread_id: str,
    assistant_id: str,
    request_config: Config | None = None,
    checkpoint_id: str | None = None,
) -> RunnableConfig:
    """Translate the HTTP run config into a native LangGraph config."""
    raw_config = request_config.model_dump(exclude_none=True) if request_config else {}
    configurable = dict(cast("dict[str, Any]", raw_config.pop("configurable", {})))
    configurable["thread_id"] = thread_id
    configurable.setdefault("checkpoint_ns", "")
    if checkpoint_id is not None:
        configurable["checkpoint_id"] = checkpoint_id
    raw_config["configurable"] = configurable
    raw_config["metadata"] = {ASSISTANT_ID_METADATA_KEY: assistant_id}
    return cast("RunnableConfig", raw_config)


async def invoke_graph(
    graph: object,
    payload: dict[str, Any],
    runnable_config: RunnableConfig,
) -> dict[str, Any]:
    """Invoke an asynchronous LangGraph."""
    async_invoke = getattr(graph, "ainvoke", None)
    if callable(async_invoke):
        result = async_invoke(payload, config=runnable_config)
        resolved = await _resolve_awaitable(result)
        return _as_payload(resolved)

    raise TypeError("checkpointed_graph_factory must return an async graph")


async def _resolve_awaitable(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


def _as_payload(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError("graph invocation must return a mapping")
    return {str(key): item for key, item in value.items()}
