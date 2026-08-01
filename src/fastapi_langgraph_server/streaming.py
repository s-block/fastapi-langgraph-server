"""Server-Sent Event streaming for native LangGraphs."""

import asyncio
import json
import logging
import uuid
from collections.abc import AsyncIterator, Mapping
from datetime import UTC, datetime
from typing import Any, Protocol, cast

from fastapi.responses import StreamingResponse
from starlette.background import BackgroundTask

from fastapi_langgraph_server.checkpoint.memory import CheckpointCapacityError
from fastapi_langgraph_server.config import AssistantConfig
from fastapi_langgraph_server.execution import (
    build_runnable_config,
    prepare_input,
)
from fastapi_langgraph_server.models import Config
from fastapi_langgraph_server.storage import ThreadStore

logger = logging.getLogger(__name__)


class _NativeStreamingGraph(Protocol):
    def astream(self, *args: Any, **kwargs: Any) -> AsyncIterator[Any]: ...


def format_sse_event(event: str, data: Any) -> str:
    """Serialize one named Server-Sent Event."""
    json_data = json.dumps(data, default=str, separators=(",", ":"))
    return f"event: {event}\ndata: {json_data}\n\n"


def _serialize_message(message: object) -> dict[str, Any] | None:
    if isinstance(message, Mapping):
        return {str(key): value for key, value in message.items()}
    content = getattr(message, "content", "")
    if not content:
        return None
    message_type = getattr(message, "type", None)
    return {
        "type": message_type if isinstance(message_type, str) else "message",
        "content": content,
        "id": getattr(message, "id", None) or str(uuid.uuid4()),
    }


def _message_event_payload(
    assistant: AssistantConfig,
    message: object,
    metadata: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]] | None:
    serialized = _serialize_message(message)
    if serialized is None:
        return None
    message_metadata = dict(metadata or {})
    message_metadata.setdefault("langgraph_step", 1)
    message_metadata.setdefault("langgraph_node", assistant.assistant_id)
    return [serialized, message_metadata]


def _normalize_native_stream_item(
    item: Any,
    modes: list[str],
) -> tuple[str, Any]:
    if (
        isinstance(item, tuple)
        and len(item) == 2
        and isinstance(item[0], str)
        and item[0] in {"values", "updates", "messages", "custom", "debug"}
    ):
        return item[0], item[1]
    return modes[0], item


async def _stream_native_graph(
    *,
    graph: object,
    assistant: AssistantConfig,
    payload: dict[str, Any],
    thread_id: str,
    modes: list[str],
    request_config: Config | None,
    checkpoint_id: str | None,
) -> AsyncIterator[str]:
    native_modes = ["messages" if mode == "messages-tuple" else mode for mode in modes]
    native_modes = list(dict.fromkeys(native_modes))
    runnable_config = build_runnable_config(
        thread_id,
        assistant.assistant_id,
        request_config,
        checkpoint_id,
    )
    astream = cast("_NativeStreamingGraph", graph).astream
    iterator = astream(payload, config=runnable_config, stream_mode=native_modes)
    async for raw_item in iterator:
        mode, item = _normalize_native_stream_item(raw_item, native_modes)
        if mode == "messages":
            message = item
            metadata: Mapping[str, Any] | None = None
            if isinstance(item, tuple) and len(item) == 2:
                message = item[0]
                if isinstance(item[1], Mapping):
                    metadata = item[1]
            event_payload = _message_event_payload(assistant, message, metadata)
            if event_payload is not None:
                yield format_sse_event("messages", event_payload)
            continue

        if mode == "values" and isinstance(item, Mapping):
            values = {str(key): value for key, value in item.items()}
            transformed = (
                assistant.output_transformer(dict(values))
                if assistant.output_transformer
                else values
            )
            yield format_sse_event("values", transformed)
            continue

        if mode in {"updates", "custom", "debug"}:
            yield format_sse_event(mode, item)


async def _set_thread_status(
    thread_id: str,
    store: ThreadStore,
    status: str,
) -> None:
    thread = await store.get_thread(thread_id)
    if thread is None:
        return
    thread.status = status
    thread.updated_at = datetime.now(UTC)
    await store.put_thread(thread)


async def stream_graph_response(
    assistant: AssistantConfig,
    input_data: dict[str, Any],
    thread_id: str,
    store: ThreadStore,
    graph: object,
    stream_mode: list[str] | None = None,
    request_config: Config | None = None,
    checkpoint_id: str | None = None,
) -> AsyncIterator[str]:
    """Run an assistant and emit LangGraph-style named SSE events."""
    modes = stream_mode or ["values"]
    run_id = str(uuid.uuid4())
    yield format_sse_event("metadata", {"run_id": run_id, "attempt": 1})
    await _set_thread_status(thread_id, store, "busy")

    try:
        payload = prepare_input(assistant, input_data)
        generator = _stream_native_graph(
            graph=graph,
            assistant=assistant,
            payload=payload,
            thread_id=thread_id,
            modes=modes,
            request_config=request_config,
            checkpoint_id=checkpoint_id,
        )
        async for event in generator:
            yield event
        yield format_sse_event("end", None)
    except asyncio.CancelledError:
        raise
    except CheckpointCapacityError:
        logger.warning(
            "Checkpoint capacity exceeded for assistant_id=%s",
            assistant.assistant_id,
        )
        yield format_sse_event(
            "error",
            {
                "error": "Checkpoint capacity exceeded",
                "code": "CHECKPOINT_CAPACITY_EXCEEDED",
            },
        )
    except Exception as exc:
        logger.error(
            "Graph streaming failed for assistant_id=%s error_type=%s",
            assistant.assistant_id,
            type(exc).__name__,
        )
        yield format_sse_event(
            "error",
            {"error": "Graph execution failed", "code": "INTERNAL_ERROR"},
        )
    finally:
        await _set_thread_status(thread_id, store, "idle")


def create_streaming_response(
    generator: AsyncIterator[str],
    *,
    background: BackgroundTask | None = None,
) -> StreamingResponse:
    """Create an SSE response with proxy-buffering disabled."""
    return StreamingResponse(
        generator,
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
        background=background,
    )
