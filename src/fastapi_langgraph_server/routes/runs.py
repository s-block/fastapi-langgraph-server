"""Streaming and non-streaming run endpoints."""

import asyncio
import logging
import secrets
import uuid
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from starlette.background import BackgroundTask

from fastapi_langgraph_server.checkpoint.memory import CheckpointCapacityError
from fastapi_langgraph_server.config import AssistantConfig
from fastapi_langgraph_server.coordination import (
    RunCapacityError,
    RunLease,
    ThreadRunConflictError,
)
from fastapi_langgraph_server.dependencies import (
    Checkpointer,
    RequestAuthorization,
    Store,
    get_assistant_config,
    get_server_config,
)
from fastapi_langgraph_server.execution import (
    build_runnable_config,
    invoke_graph,
    prepare_input,
)
from fastapi_langgraph_server.models import RunCreate, ThreadId
from fastapi_langgraph_server.routes.threads import (
    claim_thread_assistant,
    get_or_create_thread,
    thread_exists,
)
from fastapi_langgraph_server.storage import InMemoryThreadStore
from fastapi_langgraph_server.streaming import (
    create_streaming_response,
    format_sse_event,
    stream_graph_response,
)

logger = logging.getLogger(__name__)
router = APIRouter()
_DEFAULT_STREAM_MODE: tuple[str, ...] = ("values",)


def _resolve_stream_modes(
    requested_modes: Sequence[str] | None,
    assistant: AssistantConfig,
) -> list[str]:
    modes = list(
        requested_modes or assistant.default_stream_mode or _DEFAULT_STREAM_MODE
    )
    return list(dict.fromkeys(modes))


def _stream_generator(
    *,
    assistant: AssistantConfig,
    request_body: RunCreate,
    thread_id: ThreadId,
    graph: object,
    store: Store,
) -> AsyncIterator[str]:
    modes = _resolve_stream_modes(request_body.stream_mode, assistant)
    return stream_graph_response(
        assistant=assistant,
        graph=graph,
        input_data=request_body.input or {},
        thread_id=thread_id,
        store=store,
        stream_mode=modes,
        request_config=request_body.config,
        checkpoint_id=request_body.checkpoint_id,
    )


def _stream_response(
    *,
    assistant: AssistantConfig,
    request_body: RunCreate,
    thread_id: ThreadId,
    graph: object,
    store: Store,
    lease: RunLease,
    timeout_seconds: float | None,
) -> StreamingResponse:
    generator = _run_with_lease(
        _stream_generator(
            assistant=assistant,
            request_body=request_body,
            thread_id=thread_id,
            graph=graph,
            store=store,
        ),
        lease=lease,
        timeout_seconds=timeout_seconds,
    )
    return create_streaming_response(
        generator,
        background=BackgroundTask(lease.release),
    )


async def _run_with_lease(
    generator: AsyncIterator[str],
    *,
    lease: RunLease,
    timeout_seconds: float | None,
) -> AsyncIterator[str]:
    """Apply a run timeout and always release its process-local reservation."""

    async def iterate() -> AsyncIterator[str]:
        async for event in generator:
            yield event

    try:
        if timeout_seconds is None:
            async for event in iterate():
                yield event
        else:
            async with asyncio.timeout(timeout_seconds):
                async for event in iterate():
                    yield event
    except TimeoutError:
        yield format_sse_event(
            "error",
            {"error": "Graph execution timed out", "code": "RUN_TIMEOUT"},
        )
    finally:
        await lease.release()


async def _acquire_run_lease(request: Request, thread_id: str) -> RunLease:
    config = get_server_config(request)
    try:
        return await config.run_coordinator.acquire(thread_id)
    except ThreadRunConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except RunCapacityError as exc:
        raise HTTPException(status_code=429, detail=str(exc)) from exc


async def _delete_stateless_checkpoints(
    generator: AsyncIterator[str],
    *,
    checkpointer: Checkpointer,
    thread_id: str,
) -> AsyncIterator[str]:
    """Delete request-scoped checkpoints after a generated-thread stream."""
    try:
        async for event in generator:
            yield event
    finally:
        if checkpointer is not None:
            try:
                await asyncio.shield(checkpointer.adelete_thread(thread_id))
            except NotImplementedError:
                logger.warning(
                    "Configured checkpointer does not support stateless cleanup"
                )
            except Exception as exc:
                logger.error(
                    "Failed to delete stateless checkpoints error_type=%s",
                    type(exc).__name__,
                )


@router.post(
    "/threads/{thread_id}/runs/stream",
    name="runs:stream-with-thread",
)
async def stream_run_with_thread(
    thread_id: ThreadId,
    request_body: RunCreate,
    request: Request,
    checkpointer: Checkpointer,
    store: Store,
    _authorization: RequestAuthorization,
) -> StreamingResponse:
    """Run an assistant and stream events on an existing or new thread."""
    if request_body.on_completion is not None:
        raise HTTPException(
            status_code=422,
            detail="on_completion is supported only for stateless runs",
        )
    assistant = get_assistant_config(request_body.assistant_id, request)
    if request_body.if_not_exists == "reject" and not await thread_exists(
        thread_id,
        checkpointer,
        store,
    ):
        raise HTTPException(status_code=404, detail=f"Thread {thread_id} not found")
    await get_or_create_thread(thread_id, checkpointer, store)
    await claim_thread_assistant(
        thread_id,
        assistant.assistant_id,
        checkpointer,
        store,
    )
    config = get_server_config(request)
    lease = await _acquire_run_lease(request, thread_id)
    return _stream_response(
        assistant=assistant,
        request_body=request_body,
        thread_id=thread_id,
        graph=config.get_graph_runtime(assistant.assistant_id),
        store=store,
        lease=lease,
        timeout_seconds=config.run_timeout_seconds,
    )


@router.post("/runs/stream", name="runs:stream-stateless")
async def stream_run_stateless(
    request_body: RunCreate,
    request: Request,
    checkpointer: Checkpointer,
    _authorization: RequestAuthorization,
) -> StreamingResponse:
    """Run an assistant on a generated ID with optional persistence."""
    assistant = get_assistant_config(request_body.assistant_id, request)
    if request_body.if_not_exists == "reject":
        raise HTTPException(
            status_code=422,
            detail="if_not_exists=reject is invalid for a stateless run",
        )
    thread_id = secrets.token_hex(32)
    ephemeral_store = InMemoryThreadStore(max_threads=1)
    await get_or_create_thread(thread_id, checkpointer, ephemeral_store)
    await claim_thread_assistant(
        thread_id,
        assistant.assistant_id,
        checkpointer,
        ephemeral_store,
    )
    config = get_server_config(request)
    lease = await _acquire_run_lease(request, thread_id)
    generator = _stream_generator(
        assistant=assistant,
        request_body=request_body,
        thread_id=thread_id,
        graph=config.get_graph_runtime(assistant.assistant_id),
        store=ephemeral_store,
    )
    return create_streaming_response(
        _run_with_lease(
            _delete_stateless_checkpoints(
                generator,
                checkpointer=checkpointer,
                thread_id=thread_id,
            ),
            lease=lease,
            timeout_seconds=config.run_timeout_seconds,
        ),
        background=BackgroundTask(lease.release),
    )


@router.post("/threads/{thread_id}/runs", name="runs:create")
async def create_run(
    thread_id: ThreadId,
    request_body: RunCreate,
    request: Request,
    checkpointer: Checkpointer,
    store: Store,
    _authorization: RequestAuthorization,
) -> dict[str, Any]:
    """Invoke a graph without streaming."""
    unsupported = [
        option
        for option, value in (
            ("on_disconnect", request_body.on_disconnect),
            ("on_completion", request_body.on_completion),
        )
        if value is not None
    ]
    if unsupported:
        raise HTTPException(
            status_code=422,
            detail=(
                "Unsupported options for non-streaming runs: " + ", ".join(unsupported)
            ),
        )
    assistant = get_assistant_config(request_body.assistant_id, request)
    if request_body.if_not_exists == "reject" and not await thread_exists(
        thread_id,
        checkpointer,
        store,
    ):
        raise HTTPException(status_code=404, detail=f"Thread {thread_id} not found")
    await get_or_create_thread(thread_id, checkpointer, store)
    await claim_thread_assistant(
        thread_id,
        assistant.assistant_id,
        checkpointer,
        store,
    )
    config = get_server_config(request)
    payload = prepare_input(assistant, request_body.input or {})
    runnable_config = build_runnable_config(
        thread_id,
        assistant.assistant_id,
        request_body.config,
        request_body.checkpoint_id,
    )
    lease = await _acquire_run_lease(request, thread_id)
    try:
        try:
            if config.run_timeout_seconds is None:
                await invoke_graph(
                    config.get_graph_runtime(assistant.assistant_id),
                    payload,
                    runnable_config,
                )
            else:
                async with asyncio.timeout(config.run_timeout_seconds):
                    await invoke_graph(
                        config.get_graph_runtime(assistant.assistant_id),
                        payload,
                        runnable_config,
                    )
        except TimeoutError as exc:
            raise HTTPException(
                status_code=504, detail="Graph execution timed out"
            ) from exc
        except CheckpointCapacityError as exc:
            raise HTTPException(
                status_code=507,
                detail="Checkpoint capacity exceeded",
            ) from exc
    finally:
        await lease.release()
    run_id = str(uuid.uuid4())
    now = datetime.now(UTC).isoformat()
    logger.info(
        "Completed run_id=%s assistant_id=%s",
        run_id,
        assistant.assistant_id,
    )
    return {
        "run_id": run_id,
        "thread_id": thread_id,
        "assistant_id": assistant.assistant_id,
        "created_at": now,
        "updated_at": now,
        "status": "success",
        "metadata": request_body.metadata or {},
    }
