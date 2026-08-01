"""Thread creation, lookup, state, and history endpoints."""

import asyncio
import secrets
from collections.abc import Awaitable
from datetime import UTC, datetime
from typing import Any, Protocol, cast

from fastapi import APIRouter, HTTPException, Request, Response, status
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.errors import InvalidUpdateError

from fastapi_langgraph_server.coordination import (
    RunCapacityError,
    RunLease,
    ThreadRunConflictError,
)
from fastapi_langgraph_server.dependencies import (
    Checkpointer,
    RequestAuthorization,
    Store,
    get_server_config,
)
from fastapi_langgraph_server.execution import build_runnable_config
from fastapi_langgraph_server.models import (
    Checkpoint,
    HistoryRequest,
    StateRequest,
    StateUpdate,
    StateUpdateResponse,
    Thread,
    ThreadCreate,
    ThreadId,
    ThreadState,
)
from fastapi_langgraph_server.state import (
    ASSISTANT_ID_METADATA_KEY,
    checkpoint_assistant_id,
    checkpoint_config,
    checkpoint_tuple_to_state,
    get_thread_state,
)
from fastapi_langgraph_server.storage import (
    ThreadAssistantConflictError,
    ThreadStore,
    ThreadStoreCapacityError,
)

router = APIRouter()


class _StateUpdatingGraph(Protocol):
    def aupdate_state(
        self,
        config: RunnableConfig,
        values: object,
        as_node: str | None = None,
    ) -> Awaitable[RunnableConfig]: ...


def _require_checkpointer(
    checkpointer: BaseCheckpointSaver[str] | None,
) -> BaseCheckpointSaver[str]:
    if checkpointer is None:
        raise HTTPException(
            status_code=501,
            detail="Checkpoint persistence is not configured",
        )
    return checkpointer


def _parse_timestamp(value: object) -> datetime:
    if not isinstance(value, str):
        return datetime.now(UTC)
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return datetime.now(UTC)


async def _thread_from_checkpoint(
    thread_id: str,
    checkpointer: BaseCheckpointSaver[str] | None,
) -> Thread | None:
    if checkpointer is None:
        return None
    item = await checkpointer.aget_tuple(checkpoint_config(thread_id))
    if item is None:
        return None
    checkpoint = item.checkpoint
    timestamp = _parse_timestamp(checkpoint.get("ts"))
    metadata = dict(item.metadata or {})
    metadata.pop(ASSISTANT_ID_METADATA_KEY, None)
    return Thread(
        thread_id=thread_id,
        created_at=timestamp,
        updated_at=timestamp,
        metadata=metadata,
        status="idle",
        values=dict(checkpoint.get("channel_values", {})),
    )


async def get_or_create_thread(
    thread_id: str | None,
    checkpointer: BaseCheckpointSaver[str] | None,
    store: ThreadStore,
    metadata: dict[str, Any] | None = None,
) -> Thread:
    """Resolve an existing thread or persist new server-owned metadata."""
    resolved_id = thread_id or secrets.token_hex(32)
    existing = await store.get_thread(resolved_id)
    if existing is not None:
        return existing
    checkpoint_thread = await _thread_from_checkpoint(resolved_id, checkpointer)
    if checkpoint_thread is not None:
        stored_thread = checkpoint_thread.model_copy(update={"values": {}})
        await store.put_thread(stored_thread)
        return checkpoint_thread

    now = datetime.now(UTC)
    thread = Thread(
        thread_id=resolved_id,
        created_at=now,
        updated_at=now,
        metadata=metadata or {},
        status="idle",
        values={},
    )
    await store.put_thread(thread)
    return thread


async def thread_exists(
    thread_id: str,
    checkpointer: BaseCheckpointSaver[str] | None,
    store: ThreadStore,
) -> bool:
    """Check both server-owned metadata and checkpoint-backed state."""
    if await store.get_thread(thread_id) is not None:
        return True
    return await _thread_from_checkpoint(thread_id, checkpointer) is not None


async def get_thread_assistant_id(
    thread_id: str,
    checkpointer: BaseCheckpointSaver[str] | None,
    store: ThreadStore,
) -> str | None:
    """Resolve assistant ownership from metadata or durable checkpoints."""
    assistant_id = await store.get_assistant_id(thread_id)
    if assistant_id is not None or checkpointer is None:
        return assistant_id
    item = await checkpointer.aget_tuple(checkpoint_config(thread_id))
    if item is None:
        return None
    assistant_id = checkpoint_assistant_id(item)
    if assistant_id is not None:
        await store.claim_assistant(thread_id, assistant_id)
    return assistant_id


async def claim_thread_assistant(
    thread_id: str,
    assistant_id: str,
    checkpointer: BaseCheckpointSaver[str] | None,
    store: ThreadStore,
) -> None:
    """Claim a thread for one assistant and reject cross-graph reuse."""
    existing = await get_thread_assistant_id(thread_id, checkpointer, store)
    if existing not in {None, assistant_id}:
        raise HTTPException(
            status_code=409,
            detail=f"Thread {thread_id} belongs to assistant {existing}",
        )
    try:
        await store.claim_assistant(thread_id, assistant_id)
    except ThreadAssistantConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


async def _acquire_mutation_lease(request: Request, thread_id: str) -> RunLease:
    try:
        return await get_server_config(request).run_coordinator.acquire(thread_id)
    except ThreadRunConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except RunCapacityError as exc:
        raise HTTPException(status_code=429, detail=str(exc)) from exc


@router.post("/threads", name="threads:create")
async def create_thread(
    request: ThreadCreate,
    checkpointer: Checkpointer,
    store: Store,
    _authorization: RequestAuthorization,
) -> Thread:
    """Create a thread, respecting ``if_exists`` conflict behavior."""
    thread_id = request.thread_id or secrets.token_hex(32)
    exists = await thread_exists(thread_id, checkpointer, store)
    if exists and request.if_exists == "raise":
        raise HTTPException(
            status_code=409,
            detail=f"Thread {thread_id} already exists",
        )
    try:
        return await get_or_create_thread(
            thread_id,
            checkpointer,
            store,
            request.metadata,
        )
    except ThreadStoreCapacityError as exc:
        raise HTTPException(
            status_code=413, detail="Thread metadata is too large"
        ) from exc


@router.get("/threads/{thread_id}", name="threads:get")
async def get_thread(
    thread_id: ThreadId,
    checkpointer: Checkpointer,
    store: Store,
    _authorization: RequestAuthorization,
) -> Thread:
    """Get thread metadata by ID."""
    if not await thread_exists(thread_id, checkpointer, store):
        raise HTTPException(status_code=404, detail=f"Thread {thread_id} not found")
    thread = await get_or_create_thread(thread_id, checkpointer, store)
    checkpoint_thread = await _thread_from_checkpoint(thread_id, checkpointer)
    if checkpoint_thread is not None:
        thread.values = checkpoint_thread.values
        thread.updated_at = checkpoint_thread.updated_at
    return thread


@router.get("/threads/{thread_id}/state", name="threads:get-state")
async def get_state(
    thread_id: ThreadId,
    checkpointer: Checkpointer,
    store: Store,
    _authorization: RequestAuthorization,
) -> ThreadState:
    """Get the latest state for an existing thread."""
    saver = _require_checkpointer(checkpointer)
    if not await thread_exists(thread_id, saver, store):
        raise HTTPException(status_code=404, detail=f"Thread {thread_id} not found")
    return await get_thread_state(thread_id, saver)


@router.post(
    "/threads/{thread_id}/state/checkpoint",
    name="threads:get-state-checkpoint",
)
async def get_state_checkpoint(
    thread_id: ThreadId,
    request: StateRequest,
    checkpointer: Checkpointer,
    store: Store,
    _authorization: RequestAuthorization,
) -> ThreadState:
    """Get state at a specific checkpoint when persistence supports it."""
    saver = _require_checkpointer(checkpointer)
    checkpoint = request.checkpoint
    if checkpoint is not None and checkpoint.thread_id != thread_id:
        raise HTTPException(
            status_code=422,
            detail="Checkpoint thread_id must match the route thread_id",
        )
    if not await thread_exists(thread_id, saver, store):
        raise HTTPException(status_code=404, detail=f"Thread {thread_id} not found")
    await get_or_create_thread(thread_id, saver, store)
    checkpoint_id = checkpoint.checkpoint_id if checkpoint else None
    checkpoint_ns = checkpoint.checkpoint_ns if checkpoint else ""
    try:
        return await get_thread_state(
            thread_id,
            saver,
            checkpoint_id,
            checkpoint_ns,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/threads/{thread_id}/state", name="threads:update-state")
async def update_state(
    thread_id: ThreadId,
    request_body: StateUpdate,
    request: Request,
    checkpointer: Checkpointer,
    store: Store,
    _authorization: RequestAuthorization,
) -> StateUpdateResponse:
    """Update state using the assistant durably associated with the thread."""
    saver = _require_checkpointer(checkpointer)
    if not await thread_exists(thread_id, saver, store):
        raise HTTPException(status_code=404, detail=f"Thread {thread_id} not found")
    server_config = get_server_config(request)
    lease = await _acquire_mutation_lease(request, thread_id)
    try:
        await get_or_create_thread(thread_id, saver, store)
        assistant_id = await get_thread_assistant_id(thread_id, saver, store)
        if assistant_id is None:
            raise HTTPException(
                status_code=409,
                detail="Thread has no assistant owner; run an assistant first",
            )
        checkpoint = request_body.checkpoint
        if checkpoint is not None and checkpoint.thread_id != thread_id:
            raise HTTPException(
                status_code=422,
                detail="Checkpoint thread_id must match the route thread_id",
            )
        checkpoint_id = (
            checkpoint.checkpoint_id
            if checkpoint is not None
            else request_body.checkpoint_id
        )
        runnable_config = build_runnable_config(
            thread_id,
            assistant_id,
            checkpoint_id=checkpoint_id,
        )
        if checkpoint is not None:
            runnable_config["configurable"]["checkpoint_ns"] = checkpoint.checkpoint_ns
        graph = server_config.get_graph_runtime(assistant_id)
        update = getattr(graph, "aupdate_state", None)
        if not callable(update):
            raise HTTPException(
                status_code=501,
                detail="Configured graph does not support state updates",
            )

        try:
            if server_config.run_timeout_seconds is None:
                result = await cast("_StateUpdatingGraph", graph).aupdate_state(
                    runnable_config,
                    request_body.values,
                    as_node=request_body.as_node,
                )
            else:
                async with asyncio.timeout(server_config.run_timeout_seconds):
                    result = await cast("_StateUpdatingGraph", graph).aupdate_state(
                        runnable_config,
                        request_body.values,
                        as_node=request_body.as_node,
                    )
        except TimeoutError as exc:
            raise HTTPException(
                status_code=504, detail="State update timed out"
            ) from exc
        except InvalidUpdateError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
    finally:
        await lease.release()

    configurable = result.get("configurable", {})
    result_thread_id = configurable.get("thread_id")
    result_checkpoint_id = configurable.get("checkpoint_id")
    result_checkpoint_ns = configurable.get("checkpoint_ns", "")
    if (
        result_thread_id != thread_id
        or not isinstance(result_checkpoint_id, str)
        or not isinstance(result_checkpoint_ns, str)
    ):
        raise RuntimeError("graph returned an invalid checkpoint config")
    checkpoint_map = configurable.get("checkpoint_map")
    return StateUpdateResponse(
        checkpoint=Checkpoint(
            thread_id=thread_id,
            checkpoint_ns=result_checkpoint_ns,
            checkpoint_id=result_checkpoint_id,
            checkpoint_map=(
                checkpoint_map if isinstance(checkpoint_map, dict) else None
            ),
        )
    )


@router.delete(
    "/threads/{thread_id}",
    name="threads:delete",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_thread(
    thread_id: ThreadId,
    request: Request,
    checkpointer: Checkpointer,
    store: Store,
    _authorization: RequestAuthorization,
) -> Response:
    """Delete server metadata and every persisted checkpoint for a thread."""
    server_config = get_server_config(request)
    lease = await _acquire_mutation_lease(request, thread_id)
    try:
        try:
            if server_config.run_timeout_seconds is None:
                if checkpointer is not None:
                    await checkpointer.adelete_thread(thread_id)
            else:
                async with asyncio.timeout(server_config.run_timeout_seconds):
                    if checkpointer is not None:
                        await checkpointer.adelete_thread(thread_id)
        except TimeoutError as exc:
            raise HTTPException(
                status_code=504, detail="Thread deletion timed out"
            ) from exc
        except NotImplementedError as exc:
            raise HTTPException(
                status_code=501,
                detail="Configured checkpointer does not support thread deletion",
            ) from exc
        await store.delete_thread(thread_id)
    finally:
        await lease.release()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/threads/{thread_id}/history", name="threads:get-history")
async def get_history(
    thread_id: ThreadId,
    request: HistoryRequest,
    checkpointer: Checkpointer,
    _authorization: RequestAuthorization,
) -> list[ThreadState]:
    """List checkpoint-backed state history newest first."""
    saver = _require_checkpointer(checkpointer)
    states: list[ThreadState] = []
    if request.checkpoint is not None and request.checkpoint.thread_id != thread_id:
        raise HTTPException(
            status_code=422,
            detail="Checkpoint thread_id must match the route thread_id",
        )
    checkpoint_ns = (
        request.checkpoint.checkpoint_ns if request.checkpoint is not None else ""
    )
    before = None
    if isinstance(request.before, Checkpoint):
        if request.before.thread_id != thread_id:
            raise HTTPException(
                status_code=422,
                detail="Before checkpoint thread_id must match the route thread_id",
            )
        before = checkpoint_config(
            thread_id,
            request.before.checkpoint_id,
            request.before.checkpoint_ns,
        )
    elif request.before is not None:
        before = checkpoint_config(thread_id, request.before, checkpoint_ns)
    async for item in saver.alist(
        checkpoint_config(thread_id, checkpoint_ns=checkpoint_ns),
        filter=request.metadata,
        before=before,
        limit=request.limit,
    ):
        states.append(checkpoint_tuple_to_state(thread_id, item))
    return states
