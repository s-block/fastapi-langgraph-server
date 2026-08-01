"""Conversions between checkpoint state and HTTP thread-state models."""

import uuid
from datetime import UTC, datetime
from typing import Any, cast

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import BaseCheckpointSaver, CheckpointTuple

from fastapi_langgraph_server.models import Checkpoint, ThreadState

ASSISTANT_ID_METADATA_KEY = "fastapi_langgraph_server_assistant_id"


def checkpoint_config(
    thread_id: str,
    checkpoint_id: str | None = None,
    checkpoint_ns: str = "",
) -> RunnableConfig:
    """Create a saver lookup config for a thread and optional checkpoint."""
    configurable: dict[str, Any] = {
        "thread_id": thread_id,
        "checkpoint_ns": checkpoint_ns,
    }
    if checkpoint_id is not None:
        configurable["checkpoint_id"] = checkpoint_id
    return cast("RunnableConfig", {"configurable": configurable})


def checkpoint_tuple_to_state(
    thread_id: str,
    item: CheckpointTuple,
) -> ThreadState:
    """Translate LangGraph checkpoint data to the HTTP state model."""
    checkpoint = item.checkpoint
    item_configurable = item.config.get("configurable", {})
    checkpoint_ns = item_configurable.get("checkpoint_ns", "")
    if not isinstance(checkpoint_ns, str):
        checkpoint_ns = ""
    parent_id: str | None = None
    parent_ns = checkpoint_ns
    if item.parent_config is not None:
        parent_configurable = item.parent_config.get("configurable", {})
        raw_parent = parent_configurable.get("checkpoint_id")
        if isinstance(raw_parent, str):
            parent_id = raw_parent
        raw_parent_ns = parent_configurable.get("checkpoint_ns")
        if isinstance(raw_parent_ns, str):
            parent_ns = raw_parent_ns
    checkpoint_id = checkpoint.get("id")
    metadata = dict(item.metadata or {})
    metadata.pop(ASSISTANT_ID_METADATA_KEY, None)
    return ThreadState(
        values=checkpoint.get("channel_values", {}),
        next=[],
        checkpoint=Checkpoint(
            thread_id=thread_id,
            checkpoint_ns=checkpoint_ns,
            checkpoint_id=(
                checkpoint_id if isinstance(checkpoint_id, str) else str(uuid.uuid4())
            ),
        ),
        metadata=metadata,
        created_at=str(checkpoint.get("ts", datetime.now(UTC).isoformat())),
        parent_checkpoint=(
            Checkpoint(
                thread_id=thread_id,
                checkpoint_ns=parent_ns,
                checkpoint_id=parent_id,
            )
            if parent_id is not None
            else None
        ),
        tasks=[],
        interrupts=[],
    )


def checkpoint_assistant_id(item: CheckpointTuple) -> str | None:
    """Read the server-owned assistant marker from checkpoint metadata."""
    value = (item.metadata or {}).get(ASSISTANT_ID_METADATA_KEY)
    return value if isinstance(value, str) and value else None


async def get_thread_state(
    thread_id: str,
    checkpointer: BaseCheckpointSaver[str],
    checkpoint_id: str | None = None,
    checkpoint_ns: str = "",
) -> ThreadState:
    """Read checkpoint-backed state or return an empty initial state."""
    item = await checkpointer.aget_tuple(
        checkpoint_config(thread_id, checkpoint_id, checkpoint_ns)
    )
    if item is not None:
        return checkpoint_tuple_to_state(thread_id, item)
    if checkpoint_id is not None:
        raise LookupError(
            f"Checkpoint {checkpoint_id} was not found for thread {thread_id}"
        )

    state = ThreadState(
        values={},
        next=[],
        checkpoint=Checkpoint(
            thread_id=thread_id,
            checkpoint_ns="",
            checkpoint_id=str(uuid.uuid4()),
        ),
        metadata={},
        created_at=datetime.now(UTC).isoformat(),
        tasks=[],
        interrupts=[],
    )
    return state
