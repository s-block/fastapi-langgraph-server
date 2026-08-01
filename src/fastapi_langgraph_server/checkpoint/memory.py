"""Resource-bounded in-memory checkpoint persistence."""

import asyncio
from collections import OrderedDict
from collections.abc import AsyncIterator, Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from math import isfinite
from threading import RLock
from time import monotonic
from typing import Any

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    ChannelVersions,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
    DeltaChannelHistory,
    get_checkpoint_metadata,
)
from langgraph.checkpoint.memory import InMemorySaver as LangGraphInMemorySaver
from langgraph.checkpoint.serde.base import SerializerProtocol

type SizeKey = tuple[object, ...]
type SerializedValue = tuple[str, bytes]


class CheckpointCapacityError(RuntimeError):
    """Raised when a checkpoint would exceed configured memory limits."""


@dataclass(frozen=True, slots=True)
class InMemoryCheckpointConfig:
    """Safety limits for :class:`BoundedInMemorySaver`.

    Byte limits cover serialized payload bytes. Python container overhead is
    additional, so these limits are a guardrail rather than a process RSS cap.
    """

    idle_ttl_seconds: float = 14_400
    max_threads: int = 1_000
    max_namespaces_per_thread: int = 16
    max_checkpoints_per_thread: int = 1_000
    max_checkpoint_bytes: int = 1_048_576
    max_pending_write_bytes: int = 524_288
    max_total_bytes: int = 67_108_864
    max_thread_id_length: int = 256
    max_checkpoint_namespace_length: int = 256

    def __post_init__(self) -> None:
        if (
            isinstance(self.idle_ttl_seconds, bool)
            or not isinstance(self.idle_ttl_seconds, (int, float))
            or not isfinite(self.idle_ttl_seconds)
            or self.idle_ttl_seconds <= 0
        ):
            raise ValueError(
                "idle_ttl_seconds must be a finite number greater than zero"
            )
        positive_integer_fields = {
            "max_threads": self.max_threads,
            "max_namespaces_per_thread": self.max_namespaces_per_thread,
            "max_checkpoints_per_thread": self.max_checkpoints_per_thread,
            "max_checkpoint_bytes": self.max_checkpoint_bytes,
            "max_pending_write_bytes": self.max_pending_write_bytes,
            "max_total_bytes": self.max_total_bytes,
            "max_thread_id_length": self.max_thread_id_length,
            "max_checkpoint_namespace_length": self.max_checkpoint_namespace_length,
        }
        for field_name, value in positive_integer_fields.items():
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{field_name} must be an integer greater than zero")
        if self.max_checkpoint_bytes > self.max_total_bytes:
            raise ValueError("max_checkpoint_bytes must not exceed max_total_bytes")
        if self.max_pending_write_bytes > self.max_total_bytes:
            raise ValueError("max_pending_write_bytes must not exceed max_total_bytes")


@dataclass(frozen=True, slots=True)
class InMemoryCheckpointStats:
    """Non-sensitive capacity metrics for monitoring the saver."""

    threads: int
    checkpoints: int
    pending_writes: int
    serialized_bytes: int


class BoundedInMemorySaver(LangGraphInMemorySaver):
    """A bounded, lock-protected variant of LangGraph's in-memory saver.

    The saver implements both synchronous and asynchronous LangGraph checkpoint
    operations through the upstream ``InMemorySaver`` contract. It adds:

    - an idle TTL and LRU thread eviction;
    - thread, namespace, checkpoint, payload, write, and total-byte limits;
    - serialized storage using LangGraph's safe default serializer;
    - lock protection across sync and async graph invocations;
    - explicit clearing, deletion, capacity errors, and monitoring metrics.

    It remains process-local and non-durable. Use a persistent checkpointer for
    multi-process, multi-host, restart-safe, or durable production workloads.
    """

    def __init__(
        self,
        *,
        config: InMemoryCheckpointConfig | None = None,
        serde: SerializerProtocol | None = None,
        clock: Callable[[], float] = monotonic,
    ) -> None:
        super().__init__(serde=serde)
        self.config = config or InMemoryCheckpointConfig()
        self._clock = clock
        self._lock = RLock()
        self._last_access: OrderedDict[str, float] = OrderedDict()
        self._sizes: dict[SizeKey, int] = {}
        self._serialized_bytes = 0

    def get_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        """Return a checkpoint while applying expiry and input validation."""
        thread_id, _ = self._validated_location(config)
        with self._lock:
            self._purge_expired_locked()
            if thread_id not in self.storage:
                return None
            item = super().get_tuple(config)
            if item is not None:
                self._touch_locked(thread_id)
            return item

    def list(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> Iterator[CheckpointTuple]:
        """Return a stable snapshot of matching checkpoint history."""
        thread_id: str | None = None
        if config is not None:
            thread_id, _ = self._validated_location(config)
        if before is not None:
            self._validated_location(before)
        with self._lock:
            self._purge_expired_locked()
            if thread_id is not None and thread_id not in self.storage:
                return iter(())
            items = tuple(
                super().list(
                    config,
                    filter=filter,
                    before=before,
                    limit=limit,
                )
            )
            if thread_id is not None and items:
                self._touch_locked(thread_id)
            return iter(items)

    def put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        """Store a checkpoint or reject it before configured limits are crossed."""
        thread_id, checkpoint_ns = self._validated_location(config)
        checkpoint_id = self._validated_identifier(
            checkpoint["id"],
            name="checkpoint_id",
            max_length=self.config.max_thread_id_length,
        )
        estimated_size = self._estimate_checkpoint_size(
            config,
            checkpoint,
            metadata,
            new_versions,
        )
        if estimated_size > self.config.max_checkpoint_bytes:
            raise CheckpointCapacityError("checkpoint exceeds max_checkpoint_bytes")

        with self._lock:
            self._purge_expired_locked()
            self._make_room_for_thread_locked(thread_id)
            namespaces = self._known_namespaces_locked(thread_id)
            if (
                checkpoint_ns not in namespaces
                and len(namespaces) >= self.config.max_namespaces_per_thread
            ):
                raise CheckpointCapacityError(
                    "thread exceeds max_namespaces_per_thread"
                )
            checkpoints = self._known_checkpoints_locked(thread_id)
            if (checkpoint_ns, checkpoint_id) not in checkpoints and len(
                checkpoints
            ) >= self.config.max_checkpoints_per_thread:
                raise CheckpointCapacityError(
                    "thread exceeds max_checkpoints_per_thread; delete the "
                    "thread or use a persistent checkpointer"
                )
            self._make_room_for_bytes_locked(
                estimated_size,
                protected_thread=thread_id,
            )
            result = super().put(config, checkpoint, metadata, new_versions)
            self._record_checkpoint_sizes_locked(
                thread_id,
                checkpoint_ns,
                checkpoint_id,
                new_versions,
            )
            self._touch_locked(thread_id)
            return result

    def put_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        """Store pending writes with per-batch and global memory limits."""
        thread_id, checkpoint_ns = self._validated_location(config)
        checkpoint_id = self._validated_identifier(
            config["configurable"].get("checkpoint_id"),
            name="checkpoint_id",
            max_length=self.config.max_thread_id_length,
        )
        self._validated_identifier(
            task_id,
            name="task_id",
            max_length=self.config.max_thread_id_length,
        )
        write_size = (
            sum(
                self._serialized_size(self.serde.dumps_typed(value))
                + len(channel.encode())
                for channel, value in writes
            )
            + len(task_id.encode())
            + len(task_path.encode())
        )
        if write_size > self.config.max_pending_write_bytes:
            raise CheckpointCapacityError(
                "pending writes exceed max_pending_write_bytes"
            )

        with self._lock:
            self._purge_expired_locked()
            self._make_room_for_thread_locked(thread_id)
            namespaces = self._known_namespaces_locked(thread_id)
            if (
                checkpoint_ns not in namespaces
                and len(namespaces) >= self.config.max_namespaces_per_thread
            ):
                raise CheckpointCapacityError(
                    "thread exceeds max_namespaces_per_thread"
                )
            checkpoints = self._known_checkpoints_locked(thread_id)
            if (checkpoint_ns, checkpoint_id) not in checkpoints and len(
                checkpoints
            ) >= self.config.max_checkpoints_per_thread:
                raise CheckpointCapacityError(
                    "thread exceeds max_checkpoints_per_thread; delete the "
                    "thread or use a persistent checkpointer"
                )
            self._make_room_for_bytes_locked(
                write_size,
                protected_thread=thread_id,
            )
            super().put_writes(config, writes, task_id, task_path)
            self._record_write_sizes_locked(
                thread_id,
                checkpoint_ns,
                checkpoint_id,
            )
            self._touch_locked(thread_id)

    def delete_thread(self, thread_id: str) -> None:
        """Delete every checkpoint, blob, pending write, and metric for a thread."""
        validated = self._validated_identifier(
            thread_id,
            name="thread_id",
            max_length=self.config.max_thread_id_length,
        )
        with self._lock:
            self._delete_thread_locked(validated)

    def get_delta_channel_history(
        self,
        *,
        config: RunnableConfig,
        channels: Sequence[str],
    ) -> Mapping[str, DeltaChannelHistory]:
        """Read delta history without racing concurrent checkpoint mutation."""
        thread_id, _ = self._validated_location(config)
        with self._lock:
            self._purge_expired_locked()
            result = super().get_delta_channel_history(
                config=config,
                channels=channels,
            )
            if result:
                self._touch_locked(thread_id)
            return result

    async def aget_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        """Fetch and deserialize a checkpoint without blocking the event loop."""
        return await asyncio.to_thread(self.get_tuple, config)

    async def alist(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[CheckpointTuple]:
        """Build a stable history snapshot outside the event loop."""
        items = await asyncio.to_thread(
            self.list,
            config,
            filter=filter,
            before=before,
            limit=limit,
        )
        for item in items:
            yield item

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        """Serialize and store a checkpoint outside the event loop."""
        return await asyncio.to_thread(
            self.put,
            config,
            checkpoint,
            metadata,
            new_versions,
        )

    async def aput_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        """Serialize and store pending writes outside the event loop."""
        await asyncio.to_thread(
            self.put_writes,
            config,
            writes,
            task_id,
            task_path,
        )

    async def adelete_thread(self, thread_id: str) -> None:
        """Delete a complete thread outside the event loop."""
        await asyncio.to_thread(self.delete_thread, thread_id)

    async def aget_delta_channel_history(
        self,
        *,
        config: RunnableConfig,
        channels: Sequence[str],
    ) -> Mapping[str, DeltaChannelHistory]:
        """Reconstruct delta history outside the event loop."""
        return await asyncio.to_thread(
            self.get_delta_channel_history,
            config=config,
            channels=channels,
        )

    def purge_expired(self) -> int:
        """Eagerly remove idle threads and return the number deleted."""
        with self._lock:
            return self._purge_expired_locked()

    def clear(self) -> None:
        """Remove all in-memory checkpoint data."""
        with self._lock:
            self.storage.clear()
            self.writes.clear()
            self.blobs.clear()
            self._last_access.clear()
            self._sizes.clear()
            self._serialized_bytes = 0

    def stats(self) -> InMemoryCheckpointStats:
        """Return aggregate metrics without exposing thread IDs or state."""
        with self._lock:
            self._purge_expired_locked()
            checkpoint_count = sum(
                len(checkpoints)
                for namespaces in self.storage.values()
                for checkpoints in namespaces.values()
            )
            pending_write_count = sum(len(writes) for writes in self.writes.values())
            return InMemoryCheckpointStats(
                threads=len(self._last_access),
                checkpoints=checkpoint_count,
                pending_writes=pending_write_count,
                serialized_bytes=self._serialized_bytes,
            )

    def _validated_location(self, config: RunnableConfig) -> tuple[str, str]:
        configurable = config.get("configurable")
        if not isinstance(configurable, dict):
            raise ValueError("checkpoint config requires configurable values")
        thread_id = self._validated_identifier(
            configurable.get("thread_id"),
            name="thread_id",
            max_length=self.config.max_thread_id_length,
        )
        checkpoint_ns = self._validated_identifier(
            configurable.get("checkpoint_ns", ""),
            name="checkpoint_ns",
            max_length=self.config.max_checkpoint_namespace_length,
            allow_empty=True,
        )
        checkpoint_id = configurable.get("checkpoint_id")
        if checkpoint_id is not None:
            self._validated_identifier(
                checkpoint_id,
                name="checkpoint_id",
                max_length=self.config.max_thread_id_length,
            )
        return thread_id, checkpoint_ns

    @staticmethod
    def _validated_identifier(
        value: object,
        *,
        name: str,
        max_length: int,
        allow_empty: bool = False,
    ) -> str:
        if not isinstance(value, str):
            raise ValueError(f"{name} must be a string")
        if not value and not allow_empty:
            raise ValueError(f"{name} must not be empty")
        if len(value) > max_length:
            raise ValueError(f"{name} exceeds its maximum length")
        if value and not value.isprintable():
            raise ValueError(
                f"{name} must not contain control characters or other "
                "non-printable text"
            )
        return value

    @staticmethod
    def _serialized_size(value: SerializedValue) -> int:
        value_type, payload = value
        return len(value_type.encode()) + len(payload)

    def _estimate_checkpoint_size(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> int:
        values = checkpoint.get("channel_values", {})
        checkpoint_copy = {
            key: value for key, value in checkpoint.items() if key != "channel_values"
        }
        total = self._serialized_size(self.serde.dumps_typed(checkpoint_copy))
        total += self._serialized_size(
            self.serde.dumps_typed(get_checkpoint_metadata(config, metadata))
        )
        for channel in new_versions:
            if channel in values:
                total += self._serialized_size(self.serde.dumps_typed(values[channel]))
        return total

    def _record_checkpoint_sizes_locked(
        self,
        thread_id: str,
        checkpoint_ns: str,
        checkpoint_id: str,
        new_versions: ChannelVersions,
    ) -> None:
        checkpoint, metadata, _ = self.storage[thread_id][checkpoint_ns][checkpoint_id]
        self._set_size_locked(
            ("checkpoint", thread_id, checkpoint_ns, checkpoint_id),
            self._serialized_size(checkpoint) + self._serialized_size(metadata),
        )
        for channel, version in new_versions.items():
            blob_key = (thread_id, checkpoint_ns, channel, version)
            blob = self.blobs.get(blob_key)
            if blob is not None:
                self._set_size_locked(
                    ("blob", *blob_key),
                    self._serialized_size(blob),
                )

    def _record_write_sizes_locked(
        self,
        thread_id: str,
        checkpoint_ns: str,
        checkpoint_id: str,
    ) -> None:
        outer_key = (thread_id, checkpoint_ns, checkpoint_id)
        for (task_id, index), (_, channel, value, task_path) in self.writes.get(
            outer_key,
            {},
        ).items():
            self._set_size_locked(
                (
                    "write",
                    thread_id,
                    checkpoint_ns,
                    checkpoint_id,
                    task_id,
                    index,
                ),
                self._serialized_size(value)
                + len(channel.encode())
                + len(task_path.encode()),
            )

    def _set_size_locked(self, key: SizeKey, size: int) -> None:
        previous = self._sizes.get(key, 0)
        self._sizes[key] = size
        self._serialized_bytes += size - previous

    def _remove_size_locked(self, key: SizeKey) -> None:
        self._serialized_bytes -= self._sizes.pop(key, 0)

    def _make_room_for_thread_locked(self, thread_id: str) -> None:
        if thread_id in self._last_access:
            return
        while len(self._last_access) >= self.config.max_threads:
            oldest_thread = next(iter(self._last_access))
            self._delete_thread_locked(oldest_thread)

    def _known_namespaces_locked(self, thread_id: str) -> set[str]:
        namespaces = set(self.storage.get(thread_id, {}))
        namespaces.update(
            checkpoint_ns
            for stored_thread_id, checkpoint_ns, _ in self.writes
            if stored_thread_id == thread_id
        )
        return namespaces

    def _known_checkpoints_locked(
        self,
        thread_id: str,
    ) -> set[tuple[str, str]]:
        checkpoints = {
            (checkpoint_ns, checkpoint_id)
            for checkpoint_ns, namespace_checkpoints in self.storage.get(
                thread_id,
                {},
            ).items()
            for checkpoint_id in namespace_checkpoints
        }
        checkpoints.update(
            (checkpoint_ns, checkpoint_id)
            for stored_thread_id, checkpoint_ns, checkpoint_id in self.writes
            if stored_thread_id == thread_id
        )
        return checkpoints

    def _touch_locked(self, thread_id: str) -> None:
        self._last_access.pop(thread_id, None)
        self._last_access[thread_id] = self._clock()

    def _purge_expired_locked(self) -> int:
        cutoff = self._clock() - self.config.idle_ttl_seconds
        expired = [
            thread_id
            for thread_id, last_access in self._last_access.items()
            if last_access <= cutoff
        ]
        for thread_id in expired:
            self._delete_thread_locked(thread_id)
        return len(expired)

    def _delete_thread_locked(self, thread_id: str) -> None:
        super().delete_thread(thread_id)
        self._last_access.pop(thread_id, None)
        for size_key in tuple(self._sizes):
            if len(size_key) > 1 and size_key[1] == thread_id:
                self._remove_size_locked(size_key)

    def _make_room_for_bytes_locked(
        self,
        required_bytes: int,
        *,
        protected_thread: str,
    ) -> None:
        while self._serialized_bytes + required_bytes > self.config.max_total_bytes:
            evictable = next(
                (
                    thread_id
                    for thread_id in self._last_access
                    if thread_id != protected_thread
                ),
                None,
            )
            if evictable is None:
                raise CheckpointCapacityError(
                    "operation exceeds the in-memory checkpoint byte budget"
                )
            self._delete_thread_locked(evictable)


InMemorySaver = BoundedInMemorySaver
