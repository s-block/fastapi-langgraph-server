"""Thread metadata storage."""

import asyncio
from dataclasses import dataclass, field
from typing import Protocol

from cachetools import TTLCache

from fastapi_langgraph_server.models import Thread


class ThreadStoreCapacityError(RuntimeError):
    """Raised when thread metadata exceeds the in-memory storage limit."""


class ThreadAssistantConflictError(RuntimeError):
    """Raised when a thread is claimed by more than one assistant."""


@dataclass(slots=True)
class _StoredThread:
    thread: Thread
    assistant_id: str | None = None


class ThreadStore(Protocol):
    """Persistence boundary for server-owned thread metadata."""

    async def get_thread(self, thread_id: str) -> Thread | None: ...

    async def put_thread(self, thread: Thread) -> None: ...

    async def get_assistant_id(self, thread_id: str) -> str | None: ...

    async def claim_assistant(self, thread_id: str, assistant_id: str) -> None: ...

    async def delete_thread(self, thread_id: str) -> None: ...


@dataclass(slots=True)
class InMemoryThreadStore:
    """Count-bounded, TTL-based thread metadata for one server process."""

    ttl_seconds: float = 14_400
    max_threads: int = 1_000
    max_thread_bytes: int = 131_072
    _threads: TTLCache[str, _StoredThread] = field(init=False, repr=False)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be greater than zero")
        if self.max_threads <= 0:
            raise ValueError("max_threads must be greater than zero")
        if self.max_thread_bytes <= 0:
            raise ValueError("max_thread_bytes must be greater than zero")
        self._threads = TTLCache(maxsize=self.max_threads, ttl=self.ttl_seconds)

    async def get_thread(self, thread_id: str) -> Thread | None:
        """Return a defensive copy of stored thread metadata."""
        async with self._lock:
            record = self._threads.get(thread_id)
            return record.thread.model_copy(deep=True) if record is not None else None

    async def put_thread(self, thread: Thread) -> None:
        """Create or replace thread metadata."""
        if len(thread.model_dump_json().encode()) > self.max_thread_bytes:
            raise ThreadStoreCapacityError("thread metadata exceeds max_thread_bytes")
        async with self._lock:
            existing = self._threads.get(thread.thread_id)
            assistant_id = existing.assistant_id if existing is not None else None
            self._threads[thread.thread_id] = _StoredThread(
                thread=thread.model_copy(deep=True),
                assistant_id=assistant_id,
            )

    async def get_assistant_id(self, thread_id: str) -> str | None:
        """Return the assistant that owns a thread, when one has been claimed."""
        async with self._lock:
            record = self._threads.get(thread_id)
            return record.assistant_id if record is not None else None

    async def claim_assistant(self, thread_id: str, assistant_id: str) -> None:
        """Atomically associate a thread with exactly one assistant."""
        async with self._lock:
            record = self._threads.get(thread_id)
            if record is None:
                raise LookupError(f"Thread {thread_id} not found")
            if record.assistant_id not in {None, assistant_id}:
                raise ThreadAssistantConflictError(
                    f"Thread {thread_id} belongs to assistant {record.assistant_id}"
                )
            record.assistant_id = assistant_id
            self._threads[thread_id] = record

    async def delete_thread(self, thread_id: str) -> None:
        """Delete thread metadata and its assistant ownership record."""
        async with self._lock:
            self._threads.pop(thread_id, None)
