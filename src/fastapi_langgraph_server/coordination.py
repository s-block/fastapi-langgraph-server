"""Process-local coordination for bounded graph execution."""

import asyncio
from dataclasses import dataclass, field


class RunCapacityError(RuntimeError):
    """Raised when the configured process-wide run limit is full."""


class ThreadRunConflictError(RuntimeError):
    """Raised when a thread already has an active run or state mutation."""


@dataclass(slots=True)
class RunLease:
    """Idempotently releasable reservation for one thread execution."""

    _coordinator: "RunCoordinator"
    thread_id: str
    _token: object
    _released: bool = field(default=False, init=False)

    async def release(self) -> None:
        """Release the reservation once, including from cleanup callbacks."""
        if self._released:
            return
        await asyncio.shield(self._coordinator._release(self.thread_id, self._token))
        self._released = True


class RunCoordinator:
    """Reject concurrent mutations per thread and bound active process runs."""

    def __init__(self, max_concurrent_runs: int) -> None:
        if isinstance(max_concurrent_runs, bool) or max_concurrent_runs <= 0:
            raise ValueError("max_concurrent_runs must be greater than zero")
        self.max_concurrent_runs = max_concurrent_runs
        self._active: dict[str, object] = {}
        self._lock = asyncio.Lock()

    async def acquire(self, thread_id: str) -> RunLease:
        """Reserve one execution slot without queueing unbounded requests."""
        async with self._lock:
            if thread_id in self._active:
                raise ThreadRunConflictError(
                    f"Thread {thread_id} already has an active run"
                )
            if len(self._active) >= self.max_concurrent_runs:
                raise RunCapacityError("Concurrent run capacity is full")
            token = object()
            self._active[thread_id] = token
        return RunLease(self, thread_id, token)

    async def _release(self, thread_id: str, token: object) -> None:
        async with self._lock:
            if self._active.get(thread_id) is token:
                del self._active[thread_id]

    async def active_runs(self) -> int:
        """Return a non-sensitive process-local run count for monitoring."""
        async with self._lock:
            return len(self._active)
