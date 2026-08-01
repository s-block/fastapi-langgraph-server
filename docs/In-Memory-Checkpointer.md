# In-Memory Checkpointer

`BoundedInMemorySaver`, also exported as `InMemorySaver`, extends LangGraph's
in-memory saver with resource and concurrency guardrails. It remains process-local
and non-durable.

## Defaults

| Control | Default | Behavior |
| --- | ---: | --- |
| Idle thread TTL | 4 hours | Removes a complete thread during a later saver operation or `purge_expired()` |
| Thread limit | 1,000 | Evicts the least-recently-used complete thread |
| Namespaces per thread | 16 | Rejects an additional namespace |
| Checkpoints per thread | 1,000 | Rejects an additional checkpoint |
| Checkpoint payload | 1 MiB | Rejects before storage mutation |
| Pending-write batch | 512 KiB | Rejects before storage mutation |
| Total serialized payload | 64 MiB | Evicts other LRU threads, then rejects if necessary |
| Identifier length | 256 characters | Rejects invalid or oversized identifiers |

The saver uses a re-entrant lock around shared storage. Async methods move sync
serialization and storage work to a worker thread. The default LangGraph
`JsonPlusSerializer` is used with pickle fallback disabled.

Checkpoint ancestors are not individually pruned because later checkpoints can
depend on earlier channel versions. Capacity failures raise
`CheckpointCapacityError` without modifying the protected thread.

The byte limits count serialized payloads, not all Python object overhead. Monitor
real process memory separately.

## Configuration

```python
from fastapi_langgraph_server import (
    InMemoryCheckpointConfig,
    InMemorySaver,
    LangGraphServerConfig,
)

checkpointer = InMemorySaver(
    config=InMemoryCheckpointConfig(
        idle_ttl_seconds=30 * 60,
        max_threads=250,
        max_checkpoints_per_thread=500,
        max_checkpoint_bytes=512 * 1024,
        max_pending_write_bytes=256 * 1024,
        max_total_bytes=32 * 1024 * 1024,
    )
)

config = LangGraphServerConfig(
    assistants={assistant.assistant_id: assistant},
    checkpointer=checkpointer,
)

stats = checkpointer.stats()
expired = checkpointer.purge_expired()
```

Server configs are stateless by default. Instantiate and pass this saver
explicitly to enable process-local persistence; there is no process-global
checkpoint state.

## Operational boundary

Use this saver only when all of the following are acceptable:

- state is lost on restart;
- workers and hosts do not share state;
- in-flight state cannot fail over; and
- expiry is operation-driven unless `purge_expired()` is scheduled by the host.

Use a compatible persistent async saver when recovery, horizontal scaling, or
durability is required. Regardless of backend, authenticate callers, authorize
thread ownership, limit request sizes and rates, avoid unnecessary secrets in
state, and monitor capacity. `create_app` supplies request-body and run-concurrency
limits; configure an execution timeout and per-client rate limit for the workload.
