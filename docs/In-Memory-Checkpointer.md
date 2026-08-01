# In-Memory Checkpointer

`BoundedInMemorySaver`, also exported as `InMemorySaver`, extends LangGraph's
in-memory saver with resource and concurrency guardrails. Its storage lifetime
matches the current process.

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
serialization and storage work to a worker thread. It uses LangGraph's
`JsonPlusSerializer` with `pickle_fallback=False`.

Eviction operates at complete-thread granularity to preserve checkpoint ancestry
and channel versions. Capacity failures raise `CheckpointCapacityError` while
leaving the protected thread unchanged.

Byte limits measure serialized payloads. Monitor process resident memory alongside
the saver statistics for production capacity planning.

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

Instantiate and pass the saver to enable process-local persistence. Server
configurations use stateless execution when `checkpointer` is omitted.

## Suitable workloads

The in-memory saver fits:

- local development and integration tests;
- single-process applications;
- ephemeral workflows whose state lifetime matches the process; and
- bounded workloads that benefit from TTL and LRU eviction.

For shared workers, recovery, or durable state, configure a compatible persistent
async saver. Authenticate callers, authorize thread ownership, limit request sizes
and rates, keep sensitive values out of state, and monitor capacity. `create_app`
supplies request-body and run-concurrency limits; configure an execution timeout
and per-client rate limit for the workload.
