# RemoteGraph Compatibility

The server implements the subset of the LangGraph HTTP protocol listed below.
Compatibility is tested against the locked `langgraph>=1.2,<2` environment and
separate lowest-direct and highest-eligible dependency resolutions in CI.

## Endpoints

| Operation | Endpoint | Coverage |
| --- | --- | --- |
| `RemoteGraph.ainvoke` and `astream` | Run stream endpoints | Client integration tests |
| `RemoteGraph.aget_graph` | `GET /assistants/{assistant_id}/graph` | Client integration test |
| `RemoteGraph.aget_state` | Thread state endpoints | Latest and exact-checkpoint client tests |
| `RemoteGraph.aget_state_history` | `POST /threads/{thread_id}/history` | Client pagination test |
| `RemoteGraph.aupdate_state` | `POST /threads/{thread_id}/state` | Client integration test |
| SDK thread deletion | `DELETE /threads/{thread_id}` | Client integration test |
| Stateless stream | `POST /runs/stream` | Endpoint and client tests |
| Threaded stream | `POST /threads/{thread_id}/runs/stream` | Endpoint test |
| Non-streaming run | `POST /threads/{thread_id}/runs` | Endpoint test |
| Assistant lookup/search | `/assistants/...` | Endpoint tests |
| Graph/schema discovery | `/assistants/{assistant_id}/graph`, `/schemas` | Endpoint tests |
| Thread creation/lookup | `POST /threads`, `GET /threads/{thread_id}` | Endpoint tests |

Native stream modes `values`, `updates`, `messages`, `custom`, and `debug` are
forwarded as named Server-Sent Events. `messages-tuple` is accepted at the HTTP
boundary and mapped to the native `messages` mode.

Graph factories receive the configured checkpointer, or `None` when persistence
is disabled, and are called once during server configuration. The returned graph
must provide asynchronous `ainvoke` and `astream` methods. A factory is required
so the configured saver cannot be bypassed by a precompiled graph instance.

No checkpointer is configured by default. Threaded and stateless runs still
execute, but do not retain graph state; state and history endpoints return status
501. Supplying a thread ID does not change the configured persistence behavior.

When a saver is configured, threaded runs retain checkpoints according to that
saver. Stateless runs use the same saver with a generated thread ID and request
deletion when the stream closes. Server-owned thread metadata is stored separately
in the configured `ThreadStore`.

The first run durably records which assistant owns a thread in checkpoint metadata.
Subsequent runs with another assistant are rejected, allowing `aupdate_state` to
select the correct graph even after process-local thread metadata is lost. The
server-owned marker is not returned as public checkpoint metadata.

Only one run, state update, or deletion may mutate a thread at a time. A
process-wide active-run limit rejects excess work with status 429. The standalone
factory also enforces a request-body limit; execution timeouts are configurable.

## Deliberate limitations

The package does not implement:

- run join, cancel, or wait endpoints;
- background workers or delayed runs;
- cron jobs or webhooks;
- LangGraph Store APIs;
- interrupt commands, resumable streams, or subgraph streams;
- bulk state updates;
- continue-on-disconnect;
- durable user-supplied thread metadata unless a custom `ThreadStore` is supplied.

Unsupported run controls are rejected during request validation. Authentication,
object-level authorization, and per-client rate limits remain deployment policies;
see the README security section.
