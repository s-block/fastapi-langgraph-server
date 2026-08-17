# RemoteGraph Compatibility

The implemented LangGraph HTTP operations are listed below. Compatibility is
tested against the locked `langgraph>=1.2,<2` environment and separate
lowest-direct and highest-eligible dependency resolutions in CI.

This is a tested subset of the LangGraph Server API, not a claim of complete
Agent Server protocol compatibility.

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

## Scope boundaries

Only the endpoints listed above are implemented. The server does not provide the
remaining Agent Server surface, including background run management, cron jobs,
assistant mutation and version management, or stores for long-term memory.

Advanced run controls are rejected explicitly rather than ignored. These include
interrupt controls and resume commands, webhooks, delayed or resumable runs,
subgraph streaming, background continuation after disconnect, retained stateless
runs, and multitask strategies other than `reject`. Subgraph state retrieval is
also unsupported.

The compatibility tests exercise asynchronous `RemoteGraph` invocation,
streaming, graph discovery, state, history, state updates, and SDK thread
deletion. Operations not present in the table should be treated as unsupported.

Native stream modes `values`, `updates`, `messages`, `custom`, and `debug` are
forwarded as named Server-Sent Events. `messages-tuple` is accepted at the HTTP
boundary and mapped to the native `messages` mode.

Graph factories receive the configured checkpointer and are called once during
server configuration. Stateless configurations pass `None`. The returned graph
provides asynchronous `ainvoke` and `astream` methods.

The default persistence mode is stateless. Configuring a saver enables checkpoint
state, exact-checkpoint lookup, state history, state updates, and thread deletion.
The selected mode applies consistently to generated and caller-supplied thread
IDs.

When a saver is configured, threaded runs retain checkpoints according to that
saver. Stateless runs use the same saver with a generated thread ID and request
deletion when the stream closes. Server-owned thread metadata is stored separately
in the configured `ThreadStore`.

The first run durably records which assistant owns a thread in checkpoint metadata.
That ownership selects the correct graph for later state updates and protects the
thread from cross-assistant mutation. Internal ownership metadata stays
server-side.

Only one run, state update, or deletion may mutate a thread at a time. A
process-wide active-run limit rejects excess work with status 429. The standalone
factory also enforces a request-body limit; execution timeouts are configurable.

Use `request_authorizer` for application authentication and object-level policy.
The standalone application also provides request-body limits, active-run limits,
same-thread mutation coordination, and configurable execution timeouts.
