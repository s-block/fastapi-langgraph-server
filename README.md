# fastapi-langgraph-server

[![CI](https://github.com/s-block/fastapi-langgraph-server/actions/workflows/ci.yml/badge.svg)](https://github.com/s-block/fastapi-langgraph-server/actions/workflows/ci.yml)

`fastapi-langgraph-server` exposes compiled LangGraph graphs through a typed,
asynchronous FastAPI API compatible with the supported `RemoteGraph` operations.
It can add routes to an existing FastAPI application or create a standalone app.

This is an unofficial community implementation for applications that need a
focused RemoteGraph server without LangSmith Deployment. The package requires no
LangSmith account, deployment licence key, control plane, PostgreSQL, or mandatory
Redis. Infrastructure and model-provider costs still apply.

It is not a replacement for the full LangGraph Agent Server. Background workers,
task queues, scheduling, deployment management, and Studio integration are outside
its scope. The official standalone Agent Server has a different operational and
[licensing model](https://docs.langchain.com/langsmith/deploy-standalone-server).

The package supports Python 3.12, 3.13, and 3.14. It is currently alpha software;
review the [compatibility guide](docs/RemoteGraph-Compatibility.md) before relying
on a specific LangGraph Platform endpoint.

## Installation

Until the first PyPI release, install from the public repository:

```bash
pip install 'fastapi-langgraph-server @ git+https://github.com/s-block/fastapi-langgraph-server.git@main'
```

After a release is published, `pip install fastapi-langgraph-server` installs the
latest PyPI version.

## Quick start

Every graph factory receives the configured checkpointer. The default is `None`,
so this example does not retain graph state between runs.

```python
from typing import TypedDict

from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.graph import END, START, StateGraph

from fastapi_langgraph_server import (
    AssistantConfig,
    StandaloneAppConfig,
    create_app,
)


class State(TypedDict, total=False):
    question: str
    answer: str


def answer(state: State) -> State:
    return {"answer": f"You asked: {state['question']}"}


def build_graph(checkpointer: BaseCheckpointSaver[str] | None) -> object:
    builder = StateGraph(State)
    builder.add_node("answer", answer)
    builder.add_edge(START, "answer")
    builder.add_edge("answer", END)
    return builder.compile(checkpointer=checkpointer)


assistant = AssistantConfig(
    assistant_id="support",
    graph_id="support",
    name="Support graph",
    checkpointed_graph_factory=build_graph,
)

app = create_app(StandaloneAppConfig(assistants={assistant.assistant_id: assistant}))
```

Run the application with an ASGI server:

```bash
uvicorn my_api:app --host 127.0.0.1 --port 8000
```

To add the API to an existing application instead:

```python
from fastapi import FastAPI

from fastapi_langgraph_server import LangGraphServerConfig, install_routes

config = LangGraphServerConfig(assistants={assistant.assistant_id: assistant})
app = FastAPI()
install_routes(app, config, prefix="/langgraph")
```

## Checkpoint persistence

Checkpoint persistence is disabled by default. Graph factories receive `None`,
and supplying a thread or conversation ID does not implicitly enable a saver.
Runs still work, but graph state is not retained and state/history endpoints
return status 501.

Pass any compatible `BaseCheckpointSaver[str]` to `LangGraphServerConfig` or
`StandaloneAppConfig`. The same instance is used for graph compilation and for
thread state/history, regardless of whether a request supplies a thread ID.

### In-memory persistence

The included `InMemorySaver` is process-local, bounded by configurable TTL,
count, and serialized-payload limits, and loses all state on restart. Enable it
explicitly when those semantics are acceptable:

```python
from fastapi_langgraph_server import InMemorySaver

app = create_app(
    StandaloneAppConfig(
        assistants={assistant.assistant_id: assistant},
        checkpointer=InMemorySaver(),
    )
)
```

See the [in-memory saver guide](docs/In-Memory-Checkpointer.md) for its limits.

### Redis persistence

Install the optional official LangGraph Redis saver:

```bash
pip install 'fastapi-langgraph-server[redis]'
```

```python
import os

from langgraph.checkpoint.redis.aio import AsyncRedisSaver

redis_saver = AsyncRedisSaver(redis_url=os.environ["REDIS_URL"])
app = create_app(
    StandaloneAppConfig(
        assistants={assistant.assistant_id: assistant},
        checkpointer=redis_saver,
    )
)
```

`create_app` enters and exits asynchronous saver context managers. This performs
the Redis saver's required setup and cleanup. Redis deployments must satisfy the
[official checkpointer requirements](https://github.com/redis-developer/langgraph-redis#dependencies).
In particular, they need RedisJSON and RediSearch: use Redis 8 or Redis Stack for
older Redis versions, and select logical database 0 in `REDIS_URL`. Plain Redis
7 is not sufficient. The package's default installation does not include or
require Redis.

`create_app` closes saver-owned resources during shutdown. When routes are added
to an existing app, the host application owns saver lifecycle management.

Custom `ThreadStore` implementations must store thread metadata, atomically claim
and retrieve assistant ownership, and delete a complete thread. Implement the
exported `ThreadStore` protocol so state updates and deletion remain safe.

When a saver is configured, stateless `/runs/stream` requests use it with a
generated thread ID and delete that thread's checkpoints when the stream closes.
Savers intended for this endpoint should implement `adelete_thread`.

## Authentication and deployment security

The package does not choose an authentication scheme. Without an authorizer, the
routes are open. Configure `request_authorizer` before exposing them to untrusted
clients:

```python
from fastapi import HTTPException, Request


def authorize(request: Request) -> None:
    if getattr(request.state, "user", None) is None:
        raise HTTPException(status_code=401, detail="Authentication required")


config = LangGraphServerConfig(
    assistants={assistant.assistant_id: assistant},
    request_authorizer=authorize,
)
```

The authorizer runs before assistant, thread, state, history, and run handlers.
`/health` and `/info` remain public. Use application middleware if every path must
be protected. Authorization results and credentials are not copied into graph
input or checkpoint state.

Standalone CORS support is disabled by default. Configure only trusted origins:

```python
StandaloneAppConfig(
    assistants={assistant.assistant_id: assistant},
    cors_origins=("https://app.example.com",),
)
```

The standalone factory limits request bodies to 1 MiB and active runs to 100 per
process by default. It also rejects concurrent runs or state mutations for the same
thread. Configure these controls for the workload:

```python
StandaloneAppConfig(
    assistants={assistant.assistant_id: assistant},
    max_request_body_bytes=2 * 1024 * 1024,
    max_concurrent_runs=20,
    run_timeout_seconds=120,
)
```

Execution timeouts are opt-in; `run_timeout_seconds=None` leaves graph duration to
the host. Set `max_request_body_bytes=None` only when the host supplies an equivalent
control. Body-limit middleware is installed by `create_app`; applications using
`install_routes` must enforce their own request-size limit.

Deployments should additionally enforce TLS, per-client rate limits, and
per-thread ownership checks at the application or proxy boundary. Treat graph input
and checkpoint state as potentially sensitive data. The `debug` stream mode can
expose internal graph state and should be available only to trusted callers.

See [SECURITY.md](SECURITY.md) for vulnerability reporting.

## Supported surface

The package provides:

- assistant lookup, search, graph, and schema endpoints;
- thread creation, lookup, state, and checkpoint history;
- checkpoint-backed state updates and complete thread deletion;
- streaming and non-streaming graph runs;
- `values`, `updates`, `messages`, `messages-tuple`, `custom`, and `debug` stream
  modes;
- configurable input/output transformations; and
- optional bounded in-memory and official Redis checkpoint savers, plus injection
  of other compatible LangGraph savers.

Background workers, webhooks, cron, run join/cancel/wait, LangGraph Store APIs,
interrupt commands, bulk state updates, and resumable streams are not implemented.
Unsupported request controls are rejected rather than ignored. The full endpoint
table is in [RemoteGraph Compatibility](docs/RemoteGraph-Compatibility.md).

## Development

See [Development](docs/Development.md) for setup, checks, and release details.

```bash
uv sync --dev --frozen
make check
uv run pre-commit run --all-files
```

## License

MIT
