# fastapi-langgraph-server

[![CI](https://github.com/s-block/fastapi-langgraph-server/actions/workflows/ci.yml/badge.svg)](https://github.com/s-block/fastapi-langgraph-server/actions/workflows/ci.yml)

`fastapi-langgraph-server` exposes compiled LangGraph graphs through a typed,
asynchronous FastAPI API compatible with `RemoteGraph`. Use it to serve one or
more graphs from a standalone ASGI application or add the routes to an existing
FastAPI application.

Graph factories are compiled once during application configuration and receive
the selected LangGraph checkpointer. The API provides streaming and non-streaming
runs, assistant discovery, thread state, checkpoint history, state updates, and
thread deletion.

The package supports Python 3.12, 3.13, and 3.14 and is currently alpha software.
The [compatibility guide](docs/RemoteGraph-Compatibility.md) lists the tested SDK
operations and dependency range.

## Installation

Install the package from PyPI:

```bash
pip install fastapi-langgraph-server
```

## Quick start

Every graph factory receives the configured checkpointer. This example uses the
stateless default, so each run executes independently.

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
    return {"answer": f"Received: {state['question']}"}


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

## Persistence modes

The `checkpointer` supplied to `LangGraphServerConfig` or `StandaloneAppConfig`
selects the persistence mode for the application. The configured value is used
consistently for graph compilation, runs, state, and history operations.

| Mode | Configuration | Typical use |
| --- | --- | --- |
| Stateless | Omit `checkpointer` or pass `None` | Independent request execution |
| In memory | Pass `InMemorySaver()` | Bounded, process-local state |
| Redis | Pass `AsyncRedisSaver(...)` | Shared persistent state |
| Custom | Pass a compatible `BaseCheckpointSaver[str]` | Application-specific storage |

### In-memory persistence

The included `InMemorySaver` stores state for the lifetime of the current process.
It provides configurable TTL, thread-count, checkpoint-count, and serialized
payload limits:

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

Install the Redis integration:

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
Use Redis 8 or Redis Stack with RedisJSON and RediSearch, and select logical
database 0 in `REDIS_URL`.

`create_app` closes saver-owned resources during shutdown. When routes are added
to an existing app, the host application owns saver lifecycle management.

Custom `ThreadStore` implementations must store thread metadata, atomically claim
and retrieve assistant ownership, and delete a complete thread. Implement the
exported `ThreadStore` protocol so state updates and deletion remain safe.

With persistence enabled, `/runs/stream` uses a generated thread ID and deletes
its checkpoints when the stream closes. Compatible savers implement
`adelete_thread` for this lifecycle.

## Authentication and deployment security

Use `request_authorizer` to connect the routes to the host application's
authentication and authorization policy:

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
`/health` and `/info` are public health and capability endpoints. Application
middleware can apply a policy to every path.

Configure the standalone CORS allowlist with `cors_origins`:

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

Set `run_timeout_seconds` to bound graph execution time. `create_app` installs the
body-limit middleware; pair `install_routes` with the host application's request
size middleware.

Deployments should additionally enforce TLS, per-client rate limits, and
per-thread ownership checks at the application or proxy boundary. Treat graph input
and checkpoint state as potentially sensitive data. The `debug` stream mode can
expose internal graph state and should be available only to trusted callers.

See [SECURITY.md](SECURITY.md) for vulnerability reporting.

## Features

The package provides:

- assistant lookup, search, graph, and schema endpoints;
- thread creation, lookup, state, and checkpoint history;
- checkpoint-backed state updates and complete thread deletion;
- streaming and non-streaming graph runs;
- `values`, `updates`, `messages`, `messages-tuple`, `custom`, and `debug` stream
  modes;
- configurable input/output transformations; and
- bounded in-memory and Redis checkpoint persistence, plus injection of other
  compatible LangGraph savers.

The endpoint and SDK operation table is in
[RemoteGraph Compatibility](docs/RemoteGraph-Compatibility.md).

## Development

See [Development](docs/Development.md) for setup, checks, and release details.

```bash
uv sync --dev --frozen
make check
uv run pre-commit run --all-files
```

## License

MIT
