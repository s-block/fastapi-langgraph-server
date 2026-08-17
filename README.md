# Self-host LangGraph behind FastAPI

[![CI](https://github.com/s-block/fastapi-langgraph-server/actions/workflows/ci.yml/badge.svg)](https://github.com/s-block/fastapi-langgraph-server/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/fastapi-langgraph-server)](https://pypi.org/project/fastapi-langgraph-server/)
[![Python](https://img.shields.io/pypi/pyversions/fastapi-langgraph-server)](https://pypi.org/project/fastapi-langgraph-server/)
[![License](https://img.shields.io/github/license/s-block/fastapi-langgraph-server)](LICENSE)

`fastapi-langgraph-server` serves compiled LangGraph graphs through a typed,
asynchronous FastAPI API compatible with tested `RemoteGraph` client operations.
Run it as a standalone ASGI application or mount its routes into an existing
FastAPI product.

```bash
pip install fastapi-langgraph-server
```

- **FastAPI-native:** serve one or more graphs standalone or under a route prefix.
- **RemoteGraph-compatible:** invoke, stream, inspect, update, and delete graph
  state through the tested client surface.
- **Streaming and async:** forward named LangGraph stream modes over
  Server-Sent Events.
- **Flexible persistence:** run statelessly or use bounded memory, Redis, or a
  compatible custom LangGraph checkpointer.
- **Fits your application:** reuse existing middleware, authentication,
  authorization, CORS, rate limits, lifecycle, and deployment infrastructure.

The package supports Python 3.12, 3.13, and 3.14 and is currently alpha
software. Review the [compatibility guide](docs/RemoteGraph-Compatibility.md) for
the tested SDK operations and explicit scope boundaries.

## Why this package

LangChain's official [Agent Server](https://docs.langchain.com/langsmith/agent-server)
is a broader runtime for graphs, assistants, threads, runs, persistence, and task
queues, with [Cloud, standalone, and self-hosted deployment options](https://docs.langchain.com/langsmith/deployment).
Use that platform when you need its complete deployment and runtime feature set.

This package has a narrower purpose: expose the implemented `RemoteGraph`
operations from a regular FastAPI application. It is a fit when the host
application should own the HTTP stack, authentication policy, saver lifecycle,
and ASGI deployment, or when LangGraph routes need to live beside an existing
API. It does not claim complete Agent Server compatibility; unsupported endpoints
and run controls are listed in the
[compatibility guide](docs/RemoteGraph-Compatibility.md#scope-boundaries).

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

Run the standalone application with any ASGI server:

```bash
uvicorn my_api:app --host 127.0.0.1 --port 8000
```

A complete runnable version is in [examples/basic.py](examples/basic.py).

### Mount into an existing FastAPI application

Use `install_routes` to keep the host application's middleware, exception
handlers, lifespan, CORS configuration, and deployment setup:

```python
from fastapi import FastAPI

from fastapi_langgraph_server import LangGraphServerConfig, install_routes

config = LangGraphServerConfig(assistants={assistant.assistant_id: assistant})
app = FastAPI()
install_routes(app, config, prefix="/langgraph")
```

The host application owns checkpointer lifecycle management when routes are
mounted this way.

### Connect with RemoteGraph

Use the LangGraph client against the standalone URL or the mounted route prefix.
The following code runs inside an async function:

```python
from langgraph.pregel.remote import RemoteGraph

remote = RemoteGraph("support", url="http://127.0.0.1:8000")

result = await remote.ainvoke({"question": "Can another service call this graph?"})

async for update in remote.astream(
    {"question": "Stream the answer"},
    stream_mode="updates",
):
    print(update)
```

With a checkpointer configured, `RemoteGraph` can also retrieve exact and latest
thread state, page through history, update state, and work with thread deletion.
See [RemoteGraph Compatibility](docs/RemoteGraph-Compatibility.md) for the tested
methods and known exclusions.

## Common use cases

- Expose an internal LangGraph agent to another service through `RemoteGraph`.
- Add LangGraph endpoints to an existing authenticated FastAPI product.
- Serve multiple graphs behind shared middleware and authorization policy.
- Persist graph state and checkpoint history in Redis.
- Deploy a self-hosted agent service with standard ASGI tooling.

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

See the [in-memory saver guide](docs/In-Memory-Checkpointer.md) for configuration,
resource limits, and suitable workloads.

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
process by default. It also rejects concurrent runs or state mutations for the
same thread. Configure these controls for the workload:

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
per-thread ownership checks at the application or proxy boundary. Treat graph
input and checkpoint state as potentially sensitive data. The `debug` stream mode
can expose internal graph state and should be available only to trusted callers.

See [SECURITY.md](SECURITY.md) for vulnerability reporting and deployment
guidance.

## Compatibility scope

The tested surface includes:

- assistant lookup, search, graph, and schema endpoints;
- thread creation, lookup, state, and checkpoint history;
- checkpoint-backed state updates and complete thread deletion;
- streaming and non-streaming graph runs;
- `values`, `updates`, `messages`, `messages-tuple`, `custom`, and `debug` stream
  modes; and
- configurable input/output transformations.

The endpoint-to-client table and unsupported controls are documented in
[RemoteGraph Compatibility](docs/RemoteGraph-Compatibility.md).

## Contributing

See [Development](docs/Development.md) for setup, checks, and release details.
Contributions are especially useful for compatibility fixes, additional tested
`RemoteGraph` operations, checkpointer integrations, runnable examples, and
FastAPI deployment patterns.

```bash
uv sync --dev --frozen
make check
uv run pre-commit run --all-files
```

## License

MIT
