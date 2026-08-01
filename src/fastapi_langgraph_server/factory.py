"""Factories for embedding or running the LangGraph routes."""

import inspect
from collections.abc import AsyncGenerator, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from functools import partial
from typing import TYPE_CHECKING, cast

from fastapi import APIRouter, Depends, FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

if TYPE_CHECKING:
    from fastapi.params import Depends as DependsParameter

from fastapi_langgraph_server.config import (
    LangGraphServerConfig,
    StandaloneAppConfig,
)
from fastapi_langgraph_server.middleware import RequestBodyLimitMiddleware
from fastapi_langgraph_server.routes.assistants import router as assistants_router
from fastapi_langgraph_server.routes.health import router as health_router
from fastapi_langgraph_server.routes.runs import router as runs_router
from fastapi_langgraph_server.routes.threads import router as threads_router


def create_router(
    config: LangGraphServerConfig | None = None,
    *,
    prefix: str = "",
    tags: Sequence[str] | None = None,
) -> APIRouter:
    """Create a configured router containing all supported endpoints.

    Passing ``config`` binds it to this router. Omitting it allows applications
    that already set ``app.state.langgraph_config`` (for example with
    :func:`langgraph_lifespan`) to supply configuration at request time.
    """
    dependencies: list[DependsParameter] = []
    if config is not None:

        async def bind_config(request: Request) -> None:
            request.state.langgraph_config = config

        dependencies.append(Depends(bind_config))

    router = APIRouter(
        prefix=prefix,
        tags=list(tags or ()),
        dependencies=dependencies,
    )
    router.include_router(health_router)
    router.include_router(assistants_router)
    router.include_router(threads_router)
    router.include_router(runs_router)
    return router


def install_routes(
    app: FastAPI,
    config: LangGraphServerConfig,
    *,
    prefix: str = "",
    tags: Sequence[str] = ("langgraph",),
) -> None:
    """Install a configured route set into an existing FastAPI application."""
    app.include_router(create_router(config, prefix=prefix, tags=tags))


@asynccontextmanager
async def _checkpointer_lifespan(
    checkpointer: object | None,
) -> AsyncGenerator[None, None]:
    if checkpointer is None:
        yield
        return

    enter = getattr(checkpointer, "__aenter__", None)
    exit_context = getattr(checkpointer, "__aexit__", None)
    if callable(enter) and callable(exit_context):
        context_manager = cast("AbstractAsyncContextManager[object]", checkpointer)
        async with context_manager as entered:
            if entered is not checkpointer:
                raise RuntimeError("checkpointer context manager must return itself")
            yield
        return

    try:
        yield
    finally:
        close = getattr(checkpointer, "aclose", None)
        if callable(close):
            result = close()
            if inspect.isawaitable(result):
                await result


@asynccontextmanager
async def langgraph_lifespan(
    app: FastAPI,
    config: LangGraphServerConfig,
) -> AsyncGenerator[None, None]:
    """Temporarily expose config through app state for an unbound router."""
    sentinel = object()
    previous = getattr(app.state, "langgraph_config", sentinel)
    app.state.langgraph_config = config
    try:
        async with _checkpointer_lifespan(config.checkpointer):
            yield
    finally:
        if previous is sentinel:
            delattr(app.state, "langgraph_config")
        else:
            app.state.langgraph_config = previous


def create_app(config: StandaloneAppConfig) -> FastAPI:
    """Create a standalone FastAPI application for the configured graphs."""
    server_config = config.to_server_config()
    app = FastAPI(
        title=config.title,
        description=config.description,
        version=config.version,
        debug=config.debug,
        lifespan=partial(langgraph_lifespan, config=server_config),
    )
    if config.max_request_body_bytes is not None:
        app.add_middleware(
            RequestBodyLimitMiddleware,
            max_body_bytes=config.max_request_body_bytes,
        )
    if config.cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=list(config.cors_origins),
            allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
            allow_headers=["*"],
            allow_credentials="*" not in config.cors_origins,
        )
    install_routes(app, server_config)
    return app
