"""FastAPI dependencies shared by route handlers."""

import inspect
from typing import Annotated

from fastapi import Depends, HTTPException, Request
from langgraph.checkpoint.base import BaseCheckpointSaver

from fastapi_langgraph_server.config import AssistantConfig, LangGraphServerConfig
from fastapi_langgraph_server.storage import ThreadStore


def get_server_config(request: Request) -> LangGraphServerConfig:
    """Resolve route-local configuration, falling back to application state."""
    config = getattr(request.state, "langgraph_config", None)
    if config is None:
        config = getattr(request.app.state, "langgraph_config", None)
    if not isinstance(config, LangGraphServerConfig):
        raise RuntimeError("LangGraph server config is not initialized")
    return config


def get_checkpointer(request: Request) -> BaseCheckpointSaver[str] | None:
    """Return the configured checkpoint saver, if persistence is enabled."""
    return get_server_config(request).checkpointer


def get_thread_store(request: Request) -> ThreadStore:
    """Return the configured server-owned thread store."""
    return get_server_config(request).thread_store


async def authorize_request(request: Request) -> None:
    """Run the application-supplied authorization gate, when configured."""
    authorizer = get_server_config(request).request_authorizer
    if authorizer is None:
        return
    result = authorizer(request)
    if inspect.isawaitable(result):
        await result


def get_assistant_config(assistant_id: str, request: Request) -> AssistantConfig:
    """Return a configured assistant or a protocol-shaped 404 response."""
    assistant = get_server_config(request).get_assistant(assistant_id)
    if assistant is None:
        raise HTTPException(
            status_code=404,
            detail=f"Assistant {assistant_id} not found",
        )
    return assistant


ServerConfig = Annotated[LangGraphServerConfig, Depends(get_server_config)]
Checkpointer = Annotated[
    BaseCheckpointSaver[str] | None,
    Depends(get_checkpointer),
]
RequestAuthorization = Annotated[None, Depends(authorize_request)]
Store = Annotated[ThreadStore, Depends(get_thread_store)]
