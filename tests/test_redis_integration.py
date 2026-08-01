"""Optional integration coverage against an actual Redis server."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, TypedDict

import httpx
import pytest
from langgraph.checkpoint.redis.aio import AsyncRedisSaver
from langgraph.graph import END, START, StateGraph
from langgraph.pregel.remote import RemoteGraph
from langgraph_sdk.client import LangGraphClient
from langgraph_sdk.errors import NotFoundError

from fastapi_langgraph_server import AssistantConfig, StandaloneAppConfig, create_app

if TYPE_CHECKING:
    from langchain_core.runnables import RunnableConfig


class RedisState(TypedDict, total=False):
    message: str
    response: str


def respond(state: RedisState) -> RedisState:
    return {"response": f"Redis: {state['message']}"}


@pytest.mark.skipif(
    "REDIS_URL" not in os.environ,
    reason="REDIS_URL is required for the optional Redis integration test",
)
@pytest.mark.asyncio
async def test_remote_graph_contract_with_redis() -> None:
    builder = StateGraph(RedisState)
    builder.add_node("respond", respond)
    builder.add_edge(START, "respond")
    builder.add_edge("respond", END)
    assistant = AssistantConfig(
        assistant_id="redis",
        graph_id="redis",
        name="Redis graph",
        checkpointed_graph_factory=lambda saver: builder.compile(checkpointer=saver),
    )
    saver = AsyncRedisSaver(redis_url=os.environ["REDIS_URL"])
    app = create_app(
        StandaloneAppConfig(
            assistants={assistant.assistant_id: assistant},
            checkpointer=saver,
        )
    )
    transport = httpx.ASGITransport(app=app)
    config: RunnableConfig = {"configurable": {"thread_id": "redis-contract"}}

    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
        ) as http_client,
    ):
        sdk_client = LangGraphClient(http_client)
        remote = RemoteGraph("redis", client=sdk_client)
        result = await remote.ainvoke({"message": "persisted"}, config=config)
        state = await remote.aget_state(config)
        updated_config = await remote.aupdate_state(
            state.config,
            {"response": "Redis: updated"},
        )
        updated = await remote.aget_state(updated_config)
        await sdk_client.threads.delete("redis-contract")
        with pytest.raises(NotFoundError):
            await remote.aget_state(config)

    assert result["response"] == "Redis: persisted"
    assert state.values["response"] == "Redis: persisted"
    assert updated.values["response"] == "Redis: updated"
