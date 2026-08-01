"""Health and server information endpoints."""

from importlib.metadata import version

from fastapi import APIRouter

router = APIRouter()


@router.get("/health", name="health:check")
async def health_check() -> dict[str, str]:
    """Report process health without accessing graph state."""
    return {"status": "ok"}


@router.get("/info", name="health:info")
async def info() -> dict[str, str]:
    """Report the installed server package version."""
    return {
        "version": version("fastapi-langgraph-server"),
        "type": "langgraph-server",
    }
