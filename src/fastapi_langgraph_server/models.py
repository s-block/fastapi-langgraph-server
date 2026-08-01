"""Pydantic models for the supported LangGraph HTTP protocol surface."""

from datetime import datetime
from typing import Annotated, Any, Literal, Self

from fastapi import Path
from pydantic import AfterValidator, BaseModel, ConfigDict, Field, model_validator


def _printable(value: str) -> str:
    if not value.isprintable():
        raise ValueError("identifier must contain only printable characters")
    return value


ThreadId = Annotated[
    str,
    Path(min_length=1, max_length=256),
    AfterValidator(_printable),
]
ConfigTag = Annotated[
    str,
    Field(min_length=1, max_length=256),
    AfterValidator(_printable),
]
RequiredIdentifier = Annotated[
    str,
    Field(min_length=1, max_length=256),
    AfterValidator(_printable),
]
NamespaceIdentifier = Annotated[
    str,
    Field(max_length=256),
    AfterValidator(_printable),
]
StreamMode = Literal[
    "values", "updates", "messages", "messages-tuple", "custom", "debug"
]


class ProtocolModel(BaseModel):
    """Reject unknown protocol fields instead of silently discarding behavior."""

    model_config = ConfigDict(extra="forbid")


class Config(ProtocolModel):
    """Run configuration supplied by a client."""

    tags: list[ConfigTag] | None = Field(default=None, max_length=100)
    recursion_limit: int | None = Field(default=None, ge=1, le=1_000)
    configurable: dict[str, Any] | None = None

    @model_validator(mode="after")
    def validate_checkpoint_config(self) -> Self:
        """Validate reserved checkpoint fields before they reach any saver."""
        if self.configurable is None:
            return self
        for name, allow_empty in (
            ("thread_id", False),
            ("checkpoint_id", False),
            ("checkpoint_ns", True),
        ):
            value = self.configurable.get(name)
            if value is None:
                continue
            if not isinstance(value, str):
                raise ValueError(f"configurable.{name} must be a string")
            if not value and not allow_empty:
                raise ValueError(f"configurable.{name} must not be empty")
            if len(value) > 256:
                raise ValueError(f"configurable.{name} exceeds its maximum length")
            _printable(value)
        checkpoint_map = self.configurable.get("checkpoint_map")
        if checkpoint_map is not None and not isinstance(checkpoint_map, dict):
            raise ValueError("configurable.checkpoint_map must be an object")
        return self


class Checkpoint(ProtocolModel):
    """Reference to a thread checkpoint."""

    thread_id: RequiredIdentifier
    checkpoint_ns: NamespaceIdentifier = ""
    checkpoint_id: RequiredIdentifier | None = None
    checkpoint_map: dict[str, Any] | None = None


class ThreadTask(ProtocolModel):
    """Task reported as part of a thread state."""

    id: str
    name: str
    error: str | None = None
    interrupts: list[dict[str, Any]] = Field(default_factory=list)
    checkpoint: Checkpoint | None = None
    state: dict[str, Any] | None = None
    result: dict[str, Any] | None = None


class ThreadState(ProtocolModel):
    """Current or historical state of a thread."""

    values: dict[str, Any] | list[dict[str, Any]]
    next: list[str] = Field(default_factory=list)
    checkpoint: Checkpoint
    metadata: dict[str, Any] | None = None
    created_at: str | None = None
    parent_checkpoint: Checkpoint | None = None
    tasks: list[ThreadTask] = Field(default_factory=list)
    interrupts: list[dict[str, Any]] = Field(default_factory=list)


class StateRequest(ProtocolModel):
    """Request body used by the SDK to read checkpoint-backed state."""

    checkpoint: Checkpoint | None = None
    subgraphs: bool = False

    @model_validator(mode="after")
    def reject_subgraphs(self) -> Self:
        """Reject nested state until subgraph snapshots are implemented."""
        if self.subgraphs:
            raise ValueError("subgraph state is not supported")
        return self


class Thread(ProtocolModel):
    """Thread metadata returned by thread endpoints."""

    thread_id: RequiredIdentifier
    created_at: datetime
    updated_at: datetime
    metadata: dict[str, Any] | None = None
    status: str = "idle"
    values: dict[str, Any] | None = None
    interrupts: dict[str, list[dict[str, Any]]] = Field(default_factory=dict)


class ThreadCreate(ProtocolModel):
    """Request body for thread creation."""

    thread_id: RequiredIdentifier | None = None
    metadata: dict[str, Any] | None = None
    if_exists: Literal["raise", "do_nothing"] | None = None


class Assistant(ProtocolModel):
    """Assistant metadata returned to clients."""

    assistant_id: str
    graph_id: str
    config: Config = Field(default_factory=Config)
    created_at: datetime
    updated_at: datetime
    metadata: dict[str, Any] | None = None
    version: int = 1
    name: str = ""
    description: str | None = None


class AssistantSearch(ProtocolModel):
    """Assistant search filters and pagination."""

    graph_id: RequiredIdentifier | None = None
    metadata: dict[str, Any] | None = None
    limit: int = Field(default=10, ge=1, le=1000)
    offset: int = Field(default=0, ge=0)


class RunCreate(ProtocolModel):
    """Request body used to create or stream a run."""

    assistant_id: RequiredIdentifier
    input: dict[str, Any] | None = None
    metadata: dict[str, Any] | None = None
    config: Config | None = None
    checkpoint_id: RequiredIdentifier | None = None
    interrupt_before: list[str] | None = None
    interrupt_after: list[str] | None = None
    webhook: str | None = None
    multitask_strategy: Literal["reject", "interrupt", "rollback", "enqueue"] | None = (
        None
    )
    stream_mode: list[StreamMode] | None = Field(default=None, max_length=6)
    stream_subgraphs: bool = False
    stream_resumable: bool = False
    on_disconnect: Literal["cancel", "continue"] | None = None
    on_completion: Literal["delete", "keep"] | None = None
    after_seconds: int | None = Field(default=None, ge=0)
    if_not_exists: Literal["create", "reject"] | None = None
    command: dict[str, Any] | None = None
    context: dict[str, Any] | None = None
    checkpoint: Checkpoint | None = None
    checkpoint_during: bool | None = None
    feedback_keys: list[str] | None = None
    durability: Literal["sync", "async", "exit"] | None = None
    langsmith_tracer: dict[str, Any] | None = None

    @model_validator(mode="after")
    def reject_unsupported_options(self) -> Self:
        """Fail explicitly for protocol behavior this server does not provide."""
        unsupported = {
            "interrupt_before": self.interrupt_before,
            "interrupt_after": self.interrupt_after,
            "webhook": self.webhook,
            "command": self.command,
            "context": self.context,
            "checkpoint": self.checkpoint,
            "checkpoint_during": self.checkpoint_during,
            "feedback_keys": self.feedback_keys,
            "durability": self.durability,
            "langsmith_tracer": self.langsmith_tracer,
            "after_seconds": self.after_seconds,
        }
        requested = [name for name, value in unsupported.items() if value is not None]
        if self.stream_subgraphs:
            requested.append("stream_subgraphs")
        if self.stream_resumable:
            requested.append("stream_resumable")
        if self.on_disconnect == "continue":
            requested.append("on_disconnect=continue")
        if self.on_completion == "keep":
            requested.append("on_completion=keep")
        if self.multitask_strategy not in {None, "reject"}:
            requested.append(f"multitask_strategy={self.multitask_strategy}")
        if requested:
            raise ValueError("unsupported run options: " + ", ".join(sorted(requested)))
        return self


class StateUpdate(ProtocolModel):
    """Request body used to update state."""

    values: dict[str, Any] | list[dict[str, Any]] | None = None
    checkpoint_id: RequiredIdentifier | None = None
    checkpoint: Checkpoint | None = None
    as_node: str | None = None

    @model_validator(mode="after")
    def validate_checkpoint_target(self) -> Self:
        """Reject conflicting forms of the checkpoint selector."""
        if (
            self.checkpoint_id is not None
            and self.checkpoint is not None
            and self.checkpoint.checkpoint_id != self.checkpoint_id
        ):
            raise ValueError(
                "checkpoint and checkpoint_id must identify the same state"
            )
        return self


class StateUpdateResponse(ProtocolModel):
    """Checkpoint created by a successful state update."""

    checkpoint: Checkpoint


class HistoryRequest(ProtocolModel):
    """History query options."""

    limit: int = Field(default=10, ge=1, le=1000)
    before: RequiredIdentifier | Checkpoint | None = None
    metadata: dict[str, Any] | None = None
    checkpoint: Checkpoint | None = None


class GraphSchema(ProtocolModel):
    """Schemas advertised for an assistant graph."""

    graph_id: str
    input_schema: dict[str, Any] | None = None
    output_schema: dict[str, Any] | None = None
    state_schema: dict[str, Any] | None = None
    config_schema: dict[str, Any] | None = None
