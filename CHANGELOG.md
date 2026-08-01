# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Initial Python package, quality, CI, and release scaffolding.
- Embeddable and standalone FastAPI factories for LangGraph-compatible routes.
- Assistant, thread, state, history, run, health, graph, and schema endpoints.
- Native compiled LangGraph invocation and named SSE streaming.
- TTL- and count-bounded in-memory thread metadata storage.
- A bounded, TTL/LRU, lock-protected optional in-memory checkpointer with payload
  limits, aggregate metrics, and explicit capacity failures.
- Stateless operation by default, with graph factories receiving the explicitly
  configured saver or `None`.
- An optional dependency on the official LangGraph Redis checkpointer.
- Request validation for protocol controls and thread identifiers.
- Cancellation-safe streaming and request-scoped stateless cleanup.
- Reusable graph construction and graph/schema discovery.
- Integration tests covering every endpoint, an actual `RemoteGraph` client,
  and the Redis-backed persistence path.
- A runnable basic LangGraph example via `make run-testserver`.
- Consistent configured-checkpointer use for caller-supplied and generated thread
  IDs.
- Request authorization hooks, explicit CORS allowlists, security scans, CodeQL,
  dependency review, and trusted PyPI publishing.
- Real-client coverage for latest and exact checkpoint state, paginated history,
  graph discovery, checkpoint-backed state updates, and thread deletion.
- Durable assistant ownership for checkpointed threads, with cross-assistant run
  rejection and safe graph selection for state updates.
- Process-wide and per-thread run coordination, configurable execution timeouts,
  and standalone request-body limits.
- Dependency-boundary CI jobs and immutable pre-commit dependency pins.

[Unreleased]: https://github.com/s-block/fastapi-langgraph-server/commits/main
