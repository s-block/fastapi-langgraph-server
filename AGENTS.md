# fastapi-langgraph-server Project Guide

Keep this RemoteGraph-compatible FastAPI package typed, asynchronous, bounded,
and straightforward to self-host.

## Architecture and contracts

- `src/fastapi_langgraph_server/` owns the installable server package.
- `examples/` contains runnable compositions, not duplicate server logic.
- `tests/` mirrors package behavior and public compatibility contracts.
- Keep route handlers and ASGI composition thin; place reusable behavior in
  focused typed modules.
- Preserve RemoteGraph request, streaming, checkpoint, and error contracts.
- Keep network and persistence paths non-blocking, cancellation-safe, and
  bounded by explicit timeouts or limits.
- Never commit provider credentials, connection secrets, or production data.

## Python and packaging

- Support Python 3.12 through 3.14.
- Use `uv`, Hatchling, Ruff, strict mypy, pytest, and committed public-PyPI
  locks.
- Add focused tests for behavior and failure-path changes.
- Pin GitHub Actions to full commit SHAs.

## Validation

```bash
uv sync --dev --frozen
make check
uv run pre-commit run --all-files
```

`make check` covers formatting, linting, typing, docs, security checks, tests,
package builds, metadata, and isolated wheel installation.
