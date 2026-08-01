# Development

## Requirements

- Python 3.12, 3.13, or 3.14
- [`uv`](https://docs.astral.sh/uv/)
- `make`

## Setup

```bash
uv sync --dev --frozen
uv run pre-commit install
```

## Checks

Run the release gate:

```bash
make check
```

It runs formatting validation, Ruff, strict mypy, Bandit, dependency, secret, and
GitHub Actions security scans, tests, distribution metadata checks, and an
isolated wheel installation including the Redis extra. A separate integration
job exercises the `RemoteGraph` contract against an actual Redis service.

Focused targets are also available:

```bash
make format
make lint
make type-check
make secrets
make security
make test
make test-cov
make check-dist
```

Run all repository hooks before submitting a change:

```bash
uv run pre-commit run --all-files
```

## Example server

```bash
make run-testserver
```

The command binds to `127.0.0.1:8123`. OpenAPI documentation is available at
`http://127.0.0.1:8123/docs`. Override `TESTSERVER_HOST` or `TESTSERVER_PORT` when
needed.

## Dependencies

Use `uv add` and `uv remove` so `pyproject.toml` and `uv.lock` remain aligned.
Commit the lockfile with dependency changes. Runtime dependencies come from public
PyPI.

## Continuous integration

CI runs formatting, linting, strict typing, security scans, distribution checks,
CodeQL, dependency review, and tests on Python 3.12, 3.13, and 3.14. Separate jobs
resolve both the lowest declared direct dependencies and the highest eligible
dependencies, then run the complete suite. Third-party GitHub Actions and remote
pre-commit repositories are pinned to full commit SHAs; workflows receive
least-privilege token permissions.

## Releases

Publishing starts from a GitHub release whose tag exactly matches `v` plus the
version in `pyproject.toml`. The workflow rebuilds and validates the distributions,
then publishes to PyPI using trusted publishing with OIDC. It does not use a
long-lived PyPI token.
