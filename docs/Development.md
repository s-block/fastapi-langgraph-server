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

Publishing starts when a tag is pushed whose name exactly matches `v` plus the
version in `pyproject.toml`. For example, after changing the project version to
`0.2.0` and merging the release changes to `main`, create and push an annotated
tag:

```bash
git switch main
git pull --ff-only
git tag -a v0.2.0 -m "Release 0.2.0"
git push origin v0.2.0
```

The workflow reruns the complete package gate and builds one source archive and
one `py3-none-any` wheel. The wheel is pure Python and supports every platform;
release jobs install it on Linux, macOS, Windows, and Alpine Linux with musl before
publishing. PyPI publishing uses OIDC trusted publishing through the protected
`pypi` GitHub environment, so no long-lived PyPI token is stored in GitHub. After
PyPI accepts the artifacts, the workflow creates a GitHub release with generated
notes and attaches both distributions.
