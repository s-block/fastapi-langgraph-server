# Codex cloud environment

Create the Codex cloud environment for `s-block/fastapi-langgraph-server` with
the default `universal` image and these settings:

- Python: `3.12`
- uv: `0.10.4` (installed by the setup script)
- Node.js: `22`
- Setup script: `bash .codex/setup.sh`
- Maintenance script: `bash .codex/setup.sh`
- Environment variables and secrets: none required for validation

The script installs the locked development environment, including the Redis
test extra, and prepares all pre-commit environments while setup-phase internet
access is available. Normal tests can keep agent internet access disabled.
Dependency security audits may need limited access to the public package
advisory endpoints.

See the [Codex cloud environment documentation](https://developers.openai.com/codex/cloud/environments)
for the environment lifecycle and cache behavior.
