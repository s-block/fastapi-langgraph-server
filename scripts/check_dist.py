"""Validate built distributions and smoke-test the wheel in isolation."""

from __future__ import annotations

import subprocess
import sys
import tarfile
import tempfile
import tomllib
import zipfile
from pathlib import Path
from shutil import which
from typing import TypedDict, cast

_DISTRIBUTION_NAME = "fastapi-langgraph-server"
_PACKAGE_FILES = {
    "fastapi_langgraph_server/__init__.py",
    "fastapi_langgraph_server/checkpoint/__init__.py",
    "fastapi_langgraph_server/checkpoint/memory.py",
    "fastapi_langgraph_server/config.py",
    "fastapi_langgraph_server/coordination.py",
    "fastapi_langgraph_server/dependencies.py",
    "fastapi_langgraph_server/execution.py",
    "fastapi_langgraph_server/factory.py",
    "fastapi_langgraph_server/middleware.py",
    "fastapi_langgraph_server/models.py",
    "fastapi_langgraph_server/py.typed",
    "fastapi_langgraph_server/routes/__init__.py",
    "fastapi_langgraph_server/routes/assistants.py",
    "fastapi_langgraph_server/routes/health.py",
    "fastapi_langgraph_server/routes/runs.py",
    "fastapi_langgraph_server/routes/threads.py",
    "fastapi_langgraph_server/state.py",
    "fastapi_langgraph_server/storage.py",
    "fastapi_langgraph_server/streaming.py",
}
_SDIST_FILES = {
    "CHANGELOG.md",
    "LICENSE",
    "README.md",
    "SECURITY.md",
    "docs/Development.md",
    "docs/In-Memory-Checkpointer.md",
    "docs/RemoteGraph-Compatibility.md",
    "examples/__init__.py",
    "examples/basic.py",
}


class _ProjectMetadata(TypedDict):
    name: str
    version: str


class _Pyproject(TypedDict):
    project: _ProjectMetadata


def _project_metadata() -> _ProjectMetadata:
    pyproject = cast(
        "_Pyproject",
        tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8")),
    )
    return pyproject["project"]


def _uv_executable() -> str:
    executable = which("uv")
    if executable is None:
        raise RuntimeError("uv is required to validate built distributions")
    return executable


def _single_artifact(dist: Path, pattern: str) -> Path:
    matches = sorted(dist.glob(pattern))
    if len(matches) != 1:
        message = f"Expected one {pattern} artifact, found {len(matches)}"
        raise RuntimeError(message)
    return matches[0]


def _validate_wheel(wheel: Path, project: _ProjectMetadata) -> None:
    with zipfile.ZipFile(wheel) as archive:
        names = set(archive.namelist())
        metadata_files = [
            name for name in names if name.endswith(".dist-info/METADATA")
        ]
        license_files = [
            name for name in names if name.endswith(".dist-info/licenses/LICENSE")
        ]
        if len(metadata_files) != 1 or len(license_files) != 1:
            raise RuntimeError("Wheel must contain one metadata and license file")
        metadata = archive.read(metadata_files[0]).decode()
    missing = _PACKAGE_FILES - names
    if missing:
        message = f"Wheel is missing expected files: {sorted(missing)}"
        raise RuntimeError(message)
    if (
        f"Name: {project['name']}\n" not in metadata
        or f"Version: {project['version']}\n" not in metadata
    ):
        raise RuntimeError("Wheel metadata has an unexpected name or version")
    for dependency in ("fastapi", "langgraph", "starlette"):
        if f"Requires-Dist: {dependency}" not in metadata:
            raise RuntimeError(f"Wheel metadata is missing {dependency!r}")
    if "Provides-Extra: redis\n" not in metadata:
        raise RuntimeError("Wheel metadata is missing the Redis extra")
    if "Requires-Dist: langgraph-checkpoint-redis" not in metadata:
        raise RuntimeError("Wheel metadata is missing the optional Redis dependency")


def _validate_sdist(sdist: Path) -> None:
    with tarfile.open(sdist, mode="r:gz") as archive:
        names = archive.getnames()
    roots = {name.split("/", maxsplit=1)[0] for name in names if "/" in name}
    if len(roots) != 1:
        message = f"Expected one sdist root, found {sorted(roots)}"
        raise RuntimeError(message)
    root = roots.pop()
    expected = {
        *(f"{root}/{name}" for name in _SDIST_FILES),
        *(f"{root}/src/{name}" for name in _PACKAGE_FILES),
    }
    missing = expected - set(names)
    if missing:
        message = f"Source distribution is missing files: {sorted(missing)}"
        raise RuntimeError(message)


def _isolated_wheel_smoke(wheel: Path, project: _ProjectMetadata) -> None:
    with tempfile.TemporaryDirectory(
        prefix="fastapi-langgraph-server-dist-"
    ) as temp_dir:
        environment = Path(temp_dir) / ".venv"
        uv = _uv_executable()
        subprocess.run(
            [uv, "venv", "--python", sys.executable, str(environment)],
            check=True,
        )
        executable = "Scripts/python.exe" if sys.platform == "win32" else "bin/python"
        python = environment / executable
        subprocess.run(
            [uv, "pip", "install", "--python", str(python), str(wheel)],
            check=True,
        )
        base_smoke = (
            "from importlib.metadata import PackageNotFoundError, version; "
            "import fastapi; import fastapi_langgraph_server; import langgraph; "
            f"assert version({_DISTRIBUTION_NAME!r}) == {project['version']!r}; "
            "assert '.venv' in "
            "__import__('pathlib').Path(fastapi_langgraph_server.__file__).parts; "
            "\ntry:\n version('langgraph-checkpoint-redis')"
            "\nexcept PackageNotFoundError:\n pass"
            "\nelse:\n raise AssertionError('Redis must remain optional')"
        )
        subprocess.run([str(python), "-I", "-c", base_smoke], check=True)
        subprocess.run(
            [uv, "pip", "install", "--python", str(python), f"{wheel}[redis]"],
            check=True,
        )
        redis_smoke = (
            "from langgraph.checkpoint.redis.aio import AsyncRedisSaver; "
            "assert AsyncRedisSaver is not None"
        )
        subprocess.run([str(python), "-I", "-c", redis_smoke], check=True)


def main() -> None:
    """Validate exactly one wheel and sdist in the local dist directory."""
    project = _project_metadata()
    if project["name"] != _DISTRIBUTION_NAME:
        raise RuntimeError("pyproject.toml has an unexpected distribution name")
    dist = Path("dist")
    wheel = _single_artifact(dist, "*.whl")
    sdist = _single_artifact(dist, "*.tar.gz")
    _validate_wheel(wheel, project)
    _validate_sdist(sdist)
    _isolated_wheel_smoke(wheel.resolve(), project)


if __name__ == "__main__":
    main()
