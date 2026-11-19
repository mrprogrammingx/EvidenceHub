from __future__ import annotations

import importlib.util
import os
from collections.abc import Iterator
from pathlib import Path

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import Engine, create_engine

# Avoid pulling the Ryuk reaper image; containers are stopped by the context manager.
os.environ.setdefault("TESTCONTAINERS_RYUK_DISABLED", "true")

REPO_ROOT = Path(__file__).resolve().parents[2]
MIGRATIONS = [
    REPO_ROOT / "db/legacy/versions/0001_legacy_evidence.py",
    REPO_ROOT / "db/optimized/versions/0001_evidence_optimized.py",
    REPO_ROOT / "db/optimized/versions/0002_dualwrite_outbox.py",
]


def _docker_available() -> bool:
    try:
        import docker

        docker.from_env().ping()
    except Exception:
        return False
    return True


def _apply(engine: Engine, path: Path) -> None:
    spec = importlib.util.spec_from_file_location(path.stem, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    with engine.begin() as conn, Operations.context(MigrationContext.configure(conn)):
        module.upgrade()


@pytest.fixture(scope="session")
def shard_engine() -> Iterator[Engine]:
    if not _docker_available():
        pytest.skip("Docker is not available")
    from testcontainers.mysql import MySqlContainer

    with MySqlContainer("mysql:8.0", dialect="pymysql") as container:
        engine = create_engine(container.get_connection_url() + "?charset=utf8mb4")
        for path in MIGRATIONS:
            _apply(engine, path)
        yield engine
        engine.dispose()
