from __future__ import annotations

import os
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import Engine, create_engine

from .legacy_fixtures import load_script

# Avoid pulling the Ryuk reaper image; containers are stopped by the context manager.
os.environ.setdefault("TESTCONTAINERS_RYUK_DISABLED", "true")

REPO_ROOT = Path(__file__).resolve().parents[2]
MIGRATIONS = [
    REPO_ROOT / "db/legacy/versions/0001_legacy_evidence.py",
    REPO_ROOT / "db/optimized/versions/0001_evidence_optimized.py",
    REPO_ROOT / "db/optimized/versions/0002_dualwrite_outbox.py",
    REPO_ROOT / "db/optimized/versions/0003_backfill_month_cursor.py",
]


def _docker_available() -> bool:
    try:
        import docker

        docker.from_env().ping()
    except Exception:
        return False
    return True


@pytest.fixture(scope="session")
def mysql_root_url() -> Iterator[str]:
    if not _docker_available():
        pytest.skip("Docker is not available")
    from testcontainers.mysql import MySqlContainer

    with MySqlContainer("mysql:8.0", dialect="pymysql") as c:
        host, port = c.get_container_host_ip(), c.get_exposed_port(c.port)
        yield f"mysql+pymysql://root:{c.root_password}@{host}:{port}"


@pytest.fixture(scope="session")
def make_schema(mysql_root_url: str) -> Iterator[Callable[[str], Engine]]:
    engines: list[Engine] = []

    def factory(name: str) -> Engine:
        admin = create_engine(mysql_root_url)
        with admin.begin() as conn:
            conn.exec_driver_sql(f"CREATE DATABASE `{name}` CHARACTER SET utf8mb4")
        admin.dispose()
        engine = create_engine(f"{mysql_root_url}/{name}?charset=utf8mb4")
        for path in MIGRATIONS:
            module = load_script(str(path.relative_to(REPO_ROOT)))
            with engine.begin() as conn, Operations.context(MigrationContext.configure(conn)):
                module.upgrade()
        engines.append(engine)
        return engine

    yield factory
    for engine in engines:
        engine.dispose()
