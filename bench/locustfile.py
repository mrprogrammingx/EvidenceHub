"""Locust harness for the 8 critical Evidence queries against the legacy schema.

Run headless, e.g.:

    uv run locust -f bench/locustfile.py --headless \\
        --users 8 --spawn-rate 8 --run-time 60s \\
        --csv bench/out/baseline --host mysql://legacy

Each of the 8 critical queries is a Locust task, run against a randomly
chosen shard with realistic bound parameters drawn from that shard's sample
pool (see sample_pool.py). Locust's own request-stats machinery (fired via
events.request) gives p50/p66/p75/.../p99.9 and RPS per query "name" in the
--csv output, with no custom percentile math needed.
"""

from __future__ import annotations

import random
import sys
import time
from pathlib import Path
from typing import Any, ClassVar

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from bench.critical_queries import QuerySpec, load_critical_queries  # noqa: E402
from bench.sample_pool import SamplePool, build_params, load_sample_pool  # noqa: E402
from locust import User, between, events  # noqa: E402
from locust.env import Environment  # noqa: E402
from sqlalchemy import Engine, create_engine, text  # noqa: E402

from evidencehub.common.config import get_settings  # noqa: E402

QUERIES = load_critical_queries()
SETTINGS = get_settings()

_engines: list[Engine] = []
_pools: list[SamplePool] = []


@events.test_start.add_listener
def _on_test_start(environment: Environment, **kwargs: Any) -> None:
    for i in range(SETTINGS.shard_count):
        engine = create_engine(
            SETTINGS.shard_url(i), pool_pre_ping=True, pool_size=10, max_overflow=5
        )
        _engines.append(engine)
        _pools.append(load_sample_pool(engine))


@events.test_stop.add_listener
def _on_test_stop(environment: Environment, **kwargs: Any) -> None:
    for engine in _engines:
        engine.dispose()
    _engines.clear()
    _pools.clear()


class MySQLClient:
    def __init__(self) -> None:
        self._rng = random.Random()

    def run(self, query_name: str, sql_body: str) -> None:
        shard_index = self._rng.randrange(len(_engines))
        engine = _engines[shard_index]
        pool = _pools[shard_index]
        params = build_params(query_name, pool, self._rng)

        start = time.perf_counter()
        exception: Exception | None = None
        row_count = 0
        try:
            with engine.connect() as conn:
                result = conn.execute(text(sql_body), params)
                row_count = len(result.fetchall())
        except Exception as exc:
            exception = exc
        elapsed_ms = (time.perf_counter() - start) * 1000

        events.request.fire(
            request_type="mysql",
            name=query_name,
            response_time=elapsed_ms,
            response_length=row_count,
            exception=exception,
            context={},
        )


def _make_task(spec: QuerySpec) -> Any:
    def _task(user: EvidenceQueryUser) -> None:
        user.client.run(spec.name, spec.sql_body)

    _task.__name__ = f"task_{spec.name}"
    return _task


class EvidenceQueryUser(User):
    wait_time = between(0, 0.05)
    tasks: ClassVar[list[Any]] = [_make_task(spec) for spec in QUERIES]

    def __init__(self, environment: Environment) -> None:
        super().__init__(environment)
        self.client = MySQLClient()
