"""Loads queries/critical/*.sql and their `-- name:` / `-- slo:` headers.

Shared by the Locust load harness and the EXPLAIN ANALYZE capture script so
both work from the exact same query text and SLO targets.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

QUERIES_DIR = Path(__file__).resolve().parents[1] / "queries" / "critical"

_NAME_RE = re.compile(r"^--\s*name:\s*(\S+)", re.MULTILINE)
_SLO_RE = re.compile(r"^--\s*slo:\s*p95\s*<\s*(\d+)\s*ms", re.MULTILINE)
_LEADING_COMMENT_RE = re.compile(r"^\s*--.*\n?", re.MULTILINE)


@dataclass(frozen=True)
class QuerySpec:
    name: str
    slo_ms: int
    sql: str
    path: Path

    @property
    def sql_body(self) -> str:
        """The SQL with leading `-- ...` header comment lines stripped."""
        return _LEADING_COMMENT_RE.sub("", self.sql).strip()


def load_critical_queries() -> list[QuerySpec]:
    specs: list[QuerySpec] = []
    for path in sorted(QUERIES_DIR.glob("*.sql")):
        raw = path.read_text()
        name_match = _NAME_RE.search(raw)
        slo_match = _SLO_RE.search(raw)
        if not name_match or not slo_match:
            raise ValueError(f"{path} is missing a required '-- name:' or '-- slo:' header")
        specs.append(
            QuerySpec(name=name_match.group(1), slo_ms=int(slo_match.group(1)), sql=raw, path=path)
        )
    return specs
