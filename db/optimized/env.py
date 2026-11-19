"""Alembic environment for the optimized `evidence` schema (P2).

Unlike db/legacy, this never fans out implicitly: pass ``-x shard=<index>`` or
``-x shard=all``. It also uses its own version table so it coexists with the
legacy migration's `alembic_version` in the same shard database.
"""

from __future__ import annotations

import sys
from logging.config import fileConfig
from pathlib import Path

from alembic import context
from sqlalchemy import engine_from_config, pool

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from evidencehub.common.config import get_settings  # noqa: E402

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = None
VERSION_TABLE = "alembic_version_optimized"


def _selected_shard_urls() -> list[str]:
    selected = context.get_x_argument(as_dictionary=True).get("shard")
    settings = get_settings()
    if selected is None:
        raise SystemExit("Pass -x shard=<index> or -x shard=all (no implicit fan-out).")
    if selected == "all":
        return [settings.shard_url(i) for i in range(settings.shard_count)]
    return [settings.shard_url(int(selected))]


def run_migrations_online() -> None:
    for url in _selected_shard_urls():
        configuration = config.get_section(config.config_ini_section) or {}
        configuration["sqlalchemy.url"] = url
        connectable = engine_from_config(
            configuration, prefix="sqlalchemy.", poolclass=pool.NullPool
        )
        with connectable.connect() as connection:
            context.configure(
                connection=connection,
                target_metadata=target_metadata,
                version_table=VERSION_TABLE,
            )
            with context.begin_transaction():
                context.run_migrations()
        connectable.dispose()


if context.is_offline_mode():
    raise SystemExit("Offline mode is not supported for the optimized schema.")
run_migrations_online()
