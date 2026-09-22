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


def _shard_urls() -> list[str]:
    settings = get_settings()
    return [settings.shard_url(i) for i in range(settings.shard_count)]


def run_migrations_offline() -> None:
    for url in _shard_urls():
        context.configure(
            url=url,
            target_metadata=target_metadata,
            literal_binds=True,
            dialect_opts={"paramstyle": "named"},
        )
        with context.begin_transaction():
            context.run_migrations()


def run_migrations_online() -> None:
    # The legacy schema is identical across all physical shards, so one
    # invocation fans this migration out to every shard in turn. Each shard
    # keeps its own alembic_version row, so partial progress is resumable.
    for url in _shard_urls():
        configuration = config.get_section(config.config_ini_section) or {}
        configuration["sqlalchemy.url"] = url
        connectable = engine_from_config(
            configuration, prefix="sqlalchemy.", poolclass=pool.NullPool
        )
        with connectable.connect() as connection:
            context.configure(connection=connection, target_metadata=target_metadata)
            with context.begin_transaction():
                context.run_migrations()
        connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
