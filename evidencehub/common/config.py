from __future__ import annotations

from functools import lru_cache

from pydantic import computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    evidencehub_env: str = "local"
    log_level: str = "INFO"

    mysql_catalog_host: str = "127.0.0.1"
    mysql_catalog_port: int = 3306
    mysql_catalog_db: str = "evidencehub_catalog"
    mysql_catalog_user: str = "evidencehub"
    mysql_catalog_password: str = "change_me_dev_password"

    mysql_shard_hosts: str = "127.0.0.1:3307,127.0.0.1:3308,127.0.0.1:3309,127.0.0.1:3310"
    mysql_shard_db_prefix: str = "evidencehub_shard"
    mysql_shard_user: str = "evidencehub"
    mysql_shard_password: str = "change_me_dev_password"

    redis_host: str = "127.0.0.1"
    redis_port: int = 6379
    redis_password: str = ""

    seed_rows_default: int = 1_000_000
    seed_rows_full: int = 20_000_000
    seed_batch_size: int = 2_000

    api_host: str = "0.0.0.0"
    api_port: int = 8000

    @computed_field  # type: ignore[prop-decorator]
    @property
    def shard_addresses(self) -> list[tuple[str, int]]:
        addresses: list[tuple[str, int]] = []
        for raw in self.mysql_shard_hosts.split(","):
            host, _, port = raw.strip().partition(":")
            addresses.append((host, int(port)))
        return addresses

    @property
    def shard_count(self) -> int:
        return len(self.shard_addresses)

    def catalog_url(self) -> str:
        return (
            f"mysql+pymysql://{self.mysql_catalog_user}:{self.mysql_catalog_password}"
            f"@{self.mysql_catalog_host}:{self.mysql_catalog_port}/{self.mysql_catalog_db}"
            "?charset=utf8mb4"
        )

    def shard_url(self, shard_index: int) -> str:
        if not 0 <= shard_index < self.shard_count:
            raise ValueError(
                f"shard_index {shard_index} out of range for {self.shard_count} shards"
            )
        host, port = self.shard_addresses[shard_index]
        db_name = f"{self.mysql_shard_db_prefix}_{shard_index}"
        return (
            f"mysql+pymysql://{self.mysql_shard_user}:{self.mysql_shard_password}"
            f"@{host}:{port}/{db_name}?charset=utf8mb4"
        )

    def redis_url(self) -> str:
        auth = f":{self.redis_password}@" if self.redis_password else ""
        return f"redis://{auth}{self.redis_host}:{self.redis_port}/0"


@lru_cache
def get_settings() -> Settings:
    return Settings()
