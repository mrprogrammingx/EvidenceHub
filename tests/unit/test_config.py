from __future__ import annotations

import pytest

from evidencehub.common.config import Settings


def test_shard_addresses_parsed_from_comma_separated_hosts() -> None:
    settings = Settings(mysql_shard_hosts="host-a:3307,host-b:3308,host-c:3309,host-d:3310")
    assert settings.shard_addresses == [
        ("host-a", 3307),
        ("host-b", 3308),
        ("host-c", 3309),
        ("host-d", 3310),
    ]
    assert settings.shard_count == 4


def test_shard_url_builds_expected_dsn_for_index() -> None:
    settings = Settings(
        mysql_shard_hosts="host-a:3307,host-b:3308",
        mysql_shard_user="app",
        mysql_shard_password="secret",
        mysql_shard_db_prefix="evidencehub_shard",
    )
    assert settings.shard_url(0) == (
        "mysql+pymysql://app:secret@host-a:3307/evidencehub_shard_0?charset=utf8mb4"
    )
    assert settings.shard_url(1) == (
        "mysql+pymysql://app:secret@host-b:3308/evidencehub_shard_1?charset=utf8mb4"
    )


def test_shard_url_rejects_out_of_range_index() -> None:
    settings = Settings(mysql_shard_hosts="host-a:3307")
    with pytest.raises(ValueError, match="out of range"):
        settings.shard_url(1)


def test_catalog_url_includes_charset() -> None:
    settings = Settings(
        mysql_catalog_host="cat-host",
        mysql_catalog_port=3306,
        mysql_catalog_db="evidencehub_catalog",
        mysql_catalog_user="app",
        mysql_catalog_password="secret",
    )
    assert settings.catalog_url() == (
        "mysql+pymysql://app:secret@cat-host:3306/evidencehub_catalog?charset=utf8mb4"
    )


def test_redis_url_without_password() -> None:
    settings = Settings(redis_host="cache-host", redis_port=6380, redis_password="")
    assert settings.redis_url() == "redis://cache-host:6380/0"


def test_redis_url_with_password() -> None:
    settings = Settings(redis_host="cache-host", redis_port=6380, redis_password="hunter2")
    assert settings.redis_url() == "redis://:hunter2@cache-host:6380/0"
