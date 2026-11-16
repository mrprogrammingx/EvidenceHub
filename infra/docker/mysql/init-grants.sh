#!/bin/bash
# Runs once via /docker-entrypoint-initdb.d on first container init (empty
# data dir only). The official image's MYSQL_USER only gets privileges on
# MYSQL_DATABASE; bench/capture_explain.py needs SELECT on performance_schema
# (threads, events_statements_history) to capture rows_examined/timing per
# query, so grant that too.
set -euo pipefail

mysql -uroot -p"${MYSQL_ROOT_PASSWORD}" -e "GRANT SELECT ON performance_schema.* TO '${MYSQL_USER}'@'%';"
