from __future__ import annotations

import os
import re
import shutil
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from groupagent.domain import ActorContext, Role
from groupagent.persistence import PostgresStore


@pytest.fixture
def database_factory():
    dsn = os.environ.get("GROUPAGENT_TEST_DSN")
    if not dsn:
        pytest.skip("Set GROUPAGENT_TEST_DSN for isolated PostgreSQL operations tests")
    params = conninfo_to_dict(dsn)
    host = str(params.get("host", ""))
    if not (host.startswith("/") or host in {"localhost", "127.0.0.1", "::1"}):
        pytest.skip("Operations tests require an explicit local isolated PostgreSQL server")
    created = []
    with psycopg.connect(dsn, autocommit=True) as admin:

        def create(*, migrate=False):
            name = "groupagent_ops_" + uuid4().hex
            assert name != params.get("dbname") and re.fullmatch(
                r"groupagent_ops_[a-f0-9]{32}", name
            )
            admin.execute(
                sql.SQL("CREATE DATABASE {} TEMPLATE template0").format(sql.Identifier(name))
            )
            created.append(name)
            result = make_conninfo(dsn, dbname=name)
            if migrate:
                PostgresStore(result).migrate()
            return result

        yield create
        for name in reversed(created):
            assert re.fullmatch(r"groupagent_ops_[a-f0-9]{32}", name) and name != params.get(
                "dbname"
            )
            admin.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=%s AND pid<>pg_backend_pid()",
                (name,),
            )
            admin.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(name)))


@pytest.fixture
def store(database_factory):
    return PostgresStore(database_factory(migrate=True))


@pytest.fixture
def actor(store):
    group = "synthetic-ops-" + uuid4().hex
    store.register_group(group)
    return ActorContext(group_id=group, member_id="service:ops", role=Role.MAINTAINER)


@pytest.fixture
def member(actor):
    return actor.model_copy(update={"member_id": "member-a", "role": Role.MEMBER})


@pytest.fixture
def pg_bin():
    candidates = [
        Path("/opt/homebrew/Cellar/postgresql@16/16.15/bin"),
        Path("/usr/lib/postgresql/16/bin"),
    ]
    if executable := shutil.which("pg_dump"):
        candidates.append(Path(executable).resolve().parent)
    for candidate in candidates:
        if (candidate / "pg_dump").is_file() and (candidate / "pg_restore").is_file():
            return candidate
    pytest.skip("Real pg_dump and pg_restore binaries are required")
