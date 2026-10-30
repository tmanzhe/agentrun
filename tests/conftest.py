import os
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
import pytest

from agentrun.example import JOBS, MOCK_SCHEMA
from agentrun.runner import DEFAULT_DATABASE_URL, Runner


@pytest.fixture
def database_url(monkeypatch):
    base = os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL)
    try:
        admin = psycopg.connect(base, connect_timeout=2, autocommit=True)
    except psycopg.OperationalError as exc:
        pytest.skip(f"PostgreSQL is not reachable: {exc}")
    schema = f"test_agentrun_{uuid4().hex}"
    with admin:
        admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        options = conninfo_to_dict(base).get("options", "")
        isolated = make_conninfo(base, options=f"{options} -csearch_path={schema}")
        monkeypatch.setenv("DATABASE_URL", isolated)
        try:
            yield isolated
        finally:
            admin.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


@pytest.fixture
def runner(database_url):
    runner = Runner(JOBS, lease_seconds=0.2)
    with runner.connect() as conn:
        conn.execute(MOCK_SCHEMA)
    return runner
