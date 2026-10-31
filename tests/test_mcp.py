from datetime import datetime, timezone
import json
from pathlib import Path
from uuid import uuid4

from mcp.shared.memory import create_connected_server_and_client_session
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from pydantic_settings.sources import DotEnvSettingsSource, SecretsSettingsSource
import pytest

from agentrun import Job, Runner, Step
from agentrun.mcp import create_server
from agentrun.replay import render_replay
from agentrun.runner import JobReader


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture(autouse=True)
def no_config_files(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("MCP must not load dotenv or secrets files")

    monkeypatch.setattr(DotEnvSettingsSource, "_read_env_files", forbidden)
    monkeypatch.setattr(SecretsSettingsSource, "__call__", forbidden)


@pytest.fixture
async def client(database_url):
    # Keep the test schema's search_path while also denying writes at the DB level.
    options = conninfo_to_dict(database_url).get("options", "")
    read_only_url = make_conninfo(
        database_url, options=f"{options} -cdefault_transaction_read_only=on",
    )
    async with create_connected_server_and_client_session(create_server(read_only_url)) as session:
        yield session


async def call(client, name, **arguments):
    result = await client.call_tool(name, arguments)
    assert not result.isError, result.content
    assert json.loads(result.content[0].text) == result.structuredContent
    return result.structuredContent


@pytest.mark.anyio
async def test_discovery_needs_no_database_or_runner(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("Discovery must not connect or initialize a runner")

    monkeypatch.setattr(JobReader, "connect", forbidden)
    monkeypatch.setattr(Runner, "__init__", forbidden)
    async with create_connected_server_and_client_session(create_server()) as session:
        tools = (await session.list_tools()).tools
        assert {tool.name for tool in tools} == {"list_jobs", "job_status", "replay_job"}
        for tool in tools:
            assert tool.annotations.readOnlyHint is True
            assert tool.annotations.destructiveHint is False
            assert tool.annotations.openWorldHint is False
            assert tool.inputSchema["additionalProperties"] is False


@pytest.mark.anyio
async def test_list_jobs_empty_and_paginated(runner, client):
    assert await call(client, "list_jobs") == {"jobs": []}
    ids = [runner.submit("example", {"sleep": 0}, max_usd="1.25") for _ in range(3)]
    # Tie timestamps to check the stable UUID tiebreaker as well as pagination.
    with runner.connect() as conn:
        conn.execute("UPDATE jobs SET created_at = '2026-01-01T00:00:00Z'")
    expected = sorted(map(str, ids), reverse=True)
    jobs = (await call(client, "list_jobs"))["jobs"]
    assert [job["id"] for job in jobs] == expected
    assert all(job["status"] == "queued" and job["max_usd"] == "1.25" for job in jobs)
    assert all(job["job_type"] == "example" for job in jobs)
    assert all(datetime.fromisoformat(job["created_at"]) == datetime(2026, 1, 1, tzinfo=timezone.utc) for job in jobs)
    page = (await call(client, "list_jobs", limit=1, offset=1))["jobs"]
    assert page == jobs[1:2]
    assert await call(client, "list_jobs", offset=3) == {"jobs": []}


@pytest.mark.anyio
async def test_list_jobs_newest_first_and_default_limit(runner, client):
    ids = [runner.submit("example", {"sleep": 0}) for _ in range(22)]
    jobs = (await call(client, "list_jobs"))["jobs"]
    assert len(jobs) == 20
    assert [row["id"] for row in jobs] == list(map(str, reversed(ids)))[:20]
    assert len((await call(client, "list_jobs", limit=100))["jobs"]) == 22


@pytest.mark.anyio
async def test_status_and_replay_use_recorded_state_without_writes(runner, client, monkeypatch, tmp_path):
    job_id = runner.submit("example", {"prompt": "saved answer", "sleep": 0}, max_usd="1.50")
    assert runner.run_once()
    queued_id = runner.submit("example", {"sleep": 0})
    before = runner.status(job_id)
    queued_before = runner.status(queued_id)

    def snapshot():
        with runner.connect() as conn:
            return {
                table: sorted(
                    conn.execute(sql.SQL("SELECT * FROM {}").format(sql.Identifier(table))).fetchall(),
                    key=str,
                )
                for table in ("jobs", "steps", "step_attempts", "attempt_usage", "job_cancellations", "mock_calls")
            }

    database_before = snapshot()

    def forbidden(*args, **kwargs):
        pytest.fail("MCP must not execute jobs, initialize schemas or write files")

    monkeypatch.chdir(tmp_path)
    for method in ("__init__", "submit", "cancel", "run_once", "worker", "replay"):
        monkeypatch.setattr(Runner, method, forbidden)
    monkeypatch.setattr(Path, "write_text", forbidden)
    monkeypatch.setattr(Path, "mkdir", forbidden)

    for _ in range(2):
        job = await call(client, "job_status", job_id=str(job_id))
        assert job == json.loads(json.dumps(before, default=str))
        assert (job["input_tokens"], job["output_tokens"], job["cost_usd"]) == (30, 15, "0.30")
        assert len(job["steps"]) == len(job["attempts"]) == len(job["attempt_usage"]) == 3
        html = (await call(client, "replay_job", job_id=str(job_id)))["html"]
        assert html == render_replay(before)
        assert html.index("<h2>draft</h2>") < html.index("<h2>review</h2>") < html.index("<h2>polish</h2>")
        assert "saved answer" in html and "$0.30" in html
        assert (await call(client, "job_status", job_id=str(queued_id)))["status"] == "queued"
        assert "not run" in (await call(client, "replay_job", job_id=str(queued_id)))["html"]
        assert len((await call(client, "list_jobs"))["jobs"]) == 2

    assert runner.status(job_id) == before
    assert runner.status(queued_id) == queued_before
    assert snapshot() == database_before  # Includes mock execution and delivery counts.
    assert list(tmp_path.iterdir()) == []


@pytest.mark.anyio
@pytest.mark.parametrize("terminal", ["failed", "over_budget"])
async def test_failed_attempts_and_blocked_steps(database_url, client, terminal):
    executions = []

    def fail(context, value):
        executions.append(value)
        context.report_usage(input_tokens=7, output_tokens=2, cost_usd="0.125")
        raise ValueError("recorded failure")

    runner = Runner({"failure": Job((
        Step("first", fail, max_attempts=2, base_delay=0),
        Step("blocked", lambda c, v: pytest.fail("blocked step ran")),
    ))})
    job_id = runner.submit("failure", "input", max_usd="0.125" if terminal == "over_budget" else None)
    assert runner.run_once()
    before = runner.status(job_id)
    job = await call(client, "job_status", job_id=str(job_id))
    assert job["status"] == terminal
    assert job["cost_usd"] == ("0.125" if terminal == "over_budget" else "0.250")
    assert job["steps"] == []
    assert job["attempts"][0]["attempt_count"] == len(job["attempt_usage"]) == len(executions)
    html = (await call(client, "replay_job", job_id=str(job_id)))["html"]
    assert "recorded failure" in html
    assert "blocked by spend cap" in html if terminal == "over_budget" else "not run" in html
    assert runner.status(job_id) == before


@pytest.mark.anyio
async def test_pending_cancellation_is_visible(runner, client):
    job_id = runner.submit("example", {"sleep": 0})
    # A cancel request can be committed before the worker updates the jobs row.
    with runner.connect() as conn:
        conn.execute("INSERT INTO job_cancellations (job_id) VALUES (%s)", (job_id,))
    assert (await call(client, "job_status", job_id=str(job_id)))["status"] == "cancelled"
    assert (await call(client, "list_jobs"))["jobs"][0]["status"] == "cancelled"
    assert "<dt>Status</dt><dd>cancelled</dd>" in (await call(client, "replay_job", job_id=str(job_id)))["html"]
    with runner.connect() as conn:
        assert conn.execute("SELECT status FROM jobs WHERE id = %s", (job_id,)).fetchone()["status"] == "queued"


@pytest.mark.anyio
async def test_replay_partial_job_does_not_resume_expired_lease(database_url, client):
    executions = []
    runner = Runner({"partial": Job((
        Step("saved", lambda c, v: executions.append(v)),
        Step("pending", lambda c, v: pytest.fail("MCP resumed a job")),
    ))})
    job_id = runner.submit("partial", "original input")
    with runner.connect() as conn:
        claim = runner._claim(conn)
        assert runner._step(conn, claim, runner.jobs["partial"].steps[0], claim["input"]) is None
        conn.execute("UPDATE jobs SET lease_until = clock_timestamp() - interval '1 second' WHERE id = %s", (job_id,))
    before = runner.status(job_id)
    job = await call(client, "job_status", job_id=str(job_id))
    assert job["status"] == "running"
    assert len(job["steps"]) == 1 and job["steps"][0]["result"] is None
    html = (await call(client, "replay_job", job_id=str(job_id)))["html"]
    assert html.index("<h2>saved</h2>") < html.index("<h2>pending</h2>")
    assert "<summary>Output</summary><pre>null</pre>" in html
    assert "not run" in html
    assert executions == ["original input"]
    assert runner.status(job_id) == before


@pytest.mark.anyio
@pytest.mark.parametrize("name", ["job_status", "replay_job"])
async def test_missing_job_is_tool_error(runner, client, name):
    result = await client.call_tool(name, {"job_id": str(uuid4())})
    assert result.isError
    assert "Job not found" in result.content[0].text
    assert await call(client, "list_jobs") == {"jobs": []}


@pytest.mark.anyio
@pytest.mark.parametrize("name, arguments", [
    ("list_jobs", {"limit": 0}),
    ("list_jobs", {"limit": 101}),
    ("list_jobs", {"limit": True}),
    ("list_jobs", {"limit": "10"}),
    ("list_jobs", {"offset": -1}),
    ("list_jobs", {"offset": 0.5}),
    ("list_jobs", {"sql": "DELETE FROM jobs"}),
    ("job_status", {}),
    ("job_status", {"job_id": "bad-id"}),
    ("job_status", {"job_id": "'; DELETE FROM jobs; --"}),
    ("job_status", {"job_id": 1}),
    ("replay_job", {"job_id": "bad-id"}),
    ("replay_job", {"job_id": str(uuid4()), "out": "export"}),
    ("submit", {"job_type": "example"}),
    ("cancel", {"job_id": str(uuid4())}),
    ("worker", {}),
    ("run_once", {}),
])
async def test_invalid_and_write_requests_fail_before_database_access(monkeypatch, name, arguments):
    def forbidden(*args, **kwargs):
        pytest.fail("Invalid and write requests must not access the database")

    monkeypatch.setattr(JobReader, "connect", forbidden)
    async with create_connected_server_and_client_session(create_server()) as session:
        result = await session.call_tool(name, arguments)
        assert result.isError
        # A bad request leaves the session usable.
        assert len((await session.list_tools()).tools) == 3


@pytest.mark.anyio
async def test_empty_schema_is_not_initialized(database_url, client):
    for name, arguments in [
        ("list_jobs", {}),
        ("job_status", {"job_id": str(uuid4())}),
        ("replay_job", {"job_id": str(uuid4())}),
    ]:
        result = await client.call_tool(name, arguments)
        assert result.isError
        assert 'relation "jobs" does not exist' in result.content[0].text
    with JobReader(database_url).connect() as conn:
        assert conn.execute("SELECT to_regclass('jobs') AS table_name").fetchone()["table_name"] is None
