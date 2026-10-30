import json
from pathlib import Path
import signal
import subprocess
import sys
from threading import Event, Thread
import time
from uuid import UUID, uuid4

import pytest

from agentrun import Job, Runner, Step
from agentrun.runner import LostClaim, input_hash


CLI = str(Path(sys.executable).with_name("agentrun"))


def cli(*args, check=True):
    return subprocess.run([CLI, *args], capture_output=True, text=True, check=check, timeout=10)


def wait_until(check, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = check()
        if result:
            return result
        time.sleep(0.02)
    pytest.fail("Timed out waiting for database state")


def calls_for(runner, job_id):
    with runner.connect() as conn:
        return conn.execute(
            "SELECT step_name, calls, requests FROM mock_calls WHERE job_id = %s ORDER BY step_name",
            (job_id,),
        ).fetchall()


def expired(runner, job_id):
    with runner.connect() as conn:
        return conn.execute(
            "SELECT lease_until < clock_timestamp() AS expired FROM jobs WHERE id = %s", (job_id,)
        ).fetchone()["expired"]


def test_hash_and_step_names():
    assert input_hash({"b": [1, None], "a": 2}) == input_hash({"a": 2, "b": [1, None]})
    assert input_hash({"a": 1}) != input_hash({"a": 2})
    with pytest.raises(ValueError):
        input_hash(float("nan"))
    with pytest.raises(ValueError):
        Job(())
    with pytest.raises(ValueError):
        Job((Step("same", lambda c, v: v), Step("same", lambda c, v: v)))


def test_cli_submit_worker_status(database_url):
    job_id = UUID(cli("submit", "example", '{"prompt":"test","sleep":0}').stdout.strip())
    assert json.loads(cli("status", str(job_id)).stdout)["status"] == "queued"
    cli("worker", "--once")
    job = json.loads(cli("status", str(job_id)).stdout)
    assert job["status"] == "succeeded"
    assert [step["step_name"] for step in job["steps"]] == ["draft", "review", "polish"]
    assert len(job["result"]["answers"]) == 3
    assert job["result"]["prompt"] == "test"
    runner = Runner({})
    assert all(row["calls"] == row["requests"] == 1 for row in calls_for(runner, job_id))
    assert not runner.run_once()
    assert cli("status", str(uuid4()), check=False).returncode == 1
    assert cli("submit", "example", "{bad json", check=False).returncode != 0


def test_saved_null_is_reused_on_resume(database_url):
    executions = []

    def first(context, value):
        executions.append(context.idempotency_key)
        return None

    def second(context, value):
        assert value is None
        return {"done": True}

    runner = Runner({"null": Job((Step("first", first), Step("second", second)))}, lease_seconds=0.05)
    job_id = runner.submit("null", {"input": True})
    # Persist exactly the prefix of a job that would survive a worker crash.
    with runner.connect() as conn:
        claim = runner._claim(conn)
        assert runner._step(conn, claim, runner.jobs["null"].steps[0], claim["input"]) is None
    wait_until(lambda: expired(runner, job_id))
    assert runner.run_once()
    job = runner.status(job_id)
    assert job["status"] == "succeeded"
    assert job["result"] == {"done": True}
    assert len(executions) == 1
    assert len(job["steps"]) == 2


def test_cache_uses_input_hash_and_normalizes_json(database_url):
    executions = []

    def record(context, value):
        executions.append(context.idempotency_key)
        return (value,)

    definition = Job((Step("record", record),))
    runner = Runner({"record": definition})
    job_id = runner.submit("record", {})
    with runner.connect() as conn:
        claim = runner._claim(conn)
        step = definition.steps[0]
        assert runner._step(conn, claim, step, {"a": 1, "b": 2}) == [{"a": 1, "b": 2}]
        assert runner._step(conn, claim, step, {"b": 2, "a": 1}) == [{"a": 1, "b": 2}]
        assert runner._step(conn, claim, step, {"a": 2}) == [{"a": 2}]
    assert len(executions) == len(set(executions)) == 2
    assert len(runner.status(job_id)["steps"]) == 2


def test_expired_claim_is_fenced(database_url):
    executions = []
    step = Step("record", lambda context, value: executions.append(value))
    runner = Runner({"record": Job((step,))}, lease_seconds=0.05)
    job_id = runner.submit("record", "value")
    with runner.connect() as old, runner.connect() as new:
        stale = runner._claim(old)
        wait_until(lambda: expired(runner, job_id))
        current = runner._claim(new)
        assert current["claim_token"] != stale["claim_token"]
        with pytest.raises(LostClaim):
            runner._step(old, stale, step, "stale")
        with pytest.raises(LostClaim):
            runner._finish(old, stale, "stale")
        runner._step(new, current, step, "current")
        runner._finish(new, current, "done")
    assert executions == ["current"]
    assert runner.status(job_id)["result"] == "done"


def test_step_error_is_terminal_and_keeps_finished_steps(database_url):
    def fail(context, value):
        raise ValueError("model rejected input")

    runner = Runner({"bad": Job((Step("ok", lambda c, v: v), Step("bad", fail)))})
    job_id = runner.submit("bad", {"hello": "world"})
    assert runner.run_once()
    job = runner.status(job_id)
    assert job["status"] == "failed"
    assert job["error"] == "ValueError: model rejected input"
    assert [step["step_name"] for step in job["steps"]] == ["ok"]
    assert not runner.run_once()
    with pytest.raises(ValueError, match="Unknown job type"):
        runner.submit("missing")


@pytest.mark.parametrize("succeed_after", [3, 4])
def test_retry_backoff_and_exhaustion(database_url, succeed_after):
    executions = []
    starts = []
    keys = []

    def flaky(context, value):
        starts.append(time.monotonic())
        keys.append(context.idempotency_key)
        job = runner.status(context.job_id)
        # Previous failures are visible as attempts, never as cached results.
        assert [row["step_name"] for row in job["steps"]] == ["prefix"]
        attempt = next(row for row in job["attempts"] if row["step_name"] == "flaky")
        assert attempt["attempt_count"] == len(starts)
        if len(starts) < succeed_after:
            raise ValueError(f"failure {len(starts)}")
        return {"done": value}

    runner = Runner({"retry": Job((
        Step("prefix", lambda c, v: executions.append(v) or v),
        Step("flaky", flaky, max_attempts=3, base_delay=0.06),
    ))})
    job_id = runner.submit("retry", "input")
    assert runner.run_once()
    job = json.loads(cli("status", str(job_id)).stdout)
    assert executions == ["input"]
    assert len(starts) == 3
    assert len(set(keys)) == 1
    assert starts[1] - starts[0] >= 0.06
    assert starts[2] - starts[1] >= 0.12
    attempt = next(row for row in job["attempts"] if row["step_name"] == "flaky")
    assert attempt["attempt_count"] == 3
    if succeed_after == 3:
        assert job["status"] == "succeeded"
        assert job["result"] == {"done": "input"}
        assert len(job["steps"]) == 2
        assert attempt["last_error"] == "ValueError: failure 2"
    else:
        assert job["status"] == "failed"
        assert job["error"] == attempt["last_error"] == "ValueError: failure 3"
        assert [row["step_name"] for row in job["steps"]] == ["prefix"]
    assert not runner.run_once()


@pytest.mark.parametrize("recover", [False, True])
def test_timeout_retries_and_abandons_late_results(database_url, recover):
    release = Event()
    finished = Event()
    starts = []
    prefixes = []

    def slow(context, value):
        starts.append(context.idempotency_key)
        if recover and len(starts) == 2:
            return "retry success"
        release.wait(5)
        finished.set()
        return "late result"

    runner = Runner({"timeout": Job((
        Step("prefix", lambda c, v: prefixes.append(v) or v),
        Step("slow", slow, timeout=0.05, max_attempts=2, base_delay=0.02),
    ))})
    job_id = runner.submit("timeout", "input")
    try:
        started = time.monotonic()
        assert runner.run_once()
        assert time.monotonic() - started < 2  # Does not await abandoned code.
        before = runner.status(job_id)
        assert len(starts) == 2
        assert len(set(starts)) == 1
        assert prefixes == ["input"]
        attempt = next(row for row in before["attempts"] if row["step_name"] == "slow")
        assert attempt["attempt_count"] == 2
        assert attempt["last_error"] == "TimeoutError: Step slow exceeded 0.05s timeout"
        if recover:
            assert before["status"] == "succeeded"
            assert before["result"] == "retry success"
            assert len(before["steps"]) == 2
        else:
            assert before["status"] == "failed"
            assert before["error"] == attempt["last_error"]
            assert [row["step_name"] for row in before["steps"]] == ["prefix"]
    finally:
        release.set()
    assert finished.wait(2)
    assert runner.status(job_id) == before  # Late returns cannot overwrite success.
    assert not runner.run_once()


def test_cli_cancel_running_job_preserves_finished_steps(runner):
    job_id = runner.submit("example", {"sleep": 0.7})
    worker = subprocess.Popen(
        [CLI, "worker", "--once", "--lease-seconds", "0.2"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    try:
        def second_step_running():
            assert worker.poll() is None, worker.communicate()
            job = runner.status(job_id)
            if len(job["steps"]) == 1 and len(calls_for(runner, job_id)) == 2:
                return job

        before = wait_until(second_step_running)
        cancelled = json.loads(cli("cancel", str(job_id)).stdout)
        assert cancelled["status"] == "cancelled"
        worker.communicate(timeout=5)
        assert worker.returncode == 0
        after = runner.status(job_id)
        assert after["status"] == "cancelled"
        assert after["steps"][0] == before["steps"][0]
        assert [row["step_name"] for row in after["steps"]] == ["draft", "review"]
        assert [row["step_name"] for row in calls_for(runner, job_id)] == ["draft", "review"]
        assert not runner.run_once()
        assert json.loads(cli("cancel", str(job_id)).stdout)["status"] == "cancelled"

        pending = runner.submit("example", {"sleep": 0})
        assert json.loads(cli("cancel", str(pending)).stdout)["status"] == "cancelled"
        assert not runner.run_once()
        assert calls_for(runner, pending) == []
        assert cli("cancel", str(uuid4()), check=False).returncode == 1
    finally:
        if worker.poll() is None:
            worker.kill()
        worker.communicate(timeout=5)


def test_cancel_during_backoff(database_url):
    calls = []

    def fail(context, value):
        calls.append(value)
        raise ValueError("retry later")

    runner = Runner({"backoff": Job((Step("fail", fail, base_delay=10),))})
    job_id = runner.submit("backoff", "input")
    worker = Thread(target=runner.run_once, daemon=True)
    worker.start()
    try:
        wait_until(lambda: any(row["last_error"] for row in runner.status(job_id)["attempts"]))
        assert json.loads(cli("cancel", str(job_id)).stdout)["status"] == "cancelled"
        worker.join(timeout=2)
        assert not worker.is_alive()
        assert runner.status(job_id)["status"] == "cancelled"
        assert calls == ["input"]
        assert not runner.run_once()
    finally:
        runner.cancel(job_id)
        worker.join(timeout=2)


def test_m1_upgrade_reuses_saved_steps(database_url):
    executions = []
    jobs = {"upgrade": Job((
        Step("prefix", lambda c, v: executions.append(v) or v),
        Step("suffix", lambda c, v: v),
    ))}
    runner = Runner(jobs, lease_seconds=0.05)
    job_id = runner.submit("upgrade", "input")
    with runner.connect() as conn:
        claim = runner._claim(conn)
        runner._step(conn, claim, jobs["upgrade"].steps[0], "input")
        # Reproduce M1's schema and a saved prefix without any attempt records.
        conn.execute("DROP TABLE step_attempts, job_cancellations")
        conn.execute("ALTER TABLE jobs DROP CONSTRAINT jobs_status_check")
        conn.execute("""ALTER TABLE jobs ADD CONSTRAINT jobs_status_check
                        CHECK (status IN ('queued', 'running', 'succeeded', 'failed'))""")
    upgraded = Runner(jobs, lease_seconds=0.05)
    before = upgraded.status(job_id)["steps"]
    wait_until(lambda: expired(upgraded, job_id))
    assert upgraded.run_once()
    after = upgraded.status(job_id)
    assert after["status"] == "succeeded"
    assert after["steps"][:1] == before
    assert executions == ["input"]
    assert [row["step_name"] for row in after["attempts"]] == ["suffix"]
    pending = upgraded.submit("upgrade")
    assert upgraded.cancel(pending)
    assert upgraded.status(pending)["status"] == "cancelled"
    assert not upgraded.run_once()


@pytest.mark.skipif(sys.platform == "win32", reason="requires SIGKILL")
def test_sigkill_resume_never_repeats_paid_model_execution(runner):
    job_id = runner.submit("example", {"prompt": "crash test", "sleep": 1.5})
    worker = subprocess.Popen(
        [CLI, "worker", "--lease-seconds", "0.2"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    try:
        def crash_window():
            assert worker.poll() is None, worker.communicate()
            job = runner.status(job_id)
            calls = calls_for(runner, job_id)
            if len(job["steps"]) == 1 and len(calls) == 2 and expired(runner, job_id):
                return job

        before = wait_until(crash_window)
        assert before["status"] == "running"
        assert before["steps"][0]["step_name"] == "draft"
        assert json.loads(cli("status", str(job_id)).stdout)["status"] == "running"
        # The review call is already paid and committed outside the runner's
        # transaction, but its step result has not been saved yet.
        assert calls_for(runner, job_id) == [
            {"step_name": "draft", "calls": 1, "requests": 1},
            {"step_name": "review", "calls": 1, "requests": 1},
        ]
        # Even after lease expiry, SKIP LOCKED skips the live worker's row and
        # allows another queued job to make progress.
        other = runner.submit("example", {"sleep": 0})
        assert runner.run_once()
        assert runner.status(other)["status"] == "succeeded"
        assert len(runner.status(job_id)["steps"]) == 1

        worker.send_signal(signal.SIGKILL)
        worker.communicate(timeout=5)
        assert worker.returncode == -signal.SIGKILL
        assert len(runner.status(job_id)["steps"]) == 1
        assert len(calls_for(runner, job_id)) == 2  # Paid receipts survived SIGKILL.

        cli("worker", "--once", "--lease-seconds", "0.2")
        after = runner.status(job_id)
        assert after["status"] == "succeeded"
        assert len(after["steps"]) == len(after["result"]["answers"]) == 3
        assert after["steps"][0] == before["steps"][0]  # Cached prefix untouched.
        assert calls_for(runner, job_id) == [
            {"step_name": "draft", "calls": 1, "requests": 1},
            {"step_name": "polish", "calls": 1, "requests": 1},
            {"step_name": "review", "calls": 1, "requests": 2},
        ]
    finally:
        if worker.poll() is None:
            worker.kill()
        worker.communicate(timeout=5)
