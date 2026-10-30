"""Each step commits independently; a lease and row lock fence stale workers.

Step functions accept (context, input) and return JSON-compatible values. A step's
output becomes the next step's input. Keep definitions stable while jobs exist;
use a new job type name when changing a workflow.
"""

from collections.abc import Callable, Mapping
from dataclasses import dataclass
import hashlib
import json
import math
import os
import time
from typing import Any
from uuid import UUID, uuid4

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

DEFAULT_DATABASE_URL = "postgresql://postgres@localhost:5432/agentrun"

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id uuid PRIMARY KEY,
    job_type text NOT NULL,
    input jsonb NOT NULL,
    status text NOT NULL DEFAULT 'queued'
        CHECK (status IN ('queued', 'running', 'succeeded', 'failed')),
    claim_token uuid,
    lease_until timestamptz,
    result jsonb,
    error text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS jobs_pending ON jobs (created_at)
    WHERE status IN ('queued', 'running');
CREATE TABLE IF NOT EXISTS steps (
    job_id uuid NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    step_name text NOT NULL,
    input_hash text NOT NULL,
    result jsonb NOT NULL,
    completed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (job_id, step_name, input_hash)
);
"""


def input_hash(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(encoded.encode()).hexdigest()


@dataclass(frozen=True)
class StepContext:
    job_id: UUID
    step_name: str
    input_hash: str
    database_url: str

    @property
    def idempotency_key(self) -> str:
        """Pass this to external APIs to deduplicate a lost response after a crash.

        The provider must durably honor the key for the entire resume period.
        PostgreSQL alone cannot make an external side effect exactly once.
        """
        return input_hash([str(self.job_id), self.step_name, self.input_hash])


@dataclass(frozen=True)
class Step:
    name: str
    run: Callable[[StepContext, Any], Any]


@dataclass(frozen=True)
class Job:
    steps: tuple[Step, ...]

    def __post_init__(self) -> None:
        names = [step.name for step in self.steps]
        if not names or any(not name for name in names) or len(set(names)) != len(names):
            raise ValueError("A job needs nonempty, unique step names")


class LostClaim(RuntimeError):
    pass


class Runner:
    def __init__(
        self,
        jobs: Mapping[str, Job],
        database_url: str | None = None,
        *,
        lease_seconds: float = 2.0,
    ):
        if not math.isfinite(lease_seconds) or lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive and finite")
        self.jobs = dict(jobs)
        self.database_url = database_url or os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL)
        self.lease_seconds = lease_seconds
        with self.connect() as conn, conn.transaction():
            # Serialize concurrent startup DDL (CREATE IF NOT EXISTS can race).
            conn.execute("SELECT pg_advisory_xact_lock(684921038)")
            conn.execute(SCHEMA)

    def connect(self):
        return psycopg.connect(self.database_url, autocommit=True, row_factory=dict_row)

    def submit(self, job_type: str, value: Any = None) -> UUID:
        if job_type not in self.jobs:
            raise ValueError(f"Unknown job type: {job_type}")
        input_hash(value)  # Reject non-JSON input before writing a job.
        job_id = uuid4()
        with self.connect() as conn:
            conn.execute(
                "INSERT INTO jobs (id, job_type, input) VALUES (%s, %s, %s)",
                (job_id, job_type, Jsonb(value)),
            )
        return job_id

    def status(self, job_id: UUID) -> dict | None:
        with self.connect() as conn, conn.transaction():
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            job = conn.execute("SELECT * FROM jobs WHERE id = %s", (job_id,)).fetchone()
            if job is not None:
                job["steps"] = conn.execute(
                    """SELECT step_name, input_hash, result, completed_at FROM steps
                       WHERE job_id = %s ORDER BY completed_at, step_name""",
                    (job_id,),
                ).fetchall()
            return job

    def _claim(self, conn) -> dict | None:
        with conn.transaction():
            job = conn.execute(
                """SELECT * FROM jobs
                   WHERE status = 'queued'
                      OR (status = 'running' AND lease_until < clock_timestamp())
                   ORDER BY created_at, id LIMIT 1 FOR UPDATE SKIP LOCKED"""
            ).fetchone()
            if job is None:
                return None
            job["claim_token"] = uuid4()
            conn.execute(
                """UPDATE jobs SET status = 'running', claim_token = %s,
                       lease_until = clock_timestamp() + %s * interval '1 second'
                   WHERE id = %s""",
                (job["claim_token"], self.lease_seconds, job["id"]),
            )
            return job

    def _lock_claim(self, conn, job: dict) -> None:
        owner = conn.execute(
            "SELECT claim_token FROM jobs WHERE id = %s FOR UPDATE", (job["id"],)
        ).fetchone()
        if owner is None or owner["claim_token"] != job["claim_token"]:
            raise LostClaim(f"Job {job['id']} was reclaimed")

    def _step(self, conn, job: dict, step: Step, value: Any) -> Any:
        digest = input_hash(value)
        with conn.transaction():
            # A long step can outlive its lease. This lock prevents reclaiming it
            # until its transaction ends, without a heartbeat thread.
            self._lock_claim(conn, job)
            saved = conn.execute(
                """SELECT result FROM steps
                   WHERE job_id = %s AND step_name = %s AND input_hash = %s""",
                (job["id"], step.name, digest),
            ).fetchone()
            if saved is not None:
                result = saved["result"]
            else:
                context = StepContext(job["id"], step.name, digest, self.database_url)
                result = step.run(context, value)
                input_hash(result)  # Also reject NaN, which PostgreSQL JSON disallows.
                conn.execute(
                    "INSERT INTO steps (job_id, step_name, input_hash, result) VALUES (%s, %s, %s, %s)",
                    (job["id"], step.name, digest, Jsonb(result)),
                )
            conn.execute(
                """UPDATE jobs SET lease_until = clock_timestamp() + %s * interval '1 second'
                   WHERE id = %s""",
                (self.lease_seconds, job["id"]),
            )
            # Normalize even fresh outputs through JSON, so resume uses the same
            # types (e.g. a returned tuple becomes a list in both executions).
            return json.loads(json.dumps(result, allow_nan=False))

    def _finish(self, conn, job: dict, result: Any, error: str | None = None) -> None:
        with conn.transaction():
            self._lock_claim(conn, job)
            conn.execute(
                """UPDATE jobs SET status = %s, result = %s, error = %s,
                       claim_token = NULL, lease_until = NULL WHERE id = %s""",
                ("failed" if error is not None else "succeeded", Jsonb(result), error, job["id"]),
            )

    def run_once(self) -> bool:
        """Run one claimable job. Failed jobs are recorded, without retrying."""
        with self.connect() as conn:
            job = self._claim(conn)
            if job is None:
                return False
            try:
                value = job["input"]
                for step in self.jobs[job["job_type"]].steps:
                    value = self._step(conn, job, step, value)
                self._finish(conn, job, value)
            except LostClaim:
                pass  # The current owner alone may write results or final status.
            except psycopg.OperationalError:
                raise  # A broken DB connection is recovered through lease expiry.
            except Exception as exc:
                try:
                    self._finish(conn, job, None, f"{type(exc).__name__}: {exc}")
                except LostClaim:
                    pass
            return True

    def worker(self, *, once: bool = False) -> None:
        while True:
            worked = self.run_once()
            if once:
                return
            if not worked:
                time.sleep(0.2)
