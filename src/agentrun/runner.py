"""Each step commits independently; a lease and row lock fence stale workers.

Step functions accept (context, input) and return JSON-compatible values. A step's
output becomes the next step's input. Keep definitions stable while jobs exist;
use a new job type name when changing a workflow.
"""

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
import hashlib
import json
import math
import os
from queue import Empty, Queue
from threading import Thread
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
        CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled', 'over_budget')),
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
-- Also upgrade existing M1/M2 databases; old saved results have zero usage.
DO $$ BEGIN
    -- Avoid even no-op ALTER TABLE on startup: it would wait for live workers.
    IF NOT EXISTS (SELECT 1 FROM pg_attribute
                   WHERE attrelid = 'jobs'::regclass AND attname = 'max_usd' AND NOT attisdropped) THEN
        ALTER TABLE jobs ADD COLUMN max_usd numeric
            CHECK (max_usd >= 0 AND max_usd < 'Infinity'::numeric);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_attribute
                   WHERE attrelid = 'steps'::regclass AND attname = 'cost_usd' AND NOT attisdropped) THEN
        ALTER TABLE steps
            ADD COLUMN input_tokens bigint NOT NULL DEFAULT 0 CHECK (input_tokens >= 0),
            ADD COLUMN output_tokens bigint NOT NULL DEFAULT 0 CHECK (output_tokens >= 0),
            ADD COLUMN cost_usd numeric NOT NULL DEFAULT 0
                CHECK (cost_usd >= 0 AND cost_usd < 'Infinity'::numeric);
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conrelid = 'jobs'::regclass
        AND conname = 'jobs_status_check' AND pg_get_constraintdef(oid) LIKE '%over_budget%'
    ) THEN
        ALTER TABLE jobs DROP CONSTRAINT jobs_status_check;
        ALTER TABLE jobs ADD CONSTRAINT jobs_status_check
            CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled', 'over_budget'));
    END IF;
END $$;
CREATE TABLE IF NOT EXISTS step_attempts (
    job_id uuid NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    step_name text NOT NULL,
    input_hash text NOT NULL,
    attempt_count integer NOT NULL DEFAULT 0,
    last_error text,
    next_attempt_at timestamptz,
    PRIMARY KEY (job_id, step_name, input_hash)
);
CREATE TABLE IF NOT EXISTS attempt_usage (
    job_id uuid NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    step_name text NOT NULL,
    input_hash text NOT NULL,
    attempt_number integer NOT NULL,
    input_tokens bigint NOT NULL DEFAULT 0 CHECK (input_tokens >= 0),
    output_tokens bigint NOT NULL DEFAULT 0 CHECK (output_tokens >= 0),
    cost_usd numeric NOT NULL DEFAULT 0
        CHECK (cost_usd >= 0 AND cost_usd < 'Infinity'::numeric),
    PRIMARY KEY (job_id, step_name, input_hash, attempt_number)
);
CREATE TABLE IF NOT EXISTS job_cancellations (
    job_id uuid PRIMARY KEY REFERENCES jobs(id) ON DELETE CASCADE
);
-- Replay metadata is nullable for jobs recorded before M4. As above, avoid
-- taking an ALTER TABLE lock at every worker/CLI startup.
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_attribute
                   WHERE attrelid = 'jobs'::regclass AND attname = 'step_names' AND NOT attisdropped) THEN
        ALTER TABLE jobs
            ADD COLUMN step_names jsonb NOT NULL DEFAULT '[]',
            ADD COLUMN started_at timestamptz,
            ADD COLUMN finished_at timestamptz,
            ADD COLUMN last_claimed_at timestamptz,
            ADD COLUMN claim_count integer NOT NULL DEFAULT 0;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_attribute
                   WHERE attrelid = 'step_attempts'::regclass AND attname = 'started_at' AND NOT attisdropped) THEN
        ALTER TABLE step_attempts
            ADD COLUMN input jsonb,
            ADD COLUMN started_at timestamptz,
            ADD COLUMN finished_at timestamptz,
            ADD COLUMN interrupted_count integer NOT NULL DEFAULT 0;
    END IF;
END $$;
"""


def input_hash(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(encoded.encode()).hexdigest()


def dollars(value: Decimal | float | str) -> Decimal:
    try:
        amount = Decimal(str(value))
    except InvalidOperation:
        raise ValueError("USD amounts must be nonnegative and finite") from None
    if not amount.is_finite() or amount < 0:
        raise ValueError("USD amounts must be nonnegative and finite")
    return amount


@dataclass
class StepContext:
    job_id: UUID
    step_name: str
    input_hash: str
    database_url: str
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: Decimal = Decimal("0")

    _record_usage: Callable | None = field(default=None, repr=False, compare=False)

    def report_usage(
        self, *, input_tokens: int = 0, output_tokens: int = 0,
        cost_usd: Decimal | float | str = 0,
    ) -> None:
        """Add usage for a model response; multiple calls within a step accumulate."""
        for count in (input_tokens, output_tokens):
            if isinstance(count, bool) or not isinstance(count, int) or count < 0:
                raise ValueError("Token counts must be nonnegative integers")
        amount = dollars(cost_usd)
        if self._record_usage is not None:
            self._record_usage(input_tokens, output_tokens, amount)
        self.input_tokens += input_tokens
        self.output_tokens += output_tokens
        self.cost_usd += amount

    @property
    def idempotency_key(self) -> str:
        """Pass this to external APIs to deduplicate a lost response after a crash.

        The provider must durably honor the key for the entire resume period.
        PostgreSQL alone cannot make an external side effect exactly once.
        """
        return input_hash([str(self.job_id), self.step_name, self.input_hash])


@dataclass(frozen=True)
class Step:
    """Retry at base_delay * 2**(attempt - 1); timeout=None disables the limit."""

    name: str
    run: Callable[[StepContext, Any], Any]
    max_attempts: int = 3
    base_delay: float = 0.2
    timeout: float | None = None

    def __post_init__(self) -> None:
        if (
            isinstance(self.max_attempts, bool)
            or not isinstance(self.max_attempts, int)
            or self.max_attempts < 1
        ):
            raise ValueError("max_attempts must be a positive integer")
        if not math.isfinite(self.base_delay) or self.base_delay < 0:
            raise ValueError("base_delay must be nonnegative and finite")
        if self.timeout is not None and (not math.isfinite(self.timeout) or self.timeout <= 0):
            raise ValueError("timeout must be positive and finite")


@dataclass(frozen=True)
class Job:
    steps: tuple[Step, ...]

    def __post_init__(self) -> None:
        names = [step.name for step in self.steps]
        if not names or any(not name for name in names) or len(set(names)) != len(names):
            raise ValueError("A job needs nonempty, unique step names")


class LostClaim(RuntimeError):
    pass


class Cancelled(RuntimeError):
    pass


class AttemptsExhausted(RuntimeError):
    pass


class OverBudget(RuntimeError):
    pass


class JobReader:
    """Read recorded state without creating tables or loading job definitions."""

    def __init__(self, database_url: str | None = None):
        self.database_url = database_url or os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL)

    def connect(self):
        return psycopg.connect(self.database_url, autocommit=True, row_factory=dict_row)

    def list_jobs(self, limit: int = 20, offset: int = 0) -> list[dict]:
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 100:
            raise ValueError("limit must be an integer from 1 to 100")
        if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
            raise ValueError("offset must be a nonnegative integer")
        with self.connect() as conn, conn.transaction():
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            return conn.execute(
                """SELECT id, job_type,
                          CASE WHEN status IN ('queued', 'running') AND EXISTS (
                              SELECT 1 FROM job_cancellations WHERE job_id = jobs.id
                          ) THEN 'cancelled' ELSE status END AS status,
                          created_at, started_at, finished_at, max_usd
                   FROM jobs ORDER BY created_at DESC, id DESC LIMIT %s OFFSET %s""",
                (limit, offset),
            ).fetchall()

    def status(self, job_id: UUID) -> dict | None:
        with self.connect() as conn, conn.transaction():
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            job = conn.execute("SELECT * FROM jobs WHERE id = %s", (job_id,)).fetchone()
            if job is not None:
                job["steps"] = conn.execute(
                    """SELECT step_name, input_hash, result, completed_at,
                              input_tokens, output_tokens, cost_usd FROM steps
                       WHERE job_id = %s ORDER BY completed_at, step_name""",
                    (job_id,),
                ).fetchall()
                job.update(self._usage_totals(conn, job_id))
                job["attempt_usage"] = conn.execute(
                    """SELECT step_name, input_hash, attempt_number,
                              input_tokens, output_tokens, cost_usd
                       FROM attempt_usage WHERE job_id = %s
                       ORDER BY step_name, input_hash, attempt_number""", (job_id,),
                ).fetchall()
                job["attempts"] = conn.execute(
                    """SELECT step_name, input_hash, attempt_count, last_error, next_attempt_at,
                              input, started_at, finished_at, interrupted_count
                       FROM step_attempts WHERE job_id = %s ORDER BY step_name, input_hash""",
                    (job_id,),
                ).fetchall()
                if job["status"] in ("queued", "running") and conn.execute(
                    "SELECT 1 FROM job_cancellations WHERE job_id = %s", (job_id,)
                ).fetchone():
                    job["status"] = "cancelled"
            return job

    @staticmethod
    def _usage_totals(conn, job_id: UUID) -> dict:
        return conn.execute(
            """SELECT COALESCE(SUM(input_tokens), 0)::bigint AS input_tokens,
                      COALESCE(SUM(output_tokens), 0)::bigint AS output_tokens,
                      COALESCE(SUM(cost_usd), 0) AS cost_usd
               FROM (
                   SELECT input_tokens, output_tokens, cost_usd FROM attempt_usage WHERE job_id = %s
                   UNION ALL
                   SELECT input_tokens, output_tokens, cost_usd FROM steps s WHERE job_id = %s
                     AND NOT EXISTS (SELECT 1 FROM attempt_usage a
                         WHERE a.job_id = s.job_id AND a.step_name = s.step_name
                           AND a.input_hash = s.input_hash)
               ) usage""", (job_id, job_id),
        ).fetchone()


class Runner(JobReader):
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
        super().__init__(database_url)
        self.lease_seconds = lease_seconds
        with self.connect() as conn, conn.transaction():
            # Serialize concurrent startup DDL (CREATE IF NOT EXISTS can race).
            conn.execute("SELECT pg_advisory_xact_lock(684921038)")
            conn.execute(SCHEMA)

    def submit(
        self, job_type: str, value: Any = None, *, max_usd: Decimal | float | str | None = None,
    ) -> UUID:
        if job_type not in self.jobs:
            raise ValueError(f"Unknown job type: {job_type}")
        input_hash(value)  # Reject non-JSON input before writing a job.
        max_usd = None if max_usd is None else dollars(max_usd)
        job_id = uuid4()
        with self.connect() as conn:
            conn.execute(
                """INSERT INTO jobs (id, job_type, input, max_usd, step_names)
                   VALUES (%s, %s, %s, %s, %s)""",
                (job_id, job_type, Jsonb(value), max_usd,
                 Jsonb([step.name for step in self.jobs[job_type].steps])),
            )
        return job_id

    def replay(self, job_id: UUID, out_dir):
        """Export a consistent database snapshot as a standalone HTML replay."""
        from .replay import write_replay

        job = self.status(job_id)
        if job is None:
            raise ValueError("Job not found")
        return write_replay(job, out_dir)

    def cancel(self, job_id: UUID) -> bool:
        with self.connect() as conn, conn.transaction():
            if conn.execute("SELECT 1 FROM jobs WHERE id = %s", (job_id,)).fetchone() is None:
                return False
            # The worker holds NO KEY UPDATE, so this FK's KEY SHARE lock
            # permits cancellation while a step is running.
            conn.execute(
                """INSERT INTO job_cancellations (job_id)
                   SELECT id FROM jobs WHERE id = %s AND status IN ('queued', 'running')
                   ON CONFLICT DO NOTHING""",
                (job_id,),
            )
            conn.execute(
                """UPDATE jobs SET status = 'cancelled', claim_token = NULL, lease_until = NULL,
                       finished_at = clock_timestamp()
                   WHERE id IN (
                       SELECT id FROM jobs WHERE id = %s AND status IN ('queued', 'running')
                       FOR NO KEY UPDATE SKIP LOCKED
                   )""",
                (job_id,),
            )
            return True

    def _claim(self, conn) -> dict | None:
        with conn.transaction():
            job = conn.execute(
                """SELECT * FROM jobs
                   WHERE (status = 'queued'
                      OR (status = 'running' AND lease_until < clock_timestamp()))
                     AND NOT EXISTS (SELECT 1 FROM job_cancellations WHERE job_id = jobs.id)
                   ORDER BY created_at, id LIMIT 1 FOR NO KEY UPDATE SKIP LOCKED"""
            ).fetchone()
            if job is None:
                return None
            job["claim_token"] = uuid4()
            conn.execute(
                """UPDATE jobs SET status = 'running', claim_token = %s,
                       started_at = COALESCE(started_at, clock_timestamp()),
                       last_claimed_at = clock_timestamp(), claim_count = claim_count + 1,
                       lease_until = clock_timestamp() + %s * interval '1 second'
                   WHERE id = %s""",
                (job["claim_token"], self.lease_seconds, job["id"]),
            )
            return job

    def _lock_claim(self, conn, job: dict, *, check_cancel: bool = True) -> None:
        owner = conn.execute(
            "SELECT claim_token FROM jobs WHERE id = %s FOR NO KEY UPDATE", (job["id"],)
        ).fetchone()
        if owner is None or owner["claim_token"] != job["claim_token"]:
            raise LostClaim(f"Job {job['id']} was reclaimed")
        if check_cancel and self._is_cancelled(conn, job):
            raise Cancelled()

    def _is_cancelled(self, conn, job: dict) -> bool:
        return conn.execute(
            "SELECT 1 FROM job_cancellations WHERE job_id = %s", (job["id"],)
        ).fetchone() is not None

    def _renew(self, conn, job: dict) -> None:
        conn.execute(
            """UPDATE jobs SET lease_until = clock_timestamp() + %s * interval '1 second'
               WHERE id = %s""",
            (self.lease_seconds, job["id"]),
        )

    @staticmethod
    def _invoke(step: Step, context: StepContext, value: Any) -> Any:
        if step.timeout is None:
            return step.run(context, value)
        # A daemon thread bounds our wait, not the function's lifetime. Python
        # cannot safely kill a thread: timed-out code may continue doing I/O.
        # It cannot write the cache, and late results are abandoned. External
        # effects still need context.idempotency_key across concurrent retries.
        outcome = Queue(maxsize=1)
        # Abandoned code must not mutate the input of a subsequent attempt.
        value = json.loads(json.dumps(value, allow_nan=False))

        def run():
            try:
                outcome.put((True, step.run(context, value)))
            except BaseException as exc:
                outcome.put((False, exc))

        Thread(target=run, daemon=True).start()
        try:
            succeeded, result = outcome.get(timeout=step.timeout)
        except Empty:
            raise TimeoutError(f"Step {step.name} exceeded {step.timeout:g}s timeout") from None
        if not succeeded:
            raise result
        return result

    def _step(self, conn, job: dict, step: Step, value: Any) -> Any:
        digest = input_hash(value)
        key = (job["id"], step.name, digest)
        while True:
            with conn.transaction():
                self._lock_claim(conn, job)
                saved = conn.execute(
                    """SELECT result FROM steps
                       WHERE job_id = %s AND step_name = %s AND input_hash = %s""", key,
                ).fetchone()
                if saved is not None:
                    self._renew(conn, job)
                    return saved["result"]
                # Every attempt's reported usage counts, including failures,
                # timeouts and interruptions. Check before each uncached attempt.
                if job["max_usd"] is not None:
                    if self._usage_totals(conn, job["id"])["cost_usd"] >= job["max_usd"]:
                        raise OverBudget("Recorded spend reached the job's max_usd")
                attempt = conn.execute(
                    """SELECT *, EXTRACT(EPOCH FROM next_attempt_at - clock_timestamp()) AS wait
                       FROM step_attempts
                       WHERE job_id = %s AND step_name = %s AND input_hash = %s""", key,
                ).fetchone()
                if attempt is not None:
                    if attempt["attempt_count"] >= step.max_attempts:
                        raise AttemptsExhausted(attempt["last_error"] or "Attempt interrupted by worker exit")
                    # Keep the job locked during backoff to prevent reclaiming it,
                    # while polling the independently committed cancel request.
                    deadline = time.monotonic() + max(0, float(attempt["wait"] or 0))
                    while (remaining := deadline - time.monotonic()) > 0:
                        time.sleep(min(0.05, remaining))
                        self._lock_claim(conn, job)
                # Timed-out code can report more usage during backoff.
                if job["max_usd"] is not None:
                    if self._usage_totals(conn, job["id"])["cost_usd"] >= job["max_usd"]:
                        raise OverBudget("Recorded spend reached the job's max_usd")
                # Commit before user code: even an interrupted attempt counts.
                # Saved successes are checked first and never consume attempts.
                attempt = conn.execute(
                    """INSERT INTO step_attempts (job_id, step_name, input_hash, attempt_count,
                                                 input, started_at)
                       VALUES (%s, %s, %s, 1, %s, clock_timestamp())
                       ON CONFLICT (job_id, step_name, input_hash) DO UPDATE
                           SET attempt_count = step_attempts.attempt_count + 1, next_attempt_at = NULL,
                               input = EXCLUDED.input,
                               started_at = COALESCE(step_attempts.started_at, EXCLUDED.started_at),
                               interrupted_count = step_attempts.interrupted_count +
                                   CASE WHEN step_attempts.finished_at IS NULL THEN 1 ELSE 0 END,
                               finished_at = NULL
                       RETURNING attempt_count""", (*key, Jsonb(value)),
                ).fetchone()
                conn.execute(
                    """INSERT INTO attempt_usage (job_id, step_name, input_hash, attempt_number)
                       VALUES (%s, %s, %s, %s)""", (*key, attempt["attempt_count"]),
                )
                self._renew(conn, job)
            with conn.transaction():
                # Fence stale workers even when execution outlives the lease.
                self._lock_claim(conn, job)
                # Capture the key because a timed-out thread may report later.
                usage_key = (*key, attempt["attempt_count"])

                def record_usage(inputs, outputs, cost, usage_key=usage_key):
                    # Persist before returning from report_usage, independently
                    # of the result transaction. Never replay usage at finish.
                    with self.connect() as usage_conn:
                        usage_conn.execute(
                            """UPDATE attempt_usage SET input_tokens = input_tokens + %s,
                                   output_tokens = output_tokens + %s, cost_usd = cost_usd + %s
                               WHERE job_id = %s AND step_name = %s AND input_hash = %s
                                 AND attempt_number = %s""",
                            (inputs, outputs, cost, *usage_key),
                        )

                context = StepContext(
                    job["id"], step.name, digest, self.database_url, _record_usage=record_usage,
                )
                try:
                    result = self._invoke(step, context, value)
                    input_hash(result)  # Reject non-JSON results, including NaN.
                except Exception as exc:
                    error = f"{type(exc).__name__}: {exc}"
                    # Saturate at one day to keep large retry policies finite.
                    delay = min(
                        step.base_delay * 2.0 ** min(attempt["attempt_count"] - 1, 1023),
                        86400.0,
                    )
                    conn.execute(
                        """UPDATE step_attempts SET last_error = %s, finished_at = clock_timestamp(),
                               next_attempt_at = clock_timestamp() + %s * interval '1 second'
                           WHERE job_id = %s AND step_name = %s AND input_hash = %s""",
                        (error, delay, *key),
                    )
                else:
                    # Attempt usage is already durable; saving or reusing a
                    # result never adds that usage again.
                    conn.execute(
                        """INSERT INTO steps (job_id, step_name, input_hash, result,
                                              input_tokens, output_tokens, cost_usd)
                           VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                        (*key, Jsonb(result), context.input_tokens, context.output_tokens, context.cost_usd),
                    )
                    conn.execute(
                        """UPDATE step_attempts SET finished_at = clock_timestamp()
                           WHERE job_id = %s AND step_name = %s AND input_hash = %s""", key,
                    )
                    self._renew(conn, job)
                    # Normalize fresh outputs just as PostgreSQL does on resume.
                    return json.loads(json.dumps(result, allow_nan=False))
                self._renew(conn, job)

    def _finish(
        self, conn, job: dict, result: Any, error: str | None = None,
        *, status: str | None = None,
    ) -> None:
        with conn.transaction():
            self._lock_claim(conn, job, check_cancel=False)
            status = status or ("failed" if error is not None else "succeeded")
            if self._is_cancelled(conn, job):
                status, result, error = "cancelled", None, None
            conn.execute(
                """UPDATE jobs SET status = %s, result = %s, error = %s,
                       finished_at = clock_timestamp(),
                       claim_token = NULL, lease_until = NULL WHERE id = %s""",
                (status, Jsonb(result), error, job["id"]),
            )

    def run_once(self) -> bool:
        """Run one claimable job, retrying failed steps within their policy."""
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
                    error = str(exc) if isinstance(exc, AttemptsExhausted) else f"{type(exc).__name__}: {exc}"
                    self._finish(
                        conn, job, None, error,
                        status="over_budget" if isinstance(exc, OverBudget) else None,
                    )
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
