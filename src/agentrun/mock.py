"""A local mock provider with receipts committed outside worker transactions."""

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from .runner import StepContext


MOCK_SCHEMA = """
CREATE TABLE IF NOT EXISTS mock_calls (
    idempotency_key text PRIMARY KEY,
    job_id uuid NOT NULL,
    step_name text NOT NULL,
    calls integer NOT NULL DEFAULT 1,
    requests integer NOT NULL DEFAULT 1,
    result jsonb NOT NULL
);
"""


def save_response(context: StepContext, result: dict) -> dict:
    # No FK to jobs: the provider transaction must not wait on the job lock.
    with psycopg.connect(context.database_url, row_factory=dict_row) as conn:
        return conn.execute(
            """INSERT INTO mock_calls (idempotency_key, job_id, step_name, result)
               VALUES (%s, %s, %s, %s)
               ON CONFLICT (idempotency_key) DO UPDATE
                   SET requests = mock_calls.requests + 1
               RETURNING result, requests""",
            (context.idempotency_key, context.job_id, context.step_name, Jsonb(result)),
        ).fetchone()
