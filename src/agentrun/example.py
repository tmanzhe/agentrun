"""An idempotent mock provider whose receipts survive worker transactions.

Its own committed ledger stands in for an external model service. `calls` counts
paid executions, while `requests` also counts deduplicated deliveries. The sleep
AFTER committing the receipt simulates losing a response to an already paid call.
"""

import math
import time

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from .runner import Job, Step, StepContext

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


def mock_model(context: StepContext, value: dict | None) -> dict:
    value = value or {}
    delay = float(value.get("sleep", 1.0))
    if not math.isfinite(delay) or delay < 0:
        raise ValueError("sleep must be nonnegative and finite")
    result = {
        "prompt": value.get("prompt", "hello"),
        "sleep": delay,
        "answers": [*value.get("answers", []), f"mock answer from {context.step_name}"],
    }
    # No FK to jobs: this independent provider transaction must not wait on the
    # runner's job lock. In production the receipt lives at the model provider.
    with psycopg.connect(context.database_url, row_factory=dict_row) as conn:
        receipt = conn.execute(
            """INSERT INTO mock_calls (idempotency_key, job_id, step_name, result)
               VALUES (%s, %s, %s, %s)
               ON CONFLICT (idempotency_key) DO UPDATE
                   SET requests = mock_calls.requests + 1
               RETURNING result, requests""",
            (context.idempotency_key, context.job_id, context.step_name, Jsonb(result)),
        ).fetchone()
    if receipt["requests"] == 1:
        time.sleep(delay)
    # A deduplicated provider response still reports the original call's usage.
    context.report_usage(input_tokens=10, output_tokens=5, cost_usd="0.10")
    return receipt["result"]


JOBS = {"example": Job(tuple(Step(name, mock_model) for name in ("draft", "review", "polish")))}
