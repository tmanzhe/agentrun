"""An idempotent mock provider whose receipts survive worker transactions.

Its own committed ledger stands in for an external model service. `calls` counts
paid executions, while `requests` also counts deduplicated deliveries. The sleep
AFTER committing the receipt simulates losing a response to an already paid call.
"""

import math
import time

from .mock import MOCK_SCHEMA, save_response
from .runner import Job, Step, StepContext


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
    receipt = save_response(context, result)
    if receipt["requests"] == 1:
        time.sleep(delay)
    # A deduplicated provider response still reports the original call's usage.
    context.report_usage(input_tokens=10, output_tokens=5, cost_usd="0.10")
    return receipt["result"]


JOBS = {"example": Job(tuple(Step(name, mock_model) for name in ("draft", "review", "polish")))}
