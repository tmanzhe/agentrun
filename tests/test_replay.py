from decimal import Decimal
from html import escape
from html.parser import HTMLParser
from pathlib import Path
import subprocess
import sys
from uuid import uuid4

import pytest

from agentrun import Job, Runner, Step
from agentrun.replay import PAYLOAD_LIMIT


CLI = str(Path(sys.executable).with_name("agentrun"))


class StaticHTML(HTMLParser):
    def handle_starttag(self, tag, attrs):
        assert tag not in ("script", "iframe", "img", "link", "object", "embed")
        assert not any(name.startswith("on") or name in ("src", "href") for name, _ in attrs)


def test_replay_finished_job(database_url, tmp_path):
    def paid(context, value):
        context.report_usage(input_tokens=7, output_tokens=3, cost_usd="0.12")
        return {"answer": value}

    runner = Runner({"finished": Job((Step("first", paid), Step("second", paid)))})
    job_id = runner.submit("finished", {"hello": "world"}, max_usd="1.50")
    assert runner.run_once()
    before = runner.status(job_id)
    out = tmp_path / "nested" / "export"
    result = subprocess.run(
        [CLI, "replay", str(job_id), "--out", str(out)],
        capture_output=True, text=True, check=True, timeout=10,
    )
    path = out / "index.html"
    assert result.stdout.strip() == str(path)
    assert list(out.iterdir()) == [path]
    html = path.read_text()
    StaticHTML().feed(html)
    assert html.startswith("<!doctype html>")
    assert html.index("<h2>first</h2>") < html.index("<h2>second</h2>")
    assert "<dt>Status</dt><dd>succeeded</dd>" in html
    assert "<dt>Input tokens</dt><dd>14</dd>" in html
    assert "<dt>Output tokens</dt><dd>6</dd>" in html
    assert "<dt>Total cost</dt><dd>$0.24</dd>" in html
    assert "<dt>Spend cap</dt><dd>$1.50</dd>" in html
    for attempt in before["attempts"]:
        assert attempt["started_at"] <= attempt["finished_at"]
        assert escape(attempt["started_at"].isoformat(sep=" ")) in html
        assert "<dt>Attempts</dt><dd>1</dd>" in html
    assert before["started_at"] <= before["finished_at"]
    assert "(ongoing)" not in html
    assert '<details><summary>Input</summary><pre>' in html
    assert '<details><summary>Output</summary><pre>' in html
    assert "&quot;answer&quot;" in html
    assert runner.status(job_id) == before  # Export is read-only and repeatable.
    assert runner.replay(job_id, out).read_text() == html


def test_replay_escapes_database_content_and_truncates_payloads(database_url, tmp_path):
    malicious = '<script>alert("x")</script><img src=x onerror=alert(1)>&\''
    large = malicious + "x" * (PAYLOAD_LIMIT * 2) + "hidden tail"

    def fail(context, value):
        raise ValueError(malicious)

    runner = Runner({malicious: Job((
        Step(malicious, lambda c, v: large), Step("fail", fail, max_attempts=1),
    ))})
    job_id = runner.submit(malicious, {malicious: malicious, "large": large})
    assert runner.run_once()
    html = runner.replay(job_id, tmp_path).read_text()
    StaticHTML().feed(html)
    assert malicious not in html
    assert f"<h2>{escape(malicious)}</h2>" in html
    assert f"<dt>Type</dt><dd>{escape(malicious)}</dd>" in html
    assert f"ValueError: {escape(malicious)}" in html
    assert "&lt;script&gt;" in html and "&lt;img" in html
    assert "&quot;" in html and "&#x27;" in html and "&amp;" in html
    assert "[truncated]" in html
    assert "hidden tail" not in html
    assert len(html) < PAYLOAD_LIMIT * 5


def test_failed_attempt_usage_is_excluded_from_totals_and_cap(database_url):
    calls = []

    def retry(context, value):
        calls.append(context.step_name)
        if len(calls) == 1:
            context.report_usage(input_tokens=1000, output_tokens=2000, cost_usd="100")
            raise ValueError("paid but failed")
        context.report_usage(input_tokens=1, output_tokens=2, cost_usd="0.01")
        return value

    runner = Runner({"failed-spend": Job((
        Step("retry", retry, base_delay=0),
        Step("blocked", lambda c, v: calls.append("blocked")),
    ))})
    job_id = runner.submit("failed-spend", "input", max_usd="0.01")
    assert runner.run_once()
    job = runner.status(job_id)
    # The $100 failure does not stop the retry; the $0.01 success blocks the suffix.
    assert calls == ["retry", "retry"]
    assert job["status"] == "over_budget"
    assert (job["input_tokens"], job["output_tokens"], job["cost_usd"]) == (1, 2, Decimal("0.01"))
    assert job["attempts"][0]["attempt_count"] == 2


def test_replay_m3_upgrade_is_idempotent(database_url, tmp_path):
    runner = Runner({"legacy": Job((Step("saved", lambda c, v: v),))})
    job_id = runner.submit("legacy", None)
    assert runner.run_once()
    with runner.connect() as conn:
        conn.execute("""ALTER TABLE jobs DROP COLUMN step_names, DROP COLUMN started_at,
                        DROP COLUMN finished_at, DROP COLUMN last_claimed_at, DROP COLUMN claim_count""")
        conn.execute("""ALTER TABLE step_attempts DROP COLUMN input, DROP COLUMN started_at,
                        DROP COLUMN finished_at, DROP COLUMN interrupted_count""")
    Runner({})
    upgraded = Runner({})
    job = upgraded.status(job_id)
    assert job["status"] == "succeeded"
    assert len(job["steps"]) == 1
    html = upgraded.replay(job_id, tmp_path).read_text()
    assert "<h2>saved</h2>" in html
    assert "<dt>Started</dt><dd>unknown</dd>" in html
    assert "Input: unrecorded" in html
    assert "<summary>Output</summary><pre>null</pre>" in html


def test_replay_missing_job_creates_no_files(runner, tmp_path):
    out = tmp_path / "absent"
    with pytest.raises(ValueError, match="Job not found"):
        runner.replay(uuid4(), out)
    result = subprocess.run(
        [CLI, "replay", str(uuid4()), "--out", str(out)],
        capture_output=True, text=True, timeout=10,
    )
    assert result.returncode == 1
    assert "Job not found" in result.stderr
    assert not out.exists()
