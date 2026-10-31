from decimal import Decimal
import json
from pathlib import Path
import signal
import subprocess
import sys
import time
from uuid import UUID

import pytest


CLI = str(Path(sys.executable).with_name("agentrun"))
SAMPLE = Path(__file__).resolve().parents[1] / "examples" / "hn_hiring.json"


def cli(*args):
    return subprocess.run([CLI, *args], capture_output=True, text=True, check=True, timeout=10)


def receipts(runner, job_id):
    with runner.connect() as conn:
        return conn.execute(
            "SELECT step_name, calls, requests FROM mock_calls WHERE job_id = %s",
            (job_id,),
        ).fetchall()


def wait_until(check):
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        result = check()
        if result:
            return result
        time.sleep(0.02)
    pytest.fail("Timed out waiting for hiring worker")


def test_cli_hiring_sample_extracts_listings_and_replays(runner, tmp_path):
    job_id = UUID(cli("submit", "hn_hiring", SAMPLE.read_text()).stdout.strip())
    cli("worker", "--once")
    job = json.loads(cli("status", str(job_id)).stdout)
    assert job["status"] == "succeeded"
    assert [step["step_name"] for step in job["steps"]] == ["prepare_comments", "extract_hiring"]
    assert job["result"] == {
        "thread_id": 100,
        "listings": [
            {
                "comment_id": 101,
                "source_url": "https://news.ycombinator.com/item?id=101",
                "company": "Maple Labs",
                "role": "Backend Engineer",
                "location": "Toronto, Canada",
                "remote": True,
                "apply_url": "https://example.com/jobs/backend",
                "text": "Maple Labs | Backend Engineer | Toronto, Canada | REMOTE\n"
                        "Build Python tools. Full-time. Apply at our jobs page.",
            },
            {
                "comment_id": 102,
                "source_url": "https://news.ycombinator.com/item?id=102",
                "company": "Cedar & Co",
                "role": "Product Designer",
                "location": "Vancouver, Canada",
                "remote": False,
                "apply_url": "mailto:jobs@example.com",
                "text": "Cedar & Co | Product Designer | Vancouver, Canada | ONSITE\n"
                        "Email jobs@example.com.",
            },
        ],
    }
    assert job["cost_usd"] == "0.10"
    assert job["input_tokens"] > 0 and job["output_tokens"] > 0
    assert job["steps"][0]["cost_usd"] == "0"
    assert receipts(runner, job_id) == [{"step_name": "extract_hiring", "calls": 1, "requests": 1}]
    cli("replay", str(job_id), "--out", str(tmp_path))
    html = (tmp_path / "index.html").read_text()
    assert "Maple Labs" in html and "Cedar &amp; Co" in html
    assert "<h2>prepare_comments</h2>" in html and "<h2>extract_hiring</h2>" in html
    before = runner.status(job_id)
    assert not runner.run_once()
    assert runner.status(job_id) == before


def test_hiring_filters_comments_deduplicates_and_keeps_source_text(runner):
    posting = {
        "id": 201, "parent": 200,
        "text": "<p>Érable &amp; Co | Engineer | Montréal</p><p>Salary $100k<br>Full-time</p>",
    }
    job_id = runner.submit("hn_hiring", {"thread_id": 200, "comments": [
        posting, posting,
        {"id": 202, "deleted": True},
        {"id": 203, "dead": True},
        {"id": 204, "text": "<p>   </p>"},
        {"id": 205, "parent": 201, "text": "Reply | Engineer | Remote"},
        {"id": 206, "text": "Thanks for posting!"},
    ]})
    assert runner.run_once()
    job = runner.status(job_id)
    assert job["status"] == "succeeded"
    assert len(job["result"]["listings"]) == 1
    listing = job["result"]["listings"][0]
    assert listing["company"] == "Érable & Co"
    assert listing["location"] == "Montréal"
    assert listing["remote"] is None and listing["apply_url"] is None
    assert listing["text"].endswith("Salary $100k\nFull-time")
    assert listing["comment_id"] == 201
    assert [c["id"] for c in job["steps"][0]["result"]["comments"]] == [201, 206]
    assert job["cost_usd"] == Decimal("0.10")


@pytest.mark.parametrize("header,remote,location", [
    ("Acme | Engineer | REMOTE", True, None),
    ("Acme | Engineer | London | remote (UK)", True, "London"),
    ("Acme | Engineer | London | NO REMOTE", False, "London"),
    ("Acme | Engineer | London | not remote", False, "London"),
    ("Acme | Engineer | London | on-site", False, "London"),
    ("Acme | Engineer | London | Remote or onsite", True, "London"),
    ("Acme | Engineer | London", None, "London"),
])
def test_mock_hiring_header_fields(runner, header, remote, location):
    job_id = runner.submit("hn_hiring", {"thread_id": 1, "comments": [{"id": 2, "text": header}]})
    assert runner.run_once()
    job = runner.status(job_id)
    assert job["status"] == "succeeded"
    listing = job["result"]["listings"][0]
    assert listing["remote"] is remote
    assert listing["location"] == location


def test_hiring_ignores_unusable_links_and_decodes_application_url(runner):
    job_id = runner.submit("hn_hiring", {"thread_id": 1, "comments": [{
        "id": 2,
        "text": 'Acme | Engineer | Remote<p><a href="javascript:alert(1)">ignore</a>'
                '<a href="/relative">relative</a><a href="https://[invalid">broken</a>'
                '<a href="https://example.com/apply?role=eng&amp;from=hn">Apply</a>',
    }]})
    assert runner.run_once()
    job = runner.status(job_id)
    assert job["status"] == "succeeded"
    assert job["result"]["listings"][0]["apply_url"] == "https://example.com/apply?role=eng&from=hn"


@pytest.mark.parametrize("comments", [[], [{"id": 2, "deleted": True}], [{"id": 2, "text": " "}]])
def test_hiring_empty_input_needs_no_model_call(runner, comments):
    job_id = runner.submit("hn_hiring", {"thread_id": 1, "comments": comments})
    assert runner.run_once()
    job = runner.status(job_id)
    assert job["status"] == "succeeded"
    assert job["result"] == {"thread_id": 1, "listings": []}
    assert (job["input_tokens"], job["output_tokens"], job["cost_usd"]) == (0, 0, 0)
    assert receipts(runner, job_id) == []


@pytest.mark.parametrize("value", [
    None, {}, {"thread_id": True, "comments": []}, {"thread_id": 0, "comments": []},
    {"thread_id": 1, "comments": {}}, {"thread_id": 1, "comments": [None]},
    {"thread_id": 1, "comments": [{"id": True, "text": "header"}]},
    {"thread_id": 1, "comments": [{"id": 2, "text": None}]},
    {"thread_id": 1, "comments": [
        {"id": 2, "text": "Acme | Engineer | Remote"},
        {"id": 2, "text": "Other | Engineer | Remote"},
    ]},
])
def test_hiring_bad_input_fails_before_model_call(runner, value):
    job_id = runner.submit("hn_hiring", value)
    assert runner.run_once()
    job = runner.status(job_id)
    assert job["status"] == "failed" and job["error"].startswith("ValueError:")
    assert job["steps"] == []
    assert job["attempts"][0]["attempt_count"] == 1
    assert job["cost_usd"] == 0
    assert receipts(runner, job_id) == []


def test_hiring_zero_budget_blocks_model(runner):
    job_id = runner.submit("hn_hiring", json.loads(SAMPLE.read_text()), max_usd="0")
    assert runner.run_once()
    job = runner.status(job_id)
    assert job["status"] == "over_budget"
    assert job["cost_usd"] == 0
    assert receipts(runner, job_id) == []


@pytest.mark.skipif(sys.platform == "win32", reason="requires SIGKILL")
def test_hiring_sigkill_resume_reuses_mock_receipt_and_prepared_comments(runner, tmp_path):
    job_id = runner.submit("hn_hiring", json.loads(SAMPLE.read_text()), max_usd="0.20")
    # Simulate a provider response lost after committing its receipt.
    script = '''
import time
from agentrun import Runner
from agentrun.jobs import JOBS
from agentrun import hiring

save_response = hiring.save_response
def lost_response(context, result):
    receipt = save_response(context, result)
    time.sleep(60)
    return receipt
hiring.save_response = lost_response
Runner(JOBS, lease_seconds=0.05).run_once()
'''
    worker = subprocess.Popen(
        [sys.executable, "-c", script], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    try:
        def committed():
            assert worker.poll() is None, worker.communicate()
            if receipts(runner, job_id):
                return runner.status(job_id)

        before = wait_until(committed)
        assert [step["step_name"] for step in before["steps"]] == ["prepare_comments"]
        worker.send_signal(signal.SIGKILL)
        worker.communicate(timeout=5)
        assert worker.returncode == -signal.SIGKILL

        def expired():
            with runner.connect() as conn:
                return conn.execute(
                    "SELECT lease_until < clock_timestamp() AS expired FROM jobs WHERE id = %s", (job_id,),
                ).fetchone()["expired"]

        wait_until(expired)
        cli("worker", "--once")
        after = runner.status(job_id)
        assert after["status"] == "succeeded"
        assert after["steps"][:1] == before["steps"]
        assert [listing["company"] for listing in after["result"]["listings"]] == ["Maple Labs", "Cedar & Co"]
        assert after["cost_usd"] == Decimal("0.10")
        assert receipts(runner, job_id) == [{"step_name": "extract_hiring", "calls": 1, "requests": 2}]
        cli("replay", str(job_id), "--out", str(tmp_path))
        html = (tmp_path / "index.html").read_text()
        assert "cached on resume" in html and "resumed after 1 interrupted attempt(s)" in html
    finally:
        if worker.poll() is None:
            worker.kill()
        worker.communicate(timeout=5)
