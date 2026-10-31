"""Standalone, read-only HTML snapshots. Database values are always text."""

from datetime import datetime, timezone
from html import escape
import json
from pathlib import Path


PAYLOAD_LIMIT = 10_000


def text(value) -> str:
    return escape(str(value), quote=True)


def payload(label, value) -> str:
    rendered = json.dumps(value, ensure_ascii=False, indent=2, default=str)
    if len(rendered) > PAYLOAD_LIMIT:
        rendered = rendered[:PAYLOAD_LIMIT] + "\n… [truncated]"
    return f"<details><summary>{text(label)}</summary><pre>{text(rendered)}</pre></details>"


def fields(values) -> str:
    return "<dl>" + "".join(
        f"<dt>{text(label)}</dt><dd>{text(value)}</dd>" for label, value in values
    ) + "</dl>"


def timestamp(value) -> str:
    return value.isoformat(sep=" ") if value is not None else "unknown"


def duration(start, end, now) -> str:
    if start is None:
        return "unknown"
    seconds = max(0, ((end or now) - start).total_seconds())
    return f"{seconds:.3f}s" + (" (ongoing)" if end is None else "")


def render_replay(job: dict) -> str:
    """Render recorded state without running a job or writing files."""
    now = datetime.now(timezone.utc)
    saved = {(row["step_name"], row["input_hash"]): row for row in job["steps"]}
    attempts = {(row["step_name"], row["input_hash"]): row for row in job["attempts"]}
    # Preserve the submitted workflow order, including steps never attempted.
    # Older jobs have no plan; use the recorded start/completion order instead.
    keys = sorted(saved.keys() | attempts.keys(), key=lambda key: (
        attempts.get(key, {}).get("started_at")
        or saved.get(key, {}).get("completed_at") or job["created_at"], key,
    ))
    names = list(dict.fromkeys([*job["step_names"], *(key[0] for key in keys)]))
    sections = []
    for name in names:
        for key in [key for key in keys if key[0] == name] or [(name, None)]:
            step = saved.get(key)
            attempt = attempts.get(key, {})
            start = attempt.get("started_at")
            end = step["completed_at"] if step else attempt.get("finished_at")
            if step:
                status = "succeeded"
            elif attempt:
                status = job["status"] if job["status"] in ("failed", "cancelled", "over_budget") else (
                    "retrying" if end else "running / interrupted"
                )
            else:
                status = "blocked by spend cap" if job["status"] == "over_budget" else "not run"
            markers = []
            if step and job["last_claimed_at"] and step["completed_at"] < job["last_claimed_at"]:
                markers.append("cached on resume")
            if attempt.get("interrupted_count"):
                markers.append(f"resumed after {attempt['interrupted_count']} interrupted attempt(s)")
            metadata = fields([
                ("Status", status), ("Attempts", attempt.get("attempt_count", "unknown" if step else 0)),
                ("Started", timestamp(start)), ("Finished", timestamp(end)),
                ("Duration (includes retries/backoff)", duration(start, end, now)),
                ("Input tokens", step["input_tokens"] if step else 0),
                ("Output tokens", step["output_tokens"] if step else 0),
                ("Cost", f"${step['cost_usd']}" if step else "$0"),
                ("Markers", "; ".join(markers) or "none"),
            ])
            error = attempt.get("last_error")
            error_html = f"<p>Last error:</p><pre>{text(error)}</pre>" if error else ""
            input_html = payload("Input", attempt["input"]) if start else "<p>Input: unrecorded</p>"
            output_html = payload("Output", step["result"]) if step else "<p>Output: unavailable</p>"
            usage_html = "".join(
                "<h3>Attempt " + text(row["attempt_number"]) + "</h3>" + fields([
                    ("Input tokens", row["input_tokens"]),
                    ("Output tokens", row["output_tokens"]),
                    ("Attempt cost", f"${row['cost_usd']}"),
                ])
                for row in job["attempt_usage"]
                if (row["step_name"], row["input_hash"]) == key
            )
            sections.append(
                f"<section><h2>{text(name)}</h2>{metadata}{usage_html}{error_html}{input_html}{output_html}</section>"
            )
    summary = fields([
        ("Job", job["id"]), ("Type", job["job_type"]), ("Status", job["status"]),
        ("Created", timestamp(job["created_at"])), ("Started", timestamp(job["started_at"])),
        ("Finished", timestamp(job["finished_at"])),
        ("Duration", duration(job["started_at"], job["finished_at"], now)),
        ("Input tokens", job["input_tokens"]), ("Output tokens", job["output_tokens"]),
        ("Total cost", f"${job['cost_usd']}"),
        ("Spend cap", "unlimited" if job["max_usd"] is None else f"${job['max_usd']}"),
        ("Markers", "resumed" if job["claim_count"] > 1 else "none"),
    ])
    error_html = f"<p>Job error:</p><pre>{text(job['error'])}</pre>" if job["error"] else ""
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>agentrun replay {text(job['id'])}</title>
<style>
body {{ font: 16px system-ui, sans-serif; margin: 2rem auto; padding: 0 1rem;
        max-width: 960px; color: #18212b; background: #f7f9fb; }}
section {{ background: white; border: 1px solid #ccd5df; border-left: 4px solid #426b9a;
           border-radius: 6px; margin: 1rem 0; padding: 1rem; }}
h2 {{ margin-top: 0; overflow-wrap: anywhere; }}
dl {{ display: grid; grid-template-columns: minmax(100px, 1fr) 3fr; gap: .35rem 1rem; }}
dt {{ font-weight: 600; }} dd {{ margin: 0; overflow-wrap: anywhere; }}
pre {{ white-space: pre-wrap; overflow-wrap: anywhere; background: #eef2f6; padding: .75rem; }}
details {{ margin: .75rem 0; }} summary {{ cursor: pointer; font-weight: 600; }}
</style></head><body><h1>agentrun replay</h1>{summary}{error_html}
<p>Usage totals and the cap count reported usage from every attempt, including failures and timeouts. Durations are wall time.
Unknown values were not recorded by older workers.</p>
{payload('Job input', job['input'])}{payload('Job output', job['result'])}
<h2>Step timeline</h2>{''.join(sections)}</body></html>
"""


def write_replay(job: dict, out_dir: str | Path) -> Path:
    html = render_replay(job)
    directory = Path(out_dir)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "index.html"
    path.write_text(html, encoding="utf-8")
    return path
