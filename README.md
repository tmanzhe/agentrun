# agentrun

A Postgres-backed runner for long AI jobs. Each step saves its result when it
finishes. Restart a worker after a crash and it picks up from the saved steps.
The two built-in jobs use local mocks.

## Quickstart

You'll need Python 3.12+, uv, and Postgres. The included Compose file runs
PostgreSQL 17. Start it with `docker compose up -d`, or use a running database.

```sh
uv sync
export DATABASE_URL=postgresql://postgres@localhost:5432/agentrun
uv run agentrun submit example '{"prompt":"hello","sleep":3}'
uv run agentrun worker
# In another terminal, replace <job_id> with the UUID from submit:
uv run agentrun status <job_id>
uv run agentrun replay <job_id> --out replay
uv run agentrun cancel <job_id>
```

`DATABASE_URL` defaults to that URL. The CLI creates its tables on startup.
`replay` writes `replay/index.html`; open it in a browser to see the saved timeline.
`cancel` works on queued or running jobs.

The example runs draft, review, and polish. Each step reports 10 input tokens,
5 output tokens, and $0.10 in pretend usage. It saves a mock receipt before
sleeping to simulate a lost response if you kill the worker. Abandoned jobs
become claimable after the default two-second lease expires.

Use `worker --once` to try one job and exit. Set a spend threshold with:

```sh
uv run agentrun submit example '{"prompt":"hello","sleep":0}' --max-usd 1.50
```

Reported usage includes failures and timeouts. The runner checks the threshold
before each unsaved step attempt. An attempt can take the total past it.

## Your own jobs

Define workflows with `Job((Step("name", function), ...))` and pass a registry
to `Runner`; see [jobs.py](src/agentrun/jobs.py). Functions receive
`(context, input)` and return JSON-compatible values. Each output feeds the next
step. Keep definitions stable while jobs exist, or use a new job type name.

Steps default to three attempts with exponential backoff. Set `max_attempts`,
`base_delay`, and `timeout` on `Step` to change that. A timeout stops waiting for
the function; it can keep running in the background.

Avoiding duplicate charges after a lost response requires a provider that durably
honors `context.idempotency_key`. The local mock does this. In `mock_calls`,
`calls` counts mock executions; `requests` includes repeat deliveries.

## HN hiring demo

`hn_hiring` turns supplied HN comments into listings with company, role, location,
remote status and an application link, plus cleaned text and the comment URL.
The local mock reads `Company | Role | Location | REMOTE` headers. It skips
replies, deleted comments and comments without that header format. Unknown fields
stay null. The sample below produces two listings. Extraction reports $0.10 in
pretend usage when there are prepared comments.

```sh
uv run agentrun submit hn_hiring "$(cat examples/hn_hiring.json)"
uv run agentrun worker --once
# Use status or replay with the printed job ID to see the listings.
```

## MCP

Run `uv run agentrun-mcp` for a stdio server with three read-only tools:
`list_jobs`, `job_status`, and `replay_job`. It reads saved jobs from the same
`DATABASE_URL`; `replay_job` returns HTML. Create the tables with the CLI first.

## Tests

With Postgres running:

```sh
uv run pytest
```

Tests use temporary database schemas and local mocks, including crash tests
that kill and restart workers.
