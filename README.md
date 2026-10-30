# agentrun

Python 3.12+, uv, and PostgreSQL 17. Start a local database with
`docker compose up -d` (skip if Postgres is already running), then:

```sh
uv sync
export DATABASE_URL=postgresql://postgres@localhost:5432/agentrun
uv run agentrun submit example '{"prompt":"hello","sleep":3}'
uv run agentrun worker
# In another terminal, using the printed UUID:
uv run agentrun status <job_id>
uv run agentrun cancel <job_id>
uv run pytest
```

`DATABASE_URL` defaults to the URL above. Tables are created on startup.
`worker --once` tries one job and exits. Kill a worker and restart it: abandoned
jobs become claimable after a two-second lease expires; saved steps are reused.
Inspect `mock_calls` for paid execution counts (`calls`) and delivery attempts
(`requests`). The example commits a mock receipt before sleeping, so killing it
also exercises a lost model response.

Define workflows with `Job((Step("name", function), ...))` and pass a named
registry to `Runner`; see `src/agentrun/example.py` for the CLI registry. Functions
receive `(context, input)` and return JSON; each output feeds the next step.
Keep job definitions stable, or use a new job type name for a new version.
External model APIs **must durably honor `context.idempotency_key`** to avoid
duplicate charges if a crash happens between a call and saving its result.
The mock implements this; PostgreSQL alone cannot guarantee it for arbitrary APIs.
Step errors mark a job failed; automatic retries are outside M1.
