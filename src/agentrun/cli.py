import argparse
import json
from uuid import UUID

import psycopg

from .example import JOBS, MOCK_SCHEMA
from .runner import Runner


def main() -> None:
    parser = argparse.ArgumentParser(prog="agentrun")
    commands = parser.add_subparsers(dest="command", required=True)
    submit = commands.add_parser("submit", help="enqueue a named job")
    submit.add_argument("job_type", choices=sorted(JOBS))
    submit.add_argument("input", nargs="?", type=json.loads, default={})
    worker = commands.add_parser("worker", help="run jobs until interrupted")
    worker.add_argument("--once", action="store_true", help="try one job, then exit")
    worker.add_argument("--lease-seconds", type=float, default=2.0)
    status = commands.add_parser("status", help="show a job and its saved steps")
    status.add_argument("job_id", type=UUID)
    cancel = commands.add_parser("cancel", help="cancel a queued or running job")
    cancel.add_argument("job_id", type=UUID)
    args = parser.parse_args()
    try:
        runner = Runner(JOBS, lease_seconds=getattr(args, "lease_seconds", 2.0))
        with runner.connect() as conn, conn.transaction():
            conn.execute("SELECT pg_advisory_xact_lock(684921038)")
            conn.execute(MOCK_SCHEMA)
        if args.command == "submit":
            print(runner.submit(args.job_type, args.input))
        elif args.command == "worker":
            runner.worker(once=args.once)
        else:
            if args.command == "cancel":
                runner.cancel(args.job_id)
            job = runner.status(args.job_id)
            if job is None:
                parser.exit(1, "Job not found\n")
            print(json.dumps(job, default=str, indent=2))
    except (psycopg.Error, ValueError) as exc:
        parser.exit(1, f"agentrun: {exc}\n")
    except KeyboardInterrupt:
        pass
