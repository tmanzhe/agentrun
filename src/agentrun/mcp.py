"""Read-only job tools over MCP stdio. No schema setup or job execution."""

import json
from uuid import UUID

import anyio
from mcp.server.lowlevel import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, ToolAnnotations

from .replay import render_replay
from .runner import JobReader


def create_server(database_url: str | None = None) -> Server:
    reader = JobReader(database_url)
    server = Server("agentrun")
    read_only = ToolAnnotations(
        readOnlyHint=True, destructiveHint=False, openWorldHint=False,
    )
    job_schema = {
        "type": "object",
        "properties": {"job_id": {"type": "string", "format": "uuid"}},
        "required": ["job_id"],
        "additionalProperties": False,
    }

    @server.list_tools()
    async def list_tools() -> list[Tool]:
        return [
            Tool(
                name="list_jobs",
                description="List jobs newest first, with limit and offset pagination.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "limit": {"type": "integer", "minimum": 1, "maximum": 100, "default": 20},
                        "offset": {"type": "integer", "minimum": 0, "default": 0},
                    },
                    "additionalProperties": False,
                },
                annotations=read_only,
            ),
            Tool(
                name="job_status",
                description="Read a job's status, saved results, attempts and usage.",
                inputSchema=job_schema,
                annotations=read_only,
            ),
            Tool(
                name="replay_job",
                description="Return the recorded step timeline as HTML without running steps or writing files.",
                inputSchema=job_schema,
                annotations=read_only,
            ),
        ]

    def read_tool(name: str, arguments: dict) -> dict:
        if name == "list_jobs":
            result = {"jobs": reader.list_jobs(**arguments)}
        elif name in ("job_status", "replay_job"):
            try:
                job_id = UUID(arguments["job_id"])
            except ValueError:
                raise ValueError("job_id must be a UUID") from None
            job = reader.status(job_id)
            if job is None:
                raise ValueError("Job not found")
            result = job if name == "job_status" else {"html": render_replay(job)}
        else:
            raise ValueError(f"Unknown tool: {name}")
        # Match CLI output: UUIDs, timestamps and exact Decimal costs are strings.
        return json.loads(json.dumps(result, default=str))

    @server.call_tool()
    async def call_tool(name: str, arguments: dict) -> dict:
        return await anyio.to_thread.run_sync(read_tool, name, arguments)

    return server


async def serve() -> None:
    server = create_server()
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


def main() -> None:
    anyio.run(serve)


if __name__ == "__main__":
    main()
