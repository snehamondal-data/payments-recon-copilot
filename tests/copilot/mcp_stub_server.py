"""A stand-in MCP server that misbehaves on purpose, for McpBackend's failure tests.

  python mcp_stub_server.py crash|hang|malformed|unavailable|unmarked

It lists the real six tools and serves recon://as-of. The first tools/call (the orchestrator's
get_exception) answers with a canned fee-mismatch row; every later call misbehaves as asked.
"""

import json
import os
import sys
import time

import anyio
import mcp_types as types
from mcp.server import Server
from mcp.server.stdio import stdio_server

from recon.copilot.tools import tool_definitions

MODE = sys.argv[1]
calls = 0


async def list_tools(ctx, params):
    return types.ListToolsResult(tools=[types.Tool(**t) for t in tool_definitions()])


async def call_tool(ctx, params):
    global calls
    calls += 1
    if calls == 1:
        eid = params.arguments["exception_id"]
        row = {"record_id": eid, "exception_id": eid, "reason_code": "fee_mismatch", "charge_id": eid.split(":")[1]}
        return types.CallToolResult(content=[types.TextContent(type="text", text=json.dumps(
            {"records": [row], "truncated": False}))])
    if MODE == "crash":
        print("stub server: crashing on purpose", file=sys.stderr, flush=True)
        os._exit(3)
    if MODE == "hang":
        time.sleep(30)  # blocks the server's event loop, like a stuck query
    if MODE == "malformed":
        return types.CallToolResult(content=[types.TextContent(type="text", text="<html>not json</html>")])
    if MODE == "unavailable":
        return types.CallToolResult(content=[types.TextContent(type="text", text="warehouse connection lost")],
                                    is_error=True, _meta={"recon/error_kind": "backend_unavailable"})
    if MODE == "unmarked":  # an error without the recon/error_kind marker: not trusted as the model's mistake
        return types.CallToolResult(content=[types.TextContent(type="text", text="something broke")], is_error=True)
    raise SystemExit(f"unknown mode {MODE}")


async def list_resources(ctx, params):
    return types.ListResourcesResult(resources=[types.Resource(uri="recon://as-of", name="as-of")])


async def read_resource(ctx, params):
    return types.ReadResourceResult(contents=[types.TextResourceContents(uri="recon://as-of", text="2026-10-15")])


async def main():
    server = Server("stub", on_list_tools=list_tools, on_call_tool=call_tool, on_list_resources=list_resources,
                    on_read_resource=read_resource)
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


anyio.run(main)
