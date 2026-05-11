#!/usr/bin/env python3
"""
Bare-minimal MCP server from first principles.
Transport: stdio (newline-delimited JSON-RPC 2.0)
No fastmcp, no third-party deps — stdlib only.
"""

import json
import sys

# ── Tool definitions ──────────────────────────────────────────────────────────

TOOLS = [
    {
        "name": "add",
        "description": "Add two numbers together.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "a": {"type": "number", "description": "First operand"},
                "b": {"type": "number", "description": "Second operand"},
            },
            "required": ["a", "b"],
        },
    },
    {
        "name": "subtract",
        "description": "Subtract b from a.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "a": {"type": "number", "description": "Minuend"},
                "b": {"type": "number", "description": "Subtrahend"},
            },
            "required": ["a", "b"],
        },
    },
    {
        "name": "multiply",
        "description": "Multiply two numbers.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "a": {"type": "number", "description": "First factor"},
                "b": {"type": "number", "description": "Second factor"},
            },
            "required": ["a", "b"],
        },
    },
]

# ── Tool executor ─────────────────────────────────────────────────────────────

def call_tool(name: str, args: dict) -> str:
    a, b = args["a"], args["b"]
    if name == "add":
        return str(a + b)
    if name == "subtract":
        return str(a - b)
    if name == "multiply":
        return str(a * b)
    raise ValueError(f"Unknown tool: {name}")

# ── JSON-RPC helpers ──────────────────────────────────────────────────────────

def send(obj: dict) -> None:
    """Write one JSON object as a single line to stdout."""
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()

def ok(req_id, result: dict) -> None:
    send({"jsonrpc": "2.0", "id": req_id, "result": result})

def err(req_id, code: int, message: str) -> None:
    send({"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": message}})

# ── Request dispatcher ────────────────────────────────────────────────────────

def handle(req: dict) -> None:
    method = req.get("method", "")
    req_id = req.get("id")          # None for notifications

    # ── initialize ────────────────────────────────────────────────────────────
    if method == "initialize":
        ok(req_id, {
            "protocolVersion": "2024-11-05",
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "math-server", "version": "0.1.0"},
        })

    # ── initialized (notification — no response) ──────────────────────────────
    elif method == "notifications/initialized":
        pass

    # ── tools/list ────────────────────────────────────────────────────────────
    elif method == "tools/list":
        ok(req_id, {"tools": TOOLS})

    # ── tools/call ────────────────────────────────────────────────────────────
    elif method == "tools/call":
        params = req.get("params", {})
        name   = params.get("name", "")
        args   = params.get("arguments", {})
        try:
            result = call_tool(name, args)
            ok(req_id, {
                "content": [{"type": "text", "text": result}],
                "isError": False,
            })
        except Exception as exc:
            ok(req_id, {
                "content": [{"type": "text", "text": str(exc)}],
                "isError": True,
            })

    # ── unknown method ────────────────────────────────────────────────────────
    else:
        if req_id is not None:          # don't reply to unknown notifications
            err(req_id, -32601, f"Method not found: {method}")

# ── Main loop ─────────────────────────────────────────────────────────────────

def main() -> None:
    for raw_line in sys.stdin:
        line = raw_line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
        except json.JSONDecodeError as exc:
            err(None, -32700, f"Parse error: {exc}")
            continue
        handle(req)

if __name__ == "__main__":
    main()
