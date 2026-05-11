#!/usr/bin/env python3
"""
Bare-minimal MCP server — Streamable HTTP transport (MCP spec 2025-03-26).
stdlib only, no fastmcp, no third-party deps.

Single endpoint:
  POST /mcp   — client sends JSON-RPC request(s)
                server replies with:
                  • application/json          for single responses
                  • text/event-stream         when client sends
                    Accept: text/event-stream (streaming mode)

GET  /mcp    — optional; server can push server-initiated events (not used here)

Ref: https://modelcontextprotocol.io/specification/2025-03-26/basic/transports
"""

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HOST = "localhost"
PORT = 8000

# ── Tool definitions ──────────────────────────────────────────────────────────

TOOLS = [
    {
        "name": "add",
        "description": "Add two numbers.",
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

# ── Tool logic ────────────────────────────────────────────────────────────────

def call_tool(name: str, args: dict) -> str:
    a, b = args["a"], args["b"]
    match name:
        case "add":      return str(a + b)
        case "subtract": return str(a - b)
        case "multiply": return str(a * b)
        case _:          raise ValueError(f"Unknown tool: {name}")

# ── JSON-RPC dispatcher ───────────────────────────────────────────────────────

def dispatch(req: dict) -> dict | None:
    """
    Handle one JSON-RPC object.
    Returns a response dict, or None for notifications (no id).
    """
    method = req.get("method", "")
    req_id = req.get("id")          # absent on notifications

    def ok(result):
        return {"jsonrpc": "2.0", "id": req_id, "result": result}

    def error(code, msg):
        return {"jsonrpc": "2.0", "id": req_id,
                "error": {"code": code, "message": msg}}

    if method == "initialize":
        return ok({
            "protocolVersion": "2025-03-26",
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "math-http-server", "version": "0.1.0"},
        })

    if method == "notifications/initialized":
        return None                 # notification — no response

    if method == "tools/list":
        return ok({"tools": TOOLS})

    if method == "tools/call":
        params = req.get("params", {})
        name   = params.get("name", "")
        args   = params.get("arguments", {})
        try:
            result = call_tool(name, args)
            return ok({
                "content": [{"type": "text", "text": result}],
                "isError": False,
            })
        except Exception as exc:
            return ok({
                "content": [{"type": "text", "text": str(exc)}],
                "isError": True,
            })

    if req_id is not None:
        return error(-32601, f"Method not found: {method}")

    return None                     # unknown notification → silence

# ── HTTP handler ──────────────────────────────────────────────────────────────

class MCPHandler(BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        print(f"[http] {fmt % args}", flush=True)

    # ── POST /mcp ─────────────────────────────────────────────────────────────
    def do_POST(self):
        if not self.path.startswith("/mcp"):
            self._send_json(404, {"error": "Not found"})
            return

        # Read body
        length = int(self.headers.get("Content-Length", 0))
        body   = self.rfile.read(length)

        try:
            payload = json.loads(body)
        except json.JSONDecodeError as exc:
            self._send_json(400, {
                "jsonrpc": "2.0", "id": None,
                "error": {"code": -32700, "message": f"Parse error: {exc}"},
            })
            return

        # Spec allows a single object OR a batch (array)
        is_batch = isinstance(payload, list)
        requests = payload if is_batch else [payload]

        # Dispatch all, collect non-None responses
        responses = [r for req in requests if (r := dispatch(req)) is not None]

        # ── Streaming mode (Accept: text/event-stream) ────────────────────────
        # Client signals it can receive SSE; we stream each response as an event
        # then close. Useful when a batch contains many results.
        accept = self.headers.get("Accept", "")
        if "text/event-stream" in accept:
            self._stream_responses(responses)
            return

        # ── Simple JSON mode ──────────────────────────────────────────────────
        if not responses:
            # All notifications — 202 with empty body
            self.send_response(202)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            return

        # Return array for batch, single object otherwise
        body_out = responses if is_batch else responses[0]
        self._send_json(200, body_out)

    # ── GET /mcp  (optional — for server-initiated messages / resumability) ───
    # Not required for basic tool use; included so curious clients don't 404.
    def do_GET(self):
        if not self.path.startswith("/mcp"):
            self._send_plain(404, "Not found")
            return

        # Send an empty SSE stream (no server-initiated events in this server)
        self.send_response(200)
        self.send_header("Content-Type",  "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection",    "keep-alive")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        # Keep open until client disconnects
        try:
            while True:
                import time; time.sleep(30)
                # heartbeat comment to keep proxy alive
                self.wfile.write(b": ping\n\n")
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass

    # ── CORS pre-flight ───────────────────────────────────────────────────────
    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin",  "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Accept, Mcp-Session-Id")
        self.end_headers()

    # ── helpers ───────────────────────────────────────────────────────────────

    def _send_json(self, code: int, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type",   "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _send_plain(self, code: int, text: str):
        body = text.encode()
        self.send_response(code)
        self.send_header("Content-Type",   "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _stream_responses(self, responses: list):
        """Send each response as an SSE 'message' event, then close."""
        self.send_response(200)
        self.send_header("Content-Type",  "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()

        for resp in responses:
            data = json.dumps(resp)
            event = f"event: message\ndata: {data}\n\n"
            self.wfile.write(event.encode())
            self.wfile.flush()

# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    server = ThreadingHTTPServer((HOST, PORT), MCPHandler)
    print(f"MCP Streamable HTTP server on http://{HOST}:{PORT}/mcp", flush=True)
    print(f"  JSON mode    →  POST (no special Accept header)")
    print(f"  Stream mode  →  POST with Accept: text/event-stream")
    server.serve_forever()

if __name__ == "__main__":
    main()
