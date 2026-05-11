#!/usr/bin/env python3
"""
Bare-minimal MCP server — SSE transport, stdlib only.

Two HTTP endpoints (MCP spec):
  GET  /sse          — client opens a long-lived SSE stream;
                       server sends an "endpoint" event with the POST URL.
  POST /message?sessionId=<id>
                     — client POSTs JSON-RPC here;
                       server pushes the response back on the SSE stream.

No fastmcp, no aiohttp, no starlette — just http.server + threading.
"""

import json
import queue
import threading
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

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
                "a": {"type": "number"},
                "b": {"type": "number"},
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
                "a": {"type": "number"},
                "b": {"type": "number"},
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
                "a": {"type": "number"},
                "b": {"type": "number"},
            },
            "required": ["a", "b"],
        },
    },
]

# ── Session store: sessionId → Queue ──────────────────────────────────────────
# Each SSE connection gets its own queue; POST handler puts responses on it.

sessions: dict[str, queue.Queue] = {}
sessions_lock = threading.Lock()

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
    """Return a JSON-RPC response dict, or None for notifications."""
    method = req.get("method", "")
    req_id = req.get("id")          # absent on notifications

    def ok(result):
        return {"jsonrpc": "2.0", "id": req_id, "result": result}

    def error(code, msg):
        return {"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": msg}}

    if method == "initialize":
        return ok({
            "protocolVersion": "2024-11-05",
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "math-sse-server", "version": "0.1.0"},
        })

    if method == "notifications/initialized":
        return None                 # fire-and-forget, no response

    if method == "tools/list":
        return ok({"tools": TOOLS})

    if method == "tools/call":
        params = req.get("params", {})
        name   = params.get("name", "")
        args   = params.get("arguments", {})
        try:
            result = call_tool(name, args)
            return ok({"content": [{"type": "text", "text": result}], "isError": False})
        except Exception as exc:
            return ok({"content": [{"type": "text", "text": str(exc)}], "isError": True})

    if req_id is not None:
        return error(-32601, f"Method not found: {method}")

    return None                     # unknown notification → silence

# ── HTTP handler ──────────────────────────────────────────────────────────────

class MCPHandler(BaseHTTPRequestHandler):

    # suppress default request-per-line logging noise
    def log_message(self, fmt, *args):
        print(f"[http] {fmt % args}", flush=True)

    # ── GET /sse ──────────────────────────────────────────────────────────────
    def do_GET(self):
        if not self.path.startswith("/sse"):
            self._send_plain(404, "Not found")
            return

        # Assign a session id for this connection
        session_id = str(uuid.uuid4())
        q: queue.Queue = queue.Queue()
        with sessions_lock:
            sessions[session_id] = q

        print(f"[sse] new session {session_id}", flush=True)

        # SSE response headers — keep connection alive
        self.send_response(200)
        self.send_header("Content-Type",  "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection",    "keep-alive")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()

        def sse(event: str, data: str):
            """Write one SSE event to the wire."""
            payload = f"event: {event}\ndata: {data}\n\n"
            self.wfile.write(payload.encode())
            self.wfile.flush()

        # MCP spec: first event must be "endpoint" with the POST URL
        post_url = f"http://{HOST}:{PORT}/message?sessionId={session_id}"
        sse("endpoint", post_url)

        # Block here, draining the queue and streaming responses back
        try:
            while True:
                msg = q.get()       # blocks until POST handler puts something
                if msg is None:     # sentinel → close stream
                    break
                sse("message", json.dumps(msg))
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            with sessions_lock:
                sessions.pop(session_id, None)
            print(f"[sse] session {session_id} closed", flush=True)

    # ── POST /message ─────────────────────────────────────────────────────────
    def do_POST(self):
        parsed = urlparse(self.path)
        if not parsed.path.startswith("/message"):
            self._send_plain(404, "Not found")
            return

        # Parse sessionId from query string
        qs = parse_qs(parsed.query)
        session_id = (qs.get("sessionId") or [None])[0]

        with sessions_lock:
            q = sessions.get(session_id)

        if q is None:
            self._send_plain(400, f"Unknown sessionId: {session_id}")
            return

        # Read and parse the JSON-RPC body
        length = int(self.headers.get("Content-Length", 0))
        body   = self.rfile.read(length)
        try:
            req = json.loads(body)
        except json.JSONDecodeError as exc:
            self._send_plain(400, f"Bad JSON: {exc}")
            return

        # Dispatch → optionally push response onto the SSE queue
        response = dispatch(req)
        if response is not None:
            q.put(response)

        # HTTP 202 Accepted — actual result travels over SSE
        self._send_plain(202, "Accepted")

    # ── CORS pre-flight ───────────────────────────────────────────────────────
    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin",  "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    # ── helper ────────────────────────────────────────────────────────────────
    def _send_plain(self, code: int, text: str):
        body = text.encode()
        self.send_response(code)
        self.send_header("Content-Type",   "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    # ThreadingHTTPServer handles each request in its own thread,
    # so the blocking SSE loop doesn't starve POST requests.
    from http.server import ThreadingHTTPServer
    server = ThreadingHTTPServer((HOST, PORT), MCPHandler)
    print(f"MCP SSE server running on http://{HOST}:{PORT}", flush=True)
    print(f"  SSE endpoint  →  GET  http://{HOST}:{PORT}/sse")
    print(f"  RPC endpoint  →  POST http://{HOST}:{PORT}/message?sessionId=<id>")
    server.serve_forever()

if __name__ == "__main__":
    main()
