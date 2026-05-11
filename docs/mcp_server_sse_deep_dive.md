# `mcp_server_sse.py` — Technical Deep Dive

**Transport:** Server-Sent Events (SSE) over HTTP  
**MCP Spec version:** 2024-11-05  
**Python:** 3.10+  
**Dependencies:** none — standard library only (`json`, `queue`, `threading`, `uuid`, `http.server`, `urllib.parse`)

---

## Table of contents

1. [Overview](#1-overview)
2. [Architecture](#2-architecture)
3. [Transport layer — SSE over HTTP](#3-transport-layer--sse-over-http)
   - 3.1 [What SSE is](#31-what-sse-is)
   - 3.2 [Wire format](#32-wire-format)
   - 3.3 [Two-endpoint design](#33-two-endpoint-design)
   - 3.4 [Asymmetric channels](#34-asymmetric-channels)
4. [Session model](#4-session-model)
   - 4.1 [Session lifecycle](#41-session-lifecycle)
   - 4.2 [Session store](#42-session-store)
   - 4.3 [Thread safety](#43-thread-safety)
5. [Threading model](#5-threading-model)
   - 5.1 [ThreadingHTTPServer](#51-threadinghttpserver)
   - 5.2 [Queue as the inter-thread channel](#52-queue-as-the-inter-thread-channel)
   - 5.3 [Thread lifecycle diagram](#53-thread-lifecycle-diagram)
6. [MCP handshake sequence](#6-mcp-handshake-sequence)
7. [Module walkthrough](#7-module-walkthrough)
   - 7.1 [Module-level state](#71-module-level-state)
   - 7.2 [call_tool — the executor](#72-call_tool--the-executor)
   - 7.3 [dispatch — JSON-RPC dispatcher](#73-dispatch--json-rpc-dispatcher)
   - 7.4 [MCPHandler.do_GET — the SSE stream](#74-mcphandlerdo_get--the-sse-stream)
   - 7.5 [MCPHandler.do_POST — the RPC receiver](#75-mcphandlerdo_post--the-rpc-receiver)
   - 7.6 [MCPHandler.do_OPTIONS — CORS preflight](#76-mcphandlerdo_options--cors-preflight)
   - 7.7 [MCPHandler._send_plain — HTTP helper](#77-mcphandler_send_plain--http-helper)
   - 7.8 [main — server startup](#78-main--server-startup)
8. [HTTP endpoint reference](#8-http-endpoint-reference)
9. [SSE event reference](#9-sse-event-reference)
10. [Error handling](#10-error-handling)
11. [Data flow diagrams](#11-data-flow-diagrams)
12. [Comparison with stdio transport](#12-comparison-with-stdio-transport)
13. [Extending the server](#13-extending-the-server)
14. [Limitations of SSE transport](#14-limitations-of-sse-transport)
15. [Testing and debugging](#15-testing-and-debugging)

---

## 1. Overview

`mcp_server_sse.py` implements the **SSE transport** of the Model Context Protocol (MCP). Unlike the stdio transport — which uses process stdin/stdout — the SSE transport runs an HTTP server that any network client can connect to.

Communication is deliberately **asymmetric**:

- The client opens a persistent `GET /sse` connection. The server streams responses back over this long-lived channel using the SSE protocol.
- The client sends JSON-RPC requests via separate `POST /message` calls. These are ordinary short-lived HTTP requests.

This asymmetry exists because browsers cannot send data over an SSE stream (SSE is receive-only by design). MCP's SSE transport was designed to work natively in browser-based clients.

```
Client
│
├── GET /sse  ─────────────────────────────────────▶  server
│   (long-lived, blocks)          ◀── SSE events ──   │
│                                                      │
├── POST /message?sessionId=X ─────────────────────▶  │
│   ◀── HTTP 202 ─────────────────────────────────    │
│                                                      │
├── POST /message?sessionId=X ─────────────────────▶  │
│   ◀── HTTP 202 ─────────────────────────────────    │
│   ...                                                │
│                                                      │
└── closes GET connection ─────────────────────────▶  server cleans up session
```

---

## 2. Architecture

The file has six distinct layers:

```
┌────────────────────────────────────────────────────────────────┐
│  main()                  Server startup — binds port, loops    │
├────────────────────────────────────────────────────────────────┤
│  MCPHandler              HTTP request router (per-thread)      │
│    do_GET()  ──────────  SSE stream — blocks, drains queue     │
│    do_POST() ──────────  RPC receiver — dispatches, enqueues   │
│    do_OPTIONS() ───────  CORS preflight                        │
├────────────────────────────────────────────────────────────────┤
│  sessions + sessions_lock   Session store — thread-safe map    │
│                             sessionId → queue.Queue            │
├────────────────────────────────────────────────────────────────┤
│  dispatch()              JSON-RPC dispatcher — returns dict    │
├────────────────────────────────────────────────────────────────┤
│  call_tool()             Domain — tool execution logic         │
├────────────────────────────────────────────────────────────────┤
│  TOOLS                   Capability manifest — static list     │
└────────────────────────────────────────────────────────────────┘
```

The critical architectural addition over stdio is the **session store**: a thread-safe dict that maps each connected client's session ID to a `queue.Queue`. This queue is the inter-thread channel that connects the POST handler (which receives requests) to the GET handler (which streams responses).

---

## 3. Transport layer — SSE over HTTP

### 3.1 What SSE is

Server-Sent Events (SSE) is a standard HTTP-based protocol for one-way server-to-client streaming. It is defined in the HTML Living Standard and supported natively in all modern browsers.

Key properties:

- **One-directional** — server pushes to client only. Client cannot send data over the SSE stream.
- **Text-based** — events are UTF-8 text. No binary framing.
- **Persistent** — a single HTTP connection stays open for the duration of the session.
- **Auto-reconnect** — browsers automatically reconnect if the connection drops (the server does not implement reconnect logic here).
- **No library needed** — it is plain HTTP with a specific `Content-Type` and text format.

### 3.2 Wire format

An SSE stream is a sequence of events separated by blank lines. Each event has optional `event` and `data` fields:

```
event: endpoint\n
data: http://localhost:8000/message?sessionId=abc-123\n
\n
event: message\n
data: {"jsonrpc":"2.0","id":1,"result":{...}}\n
\n
event: message\n
data: {"jsonrpc":"2.0","id":2,"result":{...}}\n
\n
```

Rules:

- Each field is `field: value\n` — field name, colon, space, value, newline.
- An event ends with a **blank line** (`\n\n` — two consecutive newlines).
- The `event` field names the event type. Clients filter by event name.
- The `data` field carries the payload. Multiple `data:` lines in one event are joined with `\n`.
- Lines beginning with `:` are comments (used for keepalive pings).

In this server, every SSE event is written by the `sse()` inner function in `do_GET`:

```python
def sse(event: str, data: str):
    payload = f"event: {event}\ndata: {data}\n\n"
    self.wfile.write(payload.encode())
    self.wfile.flush()
```

The `flush()` is mandatory — without it the OS buffers the data and the client never receives it.

### 3.3 Two-endpoint design

MCP's SSE transport requires exactly two HTTP endpoints:

| Endpoint | Method | Purpose |
|---|---|---|
| `/sse` | `GET` | Client opens persistent SSE stream; server sends the POST URL as the first event |
| `/message` | `POST` | Client sends JSON-RPC requests; server enqueues responses for the SSE stream |

The `sessionId` query parameter on `/message` links a POST request to its SSE stream.

### 3.4 Asymmetric channels

The two channels carry different things in different directions:

```
GET /sse (long-lived HTTP response)
    Server ──────────────── SSE events ──────────────────▶ Client
    
POST /message (short-lived HTTP request)
    Client ──────────────── JSON-RPC body ───────────────▶ Server
    Server ──────────────── HTTP 202 ────────────────────▶ Client
```

The POST's HTTP response is just an acknowledgement (`202 Accepted`). The actual JSON-RPC result travels over the SSE stream, not the POST response. This is why testing requires two terminals — one to hold the SSE stream open and one to send POST requests.

---

## 4. Session model

### 4.1 Session lifecycle

```
Client connects GET /sse
        │
        ▼
Server generates UUID → session_id
Server creates queue.Queue → q
Server stores sessions[session_id] = q
Server sends SSE "endpoint" event with POST URL
        │
        ▼
Client POSTs to /message?sessionId=<session_id>
Server looks up sessions[session_id] → q
Server dispatches JSON-RPC → response dict
Server puts response on q
GET thread wakes up, streams response via SSE
        │
        ▼  (repeat for each request)
        │
        ▼
Client closes GET connection
BrokenPipeError caught in GET handler
finally: sessions.pop(session_id)
Session destroyed
```

### 4.2 Session store

```python
sessions: dict[str, queue.Queue] = {}
sessions_lock = threading.Lock()
```

`sessions` is a module-level dict. It exists for the lifetime of the process and is shared across all threads. Its keys are UUID strings; its values are `queue.Queue` instances.

One entry exists per connected SSE client. When the client disconnects, the entry is removed in the `finally` block of `do_GET`.

### 4.3 Thread safety

`sessions` is accessed from multiple threads simultaneously:

- The GET thread **writes** to it (on connect and disconnect)
- The POST thread **reads** from it (to find the queue for a given session ID)

Without a lock, a race condition could occur: the GET thread could be removing a session while a POST thread is reading it, causing a `KeyError` or returning a stale queue reference.

`sessions_lock` is a `threading.Lock()` that serialises all access:

```python
# GET thread — register session
with sessions_lock:
    sessions[session_id] = q

# POST thread — look up session
with sessions_lock:
    q = sessions.get(session_id)

# GET thread — remove session on disconnect
with sessions_lock:
    sessions.pop(session_id, None)
```

The `with sessions_lock:` block is a context manager that acquires the lock on entry and releases it on exit, even if an exception is raised.

The lock is held only for the dict operation — not for the blocking `q.get()` or the SSE write. This keeps the critical section small and prevents deadlocks.

---

## 5. Threading model

### 5.1 ThreadingHTTPServer

```python
from http.server import ThreadingHTTPServer
server = ThreadingHTTPServer((HOST, PORT), MCPHandler)
```

`ThreadingHTTPServer` is a subclass of `HTTPServer` that mixes in `socketserver.ThreadingMixIn`. It spawns a new thread for **each incoming HTTP request**.

This is essential for the SSE transport. The GET handler blocks indefinitely on `q.get()` — it cannot return until the client disconnects. If the server used a single-threaded handler, this blocking GET would prevent any POST requests from being processed, creating a deadlock.

With `ThreadingHTTPServer`, each request runs in its own thread:

```
Main thread:  server.serve_forever()  ← accepts connections, spawns handler threads

GET thread:   do_GET()  ← blocks on q.get(), streams SSE responses
POST thread:  do_POST() ← runs, enqueues response, returns immediately
POST thread:  do_POST() ← runs, enqueues response, returns immediately
...
```

GET threads are long-lived (one per connected client). POST threads are short-lived (one per request, terminates after `_send_plain(202)`).

### 5.2 Queue as the inter-thread channel

`queue.Queue` is Python's thread-safe FIFO queue from the standard library. It is the only shared mutable state between the GET and POST threads for a given session.

**GET thread (consumer):**

```python
while True:
    msg = q.get()       # blocks until an item is available
    if msg is None:     # sentinel value → exit the loop
        break
    sse("message", json.dumps(msg))
```

`q.get()` blocks the GET thread until an item appears in the queue. This is efficient — the thread consumes no CPU while waiting.

**POST thread (producer):**

```python
response = dispatch(req)
if response is not None:
    q.put(response)     # non-blocking — always succeeds immediately
```

`q.put()` is non-blocking (Queue has no size limit by default). It places the response dict on the queue and returns immediately. The GET thread wakes up and processes it.

**The `None` sentinel:**

Putting `None` on the queue is the shutdown signal. The GET thread's `while True` loop checks for `None` and `break`s. In this implementation `None` is never put — the loop exits via `BrokenPipeError` when the client disconnects. The sentinel is available for explicit shutdown if needed.

### 5.3 Thread lifecycle diagram

```
Time ──────────────────────────────────────────────────────────▶

Main thread:   [accept] [accept] [accept] [accept] ...
                  │        │        │
GET thread:    [do_GET──────────────────────────────────▶ done]
                           │        │
POST thread 1:          [do_POST]   │
                                    │
POST thread 2:               [do_POST]
```

Each `[accept]` spawns a new thread. GET threads live as long as the client's SSE connection. POST threads terminate after sending HTTP 202.

---

## 6. MCP handshake sequence

The MCP handshake over SSE follows the same logical sequence as stdio, but the transport differs at every step.

```
Client                                          Server
  │                                                │
  │  GET /sse                                      │
  │ ─────────────────────────────────────────────▶│  ← spawns GET thread
  │                                                │  ← generates session_id
  │  event: endpoint                               │
  │  data: http://localhost:8000/message?          │
  │        sessionId=<uuid>                        │
  │ ◀─────────────────────────────────────────────│  ← SSE stream stays open
  │                                                │
  │  POST /message?sessionId=<uuid>                │
  │  body: {"method":"initialize","id":1,...}      │
  │ ─────────────────────────────────────────────▶│  ← spawns POST thread
  │  ◀── HTTP 202 Accepted ─────────────────────  │  ← POST thread exits
  │                                                │  ← GET thread dequeues
  │  event: message                                │
  │  data: {"id":1,"result":{                      │
  │    "protocolVersion":"2024-11-05",             │
  │    "capabilities":{"tools":{}},               │
  │    "serverInfo":{...}}}                        │
  │ ◀─────────────────────────────────────────────│  ← over SSE stream
  │                                                │
  │  POST /message?sessionId=<uuid>                │
  │  body: {"method":"notifications/initialized"} │
  │ ─────────────────────────────────────────────▶│  ← no response enqueued
  │  ◀── HTTP 202 Accepted ─────────────────────  │
  │                                                │
  │  POST /message?sessionId=<uuid>                │
  │  body: {"method":"tools/list","id":2}          │
  │ ─────────────────────────────────────────────▶│
  │  ◀── HTTP 202 Accepted ─────────────────────  │
  │                                                │
  │  event: message                                │
  │  data: {"id":2,"result":{"tools":[...]}}       │
  │ ◀─────────────────────────────────────────────│  ← over SSE stream
  │                                                │
  │  POST /message?sessionId=<uuid>                │
  │  body: {"method":"tools/call","id":3,...}      │
  │ ─────────────────────────────────────────────▶│
  │                                                │
  │  event: message                                │
  │  data: {"id":3,"result":{...}}                 │
  │ ◀─────────────────────────────────────────────│
```

Key differences from stdio:

- Every request requires a full HTTP POST roundtrip (with its own TCP handshake if not kept alive).
- Every response travels over the separate SSE stream.
- The `sessionId` is what ties requests to their response stream.
- The `notifications/initialized` POST returns HTTP 202 but produces no SSE event (because `dispatch()` returns `None` for notifications, and `None` is not enqueued).

---

## 7. Module walkthrough

### 7.1 Module-level state

```python
HOST = "localhost"
PORT = 8000

sessions: dict[str, queue.Queue] = {}
sessions_lock = threading.Lock()
```

`HOST` and `PORT` control where the server binds. Change `"localhost"` to `"0.0.0.0"` to accept connections from other machines on the network.

`sessions` and `sessions_lock` are the only shared mutable state in the entire server. Everything else is either immutable (`TOOLS`) or thread-local (handler instance variables).

### 7.2 `call_tool` — the executor

```python
def call_tool(name: str, args: dict) -> str:
    a, b = args["a"], args["b"]
    match name:
        case "add":      return str(a + b)
        case "subtract": return str(a - b)
        case "multiply": return str(a * b)
        case _:          raise ValueError(f"Unknown tool: {name}")
```

Identical in purpose to the stdio version. Uses Python 3.10's `match/case` (structural pattern matching) instead of `if/elif` — functionally equivalent, slightly more readable for dispatch tables.

`call_tool` is called from `dispatch()`, which is called from `do_POST()`. It runs in a POST thread. Since `call_tool` does not touch any shared state, it is thread-safe without any locking.

### 7.3 `dispatch` — JSON-RPC dispatcher

```python
def dispatch(req: dict) -> dict | None:
    method = req.get("method", "")
    req_id = req.get("id")

    def ok(result):
        return {"jsonrpc": "2.0", "id": req_id, "result": result}

    def error(code, msg):
        return {"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": msg}}

    if method == "initialize":
        return ok({...})
    if method == "notifications/initialized":
        return None
    if method == "tools/list":
        return ok({"tools": TOOLS})
    if method == "tools/call":
        ...
    if req_id is not None:
        return error(-32601, f"Method not found: {method}")
    return None
```

Key difference from the stdio `handle()` function: `dispatch()` **returns** a dict instead of writing directly to stdout. This is because in the SSE transport, the response must travel via the queue to the GET thread — it cannot be written directly from the POST thread. The caller (`do_POST`) decides whether to enqueue the response.

`ok` and `error` are **closures** — inner functions that capture `req_id` from the enclosing scope. This avoids passing `req_id` as a parameter to every helper call.

`return None` for notifications tells `do_POST` not to enqueue anything. A notification generates no SSE event.

### 7.4 `MCPHandler.do_GET` — the SSE stream

```python
def do_GET(self):
    if not self.path.startswith("/sse"):
        self._send_plain(404, "Not found")
        return

    session_id = str(uuid.uuid4())
    q: queue.Queue = queue.Queue()
    with sessions_lock:
        sessions[session_id] = q

    self.send_response(200)
    self.send_header("Content-Type",  "text/event-stream")
    self.send_header("Cache-Control", "no-cache")
    self.send_header("Connection",    "keep-alive")
    self.send_header("Access-Control-Allow-Origin", "*")
    self.end_headers()

    def sse(event: str, data: str):
        payload = f"event: {event}\ndata: {data}\n\n"
        self.wfile.write(payload.encode())
        self.wfile.flush()

    post_url = f"http://{HOST}:{PORT}/message?sessionId={session_id}"
    sse("endpoint", post_url)

    try:
        while True:
            msg = q.get()
            if msg is None:
                break
            sse("message", json.dumps(msg))
    except (BrokenPipeError, ConnectionResetError):
        pass
    finally:
        with sessions_lock:
            sessions.pop(session_id, None)
```

This is the most complex function in the file. Step by step:

**Path check** — returns 404 for any path other than `/sse`. Keeps the server from accidentally serving other paths.

**Session creation** — `uuid.uuid4()` generates a cryptographically random UUID. There is no sequential ID counter — UUIDs cannot be guessed or enumerated by a client. The queue is created and registered in the session store under the lock.

**HTTP headers** — four headers are required for SSE:

| Header | Value | Why |
|---|---|---|
| `Content-Type` | `text/event-stream` | Tells the client to parse this as SSE |
| `Cache-Control` | `no-cache` | Prevents proxies from buffering the stream |
| `Connection` | `keep-alive` | Keeps the TCP connection open |
| `Access-Control-Allow-Origin` | `*` | Allows browser clients from any origin |

`self.end_headers()` sends the HTTP response line and all headers. After this, `self.wfile` is the raw TCP stream — writing to it sends bytes directly to the client.

**`sse()` inner function** — encapsulates the SSE wire format. Called twice: once for the `endpoint` event, then once per response in the drain loop.

**`endpoint` event** — the MCP spec requires this as the very first SSE event. It tells the client the exact URL to use for POST requests, including the session ID.

**Drain loop** — `q.get()` blocks until the POST thread enqueues a response. When a response arrives, it is serialised to JSON and sent as an SSE `message` event. The loop runs forever until one of two things happens:

1. `q.get()` returns `None` (explicit shutdown sentinel — not used in this implementation)
2. `self.wfile.write()` raises `BrokenPipeError` or `ConnectionResetError` (client disconnected)

**`finally` block** — runs unconditionally when the loop exits (whether by sentinel, exception, or any other cause). Removes the session from the store so POST requests to this session ID return 400 rather than blocking on a queue nobody reads.

### 7.5 `MCPHandler.do_POST` — the RPC receiver

```python
def do_POST(self):
    parsed = urlparse(self.path)
    if not parsed.path.startswith("/message"):
        self._send_plain(404, "Not found")
        return

    qs = parse_qs(parsed.query)
    session_id = (qs.get("sessionId") or [None])[0]

    with sessions_lock:
        q = sessions.get(session_id)

    if q is None:
        self._send_plain(400, f"Unknown sessionId: {session_id}")
        return

    length = int(self.headers.get("Content-Length", 0))
    body   = self.rfile.read(length)
    try:
        req = json.loads(body)
    except json.JSONDecodeError as exc:
        self._send_plain(400, f"Bad JSON: {exc}")
        return

    response = dispatch(req)
    if response is not None:
        q.put(response)

    self._send_plain(202, "Accepted")
```

Step by step:

**Path and query parsing** — `urlparse` splits `/message?sessionId=abc` into path and query string. `parse_qs` parses the query string into a dict of lists (each key maps to a list of values). `(qs.get("sessionId") or [None])[0]` safely extracts the first value of `sessionId`, or `None` if absent.

**Session lookup** — the lock ensures the dict read is atomic relative to GET thread writes. `sessions.get(session_id)` returns `None` if the session does not exist (client never opened SSE, or already disconnected). A missing session returns HTTP 400.

**Body read** — `Content-Length` must be present for the server to know how many bytes to read. If absent, `int(... or 0)` defaults to 0, and `rfile.read(0)` returns an empty bytes object, which `json.loads` will reject.

**Dispatch and enqueue** — `dispatch()` runs synchronously in the POST thread. If it returns a dict, that dict is put on the queue. The GET thread — which is blocking on `q.get()` — wakes up and streams the response over SSE. If `dispatch()` returns `None` (notification), nothing is enqueued and no SSE event is sent.

**HTTP 202** — the POST response is always `202 Accepted`, regardless of whether the tool call succeeded or failed. The tool result (including errors) travels over SSE, not over the POST response body. The `202` merely acknowledges receipt of the request.

### 7.6 `MCPHandler.do_OPTIONS` — CORS preflight

```python
def do_OPTIONS(self):
    self.send_response(204)
    self.send_header("Access-Control-Allow-Origin",  "*")
    self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
    self.send_header("Access-Control-Allow-Headers", "Content-Type")
    self.end_headers()
```

Browsers send an HTTP `OPTIONS` request before cross-origin POST requests (the "CORS preflight"). This handler returns the necessary `Access-Control-*` headers to approve the preflight, allowing browser-based MCP clients to connect.

`204 No Content` is the conventional response for a successful CORS preflight — it has no body.

Without this handler, browser clients silently fail to connect and log a CORS error to the console.

### 7.7 `MCPHandler._send_plain` — HTTP helper

```python
def _send_plain(self, code: int, text: str):
    body = text.encode()
    self.send_response(code)
    self.send_header("Content-Type",   "text/plain")
    self.send_header("Content-Length", str(len(body)))
    self.send_header("Access-Control-Allow-Origin", "*")
    self.end_headers()
    self.wfile.write(body)
```

A DRY helper for sending short HTTP responses with a text body. Used for:
- `202 Accepted` after every POST
- `400 Bad Request` for unknown session ID or invalid JSON
- `404 Not Found` for unrecognised paths

`Content-Length` is required so the HTTP client knows how many bytes to read. Without it, clients may hang waiting for more data.

### 7.8 `main` — server startup

```python
def main():
    from http.server import ThreadingHTTPServer
    server = ThreadingHTTPServer((HOST, PORT), MCPHandler)
    print(f"MCP SSE server running on http://{HOST}:{PORT}", flush=True)
    server.serve_forever()
```

`ThreadingHTTPServer` is imported inside `main()` — a minor style choice to keep the top-level imports lean.

`serve_forever()` enters a select/poll loop, accepting connections and spawning handler threads. It blocks indefinitely. Send `SIGINT` (Ctrl+C) to stop.

---

## 8. HTTP endpoint reference

### `GET /sse`

Opens a persistent SSE stream for the client.

**Request:**

```http
GET /sse HTTP/1.1
Host: localhost:8000
Accept: text/event-stream
```

**Response:**

```http
HTTP/1.1 200 OK
Content-Type: text/event-stream
Cache-Control: no-cache
Connection: keep-alive
Access-Control-Allow-Origin: *

event: endpoint
data: http://localhost:8000/message?sessionId=f47ac10b-58cc-4372-a567-0e02b2c3d479

event: message
data: {"jsonrpc":"2.0","id":1,"result":{...}}

event: message
data: {"jsonrpc":"2.0","id":2,"result":{...}}

```

The response never ends while the client is connected. New `event: message` blocks appear as POST requests are processed.

---

### `POST /message?sessionId=<uuid>`

Sends one JSON-RPC request to the server. The response arrives over the SSE stream.

**Request:**

```http
POST /message?sessionId=f47ac10b-58cc-4372-a567-0e02b2c3d479 HTTP/1.1
Host: localhost:8000
Content-Type: application/json
Content-Length: 89

{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"add","arguments":{"a":7,"b":5}}}
```

**Response:**

```http
HTTP/1.1 202 Accepted
Content-Type: text/plain
Content-Length: 8
Access-Control-Allow-Origin: *

Accepted
```

The `202` body is informational only. The actual JSON-RPC result appears on the SSE stream.

---

### `OPTIONS /message` (CORS preflight)

**Response:**

```http
HTTP/1.1 204 No Content
Access-Control-Allow-Origin: *
Access-Control-Allow-Methods: GET, POST, OPTIONS
Access-Control-Allow-Headers: Content-Type
```

---

## 9. SSE event reference

### `endpoint` event

Sent once, immediately after the SSE connection is established. Contains the POST URL the client must use for all subsequent requests.

```
event: endpoint
data: http://localhost:8000/message?sessionId=<uuid>

```

### `message` event

Sent once per JSON-RPC response. Contains the full JSON-RPC response object.

```
event: message
data: {"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2024-11-05","capabilities":{"tools":{}},"serverInfo":{"name":"math-sse-server","version":"0.1.0"}}}

```

Notifications (`notifications/initialized`) produce no `message` event — they are fire-and-forget with no response.

---

## 10. Error handling

### Session not found (POST)

**Trigger:** `sessionId` query parameter is missing, or refers to a session that has already closed.  
**HTTP response:** `400 Bad Request`  
**Body:** `Unknown sessionId: <value>`  
**SSE effect:** none

### Invalid JSON (POST)

**Trigger:** request body is not valid JSON.  
**HTTP response:** `400 Bad Request`  
**Body:** `Bad JSON: <json error message>`  
**SSE effect:** none

### Unknown method (dispatch)

**Trigger:** `method` not in the dispatcher's if chain, and `id` is present.  
**HTTP response:** `202 Accepted` (POST always returns 202)  
**SSE event:** `message` with JSON-RPC error body

```json
{"jsonrpc":"2.0","id":5,"error":{"code":-32601,"message":"Method not found: foo"}}
```

### Tool execution failure (dispatch)

**Trigger:** `call_tool()` raises an exception.  
**HTTP response:** `202 Accepted`  
**SSE event:** `message` with `isError: true`

```json
{"jsonrpc":"2.0","id":3,"result":{"content":[{"type":"text","text":"Unknown tool: divide"}],"isError":true}}
```

### Client disconnect (GET)

**Trigger:** client closes the TCP connection (browser tab closed, process killed, network drop).  
**Effect:** `self.wfile.write()` raises `BrokenPipeError` or `ConnectionResetError`.  
**Handler:** caught in `except (BrokenPipeError, ConnectionResetError): pass`.  
**Cleanup:** `finally` block removes session from store.

### Unhandled path

**Trigger:** any path other than `/sse` (GET) or `/message` (POST).  
**HTTP response:** `404 Not Found`

---

## 11. Data flow diagrams

### Full request lifecycle

```
Client POSTs to /message?sessionId=X
        │
        ▼
do_POST() runs in POST thread
        │
        ├── urlparse + parse_qs → session_id = "X"
        │
        ├── sessions_lock acquired
        │     q = sessions.get("X")          ← lookup queue
        │   sessions_lock released
        │
        ├── rfile.read(Content-Length) → body bytes
        │
        ├── json.loads(body) → req dict
        │
        ├── dispatch(req) → response dict (or None)
        │
        ├── if response: q.put(response)     ← enqueue
        │
        └── _send_plain(202, "Accepted")     ← HTTP response sent
                                              POST thread exits

Meanwhile, GET thread is blocking on q.get()
        │
        ▼
q.get() returns response dict               ← wakes up
        │
        ▼
sse("message", json.dumps(response))
        │
        ▼
self.wfile.write(payload.encode())          ← bytes on TCP wire
self.wfile.flush()
        │
        ▼
Client receives SSE event
```

### Session creation and teardown

```
Client opens GET /sse
        │
        ▼
do_GET() thread starts
uuid.uuid4() → session_id
queue.Queue() → q
sessions[session_id] = q  (under lock)
        │
        ▼
HTTP 200 headers sent
sse("endpoint", post_url)
        │
        ▼
while True: q.get()  ← blocking
        │
        │   ... (requests served) ...
        │
        ▼
Client closes connection
BrokenPipeError raised in wfile.write()
except clause: pass
        │
        ▼
finally:
    sessions.pop(session_id)  (under lock)
do_GET() thread exits
```

---

## 12. Comparison with stdio transport

| Aspect | stdio (`mcp_server.py`) | SSE (`mcp_server_sse.py`) |
|---|---|---|
| **Transport** | Process stdin/stdout | HTTP (TCP) |
| **Client location** | Same machine, same process tree | Any machine with network access |
| **Concurrent clients** | One (the spawning host) | Many (one thread pair per client) |
| **Response channel** | stdout (same as request channel) | SSE stream (separate from POST) |
| **Session concept** | Implicit (process lifetime) | Explicit (UUID, stored in dict) |
| **Threading** | Single-threaded | Multi-threaded (one thread per connection) |
| **Shared state** | None | `sessions` dict + `sessions_lock` |
| **Client disconnect** | stdin EOF → loop exits | BrokenPipeError → finally block |
| **Debug logging** | stderr | stderr or stdout (prefixed `[http]`) |
| **Protocol version** | 2024-11-05 | 2024-11-05 |
| **Browser compatible** | No | Yes (CORS headers included) |

The dispatcher logic (`dispatch` vs `handle`) differs only in return style: stdio writes directly to stdout; SSE returns a dict that the caller enqueues. The MCP method handling — `initialize`, `tools/list`, `tools/call` — is identical.

---

## 13. Extending the server

### Accept connections from other machines

```python
HOST = "0.0.0.0"   # bind all interfaces instead of localhost only
PORT = 8000
```

With `0.0.0.0`, clients on other machines can connect using the server's IP address.

### Add a keepalive ping

Proxies and load balancers often close idle HTTP connections after a timeout. A periodic SSE comment keeps the connection alive:

```python
import time, select

# In the drain loop inside do_GET:
while True:
    ready = select.select([q._reader], [], [], 30.0)  # 30s timeout
    if ready[0]:
        msg = q.get_nowait()
        if msg is None:
            break
        sse("message", json.dumps(msg))
    else:
        # Keepalive comment
        self.wfile.write(b": ping\n\n")
        self.wfile.flush()
```

A cleaner approach uses `queue.Queue.get(timeout=30)`:

```python
while True:
    try:
        msg = q.get(timeout=30)
        if msg is None:
            break
        sse("message", json.dumps(msg))
    except queue.Empty:
        self.wfile.write(b": ping\n\n")
        self.wfile.flush()
```

### Add authentication

Validate a token on the GET request before registering the session:

```python
def do_GET(self):
    token = self.headers.get("Authorization", "")
    if token != "Bearer my-secret-token":
        self._send_plain(401, "Unauthorized")
        return
    # ... rest of do_GET
```

### Add resources

In `dispatch()`, add the two resource handlers:

```python
if method == "resources/list":
    return ok({
        "resources": [
            {
                "uri":         "modbus://plc1/holding-registers",
                "name":        "Holding registers",
                "description": "Current values of all holding registers",
                "mimeType":    "application/json",
            }
        ]
    })

if method == "resources/read":
    uri = req.get("params", {}).get("uri", "")
    data = read_modbus_registers(uri)   # your logic here
    return ok({
        "contents": [{"uri": uri, "mimeType": "application/json", "text": data}]
    })
```

Also declare `"resources": {}` in the `initialize` response capabilities.

---

## 14. Limitations of SSE transport

| Limitation | Detail |
|---|---|
| **No streaming tool responses** | A tool call must complete before the result is enqueued. The SSE channel can stream multiple responses but not a single response incrementally. |
| **Session state is in-memory** | If the server process restarts, all sessions are lost. Clients must reconnect and reinitialise. |
| **No reconnect handling** | If the SSE connection drops and the client reconnects, it gets a new session ID. In-flight requests from the old session are lost. The MCP spec defines a `Last-Event-ID` reconnect mechanism — not implemented here. |
| **Thread-per-connection scaling** | Each connected client holds a thread in `q.get()` indefinitely. Python threads have ~8 MB stack by default. With 1000 clients this is ~8 GB of stack space. For high concurrency, use an async framework (`asyncio`, `aiohttp`). |
| **No TLS** | `http.server` does not support HTTPS natively. For production, place a reverse proxy (nginx, caddy) in front and let it handle TLS termination. |
| **MCP spec version** | The SSE transport is defined in spec version `2024-11-05`. The newer `2025-03-26` spec deprecates SSE in favour of Streamable HTTP (`mcp_server_http.py`). |

---

## 15. Testing and debugging

### Manual smoke test — three terminals

**Terminal 1 — start the server:**

```bash
python mcp_server_sse.py
# MCP SSE server running on http://localhost:8000
```

**Terminal 2 — open the SSE stream (keep this running):**

```bash
curl -N http://localhost:8000/sse
# event: endpoint
# data: http://localhost:8000/message?sessionId=<uuid>
```

Copy the `sessionId` from the `data:` line.

**Terminal 3 — send requests (replace `<id>` with the UUID):**

```bash
# Initialize
curl -s -X POST "http://localhost:8000/message?sessionId=<id>" \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"0"}}}'

# Acknowledge
curl -s -X POST "http://localhost:8000/message?sessionId=<id>" \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"notifications/initialized"}'

# List tools
curl -s -X POST "http://localhost:8000/message?sessionId=<id>" \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/list"}'

# Call add
curl -s -X POST "http://localhost:8000/message?sessionId=<id>" \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"add","arguments":{"a":7,"b":5}}}'

# Call multiply
curl -s -X POST "http://localhost:8000/message?sessionId=<id>" \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"multiply","arguments":{"a":6,"b":7}}}'
```

POST responses appear in Terminal 3 (just `Accepted`). JSON-RPC results appear in Terminal 2 as SSE events.

### Testing error paths

```bash
# Unknown session ID
curl -s -X POST "http://localhost:8000/message?sessionId=bad-id" \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'
# → HTTP 400: Unknown sessionId: bad-id

# Invalid JSON body
curl -s -X POST "http://localhost:8000/message?sessionId=<id>" \
  -H "Content-Type: application/json" \
  -d 'not json'
# → HTTP 400: Bad JSON: ...

# Unknown tool (result appears on SSE stream, not in curl output)
curl -s -X POST "http://localhost:8000/message?sessionId=<id>" \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"divide","arguments":{"a":10,"b":2}}}'
# SSE stream: event: message
#             data: {"jsonrpc":"2.0","id":5,"result":{"content":[{"type":"text","text":"Unknown tool: divide"}],"isError":true}}
```

### Connect to Claude Desktop

```json
{
  "mcpServers": {
    "math-sse": {
      "url": "http://localhost:8000/sse"
    }
  }
}
```

Start the server before launching Claude Desktop. The server must be running when Claude Desktop starts — it connects to the SSE endpoint during startup.

### Logging

The server prints connection events to stdout (not stderr, since stdout is not reserved for protocol data as it is in the stdio transport):

```
[http] "GET /sse HTTP/1.1" 200 -
[sse] new session f47ac10b-58cc-4372-a567-0e02b2c3d479
[http] "POST /message?sessionId=f47ac10b... HTTP/1.1" 202 -
[sse] session f47ac10b-58cc-4372-a567-0e02b2c3d479 closed
```

Add debug logging to `dispatch()` or `call_tool()` using `print(..., flush=True)` — the output appears in the server terminal.

---

*MCP SSE transport spec: https://modelcontextprotocol.io/specification/2024-11-05/basic/transports*  
*SSE specification (WHATWG): https://html.spec.whatwg.org/multipage/server-sent-events.html*  
*JSON-RPC 2.0 specification: https://www.jsonrpc.org/specification*  
*Python threading docs: https://docs.python.org/3/library/threading.html*  
*Python queue docs: https://docs.python.org/3/library/queue.html*
