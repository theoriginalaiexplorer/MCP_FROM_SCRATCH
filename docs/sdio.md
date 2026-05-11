# `mcp_server.py` — Technical Deep Dive

**Transport:** stdio (newline-delimited JSON-RPC 2.0)  
**MCP Spec version:** 2024-11-05  
**Python:** 3.10+  
**Dependencies:** none — standard library only (`json`, `sys`)

---

## Table of contents

1. [Overview](#1-overview)
2. [Architecture](#2-architecture)
3. [Transport layer — stdio](#3-transport-layer--stdio)
4. [Protocol layer — JSON-RPC 2.0](#4-protocol-layer--json-rpc-20)
5. [MCP handshake sequence](#5-mcp-handshake-sequence)
6. [Module walkthrough](#6-module-walkthrough)
   - 6.1 [TOOLS — the capability manifest](#61-tools--the-capability-manifest)
   - 6.2 [call_tool — the executor](#62-call_tool--the-executor)
   - 6.3 [send / ok / err — JSON-RPC helpers](#63-send--ok--err--json-rpc-helpers)
   - 6.4 [handle — the request dispatcher](#64-handle--the-request-dispatcher)
   - 6.5 [main — the read loop](#65-main--the-read-loop)
7. [Method reference](#7-method-reference)
8. [Error handling](#8-error-handling)
9. [Data flow diagrams](#9-data-flow-diagrams)
10. [Extending the server](#10-extending-the-server)
11. [Limitations of stdio transport](#11-limitations-of-stdio-transport)
12. [Testing and debugging](#12-testing-and-debugging)

---

## 1. Overview

`mcp_server.py` implements the **stdio transport** of the Model Context Protocol (MCP). It is the simplest possible MCP server: a single Python process that reads JSON-RPC requests from `stdin` and writes JSON-RPC responses to `stdout`, one object per line.

The host process (Claude Desktop, a CLI agent, or any MCP client) spawns this server as a **subprocess**. Communication happens entirely through the process's standard streams — no network socket, no HTTP server, no threads.

```
Host process
│
├── spawns ──────────────────────▶  python mcp_server.py
│                                         │
│   stdin  ──── JSON-RPC request ────────▶│
│   stdout ◀─── JSON-RPC response ────────│
│                                         │
└── kills when done ──────────────────────▶ (process exits)
```

The server exposes three tools — `add`, `subtract`, `multiply` — as a minimal but complete reference implementation. Every line of code maps directly to a concept in the MCP specification.

---

## 2. Architecture

The file has five distinct layers, each with a single responsibility:

```
┌─────────────────────────────────────────────────────────┐
│  main()          Read loop — iterates stdin line by line │
├─────────────────────────────────────────────────────────┤
│  handle()        Dispatcher — routes method to handler   │
├─────────────────────────────────────────────────────────┤
│  ok() / err()    JSON-RPC response builders              │
├─────────────────────────────────────────────────────────┤
│  send()          Transport — writes to stdout            │
├─────────────────────────────────────────────────────────┤
│  TOOLS / call_tool()   Domain — tools and their logic    │
└─────────────────────────────────────────────────────────┘
```

Each layer only calls the layer below it. There is no shared mutable state between layers. This makes the server trivially testable — you can unit-test `handle()` by passing a dict directly, without touching stdin or stdout.

---

## 3. Transport layer — stdio

### What stdio transport means

The MCP spec defines three transports. stdio is the oldest and most local:

- **stdin** carries requests from the host to the server (newline-delimited JSON)
- **stdout** carries responses from the server to the host (newline-delimited JSON)
- **stderr** is free for debug logging — the host ignores it

### Wire format

Every message is a single JSON object on a single line, terminated by `\n`. There is no length prefix, no framing byte, no envelope. The newline is the only delimiter.

```
{"jsonrpc":"2.0","id":1,"method":"initialize","params":{...}}\n
{"jsonrpc":"2.0","id":1,"result":{...}}\n
```

### Why newline-delimited JSON (NDJSON)?

- Simple to parse — `json.loads(line)` per line, no streaming JSON parser needed
- Simple to produce — `json.dumps(obj) + "\n"` per message
- Works naturally with line-buffered I/O
- Trivially testable with `echo` and `curl`

### stdout flush discipline

Every write to stdout is followed by an explicit `sys.stdout.flush()`. This is critical. Python buffers stdout by default when it detects that the output is not a terminal (i.e. when stdout is piped to another process). Without the flush, responses accumulate in the buffer and the host blocks indefinitely waiting for data that has been written but not sent.

```python
def send(obj: dict) -> None:
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()   # ← mandatory — do not remove
```

### stderr for debug logging

Since stdout is reserved for protocol messages, any debug output must go to stderr:

```python
import sys
print("debug info", file=sys.stderr)
```

The host process does not read stderr, so it will not interfere with the protocol. Claude Desktop surfaces stderr in its logs.

---

## 4. Protocol layer — JSON-RPC 2.0

MCP uses JSON-RPC 2.0 as its message format. Every message is one of three types:

### Request (host → server, expects a response)

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "tools/list",
  "params": {}
}
```

| Field | Type | Description |
|---|---|---|
| `jsonrpc` | string | Always exactly `"2.0"` |
| `id` | string or number | Unique identifier; echoed back in the response |
| `method` | string | The operation to perform |
| `params` | object or array | Arguments for the method (optional) |

### Response (server → host, in reply to a request)

**Success:**

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": { ... }
}
```

**Error:**

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "error": {
    "code": -32601,
    "message": "Method not found: foo"
  }
}
```

| Field | Type | Description |
|---|---|---|
| `jsonrpc` | string | Always `"2.0"` |
| `id` | string or number | Must match the `id` from the request |
| `result` | object | Present on success; mutually exclusive with `error` |
| `error` | object | Present on failure; has `code` (integer) and `message` (string) |

### Notification (either direction, no response expected)

```json
{
  "jsonrpc": "2.0",
  "method": "notifications/initialized"
}
```

A notification has no `id` field. The receiver must not send a response. In `handle()`, notifications are detected by checking `req.get("id")` — if `id` is absent (returns `None`), the message is a notification.

### Standard JSON-RPC error codes

| Code | Name | Used when |
|---|---|---|
| `-32700` | Parse error | `json.loads()` fails |
| `-32600` | Invalid request | Message is not a valid JSON-RPC object |
| `-32601` | Method not found | `method` is not recognised |
| `-32602` | Invalid params | Arguments are wrong type or missing |
| `-32603` | Internal error | Unexpected server-side exception |

---

## 5. MCP handshake sequence

Every MCP session begins with a mandatory three-step handshake before any tool calls are allowed.

```
Host                                    Server
 │                                         │
 │  {"method":"initialize",                │
 │   "id":1,                              │
 │   "params":{                           │
 │     "protocolVersion":"2024-11-05",    │
 │     "capabilities":{},                 │
 │     "clientInfo":{                     │
 │       "name":"claude-desktop",         │
 │       "version":"1.0"}}}               │
 │ ──────────────────────────────────────▶│
 │                                         │
 │  {"id":1,"result":{                    │  ← server declares what it supports
 │    "protocolVersion":"2024-11-05",     │
 │    "capabilities":{"tools":{}},        │
 │    "serverInfo":{                      │
 │      "name":"math-server",             │
 │      "version":"0.1.0"}}}              │
 │ ◀──────────────────────────────────────│
 │                                         │
 │  {"method":"notifications/initialized"}│  ← notification, no response
 │ ──────────────────────────────────────▶│
 │                                         │
 │           [session is ready]            │
 │                                         │
 │  {"method":"tools/list","id":2}        │
 │ ──────────────────────────────────────▶│
 │                                         │
 │  {"id":2,"result":{"tools":[...]}}     │
 │ ◀──────────────────────────────────────│
 │                                         │
 │  {"method":"tools/call","id":3,        │
 │   "params":{"name":"add",              │
 │             "arguments":{"a":3,"b":4}}}│
 │ ──────────────────────────────────────▶│
 │                                         │
 │  {"id":3,"result":{                    │
 │    "content":[{"type":"text",          │
 │                "text":"7"}],           │
 │    "isError":false}}                   │
 │ ◀──────────────────────────────────────│
```

### Step 1 — `initialize`

The host sends its protocol version and capabilities. The server responds with its own protocol version, capabilities, and identity. If the versions are incompatible, the server should return an error and the session ends.

In this implementation the version is returned verbatim (`"2024-11-05"`) without negotiation — acceptable for a minimal server.

### Step 2 — `notifications/initialized`

The host acknowledges that it received the server's `initialize` response and is ready to proceed. This is a **notification** — the server must not reply. In `handle()` this is handled by `pass`.

### Step 3 — `tools/list`

The host fetches the tool manifest. This is not strictly part of the handshake but always follows it. The host uses this manifest to build the LLM's tool calling context.

---

## 6. Module walkthrough

### 6.1 `TOOLS` — the capability manifest

```python
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
    ...
]
```

`TOOLS` is a module-level list of dicts. It is defined once at import time and never mutated at runtime. Each entry has three fields required by the MCP spec:

**`name`** — the tool identifier. The LLM uses this to refer to the tool in a `tools/call` request. Must be unique within the server. Should be lowercase with underscores for consistency.

**`description`** — natural language description consumed by the LLM. This is the only thing the LLM reads to decide when and why to call the tool. A vague description produces incorrect or missed calls. A precise description produces reliable calls.

**`inputSchema`** — a [JSON Schema](https://json-schema.org/) object describing the arguments. It has two consumers:

- the **host** uses it to validate the LLM's argument construction before calling the tool
- the **LLM** uses the `description` on each property to know what values to provide

The `required` array lists property names that must be present. Properties not in `required` are optional — the LLM may or may not include them.

#### inputSchema field breakdown

| Field | Required | Description |
|---|---|---|
| `type` | yes | Always `"object"` at the top level |
| `properties` | yes | Dict of argument name → JSON Schema |
| `required` | yes | List of mandatory argument names |

Each property schema supports standard JSON Schema keywords:

| Keyword | Description |
|---|---|
| `type` | `"number"`, `"string"`, `"boolean"`, `"array"`, `"object"` |
| `description` | Natural language hint for the LLM |
| `enum` | List of allowed values |
| `minimum` / `maximum` | Numeric bounds |
| `default` | Default value if not provided |

### 6.2 `call_tool` — the executor

```python
def call_tool(name: str, args: dict) -> str:
    a, b = args["a"], args["b"]
    if name == "add":
        return str(a + b)
    if name == "subtract":
        return str(a - b)
    if name == "multiply":
        return str(a * b)
    raise ValueError(f"Unknown tool: {name}")
```

`call_tool` is the domain layer. It receives the tool name and the arguments dict (already parsed from JSON), executes the operation, and returns a string result.

**Return type is always `str`.** MCP tool results are text content. If your tool returns a number, dict, or list, convert it to a string (or JSON string) before returning.

**Raising exceptions is intentional.** If `name` is unknown, `ValueError` is raised. In `handle()`, this is caught and returned to the host as a tool error (`isError: True`) rather than a protocol error. The session continues normally — a bad tool call does not terminate the connection.

**`args["a"]` and `args["b"]` are already Python numbers.** `json.loads()` converts JSON numbers to Python `int` or `float` automatically. No manual type conversion is needed.

### 6.3 `send` / `ok` / `err` — JSON-RPC helpers

```python
def send(obj: dict) -> None:
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()

def ok(req_id, result: dict) -> None:
    send({"jsonrpc": "2.0", "id": req_id, "result": result})

def err(req_id, code: int, message: str) -> None:
    send({"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": message}})
```

Three functions form the complete response API:

**`send(obj)`** — the only function that touches `stdout`. All output flows through here. The `+ "\n"` ensures each response is on its own line. The `flush()` ensures the host receives it immediately.

**`ok(req_id, result)`** — builds a success response. `req_id` is echoed from the request's `id` field. The host uses this to match responses to the requests that generated them (important when a client sends requests concurrently — not applicable to this single-threaded server, but required by the spec).

**`err(req_id, code, message)`** — builds an error response. `req_id` may be `None` if the error occurs before the request's `id` can be parsed (e.g. a JSON parse error). The spec permits `null` as the `id` in this case.

### 6.4 `handle` — the request dispatcher

```python
def handle(req: dict) -> None:
    method = req.get("method", "")
    req_id = req.get("id")

    if method == "initialize":
        ok(req_id, { ... })

    elif method == "notifications/initialized":
        pass

    elif method == "tools/list":
        ok(req_id, {"tools": TOOLS})

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

    else:
        if req_id is not None:
            err(req_id, -32601, f"Method not found: {method}")
```

`handle()` is the central dispatcher. Key design decisions:

**`req.get("id")` returns `None` for notifications.** JSON-RPC notifications have no `id` field. `.get()` returns `None` when the key is absent. This `None` propagates into the `ok()` response as `"id": null`, which is spec-compliant for error responses and never reached for valid notifications (which hit the `pass` branch).

**`tools/call` catches all exceptions as tool errors, not protocol errors.** There is an important distinction in MCP:

- A **protocol error** (`err()`) means the request was malformed — wrong method name, bad JSON. The session may be compromised.
- A **tool error** (`ok()` with `"isError": true`) means the tool ran but the operation failed — unknown tool name, invalid argument values. The session continues normally and the LLM can retry or try a different approach.

The `try/except` in `tools/call` handling ensures all tool failures are reported as tool errors, not as protocol errors.

**`tools/call` response content format:**

```python
{
    "content": [{"type": "text", "text": result}],
    "isError": False,
}
```

`content` is a list of content blocks. The MCP spec supports `type: "text"`, `type: "image"`, and `type: "resource"`. For text results, a single block with `type: "text"` is always sufficient.

**Unknown methods send an error only if `req_id` is not None.** If an unknown method arrives with no `id`, it is a notification — the spec forbids responding to notifications, even with an error.

### 6.5 `main` — the read loop

```python
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
```

`main()` is the event loop. It is deliberately simple:

**`for raw_line in sys.stdin`** — iterates stdin line by line, blocking until data arrives. When the host closes stdin (e.g. Claude Desktop shuts down), the iterator ends and the process exits cleanly.

**`raw_line.strip()`** — removes the trailing `\n` and any leading/trailing whitespace. Blank lines are skipped with `continue`. This makes the server robust to extra newlines from test scripts.

**`json.loads(line)`** — parses one JSON object. If the line is not valid JSON, a `json.JSONDecodeError` is caught and a parse error response (`-32700`) is sent. The loop then `continue`s to the next line — a single malformed message does not crash the server.

**`handle(req)`** — delegates to the dispatcher. No exception handling here: if `handle()` raises unexpectedly, the exception propagates to the `for` loop and terminates the server. In production you would wrap this in an additional `try/except` to log and continue.

---

## 7. Method reference

### `initialize`

**Direction:** host → server  
**Type:** request (expects response)

**Request params:**

```json
{
  "protocolVersion": "2024-11-05",
  "capabilities": {},
  "clientInfo": {
    "name": "claude-desktop",
    "version": "1.0.0"
  }
}
```

**Response result:**

```json
{
  "protocolVersion": "2024-11-05",
  "capabilities": {
    "tools": {}
  },
  "serverInfo": {
    "name": "math-server",
    "version": "0.1.0"
  }
}
```

`capabilities` declares which MCP feature categories this server supports. An empty object `{}` means the category is supported without sub-features. This server declares `tools` only — no `prompts` or `resources`.

---

### `notifications/initialized`

**Direction:** host → server  
**Type:** notification (no response)

Sent after the host receives and processes the `initialize` response. Signals that the session is fully established. The server must not reply.

---

### `tools/list`

**Direction:** host → server  
**Type:** request (expects response)

**Request params:** none (empty object or omitted)

**Response result:**

```json
{
  "tools": [
    {
      "name": "add",
      "description": "Add two numbers together.",
      "inputSchema": {
        "type": "object",
        "properties": {
          "a": {"type": "number", "description": "First operand"},
          "b": {"type": "number", "description": "Second operand"}
        },
        "required": ["a", "b"]
      }
    },
    {
      "name": "subtract",
      "description": "Subtract b from a.",
      "inputSchema": {
        "type": "object",
        "properties": {
          "a": {"type": "number", "description": "Minuend"},
          "b": {"type": "number", "description": "Subtrahend"}
        },
        "required": ["a", "b"]
      }
    },
    {
      "name": "multiply",
      "description": "Multiply two numbers.",
      "inputSchema": {
        "type": "object",
        "properties": {
          "a": {"type": "number", "description": "First factor"},
          "b": {"type": "number", "description": "Second factor"}
        },
        "required": ["a", "b"]
      }
    }
  ]
}
```

---

### `tools/call`

**Direction:** host → server  
**Type:** request (expects response)

**Request params:**

```json
{
  "name": "add",
  "arguments": {
    "a": 7,
    "b": 5
  }
}
```

**Response result (success):**

```json
{
  "content": [
    {
      "type": "text",
      "text": "12"
    }
  ],
  "isError": false
}
```

**Response result (tool error):**

```json
{
  "content": [
    {
      "type": "text",
      "text": "Unknown tool: divide"
    }
  ],
  "isError": true
}
```

Note: even a tool error returns HTTP 200 / JSON-RPC `result` (not `error`). The `isError: true` flag tells the host and LLM that the tool failed, but the session is intact.

---

## 8. Error handling

The server handles three distinct failure modes:

### Parse error (JSON invalid)

**Trigger:** `json.loads()` raises `json.JSONDecodeError`  
**Response:** JSON-RPC error with code `-32700`, `id: null`  
**Behaviour:** loop continues — next line is processed normally

```json
{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"Parse error: ..."}}
```

### Unknown method

**Trigger:** `method` not in the dispatcher's if/elif chain  
**Response:** JSON-RPC error with code `-32601`, only if `id` is present  
**Behaviour:** loop continues

```json
{"jsonrpc":"2.0","id":5,"error":{"code":-32601,"message":"Method not found: foo"}}
```

### Tool execution failure

**Trigger:** `call_tool()` raises any exception  
**Response:** JSON-RPC success with `isError: true` in result  
**Behaviour:** loop continues — this is a tool-level failure, not a protocol failure

```json
{"jsonrpc":"2.0","id":3,"result":{"content":[{"type":"text","text":"Unknown tool: divide"}],"isError":true}}
```

### What is not handled

- **Unhandled exception in `handle()`** — would propagate and terminate the process. In production, wrap `handle(req)` in a `try/except Exception` and log to stderr.
- **Malformed `tools/call` params** — if `params` is missing `name` or `arguments`, `call_tool` receives an empty string and empty dict, producing a `ValueError("Unknown tool: ")`. This surfaces as a tool error, not a crash.
- **Type errors in arguments** — if the LLM sends `{"a": "seven", "b": 5}`, the Python `+` operator will raise a `TypeError`. This is caught by the `except Exception` in the `tools/call` handler and returned as a tool error.

---

## 9. Data flow diagrams

### Complete message lifecycle

```
stdin (raw bytes)
       │
       ▼
raw_line = next(sys.stdin)          # blocking read
       │
       ▼
line = raw_line.strip()             # remove whitespace
       │
       ├── empty? ──▶ continue (skip)
       │
       ▼
req = json.loads(line)              # parse JSON
       │
       ├── JSONDecodeError? ──▶ err(None, -32700, ...) ──▶ stdout
       │
       ▼
handle(req)
       │
       ├── method == "initialize"           ──▶ ok(id, {...})  ──▶ stdout
       ├── method == "notifications/..."   ──▶ (nothing)
       ├── method == "tools/list"          ──▶ ok(id, TOOLS)   ──▶ stdout
       ├── method == "tools/call"
       │         │
       │         ├── call_tool() succeeds  ──▶ ok(id, result)  ──▶ stdout
       │         └── call_tool() raises    ──▶ ok(id, isError) ──▶ stdout
       └── unknown method (with id)        ──▶ err(id, -32601) ──▶ stdout
```

### Tool call detail

```
tools/call request arrives
       │
       ▼
params = req["params"]
name   = params["name"]          # e.g. "multiply"
args   = params["arguments"]     # e.g. {"a": 6, "b": 7}
       │
       ▼
call_tool(name, args)
       │
       ├── name == "add"      →  return str(a + b)
       ├── name == "subtract" →  return str(a - b)
       ├── name == "multiply" →  return str(a * b)
       └── unknown            →  raise ValueError
       │
       ▼
ok(req_id, {
    "content": [{"type": "text", "text": "42"}],
    "isError": False
})
       │
       ▼
stdout: {"jsonrpc":"2.0","id":3,"result":{"content":[...],"isError":false}}\n
```

---

## 10. Extending the server

### Adding a new tool

Two changes are needed — add the schema to `TOOLS`, add the logic to `call_tool`.

```python
# 1. Add to TOOLS list
{
    "name": "divide",
    "description": "Divide a by b. Returns an error if b is zero.",
    "inputSchema": {
        "type": "object",
        "properties": {
            "a": {"type": "number", "description": "Dividend"},
            "b": {"type": "number", "description": "Divisor — must not be zero"},
        },
        "required": ["a", "b"],
    },
},

# 2. Add to call_tool
if name == "divide":
    if b == 0:
        raise ValueError("Division by zero")
    return str(a / b)
```

No other code changes needed. The dispatcher, transport, and handshake are all tool-agnostic.

### Replacing math tools with real tools

```python
# Modbus register read
import pymodbus.client as mb

def call_tool(name: str, args: dict) -> str:
    if name == "read_register":
        client = mb.ModbusTcpClient(args["host"], port=args.get("port", 502))
        client.connect()
        result = client.read_holding_registers(args["address"], count=1)
        client.close()
        return str(result.registers[0])

    if name == "write_register":
        client = mb.ModbusTcpClient(args["host"], port=args.get("port", 502))
        client.connect()
        client.write_register(args["address"], args["value"])
        client.close()
        return "ok"

    raise ValueError(f"Unknown tool: {name}")
```

### Adding resources

Declare `"resources": {}` in the `initialize` response capabilities, then add two handlers to `handle()`:

```python
elif method == "resources/list":
    ok(req_id, {
        "resources": [
            {
                "uri":         "file:///data/registers.json",
                "name":        "Register snapshot",
                "description": "Current values of all Modbus holding registers",
                "mimeType":    "application/json",
            }
        ]
    })

elif method == "resources/read":
    uri = req.get("params", {}).get("uri", "")
    # fetch data for uri ...
    ok(req_id, {
        "contents": [
            {"uri": uri, "mimeType": "application/json", "text": "{ ... }"}
        ]
    })
```

### Adding prompts

Declare `"prompts": {}` in capabilities, then handle `prompts/list` and `prompts/get`:

```python
PROMPTS = [
    {
        "name": "diagnose_fault",
        "description": "Diagnose a Modbus drive fault code",
        "arguments": [
            {"name": "fault_code", "description": "Hex fault code", "required": True}
        ]
    }
]

elif method == "prompts/list":
    ok(req_id, {"prompts": PROMPTS})

elif method == "prompts/get":
    name = req.get("params", {}).get("name", "")
    args = req.get("params", {}).get("arguments", {})
    # render the prompt template with args ...
    ok(req_id, {
        "messages": [
            {"role": "user", "content": f"Diagnose fault {args.get('fault_code')} ..."}
        ]
    })
```

---

## 11. Limitations of stdio transport

Understanding the limitations helps you choose the right transport for a given deployment.

| Limitation | Detail |
|---|---|
| **Local only** | The server process must run on the same machine as the host. No remote deployment. |
| **Single client** | One host process per server process. No multiplexing. |
| **No server-initiated messages** | The server can only respond to requests. It cannot push data to the host unprompted. |
| **Process lifetime tied to session** | The server lives and dies with the host connection. No persistence between sessions without external state management. |
| **No streaming responses** | A tool call must compute its full result before responding. Incremental/streaming results require the SSE or HTTP transport. |
| **Debugging requires stderr** | You cannot use `print()` for debugging — it writes to stdout and corrupts the protocol stream. All debug output must go to `sys.stderr`. |

For remote deployment, multiple concurrent clients, or server-initiated events, use `mcp_server_sse.py` or `mcp_server_http.py` instead.

---

## 12. Testing and debugging

### Manual smoke test

The fastest way to verify the server works:

```bash
echo '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"0"}}}
{"jsonrpc":"2.0","method":"notifications/initialized"}
{"jsonrpc":"2.0","id":2,"method":"tools/list"}
{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"add","arguments":{"a":7,"b":5}}}
{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"multiply","arguments":{"a":6,"b":7}}}
{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"subtract","arguments":{"a":100,"b":58}}}' \
| python mcp_server.py
```

Expected output (one line per response, notifications produce no output):

```json
{"jsonrpc": "2.0", "id": 1, "result": {"protocolVersion": "2024-11-05", "capabilities": {"tools": {}}, "serverInfo": {"name": "math-server", "version": "0.1.0"}}}
{"jsonrpc": "2.0", "id": 2, "result": {"tools": [...]}}
{"jsonrpc": "2.0", "id": 3, "result": {"content": [{"type": "text", "text": "12"}], "isError": false}}
{"jsonrpc": "2.0", "id": 4, "result": {"content": [{"type": "text", "text": "42"}], "isError": false}}
{"jsonrpc": "2.0", "id": 5, "result": {"content": [{"type": "text", "text": "42"}], "isError": false}}
```

### Unit testing `handle()` directly

Because `handle()` writes to stdout, redirect it in tests:

```python
import io, json, sys
import mcp_server

def test_tools_list():
    captured = io.StringIO()
    sys.stdout = captured

    mcp_server.handle({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})

    sys.stdout = sys.__stdout__
    response = json.loads(captured.getvalue())

    assert response["id"] == 1
    assert len(response["result"]["tools"]) == 3
    assert response["result"]["tools"][0]["name"] == "add"
```

### Testing error paths

```bash
# Unknown method
echo '{"jsonrpc":"2.0","id":1,"method":"tools/divide"}' | python mcp_server.py
# → {"jsonrpc": "2.0", "id": 1, "error": {"code": -32601, "message": "Method not found: tools/divide"}}

# Invalid JSON
echo 'not json at all' | python mcp_server.py
# → {"jsonrpc": "2.0", "id": null, "error": {"code": -32700, "message": "Parse error: ..."}}

# Unknown tool name
echo '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"divide","arguments":{"a":10,"b":2}}}' \
| python mcp_server.py
# → {"jsonrpc": "2.0", "id": 1, "result": {"content": [{"type": "text", "text": "Unknown tool: divide"}], "isError": true}}
```

### Debug logging

Add debug output to stderr without affecting the protocol:

```python
import sys

def call_tool(name: str, args: dict) -> str:
    print(f"[debug] call_tool name={name} args={args}", file=sys.stderr)
    ...
```

Claude Desktop surfaces this in its MCP server log. In manual testing you will see it in the terminal alongside the stdout responses.

### Connecting to Claude Desktop

1. Locate the config file:
   - macOS: `~/Library/Application Support/Claude/claude_desktop_config.json`
   - Windows: `%APPDATA%\Claude\claude_desktop_config.json`

2. Add the server entry (use the absolute path to the file):

```json
{
  "mcpServers": {
    "math": {
      "command": "python",
      "args": ["/Users/yourname/projects/mcp-server-from-scratch/mcp_server.py"]
    }
  }
}
```

3. Restart Claude Desktop. The server will appear in the tools list.

4. Test it: ask Claude "what is 6 multiplied by 7?" — it should invoke the `multiply` tool and return `42`.

---

*MCP specification: https://modelcontextprotocol.io/specification/2024-11-05*  
*JSON-RPC 2.0 specification: https://www.jsonrpc.org/specification*  
*JSON Schema: https://json-schema.org/understanding-json-schema*
