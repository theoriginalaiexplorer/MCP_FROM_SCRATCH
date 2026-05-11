# mcp-server-from-scratch

Three minimal MCP (Model Context Protocol) servers built from first principles in Python.  
No `fastmcp`. No third-party libraries. Standard library only.

Each file is a self-contained server implementing the same math tools (add, subtract, multiply)  
over a different transport — so you can read them side by side and see exactly what changes.

---

## Files

| File | Transport | When to use |
|---|---|---|
| `mcp_server.py` | stdio | Local tools, CLI agents, Claude Desktop |
| `mcp_server_sse.py` | SSE (Server-Sent Events) | Persistent connections, older MCP spec (2024-11-05) |
| `mcp_server_http.py` | Streamable HTTP | Modern deployments, stateless APIs (spec 2025-03-26) |

---

## Requirements

Python 3.10 or later. No pip install needed.

```bash
python --version   # must be 3.10+
```

---

## Quickstart

### 1. stdio — `mcp_server.py`

The simplest transport. The host process spawns your server as a subprocess and communicates over stdin/stdout.

**Run a manual smoke test:**

```bash
echo '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"0"}}}
{"jsonrpc":"2.0","method":"notifications/initialized"}
{"jsonrpc":"2.0","id":2,"method":"tools/list"}
{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"add","arguments":{"a":7,"b":5}}}' \
| python mcp_server.py
```

**Expected output:**

```json
{"jsonrpc": "2.0", "id": 1, "result": {"protocolVersion": "2024-11-05", "capabilities": {"tools": {}}, "serverInfo": {"name": "math-server", "version": "0.1.0"}}}
{"jsonrpc": "2.0", "id": 2, "result": {"tools": [...]}}
{"jsonrpc": "2.0", "id": 3, "result": {"content": [{"type": "text", "text": "12"}], "isError": false}}
```

**Connect to Claude Desktop** — add to `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "math": {
      "command": "python",
      "args": ["/absolute/path/to/mcp_server.py"]
    }
  }
}
```

Config file location:
- macOS: `~/Library/Application Support/Claude/claude_desktop_config.json`
- Windows: `%APPDATA%\Claude\claude_desktop_config.json`

---

### 2. SSE — `mcp_server_sse.py`

Two HTTP endpoints. The client opens a long-lived GET stream, receives a session ID,  
then POSTs JSON-RPC messages using that ID.

**Start the server:**

```bash
python mcp_server_sse.py
# MCP SSE server running on http://localhost:8000
```

**Open the SSE stream** (keep this terminal open):

```bash
curl -N http://localhost:8000/sse
# event: endpoint
# data: http://localhost:8000/message?sessionId=<your-session-id>
```

**Send requests** — replace `<id>` with the session ID from above:

```bash
# Initialize
curl -s -X POST "http://localhost:8000/message?sessionId=<id>" \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"0"}}}'

# List tools
curl -s -X POST "http://localhost:8000/message?sessionId=<id>" \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/list"}'

# Call multiply
curl -s -X POST "http://localhost:8000/message?sessionId=<id>" \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"multiply","arguments":{"a":6,"b":7}}}'
```

Responses appear in the SSE stream terminal, not in the curl output.  
The POST returns HTTP 202 immediately — the actual result travels over the SSE channel.

**Connect to Claude Desktop:**

```json
{
  "mcpServers": {
    "math": {
      "url": "http://localhost:8000/sse"
    }
  }
}
```

---

### 3. Streamable HTTP — `mcp_server_http.py`

Single endpoint. The client POSTs JSON-RPC and gets a response back in the same HTTP call.  
No persistent session required. Supports both plain JSON and SSE streaming responses.

**Start the server:**

```bash
python mcp_server_http.py
# MCP Streamable HTTP server on http://localhost:8000/mcp
```

**Plain JSON mode** (single request → single JSON response):

```bash
# Initialize
curl -s -X POST http://localhost:8000/mcp \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"test","version":"0"}}}'

# List tools
curl -s -X POST http://localhost:8000/mcp \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/list"}'

# Call subtract
curl -s -X POST http://localhost:8000/mcp \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"subtract","arguments":{"a":10,"b":3}}}'
```

**Streaming mode** — add `Accept: text/event-stream` to get SSE responses:

```bash
curl -sN -X POST http://localhost:8000/mcp \
  -H "Content-Type: application/json" \
  -H "Accept: text/event-stream" \
  -d '{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"add","arguments":{"a":21,"b":21}}}'
```

**Batch mode** — send multiple requests in one POST:

```bash
curl -s -X POST http://localhost:8000/mcp \
  -H "Content-Type: application/json" \
  -d '[
    {"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"add","arguments":{"a":3,"b":4}}},
    {"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"multiply","arguments":{"a":3,"b":4}}}
  ]'
```

**Connect to Claude Desktop:**

```json
{
  "mcpServers": {
    "math": {
      "url": "http://localhost:8000/mcp"
    }
  }
}
```

---

## How it works

All three servers implement the same MCP handshake:

```
Client                        Server
  │                              │
  │── initialize ───────────────▶│  client declares protocol version
  │◀─ result ────────────────────│  server declares capabilities
  │                              │
  │── notifications/initialized ▶│  client acknowledges (no reply)
  │                              │
  │── tools/list ───────────────▶│  client fetches tool manifest
  │◀─ result: [{name, schema}] ──│  server sends descriptions + inputSchema
  │                              │
  │── tools/call ───────────────▶│  client invokes a tool with arguments
  │◀─ result: {content} ─────────│  server returns the result
```

The `inputSchema` field on each tool is a JSON Schema object. It serves two purposes simultaneously:

- the host uses it to validate arguments before calling the tool
- the LLM uses the `description` fields inside it to know what values to construct

This is why description quality matters. The LLM never reads your code — only the schema.

---

## Transport comparison

| | stdio | SSE | Streamable HTTP |
|---|---|---|---|
| MCP spec version | 2024-11-05 | 2024-11-05 | 2025-03-26 |
| Endpoints | none (subprocess) | `GET /sse` + `POST /message` | `POST /mcp` |
| Session required | implicit (process) | yes — sessionId | no |
| Response channel | stdout | SSE stream | same HTTP response |
| Batch support | no | no | yes |
| Best for | local / Claude Desktop | browser clients | remote / cloud deployments |

---

## Extending to real tools

To replace the math tools with something real — swap out the `TOOLS` list and `call_tool` function.  
The transport code stays identical.

```python
# Example: replace call_tool with a Modbus read
def call_tool(name: str, args: dict) -> str:
    if name == "read_register":
        client = ModbusTcpClient(args["host"])
        result = client.read_holding_registers(args["address"], 1)
        return str(result.registers[0])
```

The rest of the server — handshake, routing, transport — does not change.

---

## References

- [MCP specification](https://modelcontextprotocol.io/specification)
- [Claude Desktop MCP docs](https://docs.claude.ai/en/docs/claude-desktop/mcp)
- [JSON-RPC 2.0 spec](https://www.jsonrpc.org/specification)
