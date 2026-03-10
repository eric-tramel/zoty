# zoty

Lightweight Zotero MCP server for AI agents.

## What it does

MCP server that connects AI agents to your local Zotero library. Provides 6 tools: BM25-ranked search over titles and abstracts, collection browsing, item lookup, and paper ingestion by arXiv ID or DOI with automatic PDF attachment.

## Requirements

- Python 3.10+
- Zotero 7 desktop running
- Zotero local API enabled: Zotero Settings > Advanced > Config Editor > set `extensions.zotero.httpServer.localAPI.enabled` to `true`
- [Zoty Bridge plugin](#zoty-bridge-plugin) installed (for PDF attachment and collection assignment)

## Add to Your Agent

### Claude Code

Add from the command line:

```bash
claude mcp add zoty -- uvx zoty
```

Add to your `.mcp.json` or `~/.claude/settings.json`:

```json
{
  "mcpServers": {
    "zoty": {
      "command": "uvx",
      "args": ["zoty"]
    }
  }
}
```

### Codex

Add from the command line:

```bash
codex mcp add zoty -- uvx zoty
```

Add to your `~/.codex/config.toml`:

```toml
[mcp_servers.zoty]
command = "uvx"
args = ["zoty"]
```

## Installation

Requires [uv](https://docs.astral.sh/uv/).

Run without installing (recommended for MCP setups):

```bash
uvx zoty
```

Install persistently:

```bash
uv tool install zoty
```

Upgrade an installed copy:

```bash
uv tool upgrade zoty
```

If you run zoty with `uvx` instead of installing it, refresh to the latest published version with:

```bash
uvx --refresh zoty
```

From a local checkout:

```bash
uv run zoty

# Or install from source as a tool
uv tool install .
```

## Zoty Bridge Plugin

A tiny Zotero 7 plugin that lets zoty execute JavaScript inside Zotero's privileged context. This is needed for operations that can't go through the REST API: PDF attachment and collection assignment both require writing to Zotero's SQLite database, which locks out external processes. The bridge sidesteps this by running JS inside Zotero itself.

### Install the plugin

1. Download `zoty-bridge.xpi` from [releases](https://github.com/eric-tramel/zoty/releases), or build it yourself:
   ```bash
   make build
   ```
2. In Zotero: Tools > Add-ons > gear icon > Install Add-on From File > select the `.xpi`
3. Restart Zotero

The bridge runs an HTTP server on `localhost:24119` when Zotero is open. No configuration needed.

## Tools

| Tool | Description |
|------|-------------|
| `search_library` | BM25-ranked search over item titles and abstracts, including attachment filepaths |
| `list_collections` | List all collections with keys, names, and item counts |
| `list_collection_items` | List items in a specific collection |
| `get_item` | Full metadata for a single item by key, including attachment filepaths |
| `get_recent_items` | Recently added items, sorted by date |
| `add_paper` | Add a paper by arXiv ID or DOI with automatic PDF download and collection-scoped duplicate prevention |

## How it works

Read operations go through [pyzotero](https://github.com/urschrei/pyzotero) against Zotero's local API (`localhost:23119`). The BM25 search index builds in a background thread at startup so the MCP handshake completes immediately.

Write operations use the Zotero connector endpoint (`/connector/saveItems`) to create metadata items. PDF attachment and collection assignment go through the zoty-bridge plugin, which executes JavaScript in Zotero's privileged context. This two-path design exists because Zotero's SQLite database uses exclusive locking -- external processes can read it (immutable mode) but not write to it while Zotero is running.

arXiv traffic is throttled internally to respect arXiv's access policy. Concurrent `add_paper` calls queue transparently: metadata requests serialize with a 3-second gap, and arXiv PDF downloads are rate-limited separately.

## Development

```bash
make build   # build zotero-plugin/dist/zoty-bridge.xpi
make test    # run Python unit tests
```

## License

MIT

## Rate Limiting Across Sessions

zoty rate-limits arXiv traffic inside the running MCP server process. If several `add_paper` calls reach the same server at once, zoty queues them and drains metadata requests at arXiv-safe speed.

That limiter is not shared across separate zoty processes. If you start one zoty instance per agent, session, or editor window, each process will enforce its own limit and the combined request rate can still exceed arXiv policy.

If you expect multiple sessions to pull papers at the same time, start one long-lived zoty server and point all clients at that same instance.

Start one shared local server:

```bash
zoty --transport streamable-http --host 127.0.0.1 --port 8000
```

The shared MCP endpoint will be:

```text
http://127.0.0.1:8000/mcp
```

If you want a different endpoint path:

```bash
zoty \
  --transport streamable-http \
  --host 127.0.0.1 \
  --port 8000 \
  --streamable-http-path /zoty-mcp
```

Then point every client at the same URL:

```text
http://127.0.0.1:8000/zoty-mcp
```

For clients that support remote MCP servers by URL, the config should look like this:

```json
{
  "mcpServers": {
    "zoty": {
      "url": "http://127.0.0.1:8000/mcp"
    }
  }
}
```

Avoid this pattern when multiple sessions may import papers in parallel, because it starts a separate zoty process per client:

```json
{
  "mcpServers": {
    "zoty": {
      "command": "zoty"
    }
  }
}
```

Recommended boot sequence:

1. Boot Zotero and make sure the Zotero connector and `zoty-bridge` plugin are available.
2. Start one shared zoty server with `--transport streamable-http`.
3. Configure each agent or MCP client to connect to that existing server URL instead of launching its own copy.
4. Let the shared server serialize arXiv metadata lookups and rate-limit arXiv PDF downloads for everyone.

This keeps the agent-side behavior simple: tool calls may take a bit longer under load, but they will queue naturally instead of hammering `export.arxiv.org`.
