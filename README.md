# zoty

Lightweight Zotero MCP server for AI agents.

## What it does

MCP server that connects AI agents to your local Zotero library. Provides 6 tools: BM25-ranked search over titles and abstracts, collection browsing, item lookup, and paper ingestion by arXiv ID or DOI with automatic PDF attachment.

## Requirements

- Python 3.10+
- Zotero 7 desktop running
- Zotero local API enabled: Zotero Settings > Advanced > Config Editor > set `extensions.zotero.httpServer.localAPI.enabled` to `true`
- [Zoty Bridge plugin](#zoty-bridge-plugin) installed (for PDF attachment and collection assignment)

## Install

```bash
# Install as a uv tool (recommended)
uvx --from git+https://github.com/etramel/zoty zoty

# Or from a local checkout
uv tool install .
```

### Claude Code

Add to `.mcp.json` or `~/.claude/settings.json`:

```json
{
  "mcpServers": {
    "zoty": {
      "command": "zoty"
    }
  }
}
```

### Claude Desktop

Add to your Claude Desktop MCP config:

```json
{
  "mcpServers": {
    "zoty": {
      "command": "uvx",
      "args": ["--from", "git+https://github.com/etramel/zoty", "zoty"]
    }
  }
}
```

## Zoty Bridge Plugin

A tiny Zotero 7 plugin that lets zoty execute JavaScript inside Zotero's privileged context. This is needed for operations that can't go through the REST API: PDF attachment and collection assignment both require writing to Zotero's SQLite database, which locks out external processes. The bridge sidesteps this by running JS inside Zotero itself.

### Install the plugin

1. Download `zoty-bridge.xpi` from [releases](https://github.com/etramel/zoty/releases), or build it yourself:
   ```bash
   cd zotero-plugin && bash build.sh
   ```
2. In Zotero: Tools > Add-ons > gear icon > Install Add-on From File > select the `.xpi`
3. Restart Zotero

The bridge runs an HTTP server on `localhost:24119` when Zotero is open. No configuration needed.

## Tools

| Tool | Description |
|------|-------------|
| `search_library` | BM25-ranked search over item titles and abstracts |
| `list_collections` | List all collections with keys, names, and item counts |
| `list_collection_items` | List items in a specific collection |
| `get_item` | Full metadata for a single item by key |
| `get_recent_items` | Recently added items, sorted by date |
| `add_paper` | Add a paper by arXiv ID or DOI with automatic PDF download |

## How it works

Read operations go through [pyzotero](https://github.com/urschrei/pyzotero) against Zotero's local API (`localhost:23119`). The BM25 search index builds in a background thread at startup so the MCP handshake completes immediately.

Write operations use the Zotero connector endpoint (`/connector/saveItems`) to create metadata items. PDF attachment and collection assignment go through the zoty-bridge plugin, which executes JavaScript in Zotero's privileged context. This two-path design exists because Zotero's SQLite database uses exclusive locking -- external processes can read it (immutable mode) but not write to it while Zotero is running.

## License

MIT
