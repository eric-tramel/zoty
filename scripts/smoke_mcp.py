#!/usr/bin/env python3
"""Local MCP smoke tests against a running Zotero desktop instance.

Design rationale:
    These checks exercise zoty the way an MCP client does: they launch the
    server over stdio, perform the MCP initialize/tools handshake, and call the
    tools listed in the README. They are intentionally separate from
    `make test` because they depend on machine-local state: Zotero desktop must
    be running, Zotero's local API must be enabled, the zoty-bridge plugin must
    be installed for bridge/write-path checks, and the user's library contents
    determine which item and collection keys are valid.

    By default, the script avoids mutating Zotero. The `add_paper` smoke check
    only verifies the validation error for missing identifiers. Maintainers who
    want to exercise duplicate-prevention without adding a new item can opt
    into duplicate mode with an arXiv ID that is already present in the chosen
    collection.

Prerequisites:
    - Start Zotero desktop.
    - Enable the Zotero local API.
    - Install zoty-bridge if you want the bridge endpoint check to pass.
    - Run from the repository root with project dependencies available.

Examples:
    Basic non-mutating smoke test:

        uv run scripts/smoke_mcp.py

    Pin the collection, item, and search query used by the read-only checks:

        ZOTY_SMOKE_COLLECTION_KEY=6IXPI8NB \
        ZOTY_SMOKE_ITEM_KEY=DVCLSAV9 \
        ZOTY_SMOKE_QUERY=DriveLMM \
        uv run scripts/smoke_mcp.py

    Exercise `add_paper` duplicate-prevention without creating a new Zotero
    item. The arXiv ID must already be in the selected collection:

        ZOTY_SMOKE_ADD_PAPER_MODE=duplicate \
        ZOTY_SMOKE_COLLECTION_KEY=6IXPI8NB \
        ZOTY_SMOKE_ARXIV_ID=2503.10621 \
        uv run scripts/smoke_mcp.py

Environment variables:
    ZOTY_SMOKE_COLLECTION_KEY:
        Optional collection key. If omitted, the first non-empty collection from
        `list_collections` is used.
    ZOTY_SMOKE_ITEM_KEY:
        Optional parent item key. If omitted, the first usable key from
        `list_collection_items` or `get_recent_items` is used.
    ZOTY_SMOKE_QUERY:
        Optional search query for `search_library`. If omitted, a token from the
        selected item's title is used.
    ZOTY_SMOKE_WITHIN_QUERY:
        Optional query for `search_within_item`. Defaults to
        `ZOTY_SMOKE_QUERY`.
    ZOTY_SMOKE_ADD_PAPER_MODE:
        `validation` (default) checks the non-mutating input validation path.
        `duplicate` calls `add_paper` with `ZOTY_SMOKE_ARXIV_ID` and expects an
        "already in collection" response.
    ZOTY_SMOKE_ARXIV_ID:
        Required only for duplicate-mode `add_paper` smoke checks.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from select import select
from typing import Any
from urllib.error import URLError
from urllib.request import urlopen


PROTOCOL_VERSION = "2025-06-18"
DEFAULT_TIMEOUT_SECONDS = 45
SEARCH_RETRY_SECONDS = 120
TOOLS = [
    "search_library",
    "search_within_item",
    "list_collections",
    "list_collection_items",
    "get_item",
    "get_bibtex_and_citation_for_items",
    "get_recent_items",
    "add_paper",
]


@dataclass
class Check:
    name: str
    ok: bool
    detail: str


class McpClient:
    def __init__(self) -> None:
        self._proc = subprocess.Popen(
            [sys.executable, "-m", "zoty"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        self._next_id = 1

    def request(self, method: str, params: dict[str, Any] | None = None, *, timeout: int = DEFAULT_TIMEOUT_SECONDS) -> dict[str, Any]:
        msg_id = self._next_id
        self._next_id += 1
        message: dict[str, Any] = {"jsonrpc": "2.0", "id": msg_id, "method": method}
        if params is not None:
            message["params"] = params

        assert self._proc.stdin is not None
        self._proc.stdin.write(json.dumps(message) + "\n")
        self._proc.stdin.flush()

        deadline = time.time() + timeout
        assert self._proc.stdout is not None
        while time.time() < deadline:
            ready, _, _ = select([self._proc.stdout], [], [], 0.2)
            if ready:
                line = self._proc.stdout.readline()
                if not line:
                    continue
                response = json.loads(line)
                if response.get("id") == msg_id:
                    return response
            if self._proc.poll() is not None:
                stderr = self._read_stderr()
                raise RuntimeError(f"MCP server exited with {self._proc.returncode}: {stderr}")
        raise TimeoutError(f"Timed out waiting for {method}")

    def notify(self, method: str, params: dict[str, Any] | None = None) -> None:
        message: dict[str, Any] = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            message["params"] = params
        assert self._proc.stdin is not None
        self._proc.stdin.write(json.dumps(message) + "\n")
        self._proc.stdin.flush()

    def call_tool(self, name: str, arguments: dict[str, Any] | None = None, *, timeout: int = DEFAULT_TIMEOUT_SECONDS) -> dict[str, Any]:
        response = self.request(
            "tools/call",
            {"name": name, "arguments": arguments or {}},
            timeout=timeout,
        )
        if "error" in response:
            return {"error": response["error"]}

        content = response.get("result", {}).get("content", [])
        text = content[0].get("text", "") if content else ""
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            return {"error": f"Tool {name} returned non-JSON text", "text": text}
        if not isinstance(payload, dict):
            return {"error": f"Tool {name} returned {type(payload).__name__}", "payload": payload}
        return payload

    def close(self) -> None:
        if self._proc.poll() is not None:
            return
        self._proc.terminate()
        try:
            self._proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            self._proc.kill()
            self._proc.wait()

    def _read_stderr(self) -> str:
        if self._proc.stderr is None:
            return ""
        try:
            return self._proc.stderr.read().strip()
        except Exception:
            return ""


def _env_bool(name: str, *, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _zotero_endpoint_ok(url: str) -> tuple[bool, str]:
    try:
        with urlopen(url, timeout=5) as response:
            body = response.read(200).decode("utf-8", errors="replace")
            return 200 <= response.status < 300, body.strip()
    except URLError as exc:
        return False, str(exc)


def _first_nonempty_collection(collections: list[dict[str, Any]]) -> str:
    for collection in collections:
        if int(collection.get("numItems") or 0) > 0 and collection.get("key"):
            return str(collection["key"])
    return ""


def _first_item_key(*payloads: dict[str, Any]) -> str:
    for payload in payloads:
        for item in payload.get("items", []):
            key = item.get("key")
            if key:
                return str(key)
    return ""


def _title_query(item: dict[str, Any]) -> str:
    title = str(item.get("title") or "").strip()
    for token in title.replace(":", " ").split():
        if len(token) >= 4:
            return token
    return "zotero"


def _append(checks: list[Check], name: str, ok: bool, detail: str) -> None:
    checks.append(Check(name=name, ok=ok, detail=detail))


def run() -> int:
    checks: list[Check] = []

    local_api_ok, local_api_detail = _zotero_endpoint_ok("http://127.0.0.1:23119/connector/ping")
    _append(checks, "zotero-local-api", local_api_ok, local_api_detail or "ok")

    bridge_ok, bridge_detail = _zotero_endpoint_ok("http://127.0.0.1:24119/status")
    _append(checks, "zoty-bridge", bridge_ok, bridge_detail or "ok")

    if not local_api_ok:
        print_report(checks)
        return 1

    client = McpClient()
    try:
        client.request(
            "initialize",
            {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {"name": "zoty-smoke", "version": "0.1"},
            },
        )
        client.notify("notifications/initialized", {})

        tools_response = client.request("tools/list", {})
        tool_names = [tool["name"] for tool in tools_response.get("result", {}).get("tools", [])]
        missing_tools = [tool for tool in TOOLS if tool not in tool_names]
        _append(checks, "tools/list", not missing_tools, f"{len(tool_names)} tools; missing={missing_tools}")

        collections = client.call_tool("list_collections")
        collection_list = collections.get("collections", [])
        collection_key = os.environ.get("ZOTY_SMOKE_COLLECTION_KEY", "").strip().upper() or _first_nonempty_collection(collection_list)
        _append(checks, "list_collections", bool(collection_list), f"{len(collection_list)} collections; selected={collection_key or 'none'}")

        recent = client.call_tool("get_recent_items", {"limit": 2})
        _append(
            checks,
            "get_recent_items",
            recent.get("returned_count", 0) > 0,
            f"returned={recent.get('returned_count', 0)} total={recent.get('total')}",
        )

        collection_items: dict[str, Any] = {"items": []}
        if collection_key:
            collection_items = client.call_tool("list_collection_items", {"collection_key": collection_key, "limit": 2})
            _append(
                checks,
                "list_collection_items",
                collection_items.get("collection_found") is True and collection_items.get("returned_count", 0) > 0,
                f"collection={collection_key} returned={collection_items.get('returned_count', 0)} total={collection_items.get('total')}",
            )
        else:
            _append(checks, "list_collection_items", False, "no nonempty collection available")

        item_key = os.environ.get("ZOTY_SMOKE_ITEM_KEY", "").strip().upper() or _first_item_key(collection_items, recent)
        if not item_key:
            _append(checks, "get_item", False, "no item key available")
            _append(checks, "get_bibtex_and_citation_for_items", False, "no item key available")
            _append(checks, "search_within_item", False, "no item key available")
        else:
            item = client.call_tool("get_item", {"item_key": item_key})
            query = os.environ.get("ZOTY_SMOKE_QUERY", "").strip() or _title_query(item)
            within_query = os.environ.get("ZOTY_SMOKE_WITHIN_QUERY", "").strip() or query
            _append(checks, "get_item", item.get("key") == item_key, f"key={item.get('key')} title={item.get('title', '')[:80]}")

            citation = client.call_tool("get_bibtex_and_citation_for_items", {"item_key": item_key})
            first_citation = citation.get("items", [{}])[0] if citation.get("items") else {}
            _append(
                checks,
                "get_bibtex_and_citation_for_items",
                citation.get("total") == 1 and bool(first_citation.get("bibtex")),
                f"total={citation.get('total')} key={first_citation.get('key')}",
            )

            search = _call_search_with_retries(client, query)
            _append(
                checks,
                "search_library",
                search.get("total", 0) > 0 and not search.get("error"),
                f"query={query!r} total={search.get('total')} error={search.get('error')}",
            )

            within = client.call_tool("search_within_item", {"item_keys": [item_key], "query": within_query, "limit": 2})
            _append(
                checks,
                "search_within_item",
                within.get("total", 0) > 0 and not within.get("error"),
                f"query={within_query!r} total={within.get('total')} error={within.get('error')}",
            )

        add_mode = os.environ.get("ZOTY_SMOKE_ADD_PAPER_MODE", "validation").strip().lower()
        if add_mode == "duplicate":
            arxiv_id = os.environ.get("ZOTY_SMOKE_ARXIV_ID", "").strip()
            if not arxiv_id or not collection_key:
                _append(checks, "add_paper", False, "duplicate mode requires ZOTY_SMOKE_ARXIV_ID and collection key")
            else:
                add_result = client.call_tool("add_paper", {"arxiv_id": arxiv_id, "collection_key": collection_key}, timeout=60)
                _append(
                    checks,
                    "add_paper",
                    add_result.get("status") == "already in collection",
                    f"duplicate status={add_result.get('status')} key={add_result.get('key')}",
                )
        else:
            add_result = client.call_tool("add_paper", {})
            _append(
                checks,
                "add_paper",
                "Provide at least one" in str(add_result.get("error", "")),
                "validation-only mode; no Zotero mutation attempted",
            )
    finally:
        client.close()

    print_report(checks)
    return 0 if all(check.ok for check in checks) else 1


def _call_search_with_retries(client: McpClient, query: str) -> dict[str, Any]:
    deadline = time.time() + SEARCH_RETRY_SECONDS
    last_result: dict[str, Any] = {}
    while time.time() < deadline:
        last_result = client.call_tool("search_library", {"query": query, "limit": 3}, timeout=60)
        if not str(last_result.get("error", "")).startswith("Index is still building"):
            return last_result
        time.sleep(5)
    return last_result


def print_report(checks: list[Check]) -> None:
    width = max((len(check.name) for check in checks), default=4)
    for check in checks:
        status = "PASS" if check.ok else "FAIL"
        print(f"{status} {check.name:<{width}} {check.detail}")


if __name__ == "__main__":
    try:
        raise SystemExit(run())
    except KeyboardInterrupt:
        raise SystemExit(130)
