"""Zoty MCP server — all tool definitions and entry point."""

from __future__ import annotations

import argparse

from mcp.server.fastmcp import FastMCP

from zoty import db, connector

MCP_SERVER_NAME = "zoty"

mcp_server = FastMCP(MCP_SERVER_NAME)


@mcp_server.tool()
def search_library(
    query: str,
    collection_key: str = "",
    item_type: str = "",
    limit: int = 10,
    include_attachments: bool = False,
) -> str:
    """Find which items in your Zotero library match a keyword query.

    Uses BM25 ranking over title, abstract, and indexed attachment full text.

    Args:
        query: Search keywords (e.g. "transformer attention" not "what papers discuss attention?")
        collection_key: Optional Zotero collection key to filter results
        item_type: Optional Zotero item type filter, case-insensitive
            (e.g. "journalArticle", "preprint", "conferencePaper", "book",
            "bookSection", "thesis", "report", "webpage")
        limit: Maximum results to return (default: 10)
        include_attachments: Include resolved attachment metadata in each
            returned item. Defaults to `False`; otherwise `attachment_count`
            is still present without the heavier attachment array.

    Returns:
        JSON with ranked Zotero items under `items`, including key, title,
        creators, date, score, abstract text truncated to 500 characters,
        `attachment_count`, optional `attachments` when
        `include_attachments=True`, optional plain-text snippets, warnings for
        invalid `collection_key` / `item_type` filters, and limit metadata.
    """
    return db.search(
        query,
        collection_key=collection_key,
        item_type=item_type,
        limit=limit,
        include_attachments=include_attachments,
    )


@mcp_server.tool()
def search_within_item(
    item_key: str,
    query: str,
    limit: int = 5,
    item_keys: list[str] | None = None,
) -> str:
    """Find which passages within one or more known items match a keyword query.

    Use after `search_library` to drill into one paper, or compare passage-level
    relevance across several papers in a single call.

    Args:
        item_key: Zotero parent item key to search within. Use the `key`
            field from `search_library`, `list_collection_items`, or
            `get_recent_items` results (for example, `X9KJ2M4P`).
        item_keys: Optional additional Zotero parent item keys to search
            within together with item_key for cross-item ranking.
        query: Search keywords to match against that item's metadata and attachment chunks
        limit: Requested passage matches to return (default: 5, capped at
            25). The response includes `requested_limit`, `applied_limit`,
            `limit_cap`, and `limit_capped` so callers can detect clamping.

    Returns:
        JSON with ranked passage `matches`, including `snippet`,
        `chunk_index`, `char_start`, and `char_end` for every hit. When a
        match comes from an attachment chunk, it also includes
        `attachment_key`, `attachment_title`, and `attachment_filepath` so you
        can identify the source file for that passage. Single-item calls
        return `key` and `item`; multi-item calls return `item_keys` and
        `items`, where each item summary also includes
        `returned_match_count`, `top_score`, and `top_match_type` so agents
        can compare relevance across the requested items without extra calls.
        Matches omit the redundant parent title and include parent `key` only
        for multi-item calls.
    """
    return db.search_within_item(
        item_key=item_key,
        item_keys=item_keys,
        query=query,
        limit=limit,
    )


@mcp_server.tool()
def list_collections() -> str:
    """List all Zotero collections with their keys, names, and item counts.

    Returns:
        JSON with collection key, name, parent collection, and item count for each collection.
    """
    return db.list_collections()


@mcp_server.tool()
def list_collection_items(collection_key: str, limit: int = 50) -> str:
    """List items in a specific Zotero collection.

    Args:
        collection_key: The Zotero collection key (from list_collections)
        limit: Requested items to return before the cap is applied (default: 50)

    Returns:
        JSON with `collection_key`, `collection_found`, `items`, and limit
        metadata. Each item includes `key`, `title`, `creators`, `date`,
        truncated `abstract` (500 chars), `attachment_count`, and other summary
        fields.
    """
    return db.list_collection_items(collection_key, limit=limit)


@mcp_server.tool()
def get_item(item_key: str = "", item_keys: list[str] | None = None) -> str:
    """Get full metadata for one Zotero item or a batch of items.

    Args:
        item_key: A single Zotero item key. Use the `key` field from
            `search_library`, `list_collection_items`, or `get_recent_items`
            results (for example, `X9KJ2M4P`).
        item_keys: Optional additional Zotero item keys for batch detail
            retrieval. `item_key` and `item_keys` can be combined, and at
            least one must be provided for batch mode. Single-key requests
            keep the legacy single-item response shape.

    Returns:
        Single-key requests return JSON with complete item metadata including
        the full untruncated abstract, title, creators, date, DOI, URL, tags,
        collections, attachment counts, and attachment filepaths. Multi-key
        requests return JSON with `item_keys`, `items`, `requested`, `total`,
        and optional per-item `errors`. Very large creator lists are
        summarized to keep the payload bounded. Search results already include
        most fields, so use this only when the full abstract or full
        attachment records are needed.
    """
    return db.get_item(item_key=item_key, item_keys=item_keys)


@mcp_server.tool()
def get_bibtex_and_citation_for_items(
    item_key: str = "",
    item_keys: list[str] | None = None,
    style: str = "chicago-note-bibliography",
    locale: str = "en-US",
) -> str:
    """Get BibTeX, citation text, and bibliography text for one or more Zotero items. Provide at least one of `item_key` or `item_keys`.

    Args:
        item_key: A single Zotero item key for one item. Use the `key` field from
            `search_library`, `list_collection_items`, or `get_recent_items`
            results (for example, `X9KJ2M4P`).
        item_keys: A list of Zotero item keys for batch use. Use the `key` field from
            `search_library`, `list_collection_items`, or `get_recent_items`
            results (for example, `X9KJ2M4P`). item_key and item_keys can be
            combined, and at least one must be provided.
        style: CSL style ID to use for formatted citation and bibliography text (for example,
            'apa', 'ieee', or 'chicago-note-bibliography'); see the Zotero Style Repository
            for the full list
        locale: Citation locale to use for formatted citation and bibliography text

    Returns:
        JSON with one entry per requested item, including plain-text citation,
        plain-text bibliography, and a BibTeX export block.
    """
    return db.get_bibtex_and_citation_for_items(
        item_key=item_key,
        item_keys=item_keys,
        style=style,
        locale=locale,
    )


@mcp_server.tool()
def get_recent_items(limit: int = 10) -> str:
    """Get recently added items from the Zotero library, sorted by date added.

    Args:
        limit: Requested items to return before the cap is applied (default: 10)

    Returns:
        JSON with `items`, `total`, and limit metadata. Each item includes
        `key`, `title`, `creators`, `date`, truncated `abstract` (500 chars),
        `attachment_count`, and other summary fields.
    """
    return db.get_recent_items(limit=limit)


@mcp_server.tool()
def add_paper(arxiv_id: str = "", doi: str = "", collection_key: str = "") -> str:
    """Add a paper to Zotero by arXiv ID or DOI.

    Provide at least one of `arxiv_id` or `doi`. If both are provided,
    `arxiv_id` takes precedence.

    Fetches metadata from arXiv or CrossRef, creates the item via the Zotero
    connector, downloads the PDF, and optionally assigns to a collection.
    PDF attachment and collection assignment use the Zotero JS API via the
    zoty-bridge plugin. Zotero desktop must be running.

    Args:
        arxiv_id: arXiv paper ID (e.g. "2301.07041" or "arxiv:2301.07041").
            Required unless `doi` is provided. Takes precedence when both are
            provided.
        doi: DOI (e.g. "10.1038/s41586-021-03819-2"). Required unless
            `arxiv_id` is provided. Ignored when `arxiv_id` is provided.
        collection_key: Optional Zotero collection key to add the paper to (from list_collections)

    Returns:
        JSON with the created item's metadata on success, an "already in collection"
        status when an exact duplicate is already present in the target collection,
        or an error message.
    """
    return connector.add_paper(arxiv_id=arxiv_id, doi=doi, collection_key=collection_key)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the zoty MCP server.")
    parser.add_argument(
        "--transport",
        choices=("stdio", "sse", "streamable-http"),
        default="stdio",
        help="MCP transport to serve. Use streamable-http for one shared local server.",
    )
    parser.add_argument(
        "--host",
        help="Bind host for HTTP transports.",
    )
    parser.add_argument(
        "--port",
        type=int,
        help="Bind port for HTTP transports.",
    )
    parser.add_argument(
        "--streamable-http-path",
        help="HTTP path for the streamable MCP endpoint.",
    )
    parser.add_argument(
        "--sse-path",
        help="HTTP path for the SSE endpoint.",
    )
    parser.add_argument(
        "--message-path",
        help="HTTP path for SSE message posts.",
    )
    return parser.parse_args(argv)


def _apply_server_args(args: argparse.Namespace) -> None:
    if args.host:
        mcp_server.settings.host = args.host
    if args.port is not None:
        mcp_server.settings.port = args.port
    if args.streamable_http_path:
        mcp_server.settings.streamable_http_path = args.streamable_http_path
    if args.sse_path:
        mcp_server.settings.sse_path = args.sse_path
    if args.message_path:
        mcp_server.settings.message_path = args.message_path


def main(argv: list[str] | None = None) -> None:
    """Entry point: load the active snapshot, queue refresh work, and start MCP."""
    args = _parse_args(argv)
    _apply_server_args(args)
    db.prepare_search_index()

    mcp_server.run(transport=args.transport)
