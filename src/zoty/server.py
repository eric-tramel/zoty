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
    item_key: str = "",
    limit: int = 10,
) -> str:
    """BM25 ranked search over title, abstract, and indexed attachment full text.

    Args:
        query: Search keywords (e.g. "transformer attention" not "what papers discuss attention?")
        collection_key: Optional Zotero collection key to filter results
        item_type: Optional item type filter (e.g. "journalArticle", "preprint", "conferencePaper")
        item_key: Optional Zotero parent item key to restrict the search to one paper
        limit: Maximum results to return (default: 10)

    Returns:
        JSON with ranked search results including title, creators, date, score,
        metadata abstract, optional plain-text snippets, and attachment filepaths.
    """
    return db.search(
        query,
        collection_key=collection_key,
        item_type=item_type,
        item_key=item_key,
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
        limit: Maximum items to return (default: 50)

    Returns:
        JSON with item metadata for each item in the collection.
    """
    return db.list_collection_items(collection_key, limit=limit)


@mcp_server.tool()
def get_item(item_key: str) -> str:
    """Get full metadata for a single Zotero item.

    Args:
        item_key: The Zotero item key

    Returns:
        JSON with complete item metadata including title, creators, abstract,
        date, DOI, URL, tags, collections, and attachment filepaths.
    """
    return db.get_item(item_key)


@mcp_server.tool()
def get_bibtex_and_citation_for_items(
    item_key: str = "",
    item_keys: list[str] | None = None,
    style: str = "chicago-note-bibliography",
    locale: str = "en-US",
) -> str:
    """Get BibTeX, citation text, and bibliography text for one or more Zotero items.

    Args:
        item_key: A single Zotero item key, typically returned by search_library
        item_keys: Optional list of Zotero item keys, typically returned by search_library
        style: Citation style to use for formatted citation and bibliography text
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
        limit: Maximum items to return (default: 10)

    Returns:
        JSON with item metadata for recently added items.
    """
    return db.get_recent_items(limit=limit)


@mcp_server.tool()
def add_paper(arxiv_id: str = "", doi: str = "", collection_key: str = "") -> str:
    """Add a paper to Zotero by arXiv ID or DOI.

    Fetches metadata from arXiv or CrossRef, creates the item via the Zotero
    connector, downloads the PDF, and optionally assigns to a collection.
    PDF attachment and collection assignment use the Zotero JS API via the
    zoty-bridge plugin. Zotero desktop must be running.

    Args:
        arxiv_id: arXiv paper ID (e.g. "2301.07041" or "arxiv:2301.07041")
        doi: DOI (e.g. "10.1038/s41586-021-03819-2")
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
