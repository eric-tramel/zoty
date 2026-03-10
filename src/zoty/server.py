"""Zoty MCP server — all tool definitions and entry point."""

from __future__ import annotations

import threading

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
) -> str:
    """BM25 ranked search over Zotero library items by title and abstract.

    Args:
        query: Search keywords (e.g. "transformer attention" not "what papers discuss attention?")
        collection_key: Optional Zotero collection key to filter results
        item_type: Optional item type filter (e.g. "journalArticle", "preprint", "conferencePaper")
        limit: Maximum results to return (default: 10)

    Returns:
        JSON with ranked search results including title, creators, date, score, and truncated abstract.
    """
    return db.search(query, collection_key=collection_key, item_type=item_type, limit=limit)


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
        JSON with complete item metadata including title, creators, abstract, date, DOI, URL, tags, and collections.
    """
    return db.get_item(item_key)


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
        JSON with the created item's metadata on success, or an error message.
    """
    return connector.add_paper(arxiv_id=arxiv_id, doi=doi, collection_key=collection_key)


def main() -> None:
    """Entry point: build search index in background, start MCP server."""
    # Build search index in background thread so MCP transport starts immediately
    build_thread = threading.Thread(target=db.build_index_background, daemon=True)
    build_thread.start()

    mcp_server.run()
