"""Zotero local library access and BM25S search index."""

from __future__ import annotations

from contextlib import closing
import html
import json
from pathlib import Path
import re
import sqlite3
import sys
import threading
from typing import Any
import urllib.parse

import bm25s
from pyzotero import zotero


# Global state, protected by _index_lock
_index_lock = threading.Lock()
_bm25_retriever: bm25s.BM25 | None = None
_corpus: list[dict] = []
_zot: zotero.Zotero | None = None
_ZOTERO_DIR = Path.home() / "Zotero"
_ZOTERO_STORAGE = _ZOTERO_DIR / "storage"
_ZOTERO_DB = _ZOTERO_DIR / "zotero.sqlite"


def _get_zot() -> zotero.Zotero:
    """Return the shared pyzotero client, creating it on first call."""
    global _zot
    if _zot is None:
        _zot = zotero.Zotero("0", "user", local=True)
    return _zot


def _format_creators(creators: list[dict]) -> list[str]:
    """Turn pyzotero creator dicts into 'First Last' strings."""
    names = []
    for c in creators:
        first = c.get("firstName", "")
        last = c.get("lastName", "")
        name = c.get("name", "")
        if first or last:
            names.append(f"{first} {last}".strip())
        elif name:
            names.append(name)
    return names


def _resolve_attachment_filepath(attachment_key: str, raw_path: str) -> str:
    """Resolve a Zotero attachment path into a local filesystem path."""
    stored_path = raw_path.strip()
    if not stored_path:
        return ""

    if stored_path.startswith("storage:"):
        filename = stored_path.removeprefix("storage:")
        return str(_ZOTERO_STORAGE / attachment_key / filename)

    if stored_path.startswith("file://"):
        parsed = urllib.parse.urlparse(stored_path)
        return urllib.parse.unquote(parsed.path)

    return stored_path


def _get_item_attachments(item_key: str) -> list[dict]:
    """Return attachment metadata and resolved filepaths for one parent item."""
    key = item_key.strip()
    if not key:
        return []

    try:
        with closing(sqlite3.connect(f"file:{_ZOTERO_DB}?immutable=1", uri=True)) as db:
            cur = db.cursor()
            cur.execute(
                """SELECT child.key,
                          COALESCE(MAX(CASE WHEN f.fieldName = 'title' THEN idv.value END), ''),
                          ia.contentType,
                          ia.linkMode,
                          ia.path
                   FROM items parent
                   JOIN itemAttachments ia ON parent.itemID = ia.parentItemID
                   JOIN items child ON ia.itemID = child.itemID
                   LEFT JOIN itemData id ON child.itemID = id.itemID
                   LEFT JOIN itemDataValues idv ON id.valueID = idv.valueID
                   LEFT JOIN fields f ON id.fieldID = f.fieldID
                   WHERE parent.key = ?
                   GROUP BY child.key, ia.contentType, ia.linkMode, ia.path, child.dateAdded
                   ORDER BY child.dateAdded ASC""",
                (key,),
            )
            rows = cur.fetchall()
    except Exception:
        return []

    attachments = []
    for attachment_key, title, content_type, link_mode, raw_path in rows:
        filepath = _resolve_attachment_filepath(attachment_key, raw_path or "")
        if not filepath:
            continue
        attachments.append({
            "key": attachment_key,
            "title": title,
            "contentType": content_type or "",
            "linkMode": link_mode,
            "filepath": filepath,
        })

    return attachments


def _item_to_dict(item: dict, truncate_abstract: int = 0, *, include_attachments: bool = False) -> dict:
    """Convert a pyzotero item to a concise dict for tool output."""
    data = item.get("data", {})
    abstract = data.get("abstractNote", "")
    if truncate_abstract > 0 and len(abstract) > truncate_abstract:
        abstract = abstract[:truncate_abstract] + "..."

    collections = data.get("collections", [])
    tags = [t.get("tag", "") for t in data.get("tags", []) if t.get("tag")]

    result = {
        "key": data.get("key", ""),
        "itemType": data.get("itemType", ""),
        "title": data.get("title", ""),
        "creators": _format_creators(data.get("creators", [])),
        "date": data.get("date", ""),
        "DOI": data.get("DOI", ""),
        "url": data.get("url", ""),
        "tags": tags,
        "collections": collections,
        "abstract": abstract,
    }

    if include_attachments:
        result["attachments"] = _get_item_attachments(data.get("key", ""))

    return result


_SKIP_TYPES = {"attachment", "note", "annotation"}


def _fetch_all_items() -> list[dict]:
    """Fetch all non-attachment, non-note items from the local Zotero library."""
    zot = _get_zot()
    items = zot.everything(zot.items())
    return [i for i in items if i.get("data", {}).get("itemType") not in _SKIP_TYPES]


def _build_index() -> tuple[list[dict], bm25s.BM25 | None]:
    """Build BM25 index from all library items. Returns (corpus, retriever)."""
    items = _fetch_all_items()

    corpus: list[dict] = []
    texts: list[str] = []
    for item in items:
        data = item.get("data", {})
        title = data.get("title", "")
        abstract = data.get("abstractNote", "")
        if not title:
            continue

        text = f"{title} {abstract}".strip()
        corpus.append(item)
        texts.append(text)

    retriever = None
    if texts:
        tokens = bm25s.tokenize(texts, stopwords="en")
        retriever = bm25s.BM25()
        retriever.index(tokens)

    return corpus, retriever


def build_index_background() -> None:
    """Build the search index and swap it in under lock. Run in a thread."""
    global _bm25_retriever, _corpus
    try:
        corpus, retriever = _build_index()
        with _index_lock:
            _corpus = corpus
            _bm25_retriever = retriever
        print(f"zoty: search index ready ({len(corpus)} items)", file=sys.stderr)
    except Exception as e:
        print(f"zoty: failed to build index: {e}", file=sys.stderr)


def _normalize_item_keys(item_key: str = "", item_keys: list[str] | None = None) -> list[str]:
    """Normalize a single key and/or key list into a clean ordered list."""
    normalized: list[str] = []

    if item_key.strip():
        normalized.append(item_key.strip().upper())

    for key in item_keys or []:
        cleaned = key.strip().upper()
        if cleaned:
            normalized.append(cleaned)

    return normalized


def _xhtml_to_text(fragment: str) -> str:
    """Collapse Zotero's XHTML bibliography/citation output into plain text."""
    text = re.sub(r"<[^>]+>", " ", fragment)
    return " ".join(html.unescape(text).split())


def _fetch_item_export(
    item_key: str,
    *,
    content: str,
    style: str,
    locale: str,
) -> str:
    """Fetch one formatted export block for a Zotero item."""
    zot = _get_zot()
    exported = zot.item(
        item_key,
        format="atom",
        content=content,
        style=style,
        locale=locale,
    )

    if isinstance(exported, list):
        return exported[0] if exported else ""
    if isinstance(exported, str):
        return exported
    return ""


def search(
    query: str,
    collection_key: str = "",
    item_type: str = "",
    limit: int = 10,
) -> str:
    """BM25 ranked search over titles + abstracts. Returns JSON string."""
    limit = max(1, limit)

    with _index_lock:
        retriever = _bm25_retriever
        corpus = _corpus

    if retriever is None or not corpus:
        return json.dumps({
            "error": "Index is still building, please retry in a moment",
            "results": [],
            "query": query,
            "total": 0,
        })

    query_tokens = bm25s.tokenize([query], stopwords="en")

    # Retrieve more if filtering, to ensure we get enough results
    needs_filter = bool(collection_key or item_type)
    retrieve_k = len(corpus) if needs_filter else limit
    results, scores = retriever.retrieve(
        query_tokens, k=min(retrieve_k, len(corpus))
    )

    search_results: list[dict] = []
    for i in range(results.shape[1]):
        doc_idx = results[0, i]
        score = float(scores[0, i])
        if score <= 0:
            continue

        item = corpus[doc_idx]
        data = item.get("data", {})

        if collection_key and collection_key not in data.get("collections", []):
            continue
        if item_type and data.get("itemType", "").lower() != item_type.lower():
            continue

        search_results.append({
            **_item_to_dict(item, truncate_abstract=500, include_attachments=True),
            "score": round(score, 4),
        })

        if len(search_results) >= limit:
            break

    return json.dumps({
        "results": search_results,
        "query": query,
        "total": len(search_results),
    })


def list_collections() -> str:
    """Return all collections with keys, names, and item counts."""
    try:
        zot = _get_zot()
        collections = zot.collections()
    except Exception as e:
        return json.dumps({"error": f"Failed to fetch collections: {e}"})

    result = []
    for c in collections:
        data = c.get("data", {})
        meta = c.get("meta", {})
        result.append({
            "key": data.get("key", ""),
            "name": data.get("name", ""),
            "parentCollection": data.get("parentCollection", False),
            "numItems": meta.get("numItems", 0),
        })

    return json.dumps({"collections": result, "total": len(result)})


def list_collection_items(collection_key: str, limit: int = 50) -> str:
    """Return items in a specific collection."""
    limit = max(1, limit)
    try:
        zot = _get_zot()
        items = zot.collection_items(collection_key, limit=limit)
    except Exception as e:
        return json.dumps({"error": f"Failed to fetch collection items: {e}"})

    result = []
    for item in items:
        data = item.get("data", {})
        if data.get("itemType") in ("attachment", "note"):
            continue
        result.append(_item_to_dict(item, truncate_abstract=500))

    return json.dumps({"items": result, "total": len(result)})


def get_item(item_key: str) -> str:
    """Full metadata for a single item."""
    try:
        zot = _get_zot()
        item = zot.item(item_key)
    except Exception as e:
        return json.dumps({"error": f"Failed to fetch item {item_key}: {e}"})

    return json.dumps(_item_to_dict(item, truncate_abstract=0, include_attachments=True))


def get_bibtex_and_citation_for_items(
    item_key: str = "",
    item_keys: list[str] | None = None,
    style: str = "chicago-note-bibliography",
    locale: str = "en-US",
) -> str:
    """Return BibTeX plus formatted citation/bibliography text for one or more items."""
    requested_keys = _normalize_item_keys(item_key=item_key, item_keys=item_keys)
    if not requested_keys:
        return json.dumps({
            "error": "Provide item_key or item_keys",
            "items": [],
            "total": 0,
        })

    results: list[dict[str, str]] = []
    errors: list[dict[str, str]] = []

    for key in requested_keys:
        try:
            citation = _fetch_item_export(
                key,
                content="citation",
                style=style,
                locale=locale,
            )
            bibliography = _fetch_item_export(
                key,
                content="bib",
                style=style,
                locale=locale,
            )
            bibtex = _fetch_item_export(
                key,
                content="bibtex",
                style=style,
                locale=locale,
            )

            results.append({
                "key": key,
                "citation": _xhtml_to_text(citation),
                "bibliography": _xhtml_to_text(bibliography),
                "bibtex": bibtex.strip(),
            })
        except Exception as e:
            errors.append({
                "key": key,
                "error": f"Failed to fetch citation entry: {e}",
            })

    payload: dict[str, Any] = {
        "items": results,
        "total": len(results),
        "requested": len(requested_keys),
        "style": style,
        "locale": locale,
    }
    if errors:
        payload["errors"] = errors
        if not results:
            payload["error"] = "Failed to fetch citation entries"

    return json.dumps(payload)


def get_recent_items(limit: int = 10) -> str:
    """Recently added items, sorted by dateAdded descending."""
    limit = max(1, limit)
    try:
        zot = _get_zot()
        items = zot.items(
            limit=limit * 3,  # over-fetch to account for filtered types
            sort="dateAdded",
            direction="desc",
        )
        items = [i for i in items if i.get("data", {}).get("itemType") not in _SKIP_TYPES][:limit]
    except Exception as e:
        return json.dumps({"error": f"Failed to fetch recent items: {e}"})

    result = [_item_to_dict(item, truncate_abstract=500) for item in items]
    return json.dumps({"items": result, "total": len(result)})
