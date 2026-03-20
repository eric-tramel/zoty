"""Zotero local library access and chunked full-text BM25 search."""

from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import html
import json
from pathlib import Path
import re
import shutil
import sqlite3
import sys
import threading
from typing import Any
import urllib.parse

import bm25s
from pyzotero import zotero

from zoty.fulltext_bridge import BridgeError, ensure_parent_fulltext


_ZOTERO_DIR = Path.home() / "Zotero"
_ZOTERO_STORAGE = _ZOTERO_DIR / "storage"
_ZOTERO_DB = _ZOTERO_DIR / "zotero.sqlite"
_SIDECAR_ROOT = Path.home() / ".cache" / "zoty" / "fulltext-index"
_SCHEMA_VERSION = "1"
_SKIP_TYPES = {"attachment", "note", "annotation"}
_CACHE_CONTENT_TYPES = {
    "application/epub+zip",
    "application/pdf",
    "application/xhtml+xml",
    "text/html",
}
_CHUNK_WORDS = 200
_CHUNK_OVERLAP_WORDS = 40
_SEARCH_RESULT_LIMIT_CAP = 25


@dataclass
class _ParentRecord:
    parent_key: str
    parent_item_id: int
    item_version: int
    date_modified: str
    item_type: str
    title: str
    abstract: str
    creators: list[str]
    collections: list[str]
    tags: list[str]
    date: str
    doi: str
    url: str
    metadata_hash: str


@dataclass
class _AttachmentRecord:
    attachment_key: str
    attachment_item_id: int
    parent_key: str
    item_version: int
    content_type: str
    link_mode: int | None
    source_path: str
    cache_path: str
    storage_mod_time: int | None
    storage_hash: str
    last_processed_mod_time: int | None
    fulltext_version: int | None
    indexed_pages: int | None
    total_pages: int | None
    indexed_chars: int | None
    total_chars: int | None
    source_signature: str


@dataclass
class _AttachmentIngestResult:
    extraction_state: str
    error_text: str
    content_hash: str
    content_chars: int
    token_count: int
    chunk_count: int
    docs: list[dict[str, Any]]


@dataclass
class _SearchState:
    snapshot_id: str
    source_fingerprint: str
    retriever: bm25s.BM25 | None
    corpus_docs: list[dict[str, Any]]
    parents: dict[str, dict[str, Any]]


_index_lock = threading.Lock()
_refresh_in_progress = False
_refresh_requested = False
_search_state: _SearchState | None = None
_zot: zotero.Zotero | None = None


def _manifest_db_path() -> Path:
    return _SIDECAR_ROOT / "manifest.sqlite"


def _snapshots_dir() -> Path:
    return _SIDECAR_ROOT / "snapshots"


def _get_zot() -> zotero.Zotero:
    """Return the shared pyzotero client, creating it on first call."""
    global _zot
    if _zot is None:
        _zot = zotero.Zotero("0", "user", local=True)
    return _zot


def _format_creators(creators: list[dict]) -> list[str]:
    """Turn pyzotero creator dicts into 'First Last' strings."""
    names = []
    for creator in creators:
        first = creator.get("firstName", "")
        last = creator.get("lastName", "")
        name = creator.get("name", "")
        if first or last:
            names.append(f"{first} {last}".strip())
        elif name:
            names.append(name)
    return names


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def _stable_hash(value: Any) -> str:
    if isinstance(value, str):
        payload = value
    else:
        payload = _json_dumps(value)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _normalize_plain_text(text: str) -> str:
    return " ".join(text.split())


def _extract_query_terms(query: str) -> list[str]:
    return re.findall(r"(?u)\b\w\w+\b", query.lower())


def _safe_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _safe_file_stats(path_str: str) -> tuple[int | None, int | None]:
    if not path_str:
        return None, None

    path = Path(path_str)
    try:
        stat = path.stat()
    except OSError:
        return None, None

    return int(stat.st_mtime), stat.st_size


def _read_text_file(path_str: str) -> str:
    return Path(path_str).read_text(encoding="utf-8", errors="ignore")


def _is_url_like_path(path_str: str) -> bool:
    lowered = path_str.lower()
    return lowered.startswith(("http://", "https://", "zotero://"))


def _is_plain_text_content_type(content_type: str) -> bool:
    lowered = content_type.lower()
    return lowered.startswith("text/") or lowered in {
        "application/json",
        "application/xml",
    }


def _uses_cache_file(content_type: str) -> bool:
    return content_type.lower() in _CACHE_CONTENT_TYPES


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


def _get_item_attachment_count(item_key: str) -> int:
    """Return the number of attachments linked to one parent item."""
    key = item_key.strip()
    if not key:
        return 0

    try:
        with closing(_open_zotero_db()) as conn:
            row = conn.execute(
                """SELECT COUNT(*) AS count
                   FROM itemAttachments ia
                   JOIN items parent ON parent.itemID = ia.parentItemID
                   WHERE parent.key = ?""",
                (key,),
            ).fetchone()
    except Exception:
        return 0

    return int(row["count"]) if row else 0


def _item_to_dict(
    item: dict,
    truncate_abstract: int = 0,
    *,
    include_attachments: bool = False,
) -> dict:
    """Convert a pyzotero item to a concise dict for tool output."""
    data = item.get("data", {})
    abstract = data.get("abstractNote", "")
    if truncate_abstract > 0 and len(abstract) > truncate_abstract:
        abstract = abstract[:truncate_abstract] + "..."

    collections = data.get("collections", [])
    tags = [tag.get("tag", "") for tag in data.get("tags", []) if tag.get("tag")]

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


def _ensure_sidecar_layout() -> None:
    _snapshots_dir().mkdir(parents=True, exist_ok=True)


def _connect_manifest(*, writable: bool = False) -> sqlite3.Connection:
    _ensure_sidecar_layout()
    path = _manifest_db_path()
    if writable:
        conn = sqlite3.connect(path)
    else:
        conn = sqlite3.connect(f"file:{path}?immutable=1", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _initialize_manifest(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS parents (
            parent_key TEXT PRIMARY KEY,
            parent_item_id INTEGER NOT NULL,
            item_version INTEGER NOT NULL,
            date_modified TEXT NOT NULL,
            item_type TEXT NOT NULL,
            title TEXT NOT NULL,
            abstract TEXT NOT NULL,
            creators_json TEXT NOT NULL,
            collections_json TEXT NOT NULL,
            tags_json TEXT NOT NULL,
            date TEXT NOT NULL,
            doi TEXT NOT NULL,
            url TEXT NOT NULL,
            metadata_hash TEXT NOT NULL,
            deleted INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS attachments (
            attachment_key TEXT PRIMARY KEY,
            attachment_item_id INTEGER NOT NULL,
            parent_key TEXT NOT NULL,
            item_version INTEGER NOT NULL,
            content_type TEXT NOT NULL,
            link_mode INTEGER,
            source_path TEXT NOT NULL,
            cache_path TEXT NOT NULL,
            storage_mod_time INTEGER,
            storage_hash TEXT NOT NULL,
            last_processed_mod_time INTEGER,
            fulltext_version INTEGER,
            indexed_pages INTEGER,
            total_pages INTEGER,
            indexed_chars INTEGER,
            total_chars INTEGER,
            extraction_state TEXT NOT NULL,
            source_signature TEXT NOT NULL,
            content_hash TEXT NOT NULL,
            content_chars INTEGER NOT NULL,
            token_count INTEGER NOT NULL,
            chunk_count INTEGER NOT NULL,
            last_ingested_at TEXT NOT NULL,
            error_text TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS docs (
            doc_id TEXT PRIMARY KEY,
            parent_key TEXT NOT NULL,
            attachment_key TEXT NOT NULL,
            doc_kind TEXT NOT NULL,
            chunk_index INTEGER NOT NULL,
            char_start INTEGER NOT NULL,
            char_end INTEGER NOT NULL,
            token_count INTEGER NOT NULL,
            text TEXT NOT NULL,
            text_hash TEXT NOT NULL
        );
        """
    )
    conn.execute(
        "INSERT OR REPLACE INTO meta(key, value) VALUES ('schema_version', ?)",
        (_SCHEMA_VERSION,),
    )
    conn.commit()


def _get_meta(conn: sqlite3.Connection, key: str, default: str = "") -> str:
    row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else default


def _set_meta(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO meta(key, value) VALUES (?, ?)",
        (key, value),
    )


def _open_zotero_db() -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{_ZOTERO_DB}?immutable=1", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _compute_source_fingerprint() -> str:
    with closing(_open_zotero_db()) as conn:
        parent_row = conn.execute(
            """SELECT COUNT(*) AS count,
                      COALESCE(MAX(i.version), 0) AS max_version
               FROM items i
               JOIN itemTypesCombined it ON i.itemTypeID = it.itemTypeID
               LEFT JOIN deletedItems di ON di.itemID = i.itemID
               WHERE it.typeName NOT IN ('attachment', 'note', 'annotation')
                 AND di.itemID IS NULL
                 AND COALESCE(i.libraryID, 1) = 1"""
        ).fetchone()
        attachment_row = conn.execute(
            """SELECT COUNT(*) AS count,
                      COALESCE(MAX(child.version), 0) AS max_version,
                      COALESCE(MAX(ia.lastProcessedModificationTime), 0) AS max_processed
               FROM itemAttachments ia
               JOIN items child ON child.itemID = ia.itemID
               JOIN items parent ON parent.itemID = ia.parentItemID
               LEFT JOIN deletedItems parent_deleted ON parent_deleted.itemID = parent.itemID
               LEFT JOIN deletedItems child_deleted ON child_deleted.itemID = child.itemID
               WHERE parent_deleted.itemID IS NULL
                 AND child_deleted.itemID IS NULL
                 AND COALESCE(parent.libraryID, 1) = 1"""
        ).fetchone()
        fulltext_row = conn.execute(
            """SELECT COUNT(*) AS count,
                      COALESCE(MAX(version), 0) AS max_version
               FROM fulltextItems"""
        ).fetchone()

    fingerprint = {
        "parent_count": int(parent_row["count"]),
        "parent_max_version": int(parent_row["max_version"]),
        "attachment_count": int(attachment_row["count"]),
        "attachment_max_version": int(attachment_row["max_version"]),
        "last_processed_mod_time": int(attachment_row["max_processed"]),
        "fulltext_count": int(fulltext_row["count"]),
        "fulltext_max_version": int(fulltext_row["max_version"]),
    }
    return _json_dumps(fingerprint)


def _fetch_parent_records() -> dict[str, _ParentRecord]:
    with closing(_open_zotero_db()) as conn:
        parents: dict[str, _ParentRecord] = {}
        parent_ids: set[int] = set()

        for row in conn.execute(
            """SELECT i.itemID,
                      i.key,
                      COALESCE(i.version, 0) AS item_version,
                      COALESCE(i.dateModified, '') AS date_modified,
                      it.typeName AS item_type
               FROM items i
               JOIN itemTypesCombined it ON i.itemTypeID = it.itemTypeID
               LEFT JOIN deletedItems di ON di.itemID = i.itemID
               WHERE it.typeName NOT IN ('attachment', 'note', 'annotation')
                 AND di.itemID IS NULL
                 AND COALESCE(i.libraryID, 1) = 1"""
        ):
            parent = _ParentRecord(
                parent_key=row["key"],
                parent_item_id=int(row["itemID"]),
                item_version=int(row["item_version"]),
                date_modified=row["date_modified"] or "",
                item_type=row["item_type"] or "",
                title="",
                abstract="",
                creators=[],
                collections=[],
                tags=[],
                date="",
                doi="",
                url="",
                metadata_hash="",
            )
            parents[parent.parent_key] = parent
            parent_ids.add(parent.parent_item_id)

        if not parents:
            return {}

        id_to_key = {parent.parent_item_id: parent.parent_key for parent in parents.values()}

        for row in conn.execute(
            """SELECT id.itemID, f.fieldName, idv.value
               FROM itemData id
               JOIN itemDataValues idv ON id.valueID = idv.valueID
               JOIN fields f ON id.fieldID = f.fieldID
               WHERE f.fieldName IN ('title', 'abstractNote', 'date', 'DOI', 'url')"""
        ):
            item_id = int(row["itemID"])
            if item_id not in parent_ids:
                continue
            parent = parents[id_to_key[item_id]]
            field_name = row["fieldName"]
            value = row["value"] or ""
            if field_name == "title":
                parent.title = value
            elif field_name == "abstractNote":
                parent.abstract = value
            elif field_name == "date":
                parent.date = value
            elif field_name == "DOI":
                parent.doi = value
            elif field_name == "url":
                parent.url = value

        for row in conn.execute(
            """SELECT ic.itemID,
                      c.firstName,
                      c.lastName,
                      c.fieldMode
               FROM itemCreators ic
               JOIN creators c ON ic.creatorID = c.creatorID
               ORDER BY ic.itemID ASC, ic.orderIndex ASC"""
        ):
            item_id = int(row["itemID"])
            if item_id not in parent_ids:
                continue
            if int(row["fieldMode"] or 0) == 1:
                name = row["lastName"] or ""
            else:
                name = f"{row['firstName'] or ''} {row['lastName'] or ''}".strip()
            if name:
                parents[id_to_key[item_id]].creators.append(name)

        collections_by_item: dict[int, list[str]] = {item_id: [] for item_id in parent_ids}
        for row in conn.execute(
            """SELECT ci.itemID, c.key
               FROM collectionItems ci
               JOIN collections c ON ci.collectionID = c.collectionID
               ORDER BY ci.itemID ASC, ci.orderIndex ASC, c.key ASC"""
        ):
            item_id = int(row["itemID"])
            if item_id not in parent_ids:
                continue
            collections_by_item[item_id].append(row["key"])
        for item_id, keys in collections_by_item.items():
            parents[id_to_key[item_id]].collections = keys

        tags_by_item: dict[int, list[str]] = {item_id: [] for item_id in parent_ids}
        for row in conn.execute(
            """SELECT it.itemID, t.name
               FROM itemTags it
               JOIN tags t ON it.tagID = t.tagID
               ORDER BY it.itemID ASC, t.name ASC"""
        ):
            item_id = int(row["itemID"])
            if item_id not in parent_ids:
                continue
            tags_by_item[item_id].append(row["name"])
        for item_id, names in tags_by_item.items():
            parents[id_to_key[item_id]].tags = names

    for parent in parents.values():
        parent.metadata_hash = _stable_hash({
            "abstract": parent.abstract,
            "collections": parent.collections,
            "creators": parent.creators,
            "date": parent.date,
            "date_modified": parent.date_modified,
            "doi": parent.doi,
            "item_type": parent.item_type,
            "tags": parent.tags,
            "title": parent.title,
            "url": parent.url,
            "version": parent.item_version,
        })

    return parents


def _fetch_attachment_records(parents: dict[str, _ParentRecord]) -> dict[str, _AttachmentRecord]:
    if not parents:
        return {}

    parent_item_ids = {parent.parent_item_id for parent in parents.values()}
    parent_keys_by_id = {parent.parent_item_id: parent.parent_key for parent in parents.values()}
    attachments: dict[str, _AttachmentRecord] = {}

    with closing(_open_zotero_db()) as conn:
        for row in conn.execute(
            """SELECT child.itemID AS attachment_item_id,
                      child.key AS attachment_key,
                      ia.parentItemID AS parent_item_id,
                      COALESCE(child.version, 0) AS item_version,
                      COALESCE(ia.contentType, '') AS content_type,
                      ia.linkMode,
                      COALESCE(ia.path, '') AS raw_path,
                      ia.storageModTime,
                      COALESCE(ia.storageHash, '') AS storage_hash,
                      ia.lastProcessedModificationTime,
                      fi.version AS fulltext_version,
                      fi.indexedPages,
                      fi.totalPages,
                      fi.indexedChars,
                      fi.totalChars
               FROM itemAttachments ia
               JOIN items child ON child.itemID = ia.itemID
               LEFT JOIN fulltextItems fi ON fi.itemID = ia.itemID"""
        ):
            parent_item_id = int(row["parent_item_id"])
            if parent_item_id not in parent_item_ids:
                continue

            attachment_key = row["attachment_key"]
            source_path = _resolve_attachment_filepath(attachment_key, row["raw_path"] or "")
            cache_path = str(_ZOTERO_STORAGE / attachment_key / ".zotero-ft-cache")
            source_mtime, source_size = _safe_file_stats(source_path)
            cache_mtime, cache_size = _safe_file_stats(cache_path)

            source_signature = _stable_hash({
                "attachment_key": attachment_key,
                "cache_mtime": cache_mtime,
                "cache_path": cache_path,
                "cache_size": cache_size,
                "content_type": row["content_type"] or "",
                "fulltext_version": _safe_int(row["fulltext_version"]),
                "indexed_chars": _safe_int(row["indexedChars"]),
                "indexed_pages": _safe_int(row["indexedPages"]),
                "item_version": int(row["item_version"]),
                "last_processed_mod_time": _safe_int(row["lastProcessedModificationTime"]),
                "link_mode": _safe_int(row["linkMode"]),
                "resolved_source_path": source_path,
                "source_mtime": source_mtime,
                "source_size": source_size,
                "total_chars": _safe_int(row["totalChars"]),
                "total_pages": _safe_int(row["totalPages"]),
            })

            attachments[attachment_key] = _AttachmentRecord(
                attachment_key=attachment_key,
                attachment_item_id=int(row["attachment_item_id"]),
                parent_key=parent_keys_by_id[parent_item_id],
                item_version=int(row["item_version"]),
                content_type=row["content_type"] or "",
                link_mode=_safe_int(row["linkMode"]),
                source_path=source_path,
                cache_path=cache_path,
                storage_mod_time=source_mtime,
                storage_hash=row["storage_hash"] or "",
                last_processed_mod_time=_safe_int(row["lastProcessedModificationTime"]),
                fulltext_version=_safe_int(row["fulltext_version"]),
                indexed_pages=_safe_int(row["indexedPages"]),
                total_pages=_safe_int(row["totalPages"]),
                indexed_chars=_safe_int(row["indexedChars"]),
                total_chars=_safe_int(row["totalChars"]),
                source_signature=source_signature,
            )

    return attachments


def _should_ensure_fulltext(attachment: _AttachmentRecord) -> bool:
    if not _uses_cache_file(attachment.content_type):
        return False
    if _is_url_like_path(attachment.source_path):
        return False

    cache_path = Path(attachment.cache_path)
    if cache_path.exists():
        if attachment.source_path:
            source_path = Path(attachment.source_path)
            try:
                if (
                    source_path.exists()
                    and attachment.last_processed_mod_time is not None
                    and int(source_path.stat().st_mtime) > attachment.last_processed_mod_time
                ):
                    return True
            except OSError:
                return False
        return False

    return True


def _build_metadata_doc(parent: _ParentRecord) -> dict[str, Any] | None:
    text = _normalize_plain_text(" ".join(part for part in [parent.title, parent.title, parent.abstract] if part))
    if not text:
        return None

    return {
        "doc_id": f"meta:{parent.parent_key}",
        "parent_key": parent.parent_key,
        "attachment_key": "",
        "doc_kind": "metadata",
        "chunk_index": 0,
        "char_start": 0,
        "char_end": len(text),
        "token_count": len(text.split()),
        "text": text,
        "text_hash": _stable_hash(text),
    }


def _coverage_is_partial(attachment: _AttachmentRecord) -> bool:
    if (
        attachment.total_pages is not None
        and attachment.total_pages > 0
        and attachment.indexed_pages is not None
        and attachment.indexed_pages < attachment.total_pages
    ):
        return True
    if (
        attachment.total_chars is not None
        and attachment.total_chars > 0
        and attachment.indexed_chars is not None
        and attachment.indexed_chars < attachment.total_chars
    ):
        return True
    return False


def _chunk_text(parent_key: str, attachment_key: str, text: str) -> list[dict[str, Any]]:
    words = text.split()
    if not words:
        return []

    offsets: list[int] = []
    cursor = 0
    for word in words:
        offsets.append(cursor)
        cursor += len(word) + 1

    docs: list[dict[str, Any]] = []
    step = max(1, _CHUNK_WORDS - _CHUNK_OVERLAP_WORDS)
    chunk_index = 0
    for start in range(0, len(words), step):
        chunk_words = words[start:start + _CHUNK_WORDS]
        if not chunk_words:
            continue

        end = start + len(chunk_words)
        chunk_text = " ".join(chunk_words)
        char_start = offsets[start]
        char_end = offsets[end - 1] + len(words[end - 1])
        docs.append({
            "doc_id": f"chunk:{attachment_key}:{chunk_index}",
            "parent_key": parent_key,
            "attachment_key": attachment_key,
            "doc_kind": "attachment_chunk",
            "chunk_index": chunk_index,
            "char_start": char_start,
            "char_end": char_end,
            "token_count": len(chunk_words),
            "text": chunk_text,
            "text_hash": _stable_hash(chunk_text),
        })
        chunk_index += 1

        if end >= len(words):
            break

    return docs


def _ingest_attachment(attachment: _AttachmentRecord) -> _AttachmentIngestResult:
    try:
        if _is_plain_text_content_type(attachment.content_type):
            if not attachment.source_path or _is_url_like_path(attachment.source_path):
                return _AttachmentIngestResult(
                    extraction_state="unsupported",
                    error_text="plain-text attachment has no local file path",
                    content_hash="",
                    content_chars=0,
                    token_count=0,
                    chunk_count=0,
                    docs=[],
                )
            if not Path(attachment.source_path).exists():
                return _AttachmentIngestResult(
                    extraction_state="missing",
                    error_text="plain-text attachment file not found",
                    content_hash="",
                    content_chars=0,
                    token_count=0,
                    chunk_count=0,
                    docs=[],
                )
            raw_text = _read_text_file(attachment.source_path)
            normalized = _normalize_plain_text(raw_text)
            docs = _chunk_text(attachment.parent_key, attachment.attachment_key, normalized)
            return _AttachmentIngestResult(
                extraction_state="indexed",
                error_text="",
                content_hash=_stable_hash(normalized) if normalized else "",
                content_chars=len(normalized),
                token_count=sum(doc["token_count"] for doc in docs),
                chunk_count=len(docs),
                docs=docs,
            )

        if _uses_cache_file(attachment.content_type):
            if _is_url_like_path(attachment.source_path):
                return _AttachmentIngestResult(
                    extraction_state="unsupported",
                    error_text="linked URL attachments are not indexable locally",
                    content_hash="",
                    content_chars=0,
                    token_count=0,
                    chunk_count=0,
                    docs=[],
                )
            if not Path(attachment.cache_path).exists():
                return _AttachmentIngestResult(
                    extraction_state="pending" if attachment.last_processed_mod_time else "missing",
                    error_text="full-text cache not available",
                    content_hash="",
                    content_chars=0,
                    token_count=0,
                    chunk_count=0,
                    docs=[],
                )

            raw_text = _read_text_file(attachment.cache_path)
            normalized = _normalize_plain_text(raw_text)
            docs = _chunk_text(attachment.parent_key, attachment.attachment_key, normalized)
            return _AttachmentIngestResult(
                extraction_state="partial" if _coverage_is_partial(attachment) else "indexed",
                error_text="",
                content_hash=_stable_hash(normalized) if normalized else "",
                content_chars=len(normalized),
                token_count=sum(doc["token_count"] for doc in docs),
                chunk_count=len(docs),
                docs=docs,
            )

        return _AttachmentIngestResult(
            extraction_state="unsupported",
            error_text=f"unsupported content type: {attachment.content_type}",
            content_hash="",
            content_chars=0,
            token_count=0,
            chunk_count=0,
            docs=[],
        )
    except Exception as exc:
        return _AttachmentIngestResult(
            extraction_state="error",
            error_text=str(exc),
            content_hash="",
            content_chars=0,
            token_count=0,
            chunk_count=0,
            docs=[],
        )


def _upsert_parent(conn: sqlite3.Connection, parent: _ParentRecord) -> None:
    conn.execute(
        """INSERT OR REPLACE INTO parents(
               parent_key, parent_item_id, item_version, date_modified, item_type,
               title, abstract, creators_json, collections_json, tags_json,
               date, doi, url, metadata_hash, deleted
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)""",
        (
            parent.parent_key,
            parent.parent_item_id,
            parent.item_version,
            parent.date_modified,
            parent.item_type,
            parent.title,
            parent.abstract,
            _json_dumps(parent.creators),
            _json_dumps(parent.collections),
            _json_dumps(parent.tags),
            parent.date,
            parent.doi,
            parent.url,
            parent.metadata_hash,
        ),
    )


def _upsert_attachment(
    conn: sqlite3.Connection,
    attachment: _AttachmentRecord,
    *,
    extraction_state: str,
    content_hash: str,
    content_chars: int,
    token_count: int,
    chunk_count: int,
    last_ingested_at: str,
    error_text: str,
) -> None:
    conn.execute(
        """INSERT OR REPLACE INTO attachments(
               attachment_key, attachment_item_id, parent_key, item_version, content_type,
               link_mode, source_path, cache_path, storage_mod_time, storage_hash,
               last_processed_mod_time, fulltext_version, indexed_pages, total_pages,
               indexed_chars, total_chars, extraction_state, source_signature, content_hash,
               content_chars, token_count, chunk_count, last_ingested_at, error_text
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            attachment.attachment_key,
            attachment.attachment_item_id,
            attachment.parent_key,
            attachment.item_version,
            attachment.content_type,
            attachment.link_mode,
            attachment.source_path,
            attachment.cache_path,
            attachment.storage_mod_time,
            attachment.storage_hash,
            attachment.last_processed_mod_time,
            attachment.fulltext_version,
            attachment.indexed_pages,
            attachment.total_pages,
            attachment.indexed_chars,
            attachment.total_chars,
            extraction_state,
            attachment.source_signature,
            content_hash,
            content_chars,
            token_count,
            chunk_count,
            last_ingested_at,
            error_text,
        ),
    )


def _insert_doc(conn: sqlite3.Connection, doc: dict[str, Any]) -> None:
    conn.execute(
        """INSERT OR REPLACE INTO docs(
               doc_id, parent_key, attachment_key, doc_kind, chunk_index,
               char_start, char_end, token_count, text, text_hash
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            doc["doc_id"],
            doc["parent_key"],
            doc["attachment_key"],
            doc["doc_kind"],
            doc["chunk_index"],
            doc["char_start"],
            doc["char_end"],
            doc["token_count"],
            doc["text"],
            doc["text_hash"],
        ),
    )


def _refresh_docs_manifest(
    conn: sqlite3.Connection,
    parents: dict[str, _ParentRecord],
    attachments: dict[str, _AttachmentRecord],
) -> None:
    existing_parent_hashes = {
        row["parent_key"]: row["metadata_hash"]
        for row in conn.execute("SELECT parent_key, metadata_hash FROM parents")
    }
    existing_attachment_rows = {
        row["attachment_key"]: dict(row)
        for row in conn.execute(
            """SELECT attachment_key, source_signature, extraction_state, content_hash,
                      content_chars, token_count, chunk_count, last_ingested_at, error_text
               FROM attachments"""
        )
    }

    current_parent_keys = set(parents)
    current_attachment_keys = set(attachments)
    manifest_parent_keys = set(existing_parent_hashes)
    manifest_attachment_keys = set(existing_attachment_rows)

    removed_attachments = manifest_attachment_keys - current_attachment_keys
    if removed_attachments:
        conn.executemany(
            "DELETE FROM docs WHERE attachment_key = ?",
            [(key,) for key in sorted(removed_attachments)],
        )
        conn.executemany(
            "DELETE FROM attachments WHERE attachment_key = ?",
            [(key,) for key in sorted(removed_attachments)],
        )

    removed_parents = manifest_parent_keys - current_parent_keys
    if removed_parents:
        conn.executemany(
            "DELETE FROM docs WHERE parent_key = ?",
            [(key,) for key in sorted(removed_parents)],
        )
        conn.executemany(
            "DELETE FROM attachments WHERE parent_key = ?",
            [(key,) for key in sorted(removed_parents)],
        )
        conn.executemany(
            "DELETE FROM parents WHERE parent_key = ?",
            [(key,) for key in sorted(removed_parents)],
        )

    for parent in parents.values():
        previous_hash = existing_parent_hashes.get(parent.parent_key)
        _upsert_parent(conn, parent)
        if previous_hash != parent.metadata_hash:
            conn.execute("DELETE FROM docs WHERE doc_id = ?", (f"meta:{parent.parent_key}",))
            metadata_doc = _build_metadata_doc(parent)
            if metadata_doc:
                _insert_doc(conn, metadata_doc)

    for attachment in attachments.values():
        existing_row = existing_attachment_rows.get(attachment.attachment_key)
        if existing_row is None or existing_row["source_signature"] != attachment.source_signature:
            ingested = _ingest_attachment(attachment)
            conn.execute(
                "DELETE FROM docs WHERE attachment_key = ?",
                (attachment.attachment_key,),
            )
            _upsert_attachment(
                conn,
                attachment,
                extraction_state=ingested.extraction_state,
                content_hash=ingested.content_hash,
                content_chars=ingested.content_chars,
                token_count=ingested.token_count,
                chunk_count=ingested.chunk_count,
                last_ingested_at=_now_iso(),
                error_text=ingested.error_text,
            )
            for doc in ingested.docs:
                _insert_doc(conn, doc)
            continue

        _upsert_attachment(
            conn,
            attachment,
            extraction_state=existing_row["extraction_state"],
            content_hash=existing_row["content_hash"],
            content_chars=int(existing_row["content_chars"]),
            token_count=int(existing_row["token_count"]),
            chunk_count=int(existing_row["chunk_count"]),
            last_ingested_at=existing_row["last_ingested_at"],
            error_text=existing_row["error_text"],
        )


def _load_docs_for_snapshot(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    docs = []
    for row in conn.execute(
        """SELECT doc_id, parent_key, attachment_key, doc_kind, chunk_index,
                  char_start, char_end, token_count, text, text_hash
           FROM docs
           ORDER BY doc_id ASC"""
    ):
        docs.append({
            "doc_id": row["doc_id"],
            "parent_key": row["parent_key"],
            "attachment_key": row["attachment_key"],
            "doc_kind": row["doc_kind"],
            "chunk_index": int(row["chunk_index"]),
            "char_start": int(row["char_start"]),
            "char_end": int(row["char_end"]),
            "token_count": int(row["token_count"]),
            "text": row["text"],
            "text_hash": row["text_hash"],
        })
    return docs


def _build_snapshot(
    docs: list[dict[str, Any]],
    *,
    source_fingerprint: str,
    parent_count: int,
    attachment_count: int,
) -> tuple[str, bm25s.BM25 | None, list[dict[str, Any]]]:
    snapshot_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
    snapshot_dir = _snapshots_dir() / snapshot_id
    temp_dir = _snapshots_dir() / f".{snapshot_id}.tmp"
    if temp_dir.exists():
        shutil.rmtree(temp_dir, ignore_errors=True)
    temp_dir.mkdir(parents=True, exist_ok=True)

    retriever: bm25s.BM25 | None = None
    indexed_docs: list[dict[str, Any]] = []
    if docs:
        token_lists = bm25s.tokenize(
            [doc["text"] for doc in docs],
            stopwords="en",
            show_progress=False,
            return_ids=False,
        )
        indexed_docs = [
            doc
            for doc, tokens in zip(docs, token_lists, strict=False)
            if tokens
        ]
        indexed_tokens = [tokens for tokens in token_lists if tokens]
        if indexed_tokens:
            retriever = bm25s.BM25()
            retriever.index(indexed_tokens, show_progress=False)
            retriever.save(temp_dir / "bm25", corpus=indexed_docs)

    snapshot_meta = {
        "attachment_count": attachment_count,
        "built_at": _now_iso(),
        "doc_count": len(docs),
        "parent_count": parent_count,
        "snapshot_id": snapshot_id,
        "source_fingerprint": source_fingerprint,
    }
    (temp_dir / "snapshot.json").write_text(
        json.dumps(snapshot_meta, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    temp_dir.replace(snapshot_dir)

    return snapshot_id, retriever, indexed_docs


def _load_parent_state(conn: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    parents: dict[str, dict[str, Any]] = {}
    for row in conn.execute(
        """SELECT parent_key, date_modified, item_type, title, abstract,
                  creators_json, collections_json, tags_json, date, doi, url
           FROM parents
           WHERE deleted = 0"""
    ):
        parents[row["parent_key"]] = {
            "key": row["parent_key"],
            "dateModified": row["date_modified"],
            "itemType": row["item_type"],
            "title": row["title"],
            "abstract": row["abstract"],
            "creators": json.loads(row["creators_json"]),
            "collections": json.loads(row["collections_json"]),
            "tags": json.loads(row["tags_json"]),
            "date": row["date"],
            "DOI": row["doi"],
            "url": row["url"],
        }
    return parents


def _load_snapshot(snapshot_id: str) -> _SearchState | None:
    snapshot_dir = _snapshots_dir() / snapshot_id
    if not snapshot_dir.exists():
        return None

    snapshot_meta_path = snapshot_dir / "snapshot.json"
    if not snapshot_meta_path.exists():
        return None

    try:
        snapshot_meta = json.loads(snapshot_meta_path.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"zoty: failed to read snapshot metadata: {exc}", file=sys.stderr)
        return None

    retriever: bm25s.BM25 | None = None
    corpus_docs: list[dict[str, Any]] = []
    bm25_dir = snapshot_dir / "bm25"
    if bm25_dir.exists():
        try:
            retriever = bm25s.BM25.load(bm25_dir, load_corpus=True)
            corpus_docs = list(getattr(retriever, "corpus", []))
        except Exception as exc:
            print(f"zoty: failed to load BM25 snapshot {snapshot_id}: {exc}", file=sys.stderr)
            retriever = None
            corpus_docs = []

    try:
        with closing(_connect_manifest()) as conn:
            parents = _load_parent_state(conn)
    except Exception as exc:
        print(f"zoty: failed to load manifest state: {exc}", file=sys.stderr)
        return None

    return _SearchState(
        snapshot_id=snapshot_id,
        source_fingerprint=snapshot_meta.get("source_fingerprint", ""),
        retriever=retriever,
        corpus_docs=corpus_docs,
        parents=parents,
    )


def _install_state(state: _SearchState) -> None:
    global _search_state
    with _index_lock:
        _search_state = state


def _prune_snapshots(*keep_snapshot_ids: str) -> None:
    keep = {snapshot_id for snapshot_id in keep_snapshot_ids if snapshot_id}
    for path in _snapshots_dir().iterdir():
        if not path.is_dir():
            continue
        if path.name in keep or path.name.startswith("."):
            continue
        shutil.rmtree(path, ignore_errors=True)


def _start_refresh_thread(*, force: bool = False) -> None:
    global _refresh_in_progress, _refresh_requested
    with _index_lock:
        if _refresh_in_progress:
            if force:
                _refresh_requested = True
            return
        _refresh_in_progress = True

    thread = threading.Thread(target=build_index_background, daemon=True)
    thread.start()


def prepare_search_index(*, force_refresh: bool = False) -> None:
    """Load the active snapshot if present and queue a refresh when needed."""
    try:
        with closing(_connect_manifest(writable=True)) as conn:
            _initialize_manifest(conn)
            active_snapshot_id = _get_meta(conn, "active_snapshot_id")
            stored_fingerprint = _get_meta(conn, "last_source_fingerprint")
    except Exception as exc:
        print(f"zoty: failed to initialize sidecar manifest: {exc}", file=sys.stderr)
        active_snapshot_id = ""
        stored_fingerprint = ""

    with _index_lock:
        has_state = _search_state is not None

    loaded_snapshot = has_state
    if active_snapshot_id and not has_state:
        state = _load_snapshot(active_snapshot_id)
        if state is not None:
            _install_state(state)
            loaded_snapshot = True

    try:
        current_fingerprint = _compute_source_fingerprint()
    except Exception as exc:
        print(f"zoty: failed to inspect Zotero source fingerprint: {exc}", file=sys.stderr)
        current_fingerprint = ""

    needs_refresh = force_refresh or not loaded_snapshot or not active_snapshot_id
    if current_fingerprint and current_fingerprint != stored_fingerprint:
        needs_refresh = True

    if needs_refresh:
        _start_refresh_thread(force=force_refresh)


def _refresh_search_index_once() -> None:
    current_fingerprint = _compute_source_fingerprint()
    parents = _fetch_parent_records()
    attachments = _fetch_attachment_records(parents)

    ensure_keys = sorted({
        attachment.parent_key
        for attachment in attachments.values()
        if _should_ensure_fulltext(attachment)
    })
    if ensure_keys:
        try:
            ensure_parent_fulltext(ensure_keys, complete=False)
            current_fingerprint = _compute_source_fingerprint()
            parents = _fetch_parent_records()
            attachments = _fetch_attachment_records(parents)
        except BridgeError as exc:
            print(f"zoty: full-text ensure skipped: {exc}", file=sys.stderr)
        except Exception as exc:
            print(f"zoty: full-text ensure failed: {exc}", file=sys.stderr)

    with closing(_connect_manifest(writable=True)) as conn:
        _initialize_manifest(conn)
        previous_snapshot_id = _get_meta(conn, "active_snapshot_id")
        _set_meta(conn, "last_refresh_started_at", _now_iso())
        _set_meta(conn, "last_refresh_status", "running")
        conn.commit()

        _refresh_docs_manifest(conn, parents, attachments)
        docs = _load_docs_for_snapshot(conn)
        snapshot_id, retriever, corpus_docs = _build_snapshot(
            docs,
            source_fingerprint=current_fingerprint,
            parent_count=len(parents),
            attachment_count=len(attachments),
        )
        _set_meta(conn, "active_snapshot_id", snapshot_id)
        _set_meta(conn, "last_source_fingerprint", current_fingerprint)
        _set_meta(conn, "last_refresh_finished_at", _now_iso())
        _set_meta(conn, "last_refresh_status", "ready")
        conn.commit()

        parents_state = _load_parent_state(conn)

    _install_state(_SearchState(
        snapshot_id=snapshot_id,
        source_fingerprint=current_fingerprint,
        retriever=retriever,
        corpus_docs=corpus_docs,
        parents=parents_state,
    ))
    _prune_snapshots(snapshot_id, previous_snapshot_id)
    print(
        f"zoty: search index ready ({len(parents)} parents, {len(attachments)} attachments, {len(corpus_docs)} ranked docs)",
        file=sys.stderr,
    )


def build_index_background() -> None:
    """Refresh the sidecar manifest and swap in a new snapshot."""
    global _refresh_in_progress, _refresh_requested
    rerun = False
    try:
        _refresh_search_index_once()
    except Exception as exc:
        try:
            with closing(_connect_manifest(writable=True)) as conn:
                _initialize_manifest(conn)
                _set_meta(conn, "last_refresh_finished_at", _now_iso())
                _set_meta(conn, "last_refresh_status", f"failed: {exc}")
                conn.commit()
        except Exception:
            pass
        print(f"zoty: failed to build search index: {exc}", file=sys.stderr)
    finally:
        with _index_lock:
            rerun = _refresh_requested
            _refresh_requested = False
            _refresh_in_progress = False

    if rerun:
        _start_refresh_thread(force=True)


def _background_ensure_and_refresh(parent_keys: list[str], *, complete: bool) -> None:
    try:
        ensure_parent_fulltext(parent_keys, complete=complete)
    except Exception as exc:
        print(f"zoty: failed to ensure parent fulltext for {parent_keys}: {exc}", file=sys.stderr)
    prepare_search_index(force_refresh=True)


def schedule_parent_fulltext_refresh(parent_keys: list[str], complete: bool = False) -> None:
    cleaned_keys = []
    for key in parent_keys:
        cleaned = key.strip().upper()
        if cleaned and cleaned not in cleaned_keys:
            cleaned_keys.append(cleaned)
    if not cleaned_keys:
        return

    thread = threading.Thread(
        target=_background_ensure_and_refresh,
        args=(cleaned_keys,),
        kwargs={"complete": complete},
        daemon=True,
    )
    thread.start()


def _snippet_from_text(text: str, query_terms: list[str], *, limit: int = 240) -> str:
    normalized = _normalize_plain_text(text)
    if not normalized:
        return ""
    if len(normalized) <= limit:
        return normalized

    lowered = normalized.lower()
    hit_index = -1
    hit_length = 0
    for term in query_terms:
        idx = lowered.find(term.lower())
        if idx >= 0 and (hit_index == -1 or idx < hit_index):
            hit_index = idx
            hit_length = len(term)

    if hit_index < 0:
        return normalized[:limit]

    center = hit_index + max(1, hit_length // 2)
    start = max(0, center - (limit // 2))
    end = min(len(normalized), start + limit)
    start = max(0, end - limit)
    return normalized[start:end].strip()


def _result_from_parent(
    parent: dict[str, Any],
    *,
    score: float,
    best_doc: dict[str, Any],
    query_terms: list[str],
) -> dict[str, Any]:
    result = {
        "key": parent["key"],
        "itemType": parent["itemType"],
        "title": parent["title"],
        "creators": list(parent["creators"]),
        "date": parent["date"],
        "DOI": parent["DOI"],
        "url": parent["url"],
        "tags": list(parent["tags"]),
        "collections": list(parent["collections"]),
        "abstract": parent["abstract"][:500] + "..." if len(parent["abstract"]) > 500 else parent["abstract"],
        "attachment_count": _get_item_attachment_count(parent["key"]),
        "score": round(score, 4),
    }

    if best_doc["doc_kind"] == "attachment_chunk":
        snippet = _snippet_from_text(best_doc["text"], query_terms)
        if snippet:
            result["snippet"] = snippet
            result["snippet_attachment_key"] = best_doc["attachment_key"]
    else:
        snippet = _snippet_from_text(parent["abstract"], query_terms)
        if snippet:
            result["snippet"] = snippet

    result["_date_modified"] = parent["dateModified"]
    return result


def _search_response(
    query: str,
    results: list[dict[str, Any]],
    *,
    requested_limit: int,
    applied_limit: int,
    error: str | None = None,
) -> str:
    response: dict[str, Any] = {
        "results": results,
        "query": query,
        "total": len(results),
        "requested_limit": requested_limit,
        "applied_limit": applied_limit,
        "limit_cap": _SEARCH_RESULT_LIMIT_CAP,
        "limit_capped": requested_limit > applied_limit,
    }
    if error is not None:
        response["error"] = error
    return json.dumps(response)


def _item_summary_from_parent(parent: dict[str, Any]) -> dict[str, Any]:
    return {
        "key": parent["key"],
        "itemType": parent["itemType"],
        "title": parent["title"],
        "creators": list(parent["creators"]),
        "date": parent["date"],
        "DOI": parent["DOI"],
        "url": parent["url"],
        "tags": list(parent["tags"]),
        "collections": list(parent["collections"]),
        "abstract": parent["abstract"],
        "attachments": _get_item_attachments(parent["key"]),
    }


def _result_from_doc(
    parent: dict[str, Any],
    *,
    score: float,
    doc: dict[str, Any],
    query_terms: list[str],
    attachments_by_key: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    snippet_source = doc["text"] if doc["doc_kind"] == "attachment_chunk" else (parent["abstract"] or doc["text"])
    result = {
        "item_key": parent["key"],
        "title": parent["title"],
        "itemType": parent["itemType"],
        "score": round(score, 4),
        "match_type": doc["doc_kind"],
        "snippet": _snippet_from_text(snippet_source, query_terms),
        "chunk_index": doc["chunk_index"],
        "char_start": doc["char_start"],
        "char_end": doc["char_end"],
    }

    if doc["doc_kind"] == "attachment_chunk":
        attachment = attachments_by_key.get(doc["attachment_key"], {})
        result["attachment_key"] = doc["attachment_key"]
        result["attachment_title"] = attachment.get("title", "")
        result["attachment_filepath"] = attachment.get("filepath", "")

    return result


def search(
    query: str,
    collection_key: str = "",
    item_type: str = "",
    limit: int = 10,
) -> str:
    """BM25 ranked search over titles, abstracts, and indexed attachment full text."""
    requested_limit = max(1, limit)
    applied_limit = min(requested_limit, _SEARCH_RESULT_LIMIT_CAP)

    with _index_lock:
        state = _search_state

    if state is None:
        return _search_response(
            query,
            [],
            requested_limit=requested_limit,
            applied_limit=applied_limit,
            error="Index is still building, please retry in a moment",
        )

    if state.retriever is None or not state.corpus_docs:
        return _search_response(
            query,
            [],
            requested_limit=requested_limit,
            applied_limit=applied_limit,
        )

    query_tokens = bm25s.tokenize(
        [query],
        stopwords="en",
        show_progress=False,
        return_ids=False,
    )
    query_terms = _extract_query_terms(query)
    if not query_terms or not query_tokens or not query_tokens[0]:
        return _search_response(
            query,
            [],
            requested_limit=requested_limit,
            applied_limit=applied_limit,
        )

    max_docs = len(state.corpus_docs)
    batch_size = min(max(applied_limit * 20, 200), max_docs)
    best_by_parent: dict[str, tuple[float, dict[str, Any]]] = {}

    while batch_size > 0:
        results, scores = state.retriever.retrieve(
            query_tokens,
            corpus=state.corpus_docs,
            k=batch_size,
            show_progress=False,
        )

        found_enough = False
        for index in range(results.shape[1]):
            doc = results[0, index]
            score = float(scores[0, index])
            if score <= 0:
                continue

            parent_key = doc["parent_key"]
            parent = state.parents.get(parent_key)
            if not parent:
                continue
            if collection_key and collection_key not in parent["collections"]:
                continue
            if item_type and parent["itemType"].lower() != item_type.lower():
                continue

            previous = best_by_parent.get(parent_key)
            if previous is None or score > previous[0]:
                best_by_parent[parent_key] = (score, doc)

            if len(best_by_parent) >= applied_limit:
                found_enough = True
                break

        if found_enough or batch_size >= max_docs:
            break

        batch_size = min(max_docs, batch_size * 2)

    results_payload = [
        _result_from_parent(
            state.parents[parent_key],
            score=score,
            best_doc=doc,
            query_terms=query_terms,
        )
        for parent_key, (score, doc) in best_by_parent.items()
    ]

    results_payload.sort(key=lambda row: row["key"])
    results_payload.sort(key=lambda row: row["_date_modified"], reverse=True)
    results_payload.sort(key=lambda row: row["score"], reverse=True)
    results_payload = results_payload[:applied_limit]
    for row in results_payload:
        row.pop("_date_modified", None)

    return _search_response(
        query,
        results_payload,
        requested_limit=requested_limit,
        applied_limit=applied_limit,
    )


def search_within_item(
    item_key: str,
    query: str,
    limit: int = 5,
) -> str:
    """BM25 ranked passage search within one parent item's metadata and attachment chunks."""
    limit = max(1, limit)
    normalized_item_key = item_key.strip().upper()

    with _index_lock:
        state = _search_state

    if state is None:
        return json.dumps({
            "error": "Index is still building, please retry in a moment",
            "results": [],
            "query": query,
            "item_key": normalized_item_key,
            "total": 0,
        })

    parent = state.parents.get(normalized_item_key)
    if parent is None:
        return json.dumps({
            "error": f"Item {normalized_item_key} was not found in the search index",
            "results": [],
            "query": query,
            "item_key": normalized_item_key,
            "total": 0,
        })

    item_summary = _item_summary_from_parent(parent)

    if state.retriever is None or not state.corpus_docs:
        return json.dumps({
            "item": item_summary,
            "results": [],
            "query": query,
            "item_key": normalized_item_key,
            "total": 0,
        })

    query_tokens = bm25s.tokenize(
        [query],
        stopwords="en",
        show_progress=False,
        return_ids=False,
    )
    query_terms = _extract_query_terms(query)
    if not query_terms or not query_tokens or not query_tokens[0]:
        return json.dumps({
            "item": item_summary,
            "results": [],
            "query": query,
            "item_key": normalized_item_key,
            "total": 0,
        })

    attachments = item_summary["attachments"]
    attachments_by_key = {
        attachment["key"]: attachment
        for attachment in attachments
    }

    max_docs = len(state.corpus_docs)
    batch_size = min(max(limit * 20, 200), max_docs)
    matches: list[dict[str, Any]] = []
    seen_doc_ids: set[str] = set()

    while batch_size > 0:
        results, scores = state.retriever.retrieve(
            query_tokens,
            corpus=state.corpus_docs,
            k=batch_size,
            show_progress=False,
        )

        found_enough = False
        for index in range(results.shape[1]):
            doc = results[0, index]
            score = float(scores[0, index])
            if score <= 0:
                continue
            if doc["parent_key"] != normalized_item_key:
                continue
            if doc["doc_id"] in seen_doc_ids:
                continue

            seen_doc_ids.add(doc["doc_id"])
            matches.append(_result_from_doc(
                parent,
                score=score,
                doc=doc,
                query_terms=query_terms,
                attachments_by_key=attachments_by_key,
            ))

            if len(matches) >= limit:
                found_enough = True
                break

        if found_enough or batch_size >= max_docs:
            break

        batch_size = min(max_docs, batch_size * 2)

    return json.dumps({
        "item": item_summary,
        "results": matches[:limit],
        "query": query,
        "item_key": normalized_item_key,
        "total": len(matches[:limit]),
    })


def list_collections() -> str:
    """Return all collections with keys, names, and item counts."""
    try:
        zot = _get_zot()
        collections = zot.collections()
    except Exception as exc:
        return json.dumps({"error": f"Failed to fetch collections: {exc}"})

    result = []
    for collection in collections:
        data = collection.get("data", {})
        meta = collection.get("meta", {})
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
    except Exception as exc:
        return json.dumps({"error": f"Failed to fetch collection items: {exc}"})

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
    except Exception as exc:
        return json.dumps({"error": f"Failed to fetch item {item_key}: {exc}"})

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
        except Exception as exc:
            errors.append({
                "key": key,
                "error": f"Failed to fetch citation entry: {exc}",
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
            limit=limit * 3,
            sort="dateAdded",
            direction="desc",
        )
        items = [item for item in items if item.get("data", {}).get("itemType") not in _SKIP_TYPES][:limit]
    except Exception as exc:
        return json.dumps({"error": f"Failed to fetch recent items: {exc}"})

    result = [_item_to_dict(item, truncate_abstract=500) for item in items]
    return json.dumps({"items": result, "total": len(result)})
