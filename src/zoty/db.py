"""Zotero local library access and chunked full-text BM25 search."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
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
_LIST_RESULT_LIMIT_CAP = 100
_LIST_VIEW_MAX_CREATORS = 5
_CITATION_EXPORT_MAX_WORKERS = 4
_ITEM_DETAIL_MAX_WORKERS = 4
_DETAIL_VIEW_MAX_CREATORS = 15
_BIBTEX_MAX_AUTHORS = 10
_EMPTY_QUERY_WARNING = "Query produced no searchable terms after stop-word removal. Try more specific keywords."
_LINK_MODE_LABELS = {
    0: "imported_file",
    1: "imported_url",
    2: "linked_file",
    3: "linked_url",
}

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
_zot_lock = threading.Lock()
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
    zot = _zot
    if zot is not None:
        return zot

    with _zot_lock:
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


def _truncate_creator_names(creators: list[str], *, max_creators: int = _LIST_VIEW_MAX_CREATORS) -> list[str]:
    """Keep list/search payloads compact by capping long author lists."""
    if max_creators < 0 or len(creators) <= max_creators:
        return list(creators)

    truncated = list(creators[:max_creators])
    truncated.append(f"... and {len(creators) - max_creators} more")
    return truncated


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


def _normalize_item_date(value: str) -> str:
    normalized = " ".join(value.split())
    if not normalized:
        return ""

    parts = normalized.split(" ")
    if (
        len(parts) == 2
        and parts[0] == parts[1]
        and re.fullmatch(r"\d{4}-\d{2}-\d{2}", parts[0]) is not None
    ):
        return parts[0]

    return normalized


def _extract_query_terms(query: str) -> list[str]:
    return re.findall(r"(?u)\b\w\w+\b", query.lower())


def _safe_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _format_link_mode(value: Any) -> str:
    mode = _safe_int(value)
    if mode is None:
        return "unknown"
    return _LINK_MODE_LABELS.get(mode, f"unknown({mode})")


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


def _log_attachment_helper_error(message: str, exc: Exception) -> None:
    print(f"zoty: {message}: {exc}", file=sys.stderr)


def _get_item_attachments_by_parent(item_keys: list[str]) -> dict[str, list[dict[str, Any]]]:
    """Return attachment metadata for each requested parent item key."""
    normalized_keys: list[str] = []
    for item_key in item_keys:
        cleaned = item_key.strip().upper()
        if cleaned and cleaned not in normalized_keys:
            normalized_keys.append(cleaned)

    if not normalized_keys:
        return {}

    attachments_by_parent = {key: [] for key in normalized_keys}
    placeholders = ",".join("?" for _ in normalized_keys)

    try:
        with closing(_open_zotero_db()) as conn:
            rows = conn.execute(
                f"""SELECT parent.key AS parent_key,
                           child.key AS attachment_key,
                           COALESCE(MAX(CASE WHEN f.fieldName = 'title' THEN idv.value END), '') AS attachment_title,
                           ia.contentType AS content_type,
                           ia.linkMode AS link_mode,
                           ia.path AS raw_path
                    FROM items parent
                    JOIN itemAttachments ia ON parent.itemID = ia.parentItemID
                    JOIN items child ON ia.itemID = child.itemID
                    LEFT JOIN itemData id ON child.itemID = id.itemID
                    LEFT JOIN itemDataValues idv ON id.valueID = idv.valueID
                    LEFT JOIN fields f ON id.fieldID = f.fieldID
                    WHERE parent.key IN ({placeholders})
                    GROUP BY parent.key, child.key, ia.contentType, ia.linkMode, ia.path, child.dateAdded
                    ORDER BY parent.key ASC, child.dateAdded ASC""",
                normalized_keys,
            ).fetchall()
    except Exception as exc:
        _log_attachment_helper_error(
            f"failed to load attachment metadata for {', '.join(normalized_keys)}",
            exc,
        )
        return attachments_by_parent

    for row in rows:
        parent_key = str(row["parent_key"])
        attachment_key = str(row["attachment_key"])
        filepath = _resolve_attachment_filepath(attachment_key, row["raw_path"] or "")
        if not filepath:
            continue
        attachments_by_parent.setdefault(parent_key, []).append({
            "key": attachment_key,
            "title": row["attachment_title"],
            "contentType": row["content_type"] or "",
            "linkMode": _format_link_mode(row["link_mode"]),
            "filepath": filepath,
        })

    return attachments_by_parent


def _get_item_attachments(item_key: str) -> list[dict[str, Any]]:
    """Return attachment metadata and resolved filepaths for one parent item."""
    key = item_key.strip().upper()
    if not key:
        return []
    return _get_item_attachments_by_parent([key]).get(key, [])


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
    except Exception as exc:
        _log_attachment_helper_error(f"failed to count attachments for {key}", exc)
        return 0

    return int(row["count"]) if row else 0


def _get_item_attachment_counts(item_keys: list[str]) -> dict[str, int]:
    """Return attachment counts for many parent items in one query."""
    normalized_keys: list[str] = []
    for item_key in item_keys:
        cleaned = item_key.strip()
        if cleaned and cleaned not in normalized_keys:
            normalized_keys.append(cleaned)

    if not normalized_keys:
        return {}

    placeholders = ",".join("?" for _ in normalized_keys)
    counts = {key: 0 for key in normalized_keys}

    try:
        with closing(_open_zotero_db()) as conn:
            rows = conn.execute(
                f"""SELECT parent.key, COUNT(*) AS count
                    FROM itemAttachments ia
                    JOIN items parent ON parent.itemID = ia.parentItemID
                    WHERE parent.key IN ({placeholders})
                    GROUP BY parent.key""",
                normalized_keys,
            ).fetchall()
    except Exception as exc:
        _log_attachment_helper_error(
            f"failed to count attachments for {', '.join(normalized_keys)}",
            exc,
        )
        return counts

    for row in rows:
        counts[str(row["key"])] = int(row["count"])

    return counts


def _item_to_dict(
    item: dict,
    truncate_abstract: int = 0,
    *,
    include_attachment_count: bool = False,
    include_attachments: bool = False,
    max_creators: int = -1,
    attachments: list[dict[str, Any]] | None = None,
    attachment_count: int | None = None,
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
        "creators": _truncate_creator_names(
            _format_creators(data.get("creators", [])),
            max_creators=max_creators,
        ),
        "date": data.get("date", ""),
        "DOI": data.get("DOI", ""),
        "url": data.get("url", ""),
        "tags": tags,
        "collections": collections,
        "abstract": abstract,
    }

    if include_attachment_count:
        if attachment_count is None:
            attachment_count = _get_item_attachment_count(data.get("key", ""))
        result["attachment_count"] = attachment_count

    if include_attachments:
        resolved_attachments = attachments
        if resolved_attachments is None:
            resolved_attachments = _get_item_attachments(data.get("key", ""))
        result["attachment_count"] = len(resolved_attachments)
        result["attachments"] = resolved_attachments

    return result


def _empty_item_payload(item_key: str = "") -> dict[str, Any]:
    """Return the get_item shape with empty values for error responses."""
    return {
        "key": item_key,
        "itemType": "",
        "title": "",
        "creators": [],
        "date": "",
        "DOI": "",
        "url": "",
        "tags": [],
        "collections": [],
        "abstract": "",
        "attachment_count": 0,
        "attachments": [],
    }


def _empty_item_summary(item_key: str = "") -> dict[str, str]:
    return {
        "key": item_key,
        "title": "",
    }


def _error_payload(error: str, *, key: str = "") -> dict[str, str]:
    payload = {"error": error}
    if key:
        payload["key"] = key
    return payload


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


def _fetch_item_detail(item_key: str) -> dict[str, Any]:
    """Fetch one Zotero item detail payload."""
    return _get_zot().item(item_key)


def _xhtml_to_text(fragment: str) -> str:
    """Collapse Zotero's XHTML bibliography/citation output into plain text."""
    text = re.sub(r"<[^>]+>", " ", fragment)
    return " ".join(html.unescape(text).split())


def _strip_bibtex_field(bibtex: str, field_name: str) -> str:
    """Remove a top-level BibTeX field from each entry while preserving the rest."""
    if not bibtex:
        return ""

    field_pattern = re.compile(rf"^[ \t]*{re.escape(field_name)}[ \t]*=", re.IGNORECASE)
    output: list[str] = []
    index = 0
    length = len(bibtex)

    while index < length:
        if index == 0 or bibtex[index - 1] == "\n":
            line_end = bibtex.find("\n", index)
            if line_end == -1:
                line_end = length

            field_match = field_pattern.match(bibtex[index:line_end])
            if field_match:
                value_index = index + field_match.end()
                while value_index < length and bibtex[value_index] in " \t\r\n":
                    value_index += 1

                if value_index < length and bibtex[value_index] == "{":
                    depth = 0
                    while value_index < length:
                        char = bibtex[value_index]
                        if char == "{" and (value_index == 0 or bibtex[value_index - 1] != "\\"):
                            depth += 1
                        elif char == "}" and (value_index == 0 or bibtex[value_index - 1] != "\\"):
                            depth -= 1
                            if depth == 0:
                                value_index += 1
                                break
                        value_index += 1
                elif value_index < length and bibtex[value_index] == "\"":
                    value_index += 1
                    while value_index < length:
                        char = bibtex[value_index]
                        if char == "\"" and bibtex[value_index - 1] != "\\":
                            value_index += 1
                            break
                        value_index += 1
                else:
                    while value_index < length and bibtex[value_index] not in ",\n":
                        value_index += 1

                while value_index < length and bibtex[value_index] in " \t":
                    value_index += 1
                if value_index < length and bibtex[value_index] == ",":
                    value_index += 1
                while value_index < length and bibtex[value_index] in " \t":
                    value_index += 1
                if value_index < length and bibtex[value_index] == "\n":
                    value_index += 1

                index = value_index
                continue

        output.append(bibtex[index])
        index += 1

    return "".join(output)


def _truncate_bibtex_authors(bibtex: str, *, max_authors: int = _BIBTEX_MAX_AUTHORS) -> str:
    if not bibtex or max_authors < 0:
        return bibtex

    pattern = re.compile(
        r"(^[ \t]*author[ \t]*=[ \t]*\{)(.*?)(\}[ \t]*,?[ \t]*$)",
        re.IGNORECASE | re.MULTILINE | re.DOTALL,
    )

    def replace(match: re.Match[str]) -> str:
        authors = [
            author.strip()
            for author in re.sub(r"\s+", " ", match.group(2)).split(" and ")
            if author.strip()
        ]
        if len(authors) <= max_authors:
            return match.group(0)
        truncated = " and ".join([*authors[:max_authors], "others"])
        return f"{match.group(1)}{truncated}{match.group(3)}"

    return pattern.sub(replace, bibtex, count=1)


def _compact_bibtex_export(bibtex: str) -> str:
    """Drop fields that duplicate data already provided by other tools."""
    compacted = _strip_bibtex_field(bibtex, "abstract")
    compacted = _strip_bibtex_field(compacted, "file")
    compacted = _truncate_bibtex_authors(compacted)
    return compacted.strip()


def _response_status_code(exc: Exception) -> int | None:
    response = getattr(exc, "response", None)
    status = getattr(response, "status_code", None)
    if isinstance(status, int):
        return status

    match = re.search(r"\bCode:\s*(\d{3})\b", str(exc), re.IGNORECASE)
    if match is not None:
        return int(match.group(1))
    return None


def _response_url(exc: Exception) -> str:
    response = getattr(exc, "response", None)
    url = getattr(response, "url", "")
    if isinstance(url, str) and url:
        return url

    match = re.search(r"\bURL:\s*(\S+)", str(exc), re.IGNORECASE)
    if match is not None:
        return match.group(1)
    return ""


def _response_body(exc: Exception) -> str:
    match = re.search(r"\bResponse:\s*(.*)", str(exc), re.IGNORECASE | re.DOTALL)
    if match is None:
        return ""
    return " ".join(match.group(1).split())


def _sanitize_external_error_message(exc: Exception, *, fallback: str) -> str:
    message = " ".join(str(exc).split())
    if not message:
        return fallback

    sanitized = re.sub(r"https?://\S+", "", message)
    sanitized = re.sub(r"^(?:GET|POST|PUT|PATCH|DELETE|HEAD|OPTIONS)\b", "", sanitized, flags=re.IGNORECASE)
    sanitized = re.sub(r"(?i)\b(?:method|code|response|body)\b[^.;]*", "", sanitized)
    sanitized = re.sub(r"\s+", " ", sanitized).strip(" .,:;-")
    if sanitized and sanitized.upper() not in {"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"}:
        return sanitized

    status = _response_status_code(exc)
    if status is not None:
        return f"request failed with status {status}"
    return fallback


def _item_fetch_error_message(item_key: str, exc: Exception) -> str:
    status = _response_status_code(exc)
    if status == 404 or "not found" in str(exc).lower():
        return f"Item {item_key} was not found"
    detail = _sanitize_external_error_message(exc, fallback="request failed")
    return f"Failed to fetch item {item_key}: {detail}"


def _citation_fetch_error_message(item_key: str, style: str, exc: Exception) -> str:
    status = _response_status_code(exc)
    url = _response_url(exc).lower()
    lowered = str(exc).lower()
    response_body = _response_body(exc).lower()

    if status == 404 and (
        "/styles/" in url
        or "citationstyles.org" in url
        or "/styles/" in lowered
        or "citationstyles.org" in lowered
        or "citation style" in response_body
        or ("style" in response_body and "not found" in response_body)
    ):
        return f"Citation style {style} was not found"
    if status == 404 or "not found" in lowered:
        return f"Item {item_key} was not found"

    detail = _sanitize_external_error_message(exc, fallback="request failed")
    return f"Failed to fetch citation entry: {detail}"


def _fetch_item_exports(
    item_key: str,
    *,
    style: str,
    locale: str,
) -> dict[str, str]:
    """Fetch formatted citation, bibliography, and BibTeX blocks for one item."""
    zot = _get_zot()
    exported = zot.item(
        item_key,
        format="json",
        include="bib,citation,bibtex",
        style=style,
        locale=locale,
    )

    if not isinstance(exported, dict):
        raise TypeError(f"Unexpected export payload: {type(exported).__name__}")

    data = exported.get("data", {}) if isinstance(exported.get("data"), dict) else {}

    def _get_export_block(name: str) -> str:
        value = exported.get(name, data.get(name, ""))
        if isinstance(value, list):
            return value[0] if value else ""
        if isinstance(value, str):
            return value
        return ""

    return {
        "citation": _get_export_block("citation"),
        "bibliography": _get_export_block("bib"),
        "bibtex": _get_export_block("bibtex"),
    }


def _ensure_sidecar_layout() -> None:
    _snapshots_dir().mkdir(parents=True, exist_ok=True)


def _connect_manifest(*, writable: bool = False) -> sqlite3.Connection:
    _ensure_sidecar_layout()
    path = _manifest_db_path()
    if writable:
        conn = sqlite3.connect(path)
    else:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
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
                parent.date = _normalize_item_date(value)
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
    with _index_lock:
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
        with _index_lock:
            state = _search_state if _search_state is not None else _load_snapshot(active_snapshot_id)
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
    attachment_count: int,
    attachments: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    date_value = _normalize_item_date(str(parent.get("date", "") or ""))
    result = {
        "key": parent["key"],
        "itemType": parent["itemType"],
        "title": parent["title"],
        "creators": _truncate_creator_names(parent["creators"]),
        "date": date_value,
        "DOI": parent["DOI"],
        "url": parent["url"],
        "tags": list(parent["tags"]),
        "collections": list(parent["collections"]),
        "abstract": parent["abstract"][:500] + "..." if len(parent["abstract"]) > 500 else parent["abstract"],
        "attachment_count": attachment_count,
        "score": round(score, 4),
    }
    if attachments is not None:
        result["attachments"] = list(attachments)

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
    items: list[dict[str, Any]],
    *,
    requested_limit: int,
    applied_limit: int,
    error: str | None = None,
    warning: str | None = None,
) -> str:
    response: dict[str, Any] = {
        "items": items,
        "query": query,
        "total": len(items),
        "requested_limit": requested_limit,
        "applied_limit": applied_limit,
        "limit_cap": _SEARCH_RESULT_LIMIT_CAP,
        "limit_capped": requested_limit > applied_limit,
    }
    if error is not None:
        response["error"] = error
    if warning is not None:
        response["warning"] = warning
    return json.dumps(response)


def _apply_limit_cap(limit: int, cap: int) -> tuple[int, int]:
    requested_limit = max(1, limit)
    return requested_limit, min(requested_limit, cap)


def _limit_response_metadata(requested_limit: int, applied_limit: int, cap: int) -> dict[str, Any]:
    return {
        "requested_limit": requested_limit,
        "applied_limit": applied_limit,
        "limit_cap": cap,
        "limit_capped": requested_limit > applied_limit,
    }


def _item_summary_from_parent(parent: dict[str, Any]) -> dict[str, Any]:
    return {
        "key": parent["key"],
        "title": parent["title"],
    }


def _result_from_doc(
    parent: dict[str, Any],
    *,
    score: float,
    doc: dict[str, Any],
    query_terms: list[str],
    attachments_by_key: dict[str, dict[str, Any]],
    include_parent_key: bool,
) -> dict[str, Any]:
    snippet_source = doc["text"] if doc["doc_kind"] == "attachment_chunk" else (parent["abstract"] or doc["text"])
    result = {
        "itemType": parent["itemType"],
        "score": round(score, 4),
        "match_type": doc["doc_kind"],
        "snippet": _snippet_from_text(snippet_source, query_terms),
        "chunk_index": doc["chunk_index"],
        "char_start": doc["char_start"],
        "char_end": doc["char_end"],
    }
    if include_parent_key:
        result["key"] = parent["key"]

    if doc["doc_kind"] == "attachment_chunk":
        attachment = attachments_by_key.get(doc["attachment_key"], {})
        result["attachment_key"] = doc["attachment_key"]
        result["attachment_title"] = attachment.get("title", "")
        result["attachment_filepath"] = attachment.get("filepath", "")

    return result


def _search_within_item_response(
    *,
    query: str,
    matches: list[dict[str, Any]],
    key: str | None = None,
    item: dict[str, Any] | None = None,
    item_keys: list[str] | None = None,
    items: list[dict[str, Any]] | None = None,
    missing_item_keys: list[str] | None = None,
    error: str | None = None,
    warning: str | None = None,
) -> str:
    payload: dict[str, Any] = {
        "matches": matches,
        "query": query,
        "total": len(matches),
    }
    if item_keys is not None:
        payload["item_keys"] = list(item_keys)
        payload["items"] = items or []
        if missing_item_keys:
            payload["missing_item_keys"] = list(missing_item_keys)
    else:
        normalized_key = key or ""
        payload["key"] = normalized_key
        payload["item"] = item or _empty_item_summary(normalized_key)
    if error is not None:
        payload["error"] = error
    if warning is not None:
        payload["warning"] = warning
    return json.dumps(payload)


def search(
    query: str,
    collection_key: str = "",
    item_type: str = "",
    limit: int = 10,
    include_attachments: bool = False,
) -> str:
    """BM25 ranked search over titles, abstracts, and indexed attachment full text."""
    requested_limit = max(1, limit)
    applied_limit = min(requested_limit, _SEARCH_RESULT_LIMIT_CAP)
    normalized_collection_key = collection_key.strip().upper()
    normalized_item_type = item_type.strip().lower()

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

    filter_warnings: list[str] = []
    if normalized_collection_key:
        known_collection_keys = {
            collection.strip().upper()
            for parent in state.parents.values()
            for collection in parent.get("collections", [])
            if collection.strip()
        }
        if normalized_collection_key not in known_collection_keys:
            filter_warnings.append(
                f"Collection {normalized_collection_key} was not found in the search index",
            )
    if normalized_item_type:
        known_item_types = {
            str(parent.get("itemType", "")).lower()
            for parent in state.parents.values()
            if str(parent.get("itemType", "")).strip()
        }
        if normalized_item_type not in known_item_types:
            filter_warnings.append(
                f"Item type {item_type!r} was not found in the search index",
            )
    if filter_warnings:
        return _search_response(
            query,
            [],
            requested_limit=requested_limit,
            applied_limit=applied_limit,
            warning=" ".join(filter_warnings),
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
            warning=_EMPTY_QUERY_WARNING,
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
            if normalized_collection_key and normalized_collection_key not in parent["collections"]:
                continue
            if normalized_item_type and parent["itemType"].lower() != normalized_item_type:
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

    if include_attachments:
        attachments_by_parent = _get_item_attachments_by_parent(list(best_by_parent))
        attachment_counts = {
            parent_key: len(attachments_by_parent.get(parent_key, []))
            for parent_key in best_by_parent
        }
    else:
        attachment_counts = _get_item_attachment_counts(list(best_by_parent))
        attachments_by_parent = {}
    results_payload = [
        _result_from_parent(
            state.parents[parent_key],
            score=score,
            best_doc=doc,
            query_terms=query_terms,
            attachment_count=attachment_counts.get(parent_key, 0),
            attachments=attachments_by_parent.get(parent_key) if include_attachments else None,
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
    item_keys: list[str] | None = None,
) -> str:
    """BM25 ranked passage search within one or more parent items."""
    limit = max(1, limit)
    requested_keys = _normalize_item_keys(item_key=item_key, item_keys=item_keys)
    unique_requested_keys: list[str] = []
    for key in requested_keys:
        if key not in unique_requested_keys:
            unique_requested_keys.append(key)

    if not unique_requested_keys:
        return _search_within_item_response(
            key="",
            query=query,
            matches=[],
            error="Provide item_key or item_keys",
        )

    multi_item = len(unique_requested_keys) > 1
    normalized_item_key = unique_requested_keys[0]

    with _index_lock:
        state = _search_state

    if state is None:
        if multi_item:
            return _search_within_item_response(
                query=query,
                matches=[],
                item_keys=unique_requested_keys,
                items=[],
                error="Index is still building, please retry in a moment",
            )
        return _search_within_item_response(
            key=normalized_item_key,
            query=query,
            matches=[],
            error="Index is still building, please retry in a moment",
        )

    found_item_keys = [key for key in unique_requested_keys if key in state.parents]
    missing_item_keys = [key for key in unique_requested_keys if key not in state.parents]

    if not found_item_keys:
        if multi_item:
            return _search_within_item_response(
                query=query,
                matches=[],
                item_keys=unique_requested_keys,
                items=[],
                missing_item_keys=missing_item_keys,
                error="None of the requested item keys were found in the search index",
            )
        return _search_within_item_response(
            key=normalized_item_key,
            query=query,
            matches=[],
            error=f"Item {normalized_item_key} was not found in the search index",
        )

    if multi_item:
        items_summary = [_item_summary_from_parent(state.parents[key]) for key in found_item_keys]
    else:
        item_summary = _item_summary_from_parent(state.parents[normalized_item_key])

    if state.retriever is None or not state.corpus_docs:
        if multi_item:
            warning = None
            if missing_item_keys:
                warning = (
                    "Some requested item keys were not found in the search index: "
                    + ", ".join(missing_item_keys)
                )
            return _search_within_item_response(
                query=query,
                matches=[],
                item_keys=found_item_keys,
                items=items_summary,
                missing_item_keys=missing_item_keys,
                warning=warning,
            )
        return _search_within_item_response(
            key=normalized_item_key,
            query=query,
            matches=[],
            item=item_summary,
        )

    query_tokens = bm25s.tokenize(
        [query],
        stopwords="en",
        show_progress=False,
        return_ids=False,
    )
    query_terms = _extract_query_terms(query)
    if not query_terms or not query_tokens or not query_tokens[0]:
        warnings = [_EMPTY_QUERY_WARNING]
        if missing_item_keys:
            warnings.append(
                "Some requested item keys were not found in the search index: "
                + ", ".join(missing_item_keys),
            )
        if multi_item:
            return _search_within_item_response(
                query=query,
                matches=[],
                item_keys=found_item_keys,
                items=items_summary,
                missing_item_keys=missing_item_keys,
                warning=" ".join(warnings),
            )
        return _search_within_item_response(
            key=normalized_item_key,
            query=query,
            matches=[],
            item=item_summary,
            warning=_EMPTY_QUERY_WARNING,
        )

    attachments_by_parent = _get_item_attachments_by_parent(found_item_keys)
    attachments_lookup_by_parent = {
        parent_key: {attachment["key"]: attachment for attachment in attachments}
        for parent_key, attachments in attachments_by_parent.items()
    }

    max_docs = len(state.corpus_docs)
    batch_size = min(max(limit * 20, 200), max_docs)
    matches: list[dict[str, Any]] = []
    seen_doc_ids: set[str] = set()
    found_item_key_set = set(found_item_keys)

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
            if parent_key not in found_item_key_set:
                continue
            if doc["doc_id"] in seen_doc_ids:
                continue

            seen_doc_ids.add(doc["doc_id"])
            matches.append(_result_from_doc(
                state.parents[parent_key],
                score=score,
                doc=doc,
                query_terms=query_terms,
                attachments_by_key=attachments_lookup_by_parent.get(parent_key, {}),
                include_parent_key=multi_item,
            ))

            if len(matches) >= limit:
                found_enough = True
                break

        if found_enough or batch_size >= max_docs:
            break

        batch_size = min(max_docs, batch_size * 2)

    if multi_item:
        warning = None
        if missing_item_keys:
            warning = (
                "Some requested item keys were not found in the search index: "
                + ", ".join(missing_item_keys)
            )
        return _search_within_item_response(
            query=query,
            matches=matches[:limit],
            item_keys=found_item_keys,
            items=items_summary,
            missing_item_keys=missing_item_keys,
            warning=warning,
        )

    return _search_within_item_response(
        key=normalized_item_key,
        query=query,
        matches=matches[:limit],
        item=item_summary,
    )


def list_collections() -> str:
    """Return all collections with keys, names, and item counts."""
    try:
        zot = _get_zot()
        collections = zot.collections()
    except Exception as exc:
        return json.dumps({"collections": [], "total": 0, "error": f"Failed to fetch collections: {exc}"})

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
    requested_limit, applied_limit = _apply_limit_cap(limit, _LIST_RESULT_LIMIT_CAP)
    normalized_collection_key = collection_key.strip().upper()
    if not normalized_collection_key:
        return json.dumps({
            "collection_key": "",
            "collection_found": False,
            "items": [],
            "total": 0,
            **_limit_response_metadata(requested_limit, applied_limit, _LIST_RESULT_LIMIT_CAP),
            "error": "Provide collection_key",
        })

    try:
        zot = _get_zot()
        collections = zot.collections()
        collection_found = any(
            collection.get("data", {}).get("key", "").upper() == normalized_collection_key
            for collection in collections
        )
        if not collection_found:
            return json.dumps({
                "collection_key": normalized_collection_key,
                "collection_found": False,
                "items": [],
                "total": 0,
                **_limit_response_metadata(requested_limit, applied_limit, _LIST_RESULT_LIMIT_CAP),
                "error": f"Collection {normalized_collection_key} was not found",
            })
        items = zot.collection_items(normalized_collection_key, limit=applied_limit)
    except Exception as exc:
        return json.dumps({
            "collection_key": normalized_collection_key,
            "collection_found": False,
            "items": [],
            "total": 0,
            **_limit_response_metadata(requested_limit, applied_limit, _LIST_RESULT_LIMIT_CAP),
            "error": f"Failed to fetch collection items: {exc}",
        })

    result = []
    for item in items:
        data = item.get("data", {})
        if data.get("itemType") in _SKIP_TYPES:
            continue
        item_collections = {
            key.upper()
            for key in data.get("collections", [])
            if isinstance(key, str)
        }
        if normalized_collection_key not in item_collections:
            continue
        result.append(
            _item_to_dict(
                item,
                truncate_abstract=500,
                include_attachment_count=True,
                max_creators=_LIST_VIEW_MAX_CREATORS,
            )
        )

    return json.dumps({
        "collection_key": normalized_collection_key,
        "collection_found": True,
        "items": result,
        "total": len(result),
        **_limit_response_metadata(requested_limit, applied_limit, _LIST_RESULT_LIMIT_CAP),
    })


def get_item(item_key: str = "", item_keys: list[str] | None = None) -> str:
    """Full metadata for one item or a batch of items."""
    requested_keys = _normalize_item_keys(item_key=item_key, item_keys=item_keys)
    if not requested_keys:
        if item_keys is not None:
            return json.dumps({
                "error": "Provide item_key or item_keys",
                "items": [],
                "total": 0,
            })

        return json.dumps(_error_payload("Provide item_key"))

    attachments_by_parent = _get_item_attachments_by_parent(requested_keys)

    if len(requested_keys) == 1:
        normalized_item_key = requested_keys[0]
        try:
            item = _fetch_item_detail(normalized_item_key)
        except Exception as exc:
            return json.dumps(
                _error_payload(
                    _item_fetch_error_message(normalized_item_key, exc),
                    key=normalized_item_key,
                )
            )

        return json.dumps(
            _item_to_dict(
                item,
                truncate_abstract=0,
                include_attachments=True,
                max_creators=_DETAIL_VIEW_MAX_CREATORS,
                attachments=attachments_by_parent.get(normalized_item_key, []),
            )
        )

    items: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    max_workers = min(_ITEM_DETAIL_MAX_WORKERS, len(requested_keys))

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(_fetch_item_detail, key) for key in requested_keys]
        for key, future in zip(requested_keys, futures):
            try:
                item = future.result()
            except Exception as exc:
                errors.append({
                    "key": key,
                    "error": _item_fetch_error_message(key, exc),
                })
                continue

            items.append(
                _item_to_dict(
                    item,
                    truncate_abstract=0,
                    include_attachments=True,
                    max_creators=_DETAIL_VIEW_MAX_CREATORS,
                    attachments=attachments_by_parent.get(key, []),
                )
            )

    payload: dict[str, Any] = {
        "item_keys": requested_keys,
        "items": items,
        "requested": len(requested_keys),
        "total": len(items),
    }
    if errors:
        payload["errors"] = errors

    return json.dumps(payload)


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

    def _append_success(key: str, exports: dict[str, str]) -> None:
        results.append({
            "key": key,
            "citation": _xhtml_to_text(exports["citation"]),
            "bibliography": _xhtml_to_text(exports["bibliography"]),
            "bibtex": _compact_bibtex_export(exports["bibtex"]),
        })

    if len(requested_keys) == 1:
        key = requested_keys[0]
        try:
            exports = _fetch_item_exports(key, style=style, locale=locale)
            _append_success(key, exports)
        except Exception as exc:
            errors.append({
                "key": key,
                "error": _citation_fetch_error_message(key, style, exc),
            })
    else:
        max_workers = min(_CITATION_EXPORT_MAX_WORKERS, len(requested_keys))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(_fetch_item_exports, key, style=style, locale=locale)
                for key in requested_keys
            ]
            for key, future in zip(requested_keys, futures):
                try:
                    exports = future.result()
                    _append_success(key, exports)
                except Exception as exc:
                    errors.append({
                        "key": key,
                        "error": _citation_fetch_error_message(key, style, exc),
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
            payload["error"] = errors[0]["error"] if len(errors) == 1 else "Failed to fetch citation entries"

    return json.dumps(payload)


def get_recent_items(limit: int = 10) -> str:
    """Recently added items, sorted by dateAdded descending."""
    requested_limit, applied_limit = _apply_limit_cap(limit, _LIST_RESULT_LIMIT_CAP)
    try:
        zot = _get_zot()
        items = zot.items(
            limit=applied_limit * 3,
            sort="dateAdded",
            direction="desc",
        )
        items = [item for item in items if item.get("data", {}).get("itemType") not in _SKIP_TYPES][:applied_limit]
    except Exception as exc:
        return json.dumps({
            "items": [],
            "total": 0,
            **_limit_response_metadata(requested_limit, applied_limit, _LIST_RESULT_LIMIT_CAP),
            "error": f"Failed to fetch recent items: {exc}",
        })

    result = [
        _item_to_dict(
            item,
            truncate_abstract=500,
            include_attachment_count=True,
            max_creators=_LIST_VIEW_MAX_CREATORS,
        )
        for item in items
    ]
    return json.dumps({
        "items": result,
        "total": len(result),
        **_limit_response_metadata(requested_limit, applied_limit, _LIST_RESULT_LIMIT_CAP),
    })
