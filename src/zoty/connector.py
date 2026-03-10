"""Write path: add papers to Zotero via the connector endpoint and arXiv/CrossRef metadata.

PDF attachment and collection assignment happen via the zoty-bridge plugin's
HTTP endpoint, which executes JavaScript inside Zotero's privileged context.
"""

from __future__ import annotations

import json
import os
import random
import sqlite3
import string
import sys
import tempfile
import time
import urllib.request
import urllib.error
from pathlib import Path
from xml.etree import ElementTree

from zoty.rdp import execute_js, BridgeError

CONNECTOR_URL = "http://localhost:23119/connector/saveItems"
ARXIV_API_URL = "http://export.arxiv.org/api/query"
CROSSREF_API_URL = "https://api.crossref.org/works"

# Atom namespace for arXiv API
_ATOM = "{http://www.w3.org/2005/Atom}"

# Zotero storage
_ZOTERO_DIR = Path.home() / "Zotero"
_ZOTERO_STORAGE = _ZOTERO_DIR / "storage"
_ZOTERO_DB = _ZOTERO_DIR / "zotero.sqlite"
_COLLECTION_ASSIGN_RETRIES = 3
_COLLECTION_ASSIGN_RETRY_DELAY = 0.25


# ---------------------------------------------------------------------------
# Bridge-based Zotero operations (via zoty-bridge plugin)
# ---------------------------------------------------------------------------

def _attach_pdf_via_rdp(parent_key: str, pdf_path: str) -> dict:
    """Register a downloaded PDF with Zotero via the zoty-bridge plugin."""
    # Escape backslashes and quotes for JS string literal
    escaped_path = pdf_path.replace("\\", "\\\\").replace("'", "\\'")

    js = f"""(async () => {{
    let item = await Zotero.Items.getByLibraryAndKey(1, '{parent_key}');
    if (!item) return JSON.stringify({{error: 'parent item not found', key: '{parent_key}'}});
    let attachment = await Zotero.Attachments.importFromFile({{
        file: '{escaped_path}',
        parentItemID: item.id
    }});
    return JSON.stringify({{status: 'attached', attachmentID: attachment.id, key: attachment.key}});
}})()"""

    result = execute_js(js)
    # Parse the stringified JSON from the evaluation result
    return _parse_rdp_result(result)


def _add_to_collection_via_rdp(item_key: str, collection_key: str) -> dict:
    """Add an item to a collection via the zoty-bridge plugin."""
    js = f"""(async () => {{
    let item = await Zotero.Items.getByLibraryAndKey(1, '{item_key}');
    let collection = await Zotero.Collections.getByLibraryAndKey(1, '{collection_key}');
    if (!item || !collection) return JSON.stringify({{error: 'not found', itemKey: '{item_key}', collectionKey: '{collection_key}'}});
    await collection.addItem(item.id);
    return JSON.stringify({{status: 'added'}});
}})()"""

    result = execute_js(js)
    return _parse_rdp_result(result)


def _should_retry_collection_assignment(
    *,
    result: dict | None = None,
    error: BridgeError | None = None,
) -> bool:
    """Retry only the transient bridge failures seen after attachment."""
    if error is not None:
        message = str(error)
        return "HTTP Error 400" in message or "Bad Request" in message

    return bool(result) and result.get("error") == "not found"


def _add_to_collection_with_retry(
    item_key: str,
    collection_key: str,
    *,
    attempts: int = _COLLECTION_ASSIGN_RETRIES,
    delay: float = _COLLECTION_ASSIGN_RETRY_DELAY,
) -> dict:
    """Retry brief collection-assignment races while Zotero state settles."""
    last_result: dict | None = None

    for attempt in range(1, attempts + 1):
        try:
            result = _add_to_collection_via_rdp(item_key, collection_key)
        except BridgeError as e:
            if attempt < attempts and _should_retry_collection_assignment(error=e):
                print(
                    f"zoty: retrying collection assignment for {item_key} after bridge error: {e}",
                    file=sys.stderr,
                )
                time.sleep(delay * attempt)
                continue
            raise

        last_result = result
        if attempt < attempts and _should_retry_collection_assignment(result=result):
            print(
                f"zoty: retrying collection assignment for {item_key} after bridge response: {result}",
                file=sys.stderr,
            )
            time.sleep(delay * attempt)
            continue

        return result

    return last_result or {"error": "collection assignment failed"}


def _parse_rdp_result(bridge_response: dict) -> dict:
    """Extract the JS return value from a bridge response.

    The bridge returns {"ok": true, "result": <value>}. The JS code
    returns JSON.stringify'd objects, so we parse them back.
    """
    value = bridge_response.get("result")

    if isinstance(value, str):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return {"raw": value}

    if isinstance(value, dict):
        return value

    return bridge_response


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _zotero_key(length: int = 8) -> str:
    chars = string.ascii_uppercase + string.digits
    return "".join(random.choices(chars, k=length))


def _find_parent_key_by_title(title: str) -> str:
    """Find the most recently added item matching a title via read-only DB."""
    try:
        db = sqlite3.connect(f"file:{_ZOTERO_DB}?immutable=1", uri=True)
        cur = db.cursor()
        cur.execute(
            """SELECT i.key FROM items i
               JOIN itemData id ON i.itemID = id.itemID
               JOIN itemDataValues idv ON id.valueID = idv.valueID
               JOIN fields f ON id.fieldID = f.fieldID
               WHERE f.fieldName = 'title' AND idv.value = ?
               ORDER BY i.dateAdded DESC LIMIT 1""",
            (title,),
        )
        row = cur.fetchone()
        db.close()
        return row[0] if row else ""
    except Exception:
        return ""


def _make_pdf_filename(creators: list[dict], date: str, title: str) -> str:
    if creators:
        first = creators[0]
        author = first.get("lastName", "") or first.get("name", "Unknown")
        if len(creators) > 1:
            author += " et al."
    else:
        author = "Unknown"

    year = date[:4] if date else "Unknown"
    short_title = title[:80].rstrip()
    if len(title) > 80:
        short_title += "..."

    for ch in '/\\:*?"<>|':
        short_title = short_title.replace(ch, "")
        author = author.replace(ch, "")

    return f"{author} - {year} - {short_title}.pdf"


def _download_pdf(pdf_url: str, filename: str) -> tuple[str, Path, int] | None:
    """Download PDF to Zotero storage. Returns (att_key, dest_path, size) or None."""
    tmp = ""
    try:
        tmp = tempfile.mktemp(suffix=".pdf")
        urllib.request.urlretrieve(pdf_url, tmp)
        file_size = os.path.getsize(tmp)
        if file_size < 1000:
            os.unlink(tmp)
            return None

        att_key = _zotero_key()
        storage_dir = _ZOTERO_STORAGE / att_key
        storage_dir.mkdir(parents=True, exist_ok=True)
        dest = storage_dir / filename
        os.rename(tmp, str(dest))
        return att_key, dest, file_size
    except Exception as e:
        print(f"zoty: PDF download failed: {e}", file=sys.stderr)
        if tmp and os.path.exists(tmp):
            os.unlink(tmp)
        return None


# ---------------------------------------------------------------------------
# Metadata fetching
# ---------------------------------------------------------------------------

def _fetch_arxiv_metadata(arxiv_id: str) -> dict:
    arxiv_id = arxiv_id.strip()
    for prefix in ("arxiv:", "arXiv:", "https://arxiv.org/abs/", "http://arxiv.org/abs/"):
        if arxiv_id.startswith(prefix):
            arxiv_id = arxiv_id[len(prefix):]
    base_id = arxiv_id.split("v")[0] if "v" in arxiv_id else arxiv_id

    url = f"{ARXIV_API_URL}?id_list={base_id}"
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=15) as resp:
        body = resp.read().decode("utf-8")

    root = ElementTree.fromstring(body)
    entry = root.find(f"{_ATOM}entry")
    if entry is None:
        raise ValueError(f"No arXiv entry found for ID: {arxiv_id}")

    title = (entry.findtext(f"{_ATOM}title") or "").strip().replace("\n", " ")
    abstract = (entry.findtext(f"{_ATOM}summary") or "").strip().replace("\n", " ")
    published = (entry.findtext(f"{_ATOM}published") or "")[:10]

    authors = []
    for author_el in entry.findall(f"{_ATOM}author"):
        name = (author_el.findtext(f"{_ATOM}name") or "").strip()
        if name:
            parts = name.rsplit(" ", 1)
            if len(parts) == 2:
                authors.append({"firstName": parts[0], "lastName": parts[1], "creatorType": "author"})
            else:
                authors.append({"name": name, "creatorType": "author"})

    categories = []
    for cat_el in entry.findall(f"{_ATOM}category"):
        term = cat_el.get("term", "")
        if term:
            categories.append(term)

    return {
        "itemType": "preprint",
        "title": title,
        "creators": authors,
        "abstractNote": abstract,
        "date": published,
        "url": f"https://arxiv.org/abs/{base_id}",
        "archive": "arXiv",
        "archiveID": f"arXiv:{base_id}",
        "tags": [{"tag": c} for c in categories[:10]],
        "_pdf_url": f"https://arxiv.org/pdf/{base_id}",
    }


def _find_pdf_for_doi(doi: str) -> str:
    if "arxiv" in doi.lower():
        parts = doi.split("arXiv.", 1) if "arXiv." in doi else doi.split("arxiv.", 1)
        if len(parts) == 2:
            return f"https://arxiv.org/pdf/{parts[1]}"

    try:
        req = urllib.request.Request(
            f"https://doi.org/{doi}",
            method="HEAD",
            headers={"User-Agent": "zoty/0.1"},
        )
        req.add_header("Accept", "application/pdf")
        with urllib.request.urlopen(req, timeout=10) as resp:
            final_url = resp.url
            if "arxiv.org" in final_url:
                for seg in ("abs/", "pdf/"):
                    if seg in final_url:
                        arxiv_id = final_url.split(seg)[-1].rstrip("/")
                        return f"https://arxiv.org/pdf/{arxiv_id}"
    except Exception:
        pass

    return ""


def _fetch_crossref_metadata(doi: str) -> dict:
    doi = doi.strip()
    for prefix in ("doi:", "https://doi.org/", "http://doi.org/"):
        if doi.lower().startswith(prefix):
            doi = doi[len(prefix):]

    url = f"{CROSSREF_API_URL}/{doi}"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = json.loads(resp.read().decode("utf-8"))

    work = data.get("message", {})

    title_parts = work.get("title", [])
    title = title_parts[0] if title_parts else ""

    abstract = work.get("abstract", "")
    if "<jats:" in abstract:
        import re
        abstract = re.sub(r"<[^>]+>", "", abstract).strip()

    date_parts = work.get("published-print", work.get("published-online", work.get("created", {})))
    date_list = date_parts.get("date-parts", [[]])[0] if date_parts else []
    if len(date_list) >= 3:
        date = f"{date_list[0]}-{date_list[1]:02d}-{date_list[2]:02d}"
    elif len(date_list) >= 1:
        date = str(date_list[0])
    else:
        date = ""

    authors = []
    for a in work.get("author", []):
        given = a.get("given", "")
        family = a.get("family", "")
        if given or family:
            authors.append({"firstName": given, "lastName": family, "creatorType": "author"})

    journal = ""
    container = work.get("container-title", [])
    if container:
        journal = container[0]

    cr_type = work.get("type", "")
    type_map = {
        "journal-article": "journalArticle",
        "proceedings-article": "conferencePaper",
        "book-chapter": "bookSection",
        "book": "book",
        "posted-content": "preprint",
    }
    item_type = type_map.get(cr_type, "journalArticle")

    result: dict = {
        "itemType": item_type,
        "title": title,
        "creators": authors,
        "abstractNote": abstract,
        "date": date,
        "DOI": doi,
        "url": f"https://doi.org/{doi}",
        "tags": [],
    }
    if journal:
        result["publicationTitle"] = journal

    pdf_url = _find_pdf_for_doi(doi)
    if pdf_url:
        result["_pdf_url"] = pdf_url

    return result


# ---------------------------------------------------------------------------
# Connector push
# ---------------------------------------------------------------------------

def _push_to_connector(item: dict, source_url: str) -> dict:
    clean = {k: v for k, v in item.items() if not k.startswith("_")}
    payload = json.dumps({"items": [clean], "uri": source_url}).encode("utf-8")
    req = urllib.request.Request(
        CONNECTOR_URL,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Zotero-Allowed-Request": "true",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        status = resp.status
        body = resp.read().decode("utf-8")

    if status != 201:
        raise RuntimeError(f"Connector returned status {status}: {body}")

    return item


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def add_paper(arxiv_id: str = "", doi: str = "", collection_key: str = "") -> str:
    """Add a paper to Zotero by arXiv ID or DOI.

    Metadata item is created via the Zotero connector. PDF attachment and
    collection assignment use the zoty-bridge plugin to call Zotero's JS API.
    If the bridge plugin is not running, those steps fail gracefully
    (the metadata item and downloaded PDF are still preserved).
    """
    if not arxiv_id and not doi:
        return json.dumps({"error": "Provide at least one of arxiv_id or doi"})

    try:
        if arxiv_id:
            item = _fetch_arxiv_metadata(arxiv_id)
            source_url = item.get("url", "")
        else:
            item = _fetch_crossref_metadata(doi)
            source_url = item.get("url", "")

        # Create the metadata item via connector
        _push_to_connector(item, source_url)

        # Find the parent key for bridge operations (PDF attach + collection assign)
        parent_key = _find_parent_key_by_title(item.get("title", ""))

        # Download PDF and register it with Zotero via bridge
        pdf_url = item.get("_pdf_url", "")
        pdf_attached = False
        rdp_warning = ""
        if pdf_url and parent_key:
            filename = _make_pdf_filename(
                item.get("creators", []),
                item.get("date", ""),
                item.get("title", ""),
            )
            dl = _download_pdf(pdf_url, filename)
            if dl:
                att_key, dest, file_size = dl
                try:
                    rdp_result = _attach_pdf_via_rdp(parent_key, str(dest))
                    if rdp_result.get("error"):
                        print(f"zoty: bridge attach error: {rdp_result}", file=sys.stderr)
                    else:
                        pdf_attached = True
                        print(
                            f"zoty: attached PDF {filename} ({file_size} bytes) via bridge",
                            file=sys.stderr,
                        )
                except BridgeError as e:
                    rdp_warning = str(e)
                    print(
                        f"zoty: bridge unavailable, PDF saved to disk but not registered: {e}",
                        file=sys.stderr,
                    )

        # Collection assignment via bridge
        collection_added = False
        if collection_key and parent_key:
            try:
                coll_result = _add_to_collection_with_retry(parent_key, collection_key)
                if coll_result.get("error"):
                    print(f"zoty: bridge collection error: {coll_result}", file=sys.stderr)
                else:
                    collection_added = True
                    print(
                        f"zoty: added item {parent_key} to collection {collection_key} via bridge",
                        file=sys.stderr,
                    )
            except BridgeError as e:
                if not rdp_warning:
                    rdp_warning = str(e)
                print(
                    f"zoty: bridge unavailable for collection assignment: {e}",
                    file=sys.stderr,
                )

        # Format creators for output
        creators = []
        for c in item.get("creators", []):
            first = c.get("firstName", "")
            last = c.get("lastName", "")
            name = c.get("name", "")
            if first or last:
                creators.append(f"{first} {last}".strip())
            elif name:
                creators.append(name)

        result = {
            "status": "created",
            "title": item.get("title", ""),
            "creators": creators,
            "date": item.get("date", ""),
            "itemType": item.get("itemType", ""),
            "DOI": item.get("DOI", ""),
            "url": item.get("url", ""),
            "abstract": item.get("abstractNote", "")[:500],
            "pdf_attached": pdf_attached,
            "collection_added": collection_added,
        }
        if rdp_warning:
            result["rdp_warning"] = rdp_warning

        return json.dumps(result)

    except urllib.error.URLError as e:
        source = "arXiv" if arxiv_id else "CrossRef"
        if "Connection refused" in str(e) or "localhost" in str(e):
            return json.dumps({"error": "Cannot reach Zotero connector at localhost:23119. Is Zotero running?"})
        return json.dumps({"error": f"Failed to fetch metadata from {source}: {e}"})
    except Exception as e:
        return json.dumps({"error": f"Failed to add paper: {e}"})
