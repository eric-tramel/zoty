"""Bridge helpers for Zotero full-text extraction control.

These helpers use the existing zoty-bridge /execute endpoint. They never
transfer extracted document text, only attachment/indexing status.
"""

from __future__ import annotations

import json
from typing import Any

from zoty.rdp import BridgeError, execute_js


def _parse_bridge_result(bridge_response: dict[str, Any]) -> dict[str, Any]:
    """Unwrap a bridge response whose JS returned JSON.stringify'd data."""
    value = bridge_response.get("result")

    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, json.JSONDecodeError):
            return {"raw": value}
        if isinstance(parsed, dict):
            return parsed
        return {"result": parsed}

    if isinstance(value, dict):
        return value

    return bridge_response


def ensure_parent_fulltext(parent_keys: list[str], complete: bool = False) -> dict[str, Any]:
    """Best-effort ensure Zotero full-text extraction exists for parent item keys."""
    cleaned_keys = []
    for key in parent_keys:
        key = key.strip().upper()
        if key and key not in cleaned_keys:
            cleaned_keys.append(key)

    payload = json.dumps(cleaned_keys)
    js = f"""const parentKeys = {payload};
const complete = {json.dumps(bool(complete))};
const libraryID = Zotero.Libraries.userLibraryID;
const out = {{
  parents: [],
  totals: {{
    requestedParents: parentKeys.length,
    attachmentsSeen: 0,
    attachmentsIndexed: 0,
    attachmentsSkipped: 0,
    attachmentsFailed: 0
  }}
}};

for (const parentKey of parentKeys) {{
  const parent = Zotero.Items.getByLibraryAndKey(libraryID, parentKey);
  const parentEntry = {{
    parentKey,
    parentID: parent ? parent.id : null,
    attachments: []
  }};

  if (!parent) {{
    parentEntry.error = 'parent item not found';
    out.parents.push(parentEntry);
    continue;
  }}

  const attachmentIDs = parent.getAttachments ? parent.getAttachments() : [];
  const attachments = await Zotero.Items.getAsync(attachmentIDs);
  for (const attachment of attachments) {{
    const canIndex = Zotero.Fulltext.canIndex(attachment);
    const entry = {{
      attachmentKey: attachment.key,
      attachmentID: attachment.id,
      contentType: attachment.attachmentContentType || '',
      linkMode: attachment.attachmentLinkMode,
      canIndex,
      beforeState: null,
      afterState: null,
      indexedPages: null,
      totalPages: null,
      indexedChars: null,
      totalChars: null,
      cachePath: null,
      attachmentPath: null,
      indexed: false
    }};
    out.totals.attachmentsSeen += 1;

    try {{
      entry.attachmentPath = await attachment.getFilePathAsync();
    }} catch (e) {{
      entry.attachmentPath = '';
    }}

    try {{
      entry.cachePath = Zotero.Fulltext.getItemCacheFile(attachment).path;
    }} catch (e) {{
      entry.cachePath = '';
    }}

    if (!canIndex) {{
      out.totals.attachmentsSkipped += 1;
      parentEntry.attachments.push(entry);
      continue;
    }}

    entry.beforeState = await Zotero.Fulltext.getIndexedState(attachment);

    try {{
      if (entry.beforeState !== Zotero.Fulltext.INDEX_STATE_INDEXED || complete) {{
        await Zotero.Fulltext.indexItems([attachment.id], {{
          complete,
          ignoreErrors: false
        }});
        entry.indexed = true;
        out.totals.attachmentsIndexed += 1;
      }} else {{
        out.totals.attachmentsSkipped += 1;
      }}
    }} catch (e) {{
      entry.error = e.toString();
      out.totals.attachmentsFailed += 1;
    }}

    entry.afterState = await Zotero.Fulltext.getIndexedState(attachment);

    const fulltextRow = await Zotero.DB.rowQueryAsync(
      'SELECT indexedPages, totalPages, indexedChars, totalChars FROM fulltextItems WHERE itemID=?',
      attachment.id
    );
    if (fulltextRow) {{
      entry.indexedPages = fulltextRow.indexedPages ?? null;
      entry.totalPages = fulltextRow.totalPages ?? null;
      entry.indexedChars = fulltextRow.indexedChars ?? null;
      entry.totalChars = fulltextRow.totalChars ?? null;
    }}

    parentEntry.attachments.push(entry);
  }}

  out.parents.push(parentEntry);
}}

return JSON.stringify(out);"""

    return _parse_bridge_result(execute_js(js))


__all__ = ["ensure_parent_fulltext", "BridgeError"]
