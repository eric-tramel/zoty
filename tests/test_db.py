import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import Mock, patch

from zoty import db


class FakeMatrix:
    def __init__(self, rows):
        self._rows = rows
        width = len(rows[0]) if rows else 0
        self.shape = (len(rows), width)

    def __getitem__(self, index):
        row, col = index
        return self._rows[row][col]


class FakeRetriever:
    def retrieve(self, query_tokens, k):
        return FakeMatrix([[0]]), FakeMatrix([[2.5]])


class AttachmentPathsTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_db = db._ZOTERO_DB
        self.original_storage = db._ZOTERO_STORAGE
        db._ZOTERO_DB = Path(self.temp_dir.name) / "zotero.sqlite"
        db._ZOTERO_STORAGE = Path(self.temp_dir.name) / "storage"

        with closing(sqlite3.connect(db._ZOTERO_DB)) as conn:
            conn.executescript(
                """
                CREATE TABLE items (
                    itemID INTEGER PRIMARY KEY,
                    key TEXT NOT NULL,
                    dateAdded TEXT NOT NULL
                );
                CREATE TABLE itemAttachments (
                    itemID INTEGER PRIMARY KEY,
                    parentItemID INT,
                    linkMode INT,
                    contentType TEXT,
                    path TEXT
                );
                CREATE TABLE fields (
                    fieldID INTEGER PRIMARY KEY,
                    fieldName TEXT NOT NULL
                );
                CREATE TABLE itemDataValues (
                    valueID INTEGER PRIMARY KEY,
                    value TEXT UNIQUE
                );
                CREATE TABLE itemData (
                    itemID INTEGER NOT NULL,
                    fieldID INTEGER NOT NULL,
                    valueID INTEGER NOT NULL
                );
                """
            )
            conn.execute(
                "INSERT INTO items(itemID, key, dateAdded) VALUES (?, ?, ?)",
                (1, "PARENT1", "2026-03-10 10:00:00"),
            )
            conn.execute(
                "INSERT INTO items(itemID, key, dateAdded) VALUES (?, ?, ?)",
                (2, "ATTACH1", "2026-03-10 10:01:00"),
            )
            conn.execute(
                "INSERT INTO fields(fieldID, fieldName) VALUES (?, ?)",
                (1, "title"),
            )
            conn.execute(
                "INSERT INTO itemDataValues(valueID, value) VALUES (?, ?)",
                (1, "Attached PDF"),
            )
            conn.execute(
                "INSERT INTO itemData(itemID, fieldID, valueID) VALUES (?, ?, ?)",
                (2, 1, 1),
            )
            conn.execute(
                """INSERT INTO itemAttachments(itemID, parentItemID, linkMode, contentType, path)
                   VALUES (?, ?, ?, ?, ?)""",
                (2, 1, 0, "application/pdf", "storage:paper.pdf"),
            )
            conn.commit()

    def tearDown(self):
        db._ZOTERO_DB = self.original_db
        db._ZOTERO_STORAGE = self.original_storage
        with db._index_lock:
            db._bm25_retriever = None
            db._corpus = []
        self.temp_dir.cleanup()

    def _paper_item(self):
        return {
            "data": {
                "key": "PARENT1",
                "itemType": "preprint",
                "title": "Example Paper",
                "creators": [{"firstName": "Jane", "lastName": "Example"}],
                "date": "2026-03-10",
                "DOI": "10.1000/example",
                "url": "https://example.org/paper",
                "tags": [{"tag": "chemistry"}],
                "collections": ["COLL123"],
                "abstractNote": "Example abstract.",
            }
        }

    def test_get_item_attachments_resolves_storage_filepaths(self):
        attachments = db._get_item_attachments("PARENT1")

        self.assertEqual(
            attachments,
            [
                {
                    "key": "ATTACH1",
                    "title": "Attached PDF",
                    "contentType": "application/pdf",
                    "linkMode": 0,
                    "filepath": str(db._ZOTERO_STORAGE / "ATTACH1" / "paper.pdf"),
                }
            ],
        )

    def test_get_item_includes_attachment_filepaths(self):
        zot = Mock()
        zot.item.return_value = self._paper_item()

        with patch("zoty.db._get_zot", return_value=zot):
            result = json.loads(db.get_item("PARENT1"))

        self.assertEqual(result["key"], "PARENT1")
        self.assertEqual(result["attachments"][0]["filepath"], str(db._ZOTERO_STORAGE / "ATTACH1" / "paper.pdf"))
        self.assertEqual(result["attachments"][0]["contentType"], "application/pdf")

    def test_search_includes_attachment_filepaths(self):
        with db._index_lock:
            db._bm25_retriever = FakeRetriever()
            db._corpus = [self._paper_item()]

        with patch("zoty.db.bm25s.tokenize", return_value=["example"]):
            result = json.loads(db.search("example"))

        self.assertEqual(result["total"], 1)
        self.assertEqual(result["results"][0]["key"], "PARENT1")
        self.assertEqual(
            result["results"][0]["attachments"][0]["filepath"],
            str(db._ZOTERO_STORAGE / "ATTACH1" / "paper.pdf"),
        )


class CitationEntryTests(unittest.TestCase):
    def test_normalize_item_keys_accepts_single_and_list_inputs(self):
        result = db._normalize_item_keys(
            item_key=" item123 ",
            item_keys=[" item456 ", "", "Item789"],
        )

        self.assertEqual(result, ["ITEM123", "ITEM456", "ITEM789"])

    def test_get_citation_entries_returns_single_item_exports(self):
        zot = Mock()

        def item_side_effect(item_key, **kwargs):
            self.assertEqual(kwargs["format"], "atom")
            self.assertEqual(kwargs["style"], "apa")
            self.assertEqual(kwargs["locale"], "fr-FR")

            content = kwargs["content"]
            values = {
                "citation": [f"<span>{item_key} &amp; cite</span>"],
                "bib": [f"<div>{item_key} <i>reference</i></div>"],
                "bibtex": [f"@article{{{item_key},\n  title={{Example}}\n}}"],
            }
            return values[content]

        zot.item.side_effect = item_side_effect

        with patch("zoty.db._get_zot", return_value=zot):
            result = json.loads(
                db.get_citation_entries(
                    item_key="item123",
                    style="apa",
                    locale="fr-FR",
                )
            )

        self.assertEqual(result["total"], 1)
        self.assertEqual(result["requested"], 1)
        self.assertEqual(result["style"], "apa")
        self.assertEqual(result["locale"], "fr-FR")
        self.assertEqual(
            result["items"],
            [
                {
                    "key": "ITEM123",
                    "citation": "ITEM123 & cite",
                    "bibliography": "ITEM123 reference",
                    "bibtex": "@article{ITEM123,\n  title={Example}\n}",
                }
            ],
        )

    def test_get_citation_entries_returns_multiple_items_and_partial_errors(self):
        zot = Mock()

        def item_side_effect(item_key, **kwargs):
            if item_key == "BADKEY":
                raise RuntimeError("missing item")

            content = kwargs["content"]
            if content == "citation":
                return [f"<span>{item_key} cite</span>"]
            if content == "bib":
                return [f"<div>{item_key} ref</div>"]
            if content == "bibtex":
                return [f"@article{{{item_key}}}"]
            raise AssertionError(f"Unexpected content: {content}")

        zot.item.side_effect = item_side_effect

        with patch("zoty.db._get_zot", return_value=zot):
            result = json.loads(
                db.get_citation_entries(
                    item_keys=["good1", "badkey", "good2"],
                )
            )

        self.assertEqual(
            result["items"],
            [
                {
                    "key": "GOOD1",
                    "citation": "GOOD1 cite",
                    "bibliography": "GOOD1 ref",
                    "bibtex": "@article{GOOD1}",
                },
                {
                    "key": "GOOD2",
                    "citation": "GOOD2 cite",
                    "bibliography": "GOOD2 ref",
                    "bibtex": "@article{GOOD2}",
                },
            ],
        )
        self.assertEqual(
            result["errors"],
            [
                {
                    "key": "BADKEY",
                    "error": "Failed to fetch citation entry: missing item",
                }
            ],
        )
        self.assertEqual(result["requested"], 3)
        self.assertEqual(result["total"], 2)

    def test_get_citation_entries_requires_at_least_one_key(self):
        result = json.loads(db.get_citation_entries())

        self.assertEqual(
            result,
            {
                "error": "Provide item_key or item_keys",
                "items": [],
                "total": 0,
            },
        )


if __name__ == "__main__":
    unittest.main()
