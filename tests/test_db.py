import io
import json
import sqlite3
import tempfile
import threading
import time
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
    def __init__(self, rows):
        self._rows = rows
        self.calls = []

    def retrieve(self, query_tokens, corpus=None, k=10, show_progress=False):
        self.calls.append(
            {
                "query_tokens": query_tokens,
                "corpus": corpus,
                "k": k,
                "show_progress": show_progress,
            }
        )
        pairs = self._rows[:k]
        return (
            FakeMatrix([[doc for doc, _score in pairs]]),
            FakeMatrix([[score for _doc, score in pairs]]),
        )


class DbTestCase(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_db = db._ZOTERO_DB
        self.original_storage = db._ZOTERO_STORAGE
        self.original_sidecar_root = db._SIDECAR_ROOT
        self.original_zot = db._zot

        db._ZOTERO_DB = Path(self.temp_dir.name) / "zotero.sqlite"
        db._ZOTERO_STORAGE = Path(self.temp_dir.name) / "storage"
        db._SIDECAR_ROOT = Path(self.temp_dir.name) / "sidecar"
        db._ZOTERO_STORAGE.mkdir(parents=True, exist_ok=True)
        db._zot = None

    def tearDown(self):
        db._ZOTERO_DB = self.original_db
        db._ZOTERO_STORAGE = self.original_storage
        db._SIDECAR_ROOT = self.original_sidecar_root
        db._zot = self.original_zot
        with db._index_lock:
            db._search_state = None
            db._refresh_in_progress = False
            db._refresh_requested = False
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

    def _creator_dicts(self, count: int) -> list[dict[str, str]]:
        return [
            {"firstName": f"Author{index + 1}", "lastName": "Example"}
            for index in range(count)
        ]

    def _install_search_state(self, docs, *, parents=None):
        if parents is None:
            parents = {
                "PARENT1": {
                    "key": "PARENT1",
                    "dateModified": "2026-03-10 10:00:00",
                    "itemType": "preprint",
                    "title": "Example Paper",
                    "abstract": "Example abstract.",
                    "creators": ["Jane Example"],
                    "collections": ["COLL123"],
                    "tags": ["chemistry"],
                    "date": "2026-03-10",
                    "DOI": "10.1000/example",
                    "url": "https://example.org/paper",
                }
            }

        db._search_state = db._SearchState(
            snapshot_id="snapshot-1",
            source_fingerprint="fingerprint-1",
            retriever=FakeRetriever([(doc, score) for doc, score in docs]),
            corpus_docs=[doc for doc, _score in docs],
            parents=parents,
        )


class AttachmentPathsTests(DbTestCase):
    def setUp(self):
        super().setUp()

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

    def test_get_item_attachments_resolves_storage_filepaths(self):
        attachments = db._get_item_attachments("PARENT1")

        self.assertEqual(
            attachments,
            [
                {
                    "key": "ATTACH1",
                    "title": "Attached PDF",
                    "contentType": "application/pdf",
                    "linkMode": "imported_file",
                    "filepath": str(db._ZOTERO_STORAGE / "ATTACH1" / "paper.pdf"),
                }
            ],
        )

    def test_get_item_attachments_by_parent_logs_and_falls_back_on_failure(self):
        stderr = io.StringIO()

        with (
            patch("zoty.db._open_zotero_db", side_effect=RuntimeError("boom")),
            patch("sys.stderr", new=stderr),
        ):
            attachments_by_parent = db._get_item_attachments_by_parent(["PARENT1"])

        self.assertEqual(attachments_by_parent, {"PARENT1": []})
        self.assertIn("zoty: failed to load attachment metadata for PARENT1: boom", stderr.getvalue())

    def test_get_item_attachment_count_logs_and_falls_back_on_failure(self):
        stderr = io.StringIO()

        with (
            patch("zoty.db._open_zotero_db", side_effect=RuntimeError("boom")),
            patch("sys.stderr", new=stderr),
        ):
            count = db._get_item_attachment_count("PARENT1")

        self.assertEqual(count, 0)
        self.assertIn("zoty: failed to count attachments for PARENT1: boom", stderr.getvalue())

    def test_get_item_attachment_counts_logs_and_falls_back_on_failure(self):
        stderr = io.StringIO()

        with (
            patch("zoty.db._open_zotero_db", side_effect=RuntimeError("boom")),
            patch("sys.stderr", new=stderr),
        ):
            counts = db._get_item_attachment_counts(["PARENT1", " PARENT2 ", "PARENT1"])

        self.assertEqual(counts, {"PARENT1": 0, "PARENT2": 0})
        self.assertIn("zoty: failed to count attachments for PARENT1, PARENT2: boom", stderr.getvalue())

    def test_format_link_mode_maps_known_and_unknown_values(self):
        self.assertEqual(db._format_link_mode(0), "imported_file")
        self.assertEqual(db._format_link_mode(1), "imported_url")
        self.assertEqual(db._format_link_mode(2), "linked_file")
        self.assertEqual(db._format_link_mode(3), "linked_url")
        self.assertEqual(db._format_link_mode(99), "unknown(99)")
        self.assertEqual(db._format_link_mode(None), "unknown")

    def test_get_item_includes_attachment_filepaths(self):
        zot = Mock()
        zot.item.return_value = self._paper_item()

        with patch("zoty.db._get_zot", return_value=zot):
            result = json.loads(db.get_item("PARENT1"))

        self.assertEqual(result["key"], "PARENT1")
        self.assertEqual(result["attachment_count"], 1)
        self.assertEqual(result["attachments"][0]["filepath"], str(db._ZOTERO_STORAGE / "ATTACH1" / "paper.pdf"))
        self.assertEqual(result["attachments"][0]["contentType"], "application/pdf")

    def test_get_item_normalizes_item_key_to_uppercase(self):
        zot = Mock()
        zot.item.return_value = self._paper_item()

        with patch("zoty.db._get_zot", return_value=zot):
            json.loads(db.get_item(" parent1 "))

        zot.item.assert_called_once_with("PARENT1")

    def test_get_item_truncates_very_long_creator_lists(self):
        zot = Mock()
        item = self._paper_item()
        item["data"]["creators"] = self._creator_dicts(18)
        zot.item.return_value = item

        with patch("zoty.db._get_zot", return_value=zot):
            result = json.loads(db.get_item("PARENT1"))

        self.assertEqual(len(result["creators"]), db._DETAIL_VIEW_MAX_CREATORS + 1)
        self.assertEqual(result["creators"][0], "Author1 Example")
        self.assertEqual(result["creators"][-1], "... and 3 more")

    def test_get_item_rejects_empty_item_key(self):
        with patch("zoty.db._get_zot") as get_zot_mock:
            result = json.loads(db.get_item(""))

        self.assertEqual(result["error"], "Provide item_key")
        self.assertEqual(result["key"], "")
        self.assertEqual(result["itemType"], "")
        self.assertEqual(result["creators"], [])
        self.assertEqual(result["tags"], [])
        self.assertEqual(result["collections"], [])
        self.assertEqual(result["attachment_count"], 0)
        self.assertEqual(result["attachments"], [])
        get_zot_mock.assert_not_called()

    def test_get_item_rejects_whitespace_only_item_key(self):
        with patch("zoty.db._get_zot") as get_zot_mock:
            result = json.loads(db.get_item("   "))

        self.assertEqual(result["error"], "Provide item_key")
        self.assertEqual(result["key"], "")
        self.assertEqual(result["itemType"], "")
        self.assertEqual(result["creators"], [])
        self.assertEqual(result["tags"], [])
        self.assertEqual(result["collections"], [])
        self.assertEqual(result["attachment_count"], 0)
        self.assertEqual(result["attachments"], [])
        get_zot_mock.assert_not_called()

    def test_get_item_supports_single_key_via_item_keys_without_changing_shape(self):
        zot = Mock()
        zot.item.return_value = self._paper_item()

        with patch("zoty.db._get_zot", return_value=zot):
            result = json.loads(db.get_item(item_keys=[" parent1 "]))

        self.assertEqual(result["key"], "PARENT1")
        self.assertNotIn("items", result)
        zot.item.assert_called_once_with("PARENT1")

    def test_get_item_requires_at_least_one_key_for_batch_mode(self):
        with patch("zoty.db._get_zot") as get_zot_mock:
            result = json.loads(db.get_item(item_keys=[]))

        self.assertEqual(
            result,
            {
                "error": "Provide item_key or item_keys",
                "items": [],
                "total": 0,
            },
        )
        get_zot_mock.assert_not_called()

    def test_get_item_returns_structured_error_skeleton_for_fetch_failure(self):
        zot = Mock()
        zot.item.side_effect = RuntimeError("boom")

        with patch("zoty.db._get_zot", return_value=zot):
            result = json.loads(db.get_item("PARENT1"))

        self.assertEqual(result["key"], "PARENT1")
        self.assertEqual(result["itemType"], "")
        self.assertEqual(result["creators"], [])
        self.assertEqual(result["tags"], [])
        self.assertEqual(result["collections"], [])
        self.assertEqual(result["attachment_count"], 0)
        self.assertEqual(result["attachments"], [])
        self.assertIn("Failed to fetch item PARENT1: boom", result["error"])

    def test_get_zot_creates_only_one_client_under_concurrent_first_access(self):
        barrier = threading.Barrier(8)
        created_clients = []
        results = [None] * 8
        errors = []

        def construct(*_args, **_kwargs):
            client = object()
            created_clients.append(client)
            time.sleep(0.05)
            return client

        def worker(index):
            try:
                barrier.wait(timeout=1)
                results[index] = db._get_zot()
            except BaseException as exc:  # pragma: no cover - surfaced by assertions below
                errors.append(exc)

        with patch("zoty.db.zotero.Zotero", side_effect=construct):
            threads = [threading.Thread(target=worker, args=(index,)) for index in range(8)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=2)

        self.assertEqual(errors, [])
        self.assertEqual(len(created_clients), 1)
        self.assertTrue(all(result is not None for result in results))
        self.assertEqual({id(result) for result in results}, {id(created_clients[0])})

    def test_get_item_returns_multiple_items_and_partial_errors(self):
        zot = Mock()

        def item_side_effect(item_key):
            if item_key == "BADKEY":
                raise RuntimeError("missing item")

            item = self._paper_item()
            item["data"]["key"] = item_key
            item["data"]["title"] = f"{item_key} title"
            item["data"]["abstractNote"] = f"{item_key} abstract"
            return item

        zot.item.side_effect = item_side_effect

        with patch("zoty.db._get_zot", return_value=zot):
            result = json.loads(db.get_item(item_keys=["parent1", "badkey", "parent2"]))

        self.assertEqual(result["item_keys"], ["PARENT1", "BADKEY", "PARENT2"])
        self.assertEqual(result["requested"], 3)
        self.assertEqual(result["total"], 2)
        self.assertEqual([item["key"] for item in result["items"]], ["PARENT1", "PARENT2"])
        self.assertEqual(
            result["errors"],
            [
                {
                    "key": "BADKEY",
                    "error": "Failed to fetch item BADKEY: missing item",
                }
            ],
        )

    def test_get_item_fetches_multiple_items_concurrently_and_batches_attachments(self):
        barrier = threading.Barrier(2)

        def fetch_side_effect(item_key):
            barrier.wait(timeout=1)
            item = self._paper_item()
            item["data"]["key"] = item_key
            return item

        with (
            patch("zoty.db._fetch_item_detail", side_effect=fetch_side_effect) as fetch_mock,
            patch(
                "zoty.db._get_item_attachments_by_parent",
                return_value={"GOOD1": [], "GOOD2": []},
            ) as attachments_mock,
        ):
            result = json.loads(db.get_item(item_keys=["good1", "good2"]))

        self.assertEqual(result["total"], 2)
        self.assertNotIn("errors", result)
        self.assertEqual(fetch_mock.call_count, 2)
        self.assertEqual(attachments_mock.call_count, 1)
        attachments_mock.assert_called_once_with(["GOOD1", "GOOD2"])

    def test_list_collections_returns_structured_error_skeleton_for_fetch_failure(self):
        with patch("zoty.db._get_zot", side_effect=RuntimeError("boom")):
            result = json.loads(db.list_collections())

        self.assertEqual(result["collections"], [])
        self.assertEqual(result["total"], 0)
        self.assertIn("Failed to fetch collections: boom", result["error"])

    def test_search_includes_attachment_count(self):
        attachment_doc = {
            "doc_id": "chunk:ATTACH1:0",
            "parent_key": "PARENT1",
            "attachment_key": "ATTACH1",
            "doc_kind": "attachment_chunk",
            "chunk_index": 0,
            "char_start": 0,
            "char_end": 42,
            "token_count": 6,
            "text": "example body text that matches the query",
            "text_hash": "hash-1",
        }
        self._install_search_state([(attachment_doc, 2.5)])

        result = json.loads(db.search("example"))

        self.assertEqual(result["total"], 1)
        self.assertEqual(result["items"][0]["key"], "PARENT1")
        self.assertEqual(result["items"][0]["attachment_count"], 1)
        self.assertNotIn("attachments", result["items"][0])
        self.assertEqual(result["items"][0]["snippet_attachment_key"], "ATTACH1")

    def test_search_batches_attachment_detail_lookups_for_multiple_results(self):
        with closing(sqlite3.connect(db._ZOTERO_DB)) as conn:
            conn.execute(
                "INSERT INTO items(itemID, key, dateAdded) VALUES (?, ?, ?)",
                (3, "PARENT2", "2026-03-10 10:02:00"),
            )
            conn.execute(
                "INSERT INTO items(itemID, key, dateAdded) VALUES (?, ?, ?)",
                (4, "ATTACH2", "2026-03-10 10:03:00"),
            )
            conn.execute(
                "INSERT INTO itemDataValues(valueID, value) VALUES (?, ?)",
                (2, "Attached EPUB"),
            )
            conn.execute(
                "INSERT INTO itemData(itemID, fieldID, valueID) VALUES (?, ?, ?)",
                (4, 1, 2),
            )
            conn.execute(
                """INSERT INTO itemAttachments(itemID, parentItemID, linkMode, contentType, path)
                   VALUES (?, ?, ?, ?, ?)""",
                (4, 3, 1, "application/epub+zip", "storage:book.epub"),
            )
            conn.commit()

        parents = {
            "PARENT1": {
                "key": "PARENT1",
                "dateModified": "2026-03-10 10:00:00",
                "itemType": "preprint",
                "title": "Example Paper",
                "abstract": "Example abstract.",
                "creators": ["Jane Example"],
                "collections": ["COLL123"],
                "tags": ["chemistry"],
                "date": "2026-03-10",
                "DOI": "10.1000/example",
                "url": "https://example.org/paper",
            },
            "PARENT2": {
                "key": "PARENT2",
                "dateModified": "2026-03-11 10:00:00",
                "itemType": "preprint",
                "title": "Second Paper",
                "abstract": "Second abstract.",
                "creators": ["John Example"],
                "collections": ["COLL456"],
                "tags": ["biology"],
                "date": "2026-03-11",
                "DOI": "",
                "url": "",
            },
        }
        docs = [
            ({
                "doc_id": "meta:PARENT1",
                "parent_key": "PARENT1",
                "attachment_key": "",
                "doc_kind": "metadata",
                "chunk_index": 0,
                "char_start": 0,
                "char_end": 20,
                "token_count": 3,
                "text": "example result one",
                "text_hash": "hash-1",
            }, 4.0),
            ({
                "doc_id": "meta:PARENT2",
                "parent_key": "PARENT2",
                "attachment_key": "",
                "doc_kind": "metadata",
                "chunk_index": 0,
                "char_start": 0,
                "char_end": 20,
                "token_count": 3,
                "text": "example result two",
                "text_hash": "hash-2",
            }, 3.5),
        ]
        self._install_search_state(docs, parents=parents)

        with patch("zoty.db._open_zotero_db", wraps=db._open_zotero_db) as open_db_mock:
            result = json.loads(db.search("example", limit=2, include_attachments=True))

        self.assertEqual(result["total"], 2)
        self.assertEqual([row["key"] for row in result["items"]], ["PARENT1", "PARENT2"])
        self.assertEqual([row["attachment_count"] for row in result["items"]], [1, 1])
        self.assertEqual(
            result["items"][0]["attachments"][0]["filepath"],
            str(db._ZOTERO_STORAGE / "ATTACH1" / "paper.pdf"),
        )
        self.assertEqual(
            result["items"][1]["attachments"][0]["filepath"],
            str(db._ZOTERO_STORAGE / "ATTACH2" / "book.epub"),
        )
        self.assertEqual(open_db_mock.call_count, 1)


class CollectionItemTests(DbTestCase):
    def setUp(self):
        super().setUp()
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
            conn.executemany(
                "INSERT INTO items(itemID, key, dateAdded) VALUES (?, ?, ?)",
                [
                    (1, "ITEM1", "2026-03-10 10:00:00"),
                    (2, "ATTACH1", "2026-03-10 10:01:00"),
                ],
            )
            conn.execute("INSERT INTO fields(fieldID, fieldName) VALUES (?, ?)", (1, "title"))
            conn.execute("INSERT INTO itemDataValues(valueID, value) VALUES (?, ?)", (1, "Collection PDF"))
            conn.execute("INSERT INTO itemData(itemID, fieldID, valueID) VALUES (?, ?, ?)", (2, 1, 1))
            conn.execute(
                """INSERT INTO itemAttachments(itemID, parentItemID, linkMode, contentType, path)
                   VALUES (?, ?, ?, ?, ?)""",
                (2, 1, 0, "application/pdf", "storage:collection.pdf"),
            )
            conn.commit()

    def test_list_collection_items_returns_structured_error_for_invalid_key(self):
        zot = Mock()
        zot.collections.return_value = [
            {"data": {"key": "COLL123", "name": "Valid Collection"}}
        ]

        with patch("zoty.db._get_zot", return_value=zot):
            result = json.loads(db.list_collection_items("missing"))

        self.assertEqual(result["collection_key"], "MISSING")
        self.assertFalse(result["collection_found"])
        self.assertEqual(result["items"], [])
        self.assertEqual(result["total"], 0)
        self.assertEqual(result["requested_limit"], 50)
        self.assertEqual(result["applied_limit"], 50)
        self.assertEqual(result["limit_cap"], db._LIST_RESULT_LIMIT_CAP)
        self.assertFalse(result["limit_capped"])
        self.assertIn("not found", result["error"])
        zot.collection_items.assert_not_called()

    def test_list_collection_items_returns_structured_filtered_items_for_valid_key(self):
        zot = Mock()
        zot.collections.return_value = [
            {"data": {"key": "COLL123", "name": "Valid Collection"}}
        ]
        zot.collection_items.return_value = [
            {
                "data": {
                    "key": "ITEM1",
                    "itemType": "preprint",
                    "title": "First Paper",
                    "creators": [{"firstName": "Jane", "lastName": "Example"}],
                    "date": "2026-03-10",
                    "DOI": "10.1000/one",
                    "url": "https://example.org/one",
                    "tags": [{"tag": "chemistry"}],
                    "collections": ["COLL123"],
                    "abstractNote": "First abstract.",
                }
            },
            {
                "data": {
                    "key": "ITEM2",
                    "itemType": "preprint",
                    "title": "Wrong Collection",
                    "creators": [{"firstName": "John", "lastName": "Example"}],
                    "date": "2026-03-11",
                    "DOI": "10.1000/two",
                    "url": "https://example.org/two",
                    "tags": [],
                    "collections": ["OTHER"],
                    "abstractNote": "Second abstract.",
                }
            },
            {
                "data": {
                    "key": "ATTACH1",
                    "itemType": "attachment",
                    "title": "Attachment",
                    "creators": [],
                    "date": "",
                    "DOI": "",
                    "url": "",
                    "tags": [],
                    "collections": ["COLL123"],
                    "abstractNote": "",
                }
            },
            {
                "data": {
                    "key": "ANNOT1",
                    "itemType": "annotation",
                    "title": "Annotation",
                    "creators": [],
                    "date": "",
                    "DOI": "",
                    "url": "",
                    "tags": [],
                    "collections": ["COLL123"],
                    "abstractNote": "",
                }
            },
        ]

        with patch("zoty.db._get_zot", return_value=zot):
            result = json.loads(db.list_collection_items("coll123", limit=5))

        self.assertEqual(result["collection_key"], "COLL123")
        self.assertTrue(result["collection_found"])
        self.assertEqual(result["total"], 1)
        self.assertEqual(result["requested_limit"], 5)
        self.assertEqual(result["applied_limit"], 5)
        self.assertEqual(result["limit_cap"], db._LIST_RESULT_LIMIT_CAP)
        self.assertFalse(result["limit_capped"])
        self.assertEqual([row["key"] for row in result["items"]], ["ITEM1"])
        self.assertEqual(result["items"][0]["title"], "First Paper")
        self.assertEqual(result["items"][0]["attachment_count"], 1)
        self.assertNotIn("attachments", result["items"][0])
        zot.collection_items.assert_called_once_with("COLL123", limit=5)

    def test_list_collection_items_caps_requested_limit_and_reports_metadata(self):
        zot = Mock()
        zot.collections.return_value = [
            {"data": {"key": "COLL123", "name": "Valid Collection"}}
        ]
        zot.collection_items.return_value = [
            {
                "data": {
                    "key": "ITEM1",
                    "itemType": "preprint",
                    "title": "First Paper",
                    "creators": [{"firstName": "Jane", "lastName": "Example"}],
                    "date": "2026-03-10",
                    "DOI": "10.1000/one",
                    "url": "https://example.org/one",
                    "tags": [{"tag": "chemistry"}],
                    "collections": ["COLL123"],
                    "abstractNote": "First abstract.",
                }
            }
        ]

        with patch("zoty.db._get_zot", return_value=zot):
            result = json.loads(db.list_collection_items("coll123", limit=999))

        self.assertEqual(result["requested_limit"], 999)
        self.assertEqual(result["applied_limit"], db._LIST_RESULT_LIMIT_CAP)
        self.assertEqual(result["limit_cap"], db._LIST_RESULT_LIMIT_CAP)
        self.assertTrue(result["limit_capped"])
        zot.collection_items.assert_called_once_with("COLL123", limit=db._LIST_RESULT_LIMIT_CAP)

    def test_list_collection_items_truncates_long_creator_lists(self):
        zot = Mock()
        zot.collections.return_value = [
            {"data": {"key": "COLL123", "name": "Valid Collection"}}
        ]
        zot.collection_items.return_value = [
            {
                "data": {
                    "key": "ITEM1",
                    "itemType": "preprint",
                    "title": "First Paper",
                    "creators": self._creator_dicts(7),
                    "date": "2026-03-10",
                    "DOI": "10.1000/one",
                    "url": "https://example.org/one",
                    "tags": [{"tag": "chemistry"}],
                    "collections": ["COLL123"],
                    "abstractNote": "First abstract.",
                }
            },
        ]

        with patch("zoty.db._get_zot", return_value=zot):
            result = json.loads(db.list_collection_items("coll123", limit=5))

        self.assertEqual(
            result["items"][0]["creators"],
            [
                "Author1 Example",
                "Author2 Example",
                "Author3 Example",
                "Author4 Example",
                "Author5 Example",
                "... and 2 more",
            ],
        )


class RecentItemsTests(DbTestCase):
    def test_get_recent_items_truncates_long_creator_lists(self):
        zot = Mock()
        item = self._paper_item()
        item["data"]["creators"] = self._creator_dicts(8)
        zot.items.return_value = [item]

        with patch("zoty.db._get_zot", return_value=zot):
            result = json.loads(db.get_recent_items(limit=1))

        self.assertEqual(result["total"], 1)
        self.assertEqual(result["requested_limit"], 1)
        self.assertEqual(result["applied_limit"], 1)
        self.assertEqual(result["limit_cap"], db._LIST_RESULT_LIMIT_CAP)
        self.assertFalse(result["limit_capped"])
        self.assertEqual(
            result["items"][0]["creators"],
            [
                "Author1 Example",
                "Author2 Example",
                "Author3 Example",
                "Author4 Example",
                "Author5 Example",
                "... and 3 more",
            ],
        )

    def test_get_recent_items_caps_requested_limit_and_reports_metadata(self):
        zot = Mock()
        zot.items.return_value = [self._paper_item()]

        with patch("zoty.db._get_zot", return_value=zot):
            result = json.loads(db.get_recent_items(limit=999))

        self.assertEqual(result["requested_limit"], 999)
        self.assertEqual(result["applied_limit"], db._LIST_RESULT_LIMIT_CAP)
        self.assertEqual(result["limit_cap"], db._LIST_RESULT_LIMIT_CAP)
        self.assertTrue(result["limit_capped"])
        zot.items.assert_called_once_with(
            limit=db._LIST_RESULT_LIMIT_CAP * 3,
            sort="dateAdded",
            direction="desc",
        )

    def test_get_recent_items_returns_structured_error_skeleton_for_fetch_failure(self):
        with patch("zoty.db._get_zot", side_effect=RuntimeError("boom")):
            result = json.loads(db.get_recent_items(limit=1))

        self.assertEqual(result["items"], [])
        self.assertEqual(result["total"], 0)
        self.assertEqual(result["requested_limit"], 1)
        self.assertEqual(result["applied_limit"], 1)
        self.assertEqual(result["limit_cap"], db._LIST_RESULT_LIMIT_CAP)
        self.assertFalse(result["limit_capped"])
        self.assertIn("Failed to fetch recent items: boom", result["error"])


class ParentRecordDateNormalizationTests(DbTestCase):
    def setUp(self):
        super().setUp()
        with closing(sqlite3.connect(db._ZOTERO_DB)) as conn:
            conn.executescript(
                """
                CREATE TABLE items (
                    itemID INTEGER PRIMARY KEY,
                    key TEXT NOT NULL,
                    version INTEGER,
                    dateModified TEXT,
                    itemTypeID INTEGER NOT NULL,
                    libraryID INTEGER
                );
                CREATE TABLE itemTypesCombined (
                    itemTypeID INTEGER PRIMARY KEY,
                    typeName TEXT NOT NULL
                );
                CREATE TABLE deletedItems (
                    itemID INTEGER PRIMARY KEY
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
                CREATE TABLE itemCreators (
                    itemID INTEGER NOT NULL,
                    creatorID INTEGER NOT NULL,
                    orderIndex INTEGER NOT NULL
                );
                CREATE TABLE creators (
                    creatorID INTEGER PRIMARY KEY,
                    firstName TEXT,
                    lastName TEXT,
                    fieldMode INTEGER
                );
                CREATE TABLE collectionItems (
                    collectionID INTEGER NOT NULL,
                    itemID INTEGER NOT NULL,
                    orderIndex INTEGER NOT NULL
                );
                CREATE TABLE collections (
                    collectionID INTEGER PRIMARY KEY,
                    key TEXT NOT NULL
                );
                CREATE TABLE itemTags (
                    itemID INTEGER NOT NULL,
                    tagID INTEGER NOT NULL
                );
                CREATE TABLE tags (
                    tagID INTEGER PRIMARY KEY,
                    name TEXT NOT NULL
                );
                """
            )
            conn.execute(
                """INSERT INTO itemTypesCombined(itemTypeID, typeName)
                   VALUES (?, ?)""",
                (1, "preprint"),
            )
            conn.execute(
                """INSERT INTO items(itemID, key, version, dateModified, itemTypeID, libraryID)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (1, "PARENT1", 3, "2026-03-10 10:00:00", 1, 1),
            )
            conn.executemany(
                "INSERT INTO fields(fieldID, fieldName) VALUES (?, ?)",
                [
                    (1, "title"),
                    (2, "date"),
                ],
            )
            conn.executemany(
                "INSERT INTO itemDataValues(valueID, value) VALUES (?, ?)",
                [
                    (1, "Normalized Date Paper"),
                    (2, "2025-09-17 2025-09-17"),
                ],
            )
            conn.executemany(
                "INSERT INTO itemData(itemID, fieldID, valueID) VALUES (?, ?, ?)",
                [
                    (1, 1, 1),
                    (1, 2, 2),
                ],
            )
            conn.commit()

    def test_normalize_item_date_collapses_duplicate_iso_dates_only(self):
        self.assertEqual(db._normalize_item_date("2025-09-17 2025-09-17"), "2025-09-17")
        self.assertEqual(db._normalize_item_date(" 2025-09-17   2025-09-17 "), "2025-09-17")
        self.assertEqual(db._normalize_item_date("2025-09-17 2025-10-01"), "2025-09-17 2025-10-01")
        self.assertEqual(db._normalize_item_date("Spring 2025 Spring 2025"), "Spring 2025 Spring 2025")

    def test_fetch_parent_records_normalizes_duplicated_date_field(self):
        parents = db._fetch_parent_records()

        self.assertEqual(parents["PARENT1"].date, "2025-09-17")
        self.assertEqual(parents["PARENT1"].title, "Normalized Date Paper")


class RecentItemsTests(DbTestCase):
    def setUp(self):
        super().setUp()
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
            conn.executemany(
                "INSERT INTO items(itemID, key, dateAdded) VALUES (?, ?, ?)",
                [
                    (1, "ITEM1", "2026-03-10 10:00:00"),
                    (2, "ATTACH1", "2026-03-10 10:01:00"),
                ],
            )
            conn.execute("INSERT INTO fields(fieldID, fieldName) VALUES (?, ?)", (1, "title"))
            conn.execute("INSERT INTO itemDataValues(valueID, value) VALUES (?, ?)", (1, "Recent PDF"))
            conn.execute("INSERT INTO itemData(itemID, fieldID, valueID) VALUES (?, ?, ?)", (2, 1, 1))
            conn.execute(
                """INSERT INTO itemAttachments(itemID, parentItemID, linkMode, contentType, path)
                   VALUES (?, ?, ?, ?, ?)""",
                (2, 1, 0, "application/pdf", "storage:recent.pdf"),
            )
            conn.commit()

    def test_get_recent_items_includes_attachment_count(self):
        zot = Mock()
        zot.items.return_value = [
            {
                "data": {
                    "key": "ITEM1",
                    "itemType": "preprint",
                    "title": "Recent Paper",
                    "creators": [{"firstName": "Jane", "lastName": "Example"}],
                    "date": "2026-03-10",
                    "DOI": "10.1000/recent",
                    "url": "https://example.org/recent",
                    "tags": [{"tag": "ml"}],
                    "collections": ["COLL123"],
                    "abstractNote": "Recent abstract.",
                }
            },
            {
                "data": {
                    "key": "ATTACH1",
                    "itemType": "attachment",
                    "title": "Attachment",
                    "creators": [],
                    "date": "",
                    "DOI": "",
                    "url": "",
                    "tags": [],
                    "collections": [],
                    "abstractNote": "",
                }
            },
        ]

        with patch("zoty.db._get_zot", return_value=zot):
            result = json.loads(db.get_recent_items(limit=1))

        self.assertEqual(result["total"], 1)
        self.assertEqual(result["requested_limit"], 1)
        self.assertEqual(result["applied_limit"], 1)
        self.assertEqual(result["limit_cap"], db._LIST_RESULT_LIMIT_CAP)
        self.assertFalse(result["limit_capped"])
        self.assertEqual(result["items"][0]["key"], "ITEM1")
        self.assertEqual(result["items"][0]["attachment_count"], 1)
        self.assertNotIn("attachments", result["items"][0])
        zot.items.assert_called_once_with(limit=3, sort="dateAdded", direction="desc")


class SearchBehaviorTests(DbTestCase):
    def setUp(self):
        super().setUp()
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
            conn.executemany(
                "INSERT INTO items(itemID, key, dateAdded) VALUES (?, ?, ?)",
                [
                    (1, "PARENT1", "2026-03-10 10:00:00"),
                    (2, "ATTACH1", "2026-03-10 10:01:00"),
                    (3, "ATTACH2", "2026-03-10 10:02:00"),
                ],
            )
            conn.execute("INSERT INTO fields(fieldID, fieldName) VALUES (?, ?)", (1, "title"))
            conn.executemany(
                "INSERT INTO itemDataValues(valueID, value) VALUES (?, ?)",
                [
                    (1, "Attached PDF"),
                    (2, "Second PDF"),
                ],
            )
            conn.executemany(
                "INSERT INTO itemData(itemID, fieldID, valueID) VALUES (?, ?, ?)",
                [
                    (2, 1, 1),
                    (3, 1, 2),
                ],
            )
            conn.executemany(
                """INSERT INTO itemAttachments(itemID, parentItemID, linkMode, contentType, path)
                   VALUES (?, ?, ?, ?, ?)""",
                [
                    (2, 1, 0, "application/pdf", "storage:paper.pdf"),
                    (3, 1, 0, "application/pdf", "storage:paper-2.pdf"),
                ],
            )
            conn.commit()

    def test_search_returns_error_when_snapshot_not_loaded(self):
        db._search_state = None

        result = json.loads(db.search("body only"))

        self.assertEqual(result["error"], "Index is still building, please retry in a moment")
        self.assertEqual(result["items"], [])

    def test_body_only_query_returns_parent_with_attachment_snippet(self):
        attachment_doc = {
            "doc_id": "chunk:ATTACH1:0",
            "parent_key": "PARENT1",
            "attachment_key": "ATTACH1",
            "doc_kind": "attachment_chunk",
            "chunk_index": 0,
            "char_start": 0,
            "char_end": 80,
            "token_count": 12,
            "text": "This body text contains meatpotatoes evidence deep in the paper body.",
            "text_hash": "hash-body",
        }
        self._install_search_state([(attachment_doc, 7.25)])

        result = json.loads(db.search("meatpotatoes"))

        self.assertEqual(result["total"], 1)
        self.assertEqual(result["items"][0]["key"], "PARENT1")
        self.assertIn("meatpotatoes", result["items"][0]["snippet"].lower())
        self.assertEqual(result["items"][0]["snippet_attachment_key"], "ATTACH1")

    def test_search_truncates_long_creator_lists(self):
        attachment_doc = {
            "doc_id": "chunk:ATTACH1:0",
            "parent_key": "PARENT1",
            "attachment_key": "ATTACH1",
            "doc_kind": "attachment_chunk",
            "chunk_index": 0,
            "char_start": 0,
            "char_end": 80,
            "token_count": 12,
            "text": "This body text contains meatpotatoes evidence deep in the paper body.",
            "text_hash": "hash-body",
        }
        parents = {
            "PARENT1": {
                "key": "PARENT1",
                "dateModified": "2026-03-10 10:00:00",
                "itemType": "preprint",
                "title": "Example Paper",
                "abstract": "Example abstract.",
                "creators": [f"Author {index + 1}" for index in range(7)],
                "collections": ["COLL123"],
                "tags": ["chemistry"],
                "date": "2026-03-10",
                "DOI": "10.1000/example",
                "url": "https://example.org/paper",
            }
        }
        self._install_search_state([(attachment_doc, 7.25)], parents=parents)

        result = json.loads(db.search("meatpotatoes"))

        self.assertEqual(
            result["items"][0]["creators"],
            [
                "Author 1",
                "Author 2",
                "Author 3",
                "Author 4",
                "Author 5",
                "... and 2 more",
            ],
        )

    def test_search_normalizes_duplicated_iso_dates_in_results(self):
        metadata_doc = {
            "doc_id": "meta:PARENT1",
            "parent_key": "PARENT1",
            "attachment_key": "",
            "doc_kind": "metadata",
            "chunk_index": 0,
            "char_start": 0,
            "char_end": 60,
            "token_count": 8,
            "text": "Example Paper Example Paper abstract novelty signal",
            "text_hash": "hash-meta",
        }
        parents = {
            "PARENT1": {
                "key": "PARENT1",
                "dateModified": "2026-03-10 10:00:00",
                "itemType": "preprint",
                "title": "Example Paper",
                "abstract": "Example abstract.",
                "creators": ["Jane Example"],
                "collections": ["COLL123"],
                "tags": ["chemistry"],
                "date": "2025-09-17 2025-09-17",
                "DOI": "10.1000/example",
                "url": "https://example.org/paper",
            }
        }
        self._install_search_state([(metadata_doc, 6.5)], parents=parents)

        result = json.loads(db.search("novelty"))

        self.assertEqual(result["items"][0]["date"], "2025-09-17")

    def test_metadata_query_uses_abstract_snippet_without_attachment_key(self):
        metadata_doc = {
            "doc_id": "meta:PARENT1",
            "parent_key": "PARENT1",
            "attachment_key": "",
            "doc_kind": "metadata",
            "chunk_index": 0,
            "char_start": 0,
            "char_end": 60,
            "token_count": 8,
            "text": "Example Paper Example Paper abstract novelty signal",
            "text_hash": "hash-meta",
        }
        self._install_search_state([(metadata_doc, 6.5)])

        result = json.loads(db.search("novelty"))

        self.assertEqual(result["total"], 1)
        self.assertEqual(result["items"][0]["key"], "PARENT1")
        self.assertIn("example abstract", result["items"][0]["snippet"].lower())
        self.assertNotIn("snippet_attachment_key", result["items"][0])

    def test_multiple_matching_chunks_collapse_to_one_parent(self):
        parents = {
            "PARENT1": {
                "key": "PARENT1",
                "dateModified": "2026-03-10 10:00:00",
                "itemType": "preprint",
                "title": "First Paper",
                "abstract": "First abstract.",
                "creators": ["Jane Example"],
                "collections": ["COLL123"],
                "tags": ["chemistry"],
                "date": "2026-03-10",
                "DOI": "10.1000/one",
                "url": "https://example.org/one",
            },
            "PARENT2": {
                "key": "PARENT2",
                "dateModified": "2026-03-09 09:00:00",
                "itemType": "preprint",
                "title": "Second Paper",
                "abstract": "Second abstract.",
                "creators": ["John Example"],
                "collections": ["COLL123"],
                "tags": ["physics"],
                "date": "2026-03-09",
                "DOI": "10.1000/two",
                "url": "https://example.org/two",
            },
        }
        docs = [
            ({
                "doc_id": "chunk:ATTACH1:0",
                "parent_key": "PARENT1",
                "attachment_key": "ATTACH1",
                "doc_kind": "attachment_chunk",
                "chunk_index": 0,
                "char_start": 0,
                "char_end": 30,
                "token_count": 4,
                "text": "shared body text first hit",
                "text_hash": "hash-1",
            }, 9.0),
            ({
                "doc_id": "chunk:ATTACH2:0",
                "parent_key": "PARENT1",
                "attachment_key": "ATTACH2",
                "doc_kind": "attachment_chunk",
                "chunk_index": 0,
                "char_start": 0,
                "char_end": 30,
                "token_count": 4,
                "text": "shared body text second hit",
                "text_hash": "hash-2",
            }, 8.5),
            ({
                "doc_id": "meta:PARENT2",
                "parent_key": "PARENT2",
                "attachment_key": "",
                "doc_kind": "metadata",
                "chunk_index": 0,
                "char_start": 0,
                "char_end": 30,
                "token_count": 4,
                "text": "shared body text second paper",
                "text_hash": "hash-3",
            }, 7.0),
        ]
        self._install_search_state(docs, parents=parents)

        result = json.loads(db.search("shared"))

        self.assertEqual(result["total"], 2)
        self.assertEqual([row["key"] for row in result["items"]], ["PARENT1", "PARENT2"])
        self.assertEqual(result["items"][0]["score"], 9.0)

    def test_collection_and_item_type_filters_apply_at_parent_level(self):
        parents = {
            "PARENT1": {
                "key": "PARENT1",
                "dateModified": "2026-03-10 10:00:00",
                "itemType": "preprint",
                "title": "First Paper",
                "abstract": "First abstract.",
                "creators": ["Jane Example"],
                "collections": ["KEEP"],
                "tags": [],
                "date": "2026-03-10",
                "DOI": "",
                "url": "",
            },
            "PARENT2": {
                "key": "PARENT2",
                "dateModified": "2026-03-11 10:00:00",
                "itemType": "journalArticle",
                "title": "Second Paper",
                "abstract": "Second abstract.",
                "creators": ["John Example"],
                "collections": ["DROP"],
                "tags": [],
                "date": "2026-03-11",
                "DOI": "",
                "url": "",
            },
        }
        docs = [
            ({
                "doc_id": "meta:PARENT2",
                "parent_key": "PARENT2",
                "attachment_key": "",
                "doc_kind": "metadata",
                "chunk_index": 0,
                "char_start": 0,
                "char_end": 20,
                "token_count": 3,
                "text": "query match second",
                "text_hash": "hash-2",
            }, 8.0),
            ({
                "doc_id": "meta:PARENT1",
                "parent_key": "PARENT1",
                "attachment_key": "",
                "doc_kind": "metadata",
                "chunk_index": 0,
                "char_start": 0,
                "char_end": 20,
                "token_count": 3,
                "text": "query match first",
                "text_hash": "hash-1",
            }, 7.0),
        ]
        self._install_search_state(docs, parents=parents)

        result = json.loads(db.search("query", collection_key="KEEP", item_type="preprint"))

        self.assertEqual(result["total"], 1)
        self.assertEqual(result["items"][0]["key"], "PARENT1")

    def test_search_warns_for_unknown_collection_key_filter(self):
        self._install_search_state([
            ({
                "doc_id": "meta:PARENT1",
                "parent_key": "PARENT1",
                "attachment_key": "",
                "doc_kind": "metadata",
                "chunk_index": 0,
                "char_start": 0,
                "char_end": 30,
                "token_count": 4,
                "text": "query match metadata",
                "text_hash": "hash-1",
            }, 7.0),
        ])

        result = json.loads(db.search("query", collection_key="missing"))

        self.assertEqual(result["items"], [])
        self.assertEqual(result["total"], 0)
        self.assertEqual(
            result["warning"],
            "Collection MISSING was not found in the search index",
        )

    def test_search_warns_for_unknown_item_type_filter(self):
        self._install_search_state([
            ({
                "doc_id": "meta:PARENT1",
                "parent_key": "PARENT1",
                "attachment_key": "",
                "doc_kind": "metadata",
                "chunk_index": 0,
                "char_start": 0,
                "char_end": 30,
                "token_count": 4,
                "text": "query match metadata",
                "text_hash": "hash-1",
            }, 7.0),
        ])

        result = json.loads(db.search("query", item_type="invalidType"))

        self.assertEqual(result["items"], [])
        self.assertEqual(result["total"], 0)
        self.assertEqual(
            result["warning"],
            "Item type 'invalidType' was not found in the search index",
        )

    def test_search_caps_large_requested_limits_and_reports_metadata(self):
        parents = {}
        docs = []
        for index in range(600):
            parent_key = f"PARENT{index + 1}"
            parents[parent_key] = {
                "key": parent_key,
                "dateModified": f"2026-03-{(index % 28) + 1:02d} 10:00:00",
                "itemType": "preprint",
                "title": f"Paper {index + 1}",
                "abstract": f"Abstract {index + 1}.",
                "creators": [f"Author {index + 1}"],
                "collections": ["COLL123"],
                "tags": [],
                "date": f"2026-03-{(index % 28) + 1:02d}",
                "DOI": "",
                "url": "",
            }
            docs.append(
                (
                    {
                        "doc_id": f"meta:{parent_key}",
                        "parent_key": parent_key,
                        "attachment_key": "",
                        "doc_kind": "metadata",
                        "chunk_index": 0,
                        "char_start": 0,
                        "char_end": 40,
                        "token_count": 4,
                        "text": f"query match {index + 1}",
                        "text_hash": f"hash-{index + 1}",
                    },
                    float(1000 - index),
                )
            )
        self._install_search_state(docs, parents=parents)

        result = json.loads(db.search("query", limit=1000))

        self.assertEqual(result["requested_limit"], 1000)
        self.assertEqual(result["applied_limit"], db._SEARCH_RESULT_LIMIT_CAP)
        self.assertEqual(result["limit_cap"], db._SEARCH_RESULT_LIMIT_CAP)
        self.assertTrue(result["limit_capped"])
        self.assertEqual(result["total"], db._SEARCH_RESULT_LIMIT_CAP)
        self.assertEqual(len(result["items"]), db._SEARCH_RESULT_LIMIT_CAP)
        self.assertEqual([call["k"] for call in db._search_state.retriever.calls], [500])
        self.assertEqual(result["items"][0]["key"], "PARENT1")

    def test_search_returns_warning_when_query_has_no_searchable_terms(self):
        self._install_search_state([
            ({
                "doc_id": "meta:PARENT1",
                "parent_key": "PARENT1",
                "attachment_key": "",
                "doc_kind": "metadata",
                "chunk_index": 0,
                "char_start": 0,
                "char_end": 20,
                "token_count": 3,
                "text": "query match first",
                "text_hash": "hash-1",
            }, 7.0),
        ])

        result = json.loads(db.search("the and or", limit=3))

        self.assertEqual(result["items"], [])
        self.assertEqual(result["total"], 0)
        self.assertEqual(result["warning"], db._EMPTY_QUERY_WARNING)
        self.assertEqual(db._search_state.retriever.calls, [])

    def test_search_does_not_return_warning_for_valid_zero_match_query(self):
        self._install_search_state([
            ({
                "doc_id": "meta:PARENT1",
                "parent_key": "PARENT1",
                "attachment_key": "",
                "doc_kind": "metadata",
                "chunk_index": 0,
                "char_start": 0,
                "char_end": 20,
                "token_count": 3,
                "text": "query match first",
                "text_hash": "hash-1",
            }, 0.0),
        ])

        result = json.loads(db.search("quantum topology", limit=3))

        self.assertEqual(result["items"], [])
        self.assertEqual(result["total"], 0)
        self.assertNotIn("warning", result)

    def test_search_within_item_returns_multiple_ranked_matches_for_one_parent(self):
        parents = {
            "PARENT1": {
                "key": "PARENT1",
                "dateModified": "2026-03-10 10:00:00",
                "itemType": "preprint",
                "title": "First Paper",
                "abstract": "First abstract with metadata match.",
                "creators": ["Jane Example"],
                "collections": ["COLL123"],
                "tags": [],
                "date": "2026-03-10",
                "DOI": "",
                "url": "",
            },
            "PARENT2": {
                "key": "PARENT2",
                "dateModified": "2026-03-11 10:00:00",
                "itemType": "preprint",
                "title": "Second Paper",
                "abstract": "Second abstract.",
                "creators": ["John Example"],
                "collections": ["COLL123"],
                "tags": [],
                "date": "2026-03-11",
                "DOI": "",
                "url": "",
            },
        }
        docs = [
            ({
                "doc_id": "chunk:ATTACH1:0",
                "parent_key": "PARENT1",
                "attachment_key": "ATTACH1",
                "doc_kind": "attachment_chunk",
                "chunk_index": 0,
                "char_start": 0,
                "char_end": 25,
                "token_count": 4,
                "text": "query match strongest chunk",
                "text_hash": "hash-1",
            }, 9.0),
            ({
                "doc_id": "chunk:ATTACH2:0",
                "parent_key": "PARENT1",
                "attachment_key": "ATTACH2",
                "doc_kind": "attachment_chunk",
                "chunk_index": 1,
                "char_start": 30,
                "char_end": 60,
                "token_count": 4,
                "text": "query match second chunk",
                "text_hash": "hash-2",
            }, 8.5),
            ({
                "doc_id": "meta:PARENT1",
                "parent_key": "PARENT1",
                "attachment_key": "",
                "doc_kind": "metadata",
                "chunk_index": 0,
                "char_start": 0,
                "char_end": 30,
                "token_count": 4,
                "text": "query match metadata",
                "text_hash": "hash-3",
            }, 7.5),
            ({
                "doc_id": "meta:PARENT2",
                "parent_key": "PARENT2",
                "attachment_key": "",
                "doc_kind": "metadata",
                "chunk_index": 0,
                "char_start": 0,
                "char_end": 20,
                "token_count": 3,
                "text": "query match outside",
                "text_hash": "hash-4",
            }, 10.0),
        ]
        self._install_search_state(docs, parents=parents)

        result = json.loads(db.search_within_item("parent1", "query", limit=3))

        self.assertEqual(result["key"], "PARENT1")
        self.assertEqual(result["item"], {"key": "PARENT1", "title": "First Paper"})
        self.assertNotIn("abstract", result["item"])
        self.assertNotIn("attachments", result["item"])
        self.assertEqual(result["requested_limit"], 3)
        self.assertEqual(result["applied_limit"], 3)
        self.assertEqual(result["limit_cap"], db._SEARCH_RESULT_LIMIT_CAP)
        self.assertFalse(result["limit_capped"])
        self.assertEqual(result["total"], 3)
        self.assertEqual(
            [row["match_type"] for row in result["matches"]],
            ["attachment_chunk", "attachment_chunk", "metadata"],
        )
        self.assertNotIn("key", result["matches"][0])
        self.assertNotIn("title", result["matches"][0])
        self.assertEqual(result["matches"][0]["attachment_key"], "ATTACH1")
        self.assertEqual(result["matches"][1]["attachment_key"], "ATTACH2")
        self.assertNotIn("attachment_key", result["matches"][2])

    def test_search_within_item_returns_lean_item_summary_when_query_has_no_terms(self):
        self._install_search_state([
            ({
                "doc_id": "meta:PARENT1",
                "parent_key": "PARENT1",
                "attachment_key": "",
                "doc_kind": "metadata",
                "chunk_index": 0,
                "char_start": 0,
                "char_end": 20,
                "token_count": 3,
                "text": "query match first",
                "text_hash": "hash-1",
            }, 7.0),
        ])

        result = json.loads(db.search_within_item("parent1", "the and", limit=3))

        self.assertEqual(result["item"], {"key": "PARENT1", "title": "Example Paper"})
        self.assertEqual(result["matches"], [])
        self.assertEqual(result["total"], 0)
        self.assertEqual(result["requested_limit"], 3)
        self.assertEqual(result["applied_limit"], 3)
        self.assertEqual(result["limit_cap"], db._SEARCH_RESULT_LIMIT_CAP)
        self.assertFalse(result["limit_capped"])
        self.assertEqual(result["warning"], db._EMPTY_QUERY_WARNING)
        self.assertEqual(db._search_state.retriever.calls, [])

    def test_search_within_item_does_not_return_warning_for_valid_zero_match_query(self):
        self._install_search_state([
            ({
                "doc_id": "meta:PARENT1",
                "parent_key": "PARENT1",
                "attachment_key": "",
                "doc_kind": "metadata",
                "chunk_index": 0,
                "char_start": 0,
                "char_end": 20,
                "token_count": 3,
                "text": "query match first",
                "text_hash": "hash-1",
            }, 0.0),
        ])

        result = json.loads(db.search_within_item("parent1", "quantum topology", limit=3))

        self.assertEqual(result["item"], {"key": "PARENT1", "title": "Example Paper"})
        self.assertEqual(result["matches"], [])
        self.assertEqual(result["total"], 0)
        self.assertEqual(result["requested_limit"], 3)
        self.assertEqual(result["applied_limit"], 3)
        self.assertEqual(result["limit_cap"], db._SEARCH_RESULT_LIMIT_CAP)
        self.assertFalse(result["limit_capped"])
        self.assertNotIn("warning", result)

    def test_search_within_item_returns_error_for_unknown_item(self):
        self._install_search_state([
            ({
                "doc_id": "meta:PARENT1",
                "parent_key": "PARENT1",
                "attachment_key": "",
                "doc_kind": "metadata",
                "chunk_index": 0,
                "char_start": 0,
                "char_end": 20,
                "token_count": 3,
                "text": "query match first",
                "text_hash": "hash-1",
            }, 7.0),
        ])

        result = json.loads(db.search_within_item("missing", "query"))

        self.assertEqual(result["key"], "MISSING")
        self.assertEqual(result["item"], {"key": "MISSING", "title": ""})
        self.assertEqual(result["matches"], [])
        self.assertEqual(result["requested_limit"], 5)
        self.assertEqual(result["applied_limit"], 5)
        self.assertEqual(result["limit_cap"], db._SEARCH_RESULT_LIMIT_CAP)
        self.assertFalse(result["limit_capped"])
        self.assertIn("was not found", result["error"])

    def test_search_within_item_supports_multi_item_queries(self):
        parents = {
            "PARENT1": {
                "key": "PARENT1",
                "dateModified": "2026-03-10 10:00:00",
                "itemType": "preprint",
                "title": "First Paper",
                "abstract": "First abstract.",
                "creators": ["Jane Example"],
                "collections": ["COLL123"],
                "tags": [],
                "date": "2026-03-10",
                "DOI": "",
                "url": "",
            },
            "PARENT2": {
                "key": "PARENT2",
                "dateModified": "2026-03-11 10:00:00",
                "itemType": "preprint",
                "title": "Second Paper",
                "abstract": "Second abstract.",
                "creators": ["John Example"],
                "collections": ["COLL123"],
                "tags": [],
                "date": "2026-03-11",
                "DOI": "",
                "url": "",
            },
        }
        docs = [
            ({
                "doc_id": "meta:PARENT2",
                "parent_key": "PARENT2",
                "attachment_key": "",
                "doc_kind": "metadata",
                "chunk_index": 0,
                "char_start": 0,
                "char_end": 20,
                "token_count": 3,
                "text": "query match second",
                "text_hash": "hash-2",
            }, 8.0),
            ({
                "doc_id": "meta:PARENT1",
                "parent_key": "PARENT1",
                "attachment_key": "",
                "doc_kind": "metadata",
                "chunk_index": 0,
                "char_start": 0,
                "char_end": 20,
                "token_count": 3,
                "text": "query match first",
                "text_hash": "hash-1",
            }, 7.0),
        ]
        self._install_search_state(docs, parents=parents)

        result = json.loads(
            db.search_within_item(
                item_key="parent1",
                item_keys=["parent2"],
                query="query",
                limit=2,
            )
        )

        self.assertEqual(result["item_keys"], ["PARENT1", "PARENT2"])
        self.assertEqual(
            result["items"],
            [
                {"key": "PARENT1", "title": "First Paper"},
                {"key": "PARENT2", "title": "Second Paper"},
            ],
        )
        self.assertEqual(result["match_counts"], {"PARENT1": 1, "PARENT2": 1})
        self.assertEqual(result["requested_limit"], 2)
        self.assertEqual(result["applied_limit"], 2)
        self.assertEqual(result["limit_cap"], db._SEARCH_RESULT_LIMIT_CAP)
        self.assertFalse(result["limit_capped"])
        self.assertEqual(result["total"], 2)
        self.assertEqual([row["key"] for row in result["matches"]], ["PARENT2", "PARENT1"])
        self.assertNotIn("title", result["matches"][0])
        self.assertNotIn("title", result["matches"][1])

    def test_search_within_item_caps_large_requested_limits_and_reports_metadata(self):
        docs = [
            ({
                "doc_id": f"meta:PARENT1:{index}",
                "parent_key": "PARENT1",
                "attachment_key": "",
                "doc_kind": "metadata",
                "chunk_index": index,
                "char_start": index * 10,
                "char_end": (index * 10) + 20,
                "token_count": 3,
                "text": f"query match {index}",
                "text_hash": f"hash-{index}",
            }, 1000.0 - index)
            for index in range(600)
        ]
        self._install_search_state(docs)

        with patch("zoty.db._get_item_attachments_by_parent", return_value={"PARENT1": []}):
            result = json.loads(db.search_within_item("parent1", "query", limit=999))

        self.assertEqual(result["requested_limit"], 999)
        self.assertEqual(result["applied_limit"], db._SEARCH_RESULT_LIMIT_CAP)
        self.assertEqual(result["limit_cap"], db._SEARCH_RESULT_LIMIT_CAP)
        self.assertTrue(result["limit_capped"])
        self.assertEqual(result["total"], db._SEARCH_RESULT_LIMIT_CAP)
        self.assertEqual(len(result["matches"]), db._SEARCH_RESULT_LIMIT_CAP)
        self.assertEqual(db._search_state.retriever.calls[0]["k"], 500)


class SnapshotLifecycleTests(DbTestCase):
    def _make_parent(self, parent_key="PARENT1"):
        return db._ParentRecord(
            parent_key=parent_key,
            parent_item_id=1,
            item_version=1,
            date_modified="2026-03-10 10:00:00",
            item_type="preprint",
            title="Snapshot Paper",
            abstract="Snapshot abstract mentions alpha beta.",
            creators=["Jane Example"],
            collections=["COLL123"],
            tags=["chemistry"],
            date="2026-03-10",
            doi="10.1000/snapshot",
            url="https://example.org/snapshot",
            metadata_hash="parent-hash",
        )

    def _make_attachment(self, *, signature="sig-1", attachment_key="ATTACH1"):
        return db._AttachmentRecord(
            attachment_key=attachment_key,
            attachment_item_id=2,
            parent_key="PARENT1",
            item_version=1,
            content_type="application/pdf",
            link_mode=0,
            source_path=str(Path(self.temp_dir.name) / "paper.pdf"),
            cache_path=str(Path(self.temp_dir.name) / ".zotero-ft-cache"),
            storage_mod_time=1,
            storage_hash="",
            last_processed_mod_time=1,
            fulltext_version=1,
            indexed_pages=10,
            total_pages=10,
            indexed_chars=None,
            total_chars=None,
            source_signature=signature,
        )

    def test_connect_manifest_uses_read_only_uri_for_read_connections(self):
        mock_conn = Mock()

        with patch("zoty.db.sqlite3.connect", return_value=mock_conn) as connect_mock:
            conn = db._connect_manifest()

        self.assertIs(conn, mock_conn)
        connect_mock.assert_called_once_with(
            f"file:{db._manifest_db_path()}?mode=ro",
            uri=True,
        )
        self.assertEqual(mock_conn.row_factory, sqlite3.Row)

    def test_prepare_search_index_loads_existing_snapshot_and_skips_refresh(self):
        parent = self._make_parent()
        doc = db._build_metadata_doc(parent)
        self.assertIsNotNone(doc)

        with closing(db._connect_manifest(writable=True)) as conn:
            db._initialize_manifest(conn)
            db._upsert_parent(conn, parent)
            db._insert_doc(conn, doc)
            snapshot_id, _retriever, _corpus_docs = db._build_snapshot(
                [doc],
                source_fingerprint="fingerprint-1",
                parent_count=1,
                attachment_count=0,
            )
            db._set_meta(conn, "active_snapshot_id", snapshot_id)
            db._set_meta(conn, "last_source_fingerprint", "fingerprint-1")
            conn.commit()

        db._search_state = None

        with (
            patch("zoty.db._compute_source_fingerprint", return_value="fingerprint-1"),
            patch("zoty.db._start_refresh_thread") as refresh_mock,
        ):
            db.prepare_search_index()

        self.assertIsNotNone(db._search_state)
        self.assertEqual(db._search_state.snapshot_id, snapshot_id)
        self.assertIn("PARENT1", db._search_state.parents)
        refresh_mock.assert_not_called()

    def test_prepare_search_index_holds_lock_during_snapshot_load(self):
        parent = self._make_parent()
        doc = db._build_metadata_doc(parent)
        self.assertIsNotNone(doc)

        with closing(db._connect_manifest(writable=True)) as conn:
            db._initialize_manifest(conn)
            db._upsert_parent(conn, parent)
            db._insert_doc(conn, doc)
            snapshot_id, _retriever, _corpus_docs = db._build_snapshot(
                [doc],
                source_fingerprint="fingerprint-1",
                parent_count=1,
                attachment_count=0,
            )
            db._set_meta(conn, "active_snapshot_id", snapshot_id)
            db._set_meta(conn, "last_source_fingerprint", "fingerprint-1")
            conn.commit()

        db._search_state = None

        load_started = threading.Event()
        release_load = threading.Event()
        real_load_snapshot = db._load_snapshot

        def slow_load(snapshot_to_load):
            load_started.set()
            release_load.wait(timeout=1)
            return real_load_snapshot(snapshot_to_load)

        with (
            patch("zoty.db._compute_source_fingerprint", return_value="fingerprint-1"),
            patch("zoty.db._start_refresh_thread") as refresh_mock,
            patch("zoty.db._load_snapshot", side_effect=slow_load),
        ):
            thread = threading.Thread(target=db.prepare_search_index)
            thread.start()
            self.assertTrue(load_started.wait(timeout=1))
            acquired = db._index_lock.acquire(blocking=False)
            if acquired:
                db._index_lock.release()
            self.assertFalse(acquired)
            release_load.set()
            thread.join(timeout=1)

        self.assertFalse(thread.is_alive())
        self.assertIsNotNone(db._search_state)
        self.assertEqual(db._search_state.snapshot_id, snapshot_id)
        refresh_mock.assert_not_called()

    def test_prepare_search_index_requests_refresh_when_fingerprint_changes(self):
        parent = self._make_parent()
        doc = db._build_metadata_doc(parent)

        with closing(db._connect_manifest(writable=True)) as conn:
            db._initialize_manifest(conn)
            db._upsert_parent(conn, parent)
            db._insert_doc(conn, doc)
            snapshot_id, _retriever, _corpus_docs = db._build_snapshot(
                [doc],
                source_fingerprint="fingerprint-1",
                parent_count=1,
                attachment_count=0,
            )
            db._set_meta(conn, "active_snapshot_id", snapshot_id)
            db._set_meta(conn, "last_source_fingerprint", "fingerprint-1")
            conn.commit()

        db._search_state = None

        with (
            patch("zoty.db._compute_source_fingerprint", return_value="fingerprint-2"),
            patch("zoty.db._start_refresh_thread") as refresh_mock,
        ):
            db.prepare_search_index()

        refresh_mock.assert_called_once_with(force=False)

    def test_refresh_docs_manifest_reuses_unchanged_attachment_docs(self):
        parent = self._make_parent()
        attachment = self._make_attachment(signature="sig-1")
        ingested = db._AttachmentIngestResult(
            extraction_state="indexed",
            error_text="",
            content_hash="content-hash",
            content_chars=100,
            token_count=20,
            chunk_count=1,
            docs=[
                {
                    "doc_id": "chunk:ATTACH1:0",
                    "parent_key": "PARENT1",
                    "attachment_key": "ATTACH1",
                    "doc_kind": "attachment_chunk",
                    "chunk_index": 0,
                    "char_start": 0,
                    "char_end": 50,
                    "token_count": 20,
                    "text": "alpha beta gamma",
                    "text_hash": "hash-doc",
                }
            ],
        )

        with closing(db._connect_manifest(writable=True)) as conn:
            db._initialize_manifest(conn)
            with patch("zoty.db._ingest_attachment", return_value=ingested) as ingest_mock:
                db._refresh_docs_manifest(conn, {"PARENT1": parent}, {"ATTACH1": attachment})
                self.assertEqual(ingest_mock.call_count, 1)

            with patch("zoty.db._ingest_attachment", return_value=ingested) as ingest_mock:
                db._refresh_docs_manifest(conn, {"PARENT1": parent}, {"ATTACH1": attachment})
                self.assertEqual(ingest_mock.call_count, 0)

            changed_attachment = self._make_attachment(signature="sig-2")
            with patch("zoty.db._ingest_attachment", return_value=ingested) as ingest_mock:
                db._refresh_docs_manifest(conn, {"PARENT1": parent}, {"ATTACH1": changed_attachment})
                self.assertEqual(ingest_mock.call_count, 1)

    def test_prune_snapshots_keeps_active_and_previous_only(self):
        snapshots_dir = db._snapshots_dir()
        snapshots_dir.mkdir(parents=True, exist_ok=True)
        for name in ("snap-1", "snap-2", "snap-3"):
            (snapshots_dir / name).mkdir(parents=True, exist_ok=True)

        db._prune_snapshots("snap-3", "snap-2")

        self.assertTrue((snapshots_dir / "snap-3").exists())
        self.assertTrue((snapshots_dir / "snap-2").exists())
        self.assertFalse((snapshots_dir / "snap-1").exists())

    def test_prune_snapshots_waits_for_index_lock(self):
        snapshots_dir = db._snapshots_dir()
        snapshots_dir.mkdir(parents=True, exist_ok=True)
        for name in ("snap-1", "snap-2", "snap-3"):
            (snapshots_dir / name).mkdir(parents=True, exist_ok=True)

        delete_started = threading.Event()
        release_delete = threading.Event()
        real_rmtree = db.shutil.rmtree

        def blocking_rmtree(path, ignore_errors=False):
            delete_started.set()
            release_delete.wait(timeout=1)
            return real_rmtree(path, ignore_errors=ignore_errors)

        with patch("zoty.db.shutil.rmtree", side_effect=blocking_rmtree):
            with db._index_lock:
                thread = threading.Thread(
                    target=db._prune_snapshots,
                    args=("snap-3", "snap-2"),
                )
                thread.start()
                self.assertFalse(delete_started.wait(timeout=0.1))
                self.assertTrue((snapshots_dir / "snap-1").exists())
                release_delete.set()

            thread.join(timeout=1)

        self.assertFalse(thread.is_alive())
        self.assertTrue((snapshots_dir / "snap-3").exists())
        self.assertTrue((snapshots_dir / "snap-2").exists())
        self.assertFalse((snapshots_dir / "snap-1").exists())


class CitationEntryTests(DbTestCase):
    def test_normalize_item_keys_accepts_single_and_list_inputs(self):
        result = db._normalize_item_keys(
            item_key=" item123 ",
            item_keys=[" item456 ", "", "Item789"],
        )

        self.assertEqual(result, ["ITEM123", "ITEM456", "ITEM789"])

    def test_get_bibtex_and_citation_for_items_returns_single_item_exports(self):
        zot = Mock()

        def item_side_effect(item_key, **kwargs):
            self.assertEqual(kwargs["format"], "json")
            self.assertEqual(kwargs["include"], "bib,citation,bibtex")
            self.assertEqual(kwargs["style"], "apa")
            self.assertEqual(kwargs["locale"], "fr-FR")

            return {
                "citation": [f"<span>{item_key} &amp; cite</span>"],
                "bib": [f"<div>{item_key} <i>reference</i></div>"],
                "bibtex": [
                    (
                        f"@article{{{item_key},\n"
                        "  title={Example},\n"
                        "  abstract={Detailed summary with {nested} braces},\n"
                        "  year={2026}\n"
                        "}"
                    )
                ],
            }

        zot.item.side_effect = item_side_effect

        with patch("zoty.db._get_zot", return_value=zot):
            result = json.loads(
                db.get_bibtex_and_citation_for_items(
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
                    "bibtex": "@article{ITEM123,\n  title={Example},\n  year={2026}\n}",
                }
            ],
        )

    def test_get_bibtex_and_citation_for_items_returns_multiple_items_and_partial_errors(self):
        zot = Mock()

        def item_side_effect(item_key, **kwargs):
            if item_key == "BADKEY":
                raise RuntimeError("missing item")

            self.assertEqual(kwargs["format"], "json")
            self.assertEqual(kwargs["include"], "bib,citation,bibtex")
            return {
                "citation": [f"<span>{item_key} cite</span>"],
                "bib": [f"<div>{item_key} ref</div>"],
                "bibtex": [f"@article{{{item_key}}}"],
            }

        zot.item.side_effect = item_side_effect

        with patch("zoty.db._get_zot", return_value=zot):
            result = json.loads(
                db.get_bibtex_and_citation_for_items(
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

    def test_get_bibtex_and_citation_for_items_makes_one_export_call_per_item(self):
        zot = Mock()
        zot.item.return_value = {
            "citation": ["<span>cite</span>"],
            "bib": ["<div>ref</div>"],
            "bibtex": ["@article{X}"],
        }

        with patch("zoty.db._get_zot", return_value=zot):
            result = json.loads(
                db.get_bibtex_and_citation_for_items(
                    item_keys=["good1", "good2"],
                    style="apa",
                    locale="en-GB",
                )
            )

        self.assertEqual(result["total"], 2)
        self.assertEqual(zot.item.call_count, 2)
        self.assertCountEqual([args[0] for args, _kwargs in zot.item.call_args_list], ["GOOD1", "GOOD2"])

        for _args, kwargs in zot.item.call_args_list:
            self.assertEqual(kwargs["format"], "json")
            self.assertEqual(kwargs["include"], "bib,citation,bibtex")
            self.assertEqual(kwargs["style"], "apa")
            self.assertEqual(kwargs["locale"], "en-GB")
            self.assertNotIn("content", kwargs)

    def test_get_bibtex_and_citation_for_items_fetches_multiple_exports_concurrently(self):
        barrier = threading.Barrier(2)

        def fetch_side_effect(item_key, *, style, locale):
            self.assertEqual(style, "apa")
            self.assertEqual(locale, "en-GB")
            barrier.wait(timeout=1)
            return {
                "citation": f"<span>{item_key} cite</span>",
                "bibliography": f"<div>{item_key} ref</div>",
                "bibtex": f"@article{{{item_key},\n  abstract={{A long abstract}}\n}}",
            }

        with patch("zoty.db._fetch_item_exports", side_effect=fetch_side_effect):
            result = json.loads(
                db.get_bibtex_and_citation_for_items(
                    item_keys=["good1", "good2"],
                    style="apa",
                    locale="en-GB",
                )
            )

        self.assertEqual(result["total"], 2)
        self.assertNotIn("errors", result)
        for item in result["items"]:
            self.assertNotIn("abstract", item["bibtex"].lower())

    def test_get_bibtex_and_citation_for_items_requires_at_least_one_key(self):
        result = json.loads(db.get_bibtex_and_citation_for_items())

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
