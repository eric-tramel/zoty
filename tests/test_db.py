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
    def __init__(self, rows):
        self._rows = rows

    def retrieve(self, query_tokens, corpus=None, k=10, show_progress=False):
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

        db._ZOTERO_DB = Path(self.temp_dir.name) / "zotero.sqlite"
        db._ZOTERO_STORAGE = Path(self.temp_dir.name) / "storage"
        db._SIDECAR_ROOT = Path(self.temp_dir.name) / "sidecar"
        db._ZOTERO_STORAGE.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        db._ZOTERO_DB = self.original_db
        db._ZOTERO_STORAGE = self.original_storage
        db._SIDECAR_ROOT = self.original_sidecar_root
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
        self.assertEqual(result["results"][0]["key"], "PARENT1")
        self.assertEqual(
            result["results"][0]["attachments"][0]["filepath"],
            str(db._ZOTERO_STORAGE / "ATTACH1" / "paper.pdf"),
        )
        self.assertEqual(result["results"][0]["snippet_attachment_key"], "ATTACH1")


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
        self.assertEqual(result["results"], [])

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
        self.assertEqual(result["results"][0]["key"], "PARENT1")
        self.assertIn("meatpotatoes", result["results"][0]["snippet"].lower())
        self.assertEqual(result["results"][0]["snippet_attachment_key"], "ATTACH1")

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
        self.assertEqual(result["results"][0]["key"], "PARENT1")
        self.assertIn("example abstract", result["results"][0]["snippet"].lower())
        self.assertNotIn("snippet_attachment_key", result["results"][0])

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
        self.assertEqual([row["key"] for row in result["results"]], ["PARENT1", "PARENT2"])
        self.assertEqual(result["results"][0]["score"], 9.0)

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
        self.assertEqual(result["results"][0]["key"], "PARENT1")

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

        self.assertEqual(result["item_key"], "PARENT1")
        self.assertEqual(result["item"]["key"], "PARENT1")
        self.assertEqual(result["total"], 3)
        self.assertEqual(
            [row["match_type"] for row in result["results"]],
            ["attachment_chunk", "attachment_chunk", "metadata"],
        )
        self.assertEqual(result["results"][0]["attachment_key"], "ATTACH1")
        self.assertEqual(result["results"][1]["attachment_key"], "ATTACH2")
        self.assertNotIn("attachment_key", result["results"][2])

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

        self.assertEqual(result["item_key"], "MISSING")
        self.assertEqual(result["results"], [])
        self.assertIn("was not found", result["error"])


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
                    "bibtex": "@article{ITEM123,\n  title={Example}\n}",
                }
            ],
        )

    def test_get_bibtex_and_citation_for_items_returns_multiple_items_and_partial_errors(self):
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
