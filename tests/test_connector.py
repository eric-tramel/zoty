from contextlib import closing
import json
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from zoty import connector


class FakeClock:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


class ArxivRateLimiterTests(unittest.TestCase):
    def test_serialized_rate_limiter_waits_after_each_call(self):
        clock = FakeClock()
        limiter = connector._SerializedRateLimiter(
            min_interval=3.0,
            clock=clock.monotonic,
            sleep=clock.sleep,
        )
        starts = []

        def record(label):
            starts.append((label, clock.now))
            clock.now += 0.5
            return label

        self.assertEqual(limiter.run(record, "first"), "first")
        self.assertEqual(limiter.run(record, "second"), "second")

        self.assertEqual(starts, [("first", 0.0), ("second", 3.5)])
        self.assertEqual(clock.sleeps, [3.0])

    def test_serialized_rate_limiter_waits_after_errors(self):
        clock = FakeClock()
        limiter = connector._SerializedRateLimiter(
            min_interval=3.0,
            clock=clock.monotonic,
            sleep=clock.sleep,
        )
        starts = []

        def fail():
            starts.append(("fail", clock.now))
            clock.now += 0.2
            raise RuntimeError("boom")

        def succeed():
            starts.append(("success", clock.now))
            clock.now += 0.1
            return "ok"

        with self.assertRaises(RuntimeError):
            limiter.run(fail)

        self.assertEqual(limiter.run(succeed), "ok")

        self.assertEqual(starts, [("fail", 0.0), ("success", 3.2)])
        self.assertEqual(clock.sleeps, [3.0])

    def test_serialized_rate_limiter_releases_lock_during_call(self):
        limiter = connector._SerializedRateLimiter(min_interval=0.0)
        started = threading.Event()
        release = threading.Event()
        done = threading.Event()
        errors = []

        def blocking_call():
            started.set()
            release.wait(timeout=1.0)
            return "ok"

        def worker():
            try:
                limiter.run(blocking_call)
            except Exception as exc:  # pragma: no cover - defensive
                errors.append(exc)
            finally:
                done.set()

        thread = threading.Thread(target=worker)
        thread.start()
        self.assertTrue(started.wait(timeout=1.0))

        # If run() holds _lock during func(), this acquire times out.
        acquired = limiter._lock.acquire(timeout=0.1)
        if acquired:
            limiter._lock.release()

        release.set()
        self.assertTrue(done.wait(timeout=1.0))
        thread.join(timeout=1.0)

        self.assertTrue(acquired)
        self.assertEqual(errors, [])

    def test_sliding_window_rate_limiter_allows_burst_then_waits(self):
        clock = FakeClock()
        limiter = connector._SlidingWindowRateLimiter(
            max_calls=4,
            period=1.0,
            clock=clock.monotonic,
            sleep=clock.sleep,
        )
        starts = []

        for _ in range(5):
            limiter.run(lambda: starts.append(clock.now))

        self.assertEqual(starts, [0.0, 0.0, 0.0, 0.0, 1.0])
        self.assertEqual(clock.sleeps, [1.0])

    def test_fetch_arxiv_metadata_uses_rate_limiter(self):
        feed = """<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <title>Example Title</title>
    <summary>Example abstract.</summary>
    <published>2024-01-02T00:00:00Z</published>
    <author><name>Jane Example</name></author>
    <category term="cs.AI" />
  </entry>
</feed>"""

        with patch.object(connector._ARXIV_METADATA_LIMITER, "run", return_value=feed) as run_mock:
            result = connector._fetch_arxiv_metadata("arxiv:1234.5678v2")

        self.assertEqual(result["title"], "Example Title")
        self.assertEqual(result["archiveID"], "arXiv:1234.5678")
        self.assertEqual(result["creators"][0]["lastName"], "Example")
        run_mock.assert_called_once()

    def test_fetch_arxiv_metadata_rejects_error_entries(self):
        feed = """<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <id>http://arxiv.org/api/errors#id_list=not-a-real-id</id>
    <title>Error</title>
    <summary>Invalid id_list parameter.</summary>
  </entry>
</feed>"""

        with patch.object(connector._ARXIV_METADATA_LIMITER, "run", return_value=feed):
            with self.assertRaisesRegex(ValueError, r"Invalid arXiv ID: not-a-real-id\. No paper found\."):
                connector._fetch_arxiv_metadata("not-a-real-id")

    def test_add_paper_returns_validation_error_for_invalid_arxiv_id(self):
        with patch(
            "zoty.connector._fetch_arxiv_metadata",
            side_effect=ValueError("Invalid arXiv ID: not-a-real-id. No paper found."),
        ):
            result = json.loads(connector.add_paper(arxiv_id="not-a-real-id"))

        self.assertEqual(result["status"], "error")
        self.assertEqual(result["title"], "")
        self.assertEqual(result["creators"], [])
        self.assertEqual(result["date"], "")
        self.assertEqual(result["itemType"], "")
        self.assertEqual(result["DOI"], "")
        self.assertEqual(result["url"], "")
        self.assertEqual(result["abstract"], "")
        self.assertFalse(result["pdf_attached"])
        self.assertFalse(result["collection_added"])
        self.assertEqual(result["collection_key"], "")
        self.assertEqual(result["error"], "Invalid arXiv ID: not-a-real-id. No paper found.")

    @patch("zoty.connector._retrieve_url", autospec=True)
    def test_download_with_rate_limit_uses_pdf_limiter_for_arxiv_urls(self, retrieve_mock):
        with patch.object(
            connector._ARXIV_PDF_LIMITER,
            "run",
            side_effect=lambda func, *args, **kwargs: func(*args, **kwargs),
        ) as run_mock:
            connector._download_with_rate_limit(
                "https://arxiv.org/pdf/1234.5678",
                "/tmp/example.pdf",
            )

        run_mock.assert_called_once()
        retrieve_mock.assert_called_once_with(
            "https://arxiv.org/pdf/1234.5678",
            "/tmp/example.pdf",
        )

    @patch("zoty.connector._retrieve_url", autospec=True)
    def test_download_with_rate_limit_bypasses_pdf_limiter_for_other_hosts(self, retrieve_mock):
        with patch.object(connector._ARXIV_PDF_LIMITER, "run") as run_mock:
            connector._download_with_rate_limit(
                "https://example.org/paper.pdf",
                "/tmp/example.pdf",
            )

        run_mock.assert_not_called()
        retrieve_mock.assert_called_once_with(
            "https://example.org/paper.pdf",
            "/tmp/example.pdf",
        )


class DownloadPdfSecurityTests(unittest.TestCase):
    def test_download_pdf_uses_mkstemp_and_closes_fd(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            temp_pdf = tmpdir_path / "download.pdf"

            def fake_download(_url, path):
                Path(path).write_bytes(b"x" * 2000)

            with (
                patch("zoty.connector.tempfile.mkstemp", return_value=(17, str(temp_pdf))) as mkstemp_mock,
                patch("zoty.connector.tempfile.mktemp", side_effect=AssertionError("mktemp should not be used")),
                patch("zoty.connector.os.close") as close_mock,
                patch("zoty.connector._download_with_rate_limit", side_effect=fake_download),
                patch("zoty.connector._zotero_key", return_value="ATTACH1"),
                patch("zoty.connector._ZOTERO_STORAGE", tmpdir_path),
            ):
                result = connector._download_pdf("https://example.org/paper.pdf", "paper.pdf")
                self.assertEqual(result[0], "ATTACH1")
                self.assertEqual(result[2], 2000)
                self.assertEqual(result[1], tmpdir_path / "ATTACH1" / "paper.pdf")
                self.assertTrue(result[1].exists())
                mkstemp_mock.assert_called_once_with(suffix=".pdf")
                close_mock.assert_called_once_with(17)


class CollectionAssignmentRetryTests(unittest.TestCase):
    @patch("zoty.connector.time.sleep", autospec=True)
    def test_retries_bridge_http_400(self, sleep_mock):
        error = connector.BridgeError("Bridge request failed: HTTP Error 400: Bad Request")
        with patch(
            "zoty.connector._add_to_collection_via_rdp",
            side_effect=[error, {"status": "added"}],
        ) as add_mock:
            result = connector._add_to_collection_with_retry("ITEM123", "COLL123")

        self.assertEqual(result, {"status": "added"})
        self.assertEqual(add_mock.call_count, 2)
        sleep_mock.assert_called_once_with(connector._COLLECTION_ASSIGN_RETRY_DELAY)

    @patch("zoty.connector.time.sleep", autospec=True)
    def test_retries_not_found_response(self, sleep_mock):
        with patch(
            "zoty.connector._add_to_collection_via_rdp",
            side_effect=[
                {"error": "not found", "itemKey": "ITEM123", "collectionKey": "COLL123"},
                {"status": "added"},
            ],
        ) as add_mock:
            result = connector._add_to_collection_with_retry("ITEM123", "COLL123")

        self.assertEqual(result, {"status": "added"})
        self.assertEqual(add_mock.call_count, 2)
        sleep_mock.assert_called_once_with(connector._COLLECTION_ASSIGN_RETRY_DELAY)

    @patch("zoty.connector.time.sleep", autospec=True)
    def test_does_not_retry_bridge_unavailable(self, sleep_mock):
        error = connector.BridgeError(
            "Cannot connect to zoty-bridge at 127.0.0.1:24119. "
            "Is Zotero running with the Zoty Bridge plugin?"
        )
        with patch("zoty.connector._add_to_collection_via_rdp", side_effect=error) as add_mock:
            with self.assertRaises(connector.BridgeError):
                connector._add_to_collection_with_retry("ITEM123", "COLL123")

        self.assertEqual(add_mock.call_count, 1)
        sleep_mock.assert_not_called()


class BridgeKeySanitizationTests(unittest.TestCase):
    @patch("zoty.connector.execute_js", autospec=True)
    def test_add_to_collection_via_rdp_normalizes_and_escapes_valid_keys(self, execute_mock):
        execute_mock.return_value = {"ok": True, "result": json.dumps({"status": "added"})}

        result = connector._add_to_collection_via_rdp(" item123 ", "coll456")
        code = execute_mock.call_args.args[0]

        self.assertEqual(result, {"status": "added"})
        self.assertIn(f"const itemKey = {json.dumps('ITEM123')};", code)
        self.assertIn(f"const collectionKey = {json.dumps('COLL456')};", code)

    @patch("zoty.connector.execute_js", autospec=True)
    def test_attach_pdf_via_rdp_escapes_malicious_parent_key(self, execute_mock):
        execute_mock.return_value = {
            "ok": True,
            "result": json.dumps({"status": "attached", "attachmentID": 7, "key": "ATTACH1"}),
        }

        parent_key = 'parent123";\nalert(1);//'
        pdf_path = "/tmp/evil's.pdf"

        result = connector._attach_pdf_via_rdp(parent_key, pdf_path)
        code = execute_mock.call_args.args[0]

        self.assertEqual(result, {"status": "attached", "attachmentID": 7, "key": "ATTACH1"})
        self.assertIn(f"const parentKey = {json.dumps(parent_key.strip().upper())};", code)
        self.assertIn(f"file: {json.dumps(pdf_path)}", code)

    def test_add_to_collection_via_rdp_rejects_blank_keys(self):
        with self.assertRaises(ValueError):
            connector._add_to_collection_via_rdp("ITEM123", "   ")


class CollectionDuplicateDetectionTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_db = connector._ZOTERO_DB
        connector._ZOTERO_DB = Path(self.temp_dir.name) / "zotero.sqlite"

        with closing(sqlite3.connect(connector._ZOTERO_DB)) as db:
            db.executescript(
                """
                CREATE TABLE items (
                    itemID INTEGER PRIMARY KEY,
                    key TEXT NOT NULL,
                    dateAdded TEXT NOT NULL
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
                CREATE TABLE collections (
                    collectionID INTEGER PRIMARY KEY,
                    key TEXT NOT NULL
                );
                CREATE TABLE collectionItems (
                    collectionID INTEGER NOT NULL,
                    itemID INTEGER NOT NULL
                );
                """
            )
            db.executemany(
                "INSERT INTO fields(fieldID, fieldName) VALUES (?, ?)",
                [
                    (1, "title"),
                    (59, "DOI"),
                    (97, "archiveID"),
                ],
            )
            db.execute("INSERT INTO collections(collectionID, key) VALUES (?, ?)", (1, "COLL123"))
            db.commit()

    def tearDown(self):
        connector._ZOTERO_DB = self.original_db
        self.temp_dir.cleanup()

    def _insert_item(
        self,
        *,
        item_id: int,
        key: str,
        title: str,
        date_added: str,
        archive_id: str = "",
        doi: str = "",
        collection_id: int = 1,
    ) -> None:
        with closing(sqlite3.connect(connector._ZOTERO_DB)) as db:
            db.execute(
                "INSERT INTO items(itemID, key, dateAdded) VALUES (?, ?, ?)",
                (item_id, key, date_added),
            )
            db.execute(
                "INSERT INTO collectionItems(collectionID, itemID) VALUES (?, ?)",
                (collection_id, item_id),
            )

            values = [(item_id * 10 + 1, title)]
            if archive_id:
                values.append((item_id * 10 + 2, archive_id))
            if doi:
                values.append((item_id * 10 + 3, doi))

            db.executemany(
                "INSERT INTO itemDataValues(valueID, value) VALUES (?, ?)",
                values,
            )
            db.execute(
                "INSERT INTO itemData(itemID, fieldID, valueID) VALUES (?, ?, ?)",
                (item_id, 1, item_id * 10 + 1),
            )
            if archive_id:
                db.execute(
                    "INSERT INTO itemData(itemID, fieldID, valueID) VALUES (?, ?, ?)",
                    (item_id, 97, item_id * 10 + 2),
                )
            if doi:
                db.execute(
                    "INSERT INTO itemData(itemID, fieldID, valueID) VALUES (?, ?, ?)",
                    (item_id, 59, item_id * 10 + 3),
                )
            db.commit()

    def test_finds_existing_item_by_title_within_collection(self):
        self._insert_item(
            item_id=1,
            key="ITEM123",
            title="Example Title",
            date_added="2026-03-10 10:00:00",
        )

        result = connector._find_existing_item_in_collection(
            "Example Title",
            "COLL123",
        )

        self.assertEqual(result, ("ITEM123", "Example Title"))

    def test_finds_existing_item_by_normalized_arxiv_id_within_collection(self):
        self._insert_item(
            item_id=2,
            key="ITEM456",
            title="Different Stored Title",
            date_added="2026-03-10 11:00:00",
            archive_id="arXiv:1234.5678",
        )

        result = connector._find_existing_item_in_collection(
            "Fresh Metadata Title",
            "COLL123",
            arxiv_id="arxiv:1234.5678v2",
        )

        self.assertEqual(result, ("ITEM456", "Different Stored Title"))

    def test_add_paper_short_circuits_before_arxiv_fetch_for_existing_collection_item(self):
        self._insert_item(
            item_id=3,
            key="ITEMFAST",
            title="Stored Title",
            date_added="2026-03-10 12:00:00",
            archive_id="arXiv:1234.5678",
        )

        with (
            patch("zoty.connector._fetch_arxiv_metadata") as fetch_mock,
            patch("zoty.connector._push_to_connector") as push_mock,
        ):
            result = json.loads(
                connector.add_paper(arxiv_id="arxiv:1234.5678v2", collection_key="COLL123")
            )

        self.assertEqual(result["status"], "already in collection")
        self.assertEqual(result["title"], "Stored Title")
        self.assertEqual(result["key"], "ITEMFAST")
        self.assertEqual(result["collection_key"], "COLL123")
        fetch_mock.assert_not_called()
        push_mock.assert_not_called()

    def test_add_paper_short_circuits_before_doi_fetch_for_existing_collection_item(self):
        self._insert_item(
            item_id=4,
            key="ITEMDOI",
            title="DOI Title",
            date_added="2026-03-10 12:30:00",
            doi="10.1000/example",
        )

        with (
            patch("zoty.connector._fetch_crossref_metadata") as fetch_mock,
            patch("zoty.connector._push_to_connector") as push_mock,
        ):
            result = json.loads(
                connector.add_paper(doi="doi:10.1000/example", collection_key="COLL123")
            )

        self.assertEqual(result["status"], "already in collection")
        self.assertEqual(result["title"], "DOI Title")
        self.assertEqual(result["key"], "ITEMDOI")
        self.assertEqual(result["collection_key"], "COLL123")
        fetch_mock.assert_not_called()
        push_mock.assert_not_called()

    def test_add_paper_returns_already_in_collection_after_metadata_lookup_without_creating_duplicate(self):
        item = {
            "title": "Example Title",
            "creators": [{"firstName": "Jane", "lastName": "Example"}],
            "date": "2026-03-10",
            "itemType": "preprint",
            "archiveID": "arXiv:1234.5678",
            "url": "https://arxiv.org/abs/1234.5678",
        }

        with (
            patch("zoty.connector._fetch_arxiv_metadata", return_value=item),
            patch(
                "zoty.connector._find_existing_item_in_collection",
                side_effect=[("", ""), ("ITEM789", "Example Title")],
            ),
            patch("zoty.connector._push_to_connector") as push_mock,
        ):
            result = json.loads(
                connector.add_paper(arxiv_id="1234.5678", collection_key="COLL123")
            )

        self.assertEqual(result["status"], "already in collection")
        self.assertEqual(result["title"], "Example Title")
        self.assertEqual(result["key"], "ITEM789")
        self.assertEqual(result["collection_key"], "COLL123")
        push_mock.assert_not_called()


class AddPaperRefreshSchedulingTests(unittest.TestCase):
    def test_add_paper_schedules_background_fulltext_refresh_after_pdf_attach(self):
        item = {
            "title": "Example Title",
            "creators": [{"firstName": "Jane", "lastName": "Example"}],
            "date": "2026-03-10",
            "itemType": "preprint",
            "abstractNote": "Example abstract.",
            "url": "https://arxiv.org/abs/1234.5678",
            "_pdf_url": "https://arxiv.org/pdf/1234.5678",
        }

        with (
            patch("zoty.connector._find_existing_item_in_collection", side_effect=[("", ""), ("", "")]),
            patch("zoty.connector._fetch_arxiv_metadata", return_value=item),
            patch("zoty.connector._push_to_connector"),
            patch("zoty.connector._find_parent_key_by_title", return_value="PARENT1"),
            patch("zoty.connector._download_pdf", return_value=("ATTACH1", Path("/tmp/fake.pdf"), 4096)),
            patch("zoty.connector._attach_pdf_via_rdp", return_value={"status": "attached"}),
            patch("zoty.connector._add_to_collection_with_retry", return_value={"status": "added"}),
            patch("zoty.db.schedule_parent_fulltext_refresh") as schedule_mock,
        ):
            result = json.loads(connector.add_paper(arxiv_id="1234.5678", collection_key="COLL123"))

        self.assertEqual(result["status"], "created")
        self.assertTrue(result["pdf_attached"])
        self.assertTrue(result["collection_added"])
        schedule_mock.assert_called_once_with(["PARENT1"])


if __name__ == "__main__":
    unittest.main()
