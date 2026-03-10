import unittest
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


if __name__ == "__main__":
    unittest.main()
