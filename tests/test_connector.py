import unittest
from unittest.mock import patch

from zoty import connector


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
