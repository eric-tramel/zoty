import unittest
from unittest.mock import patch

from zoty import fulltext_bridge


class EnsureParentFulltextTests(unittest.TestCase):
    def test_normalizes_parent_keys_and_parses_string_payload(self):
        bridge_response = {
            "ok": True,
            "result": '{"parents":[{"parentKey":"ABC123","attachments":[]}],"totals":{"requestedParents":2}}',
        }

        with patch("zoty.fulltext_bridge.execute_js", return_value=bridge_response) as execute_mock:
            result = fulltext_bridge.ensure_parent_fulltext([" abc123 ", "ABC123", "def456"])

        self.assertEqual(result["parents"][0]["parentKey"], "ABC123")
        self.assertEqual(result["totals"]["requestedParents"], 2)
        sent_js = execute_mock.call_args.args[0]
        self.assertIn('["ABC123", "DEF456"]', sent_js)
        self.assertIn("const complete = false;", sent_js)
        self.assertIn("SELECT indexedPages, totalPages, indexedChars, totalChars FROM fulltextItems", sent_js)
        self.assertNotIn("Zotero.Fulltext.getPages", sent_js)

    def test_accepts_already_parsed_dict_payload(self):
        bridge_response = {
            "ok": True,
            "result": {
                "parents": [{"parentKey": "ABC123", "attachments": [{"attachmentKey": "ATT1"}]}],
                "totals": {"attachmentsIndexed": 1},
            },
        }

        with patch("zoty.fulltext_bridge.execute_js", return_value=bridge_response):
            result = fulltext_bridge.ensure_parent_fulltext(["ABC123"], complete=True)

        self.assertEqual(result["parents"][0]["attachments"][0]["attachmentKey"], "ATT1")
        self.assertEqual(result["totals"]["attachmentsIndexed"], 1)
