import unittest
from unittest.mock import Mock, patch

import zoty.server as server


class ServerMainTests(unittest.TestCase):
    def setUp(self):
        self.original_host = server.mcp_server.settings.host
        self.original_port = server.mcp_server.settings.port
        self.original_streamable_http_path = server.mcp_server.settings.streamable_http_path
        self.original_sse_path = server.mcp_server.settings.sse_path
        self.original_message_path = server.mcp_server.settings.message_path

    def tearDown(self):
        server.mcp_server.settings.host = self.original_host
        server.mcp_server.settings.port = self.original_port
        server.mcp_server.settings.streamable_http_path = self.original_streamable_http_path
        server.mcp_server.settings.sse_path = self.original_sse_path
        server.mcp_server.settings.message_path = self.original_message_path

    def test_main_applies_http_server_flags(self):
        thread_mock = Mock()

        with (
            patch("zoty.server.threading.Thread", return_value=thread_mock) as thread_ctor,
            patch.object(server.mcp_server, "run") as run_mock,
        ):
            server.main(
                [
                    "--transport",
                    "streamable-http",
                    "--host",
                    "127.0.0.1",
                    "--port",
                    "8765",
                    "--streamable-http-path",
                    "/shared-mcp",
                ]
            )

        thread_ctor.assert_called_once_with(target=server.db.build_index_background, daemon=True)
        thread_mock.start.assert_called_once_with()
        run_mock.assert_called_once_with(transport="streamable-http")
        self.assertEqual(server.mcp_server.settings.host, "127.0.0.1")
        self.assertEqual(server.mcp_server.settings.port, 8765)
        self.assertEqual(server.mcp_server.settings.streamable_http_path, "/shared-mcp")

    def test_main_applies_sse_paths(self):
        thread_mock = Mock()

        with (
            patch("zoty.server.threading.Thread", return_value=thread_mock),
            patch.object(server.mcp_server, "run") as run_mock,
        ):
            server.main(
                [
                    "--transport",
                    "sse",
                    "--sse-path",
                    "/events",
                    "--message-path",
                    "/messages",
                ]
            )

        run_mock.assert_called_once_with(transport="sse")
        self.assertEqual(server.mcp_server.settings.sse_path, "/events")
        self.assertEqual(server.mcp_server.settings.message_path, "/messages")


class ServerToolTests(unittest.TestCase):
    def test_get_bibtex_and_citation_for_items_delegates_to_db(self):
        with patch.object(server.db, "get_bibtex_and_citation_for_items", return_value='{"items": []}') as db_mock:
            result = server.get_bibtex_and_citation_for_items(
                item_key="ITEM123",
                item_keys=["ITEM456"],
                style="apa",
                locale="en-GB",
            )

        self.assertEqual(result, '{"items": []}')
        db_mock.assert_called_once_with(
            item_key="ITEM123",
            item_keys=["ITEM456"],
            style="apa",
            locale="en-GB",
        )
