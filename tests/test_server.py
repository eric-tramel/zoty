import asyncio
import unittest
from unittest.mock import patch

import zoty.server as server


def _get_registered_tool(name: str):
    async def get_tool():
        tools = await server.mcp_server.list_tools()
        for tool in tools:
            if tool.name == name:
                return tool
        raise AssertionError(f"{name} tool was not registered")

    return asyncio.run(get_tool())


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
        with (
            patch.object(server.db, "prepare_search_index") as prepare_mock,
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

        prepare_mock.assert_called_once_with()
        run_mock.assert_called_once_with(transport="streamable-http")
        self.assertEqual(server.mcp_server.settings.host, "127.0.0.1")
        self.assertEqual(server.mcp_server.settings.port, 8765)
        self.assertEqual(server.mcp_server.settings.streamable_http_path, "/shared-mcp")

    def test_main_applies_sse_paths(self):
        with (
            patch.object(server.db, "prepare_search_index") as prepare_mock,
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

        prepare_mock.assert_called_once_with()
        run_mock.assert_called_once_with(transport="sse")
        self.assertEqual(server.mcp_server.settings.sse_path, "/events")
        self.assertEqual(server.mcp_server.settings.message_path, "/messages")


class ServerToolTests(unittest.TestCase):
    def test_search_tool_docstrings_are_distinct(self):
        self.assertEqual(
            server.search_library.__doc__.splitlines()[0],
            "Find which items in your Zotero library match a keyword query.",
        )
        self.assertEqual(
            server.search_within_item.__doc__.splitlines()[0],
            "Find which passages within one or more known items match a keyword query.",
        )

    def test_list_and_recent_tool_docstrings_describe_fields_and_caps(self):
        list_doc = " ".join(server.list_collection_items.__doc__.split())
        recent_doc = " ".join(server.get_recent_items.__doc__.split())
        self.assertIn("default: 50, capped at 25", list_doc)
        self.assertIn("`limit=0` returns an empty result set", list_doc)
        self.assertIn("`requested_limit`, `applied_limit`, `limit_cap`, and `limit_capped`", list_doc)
        self.assertIn("truncated `abstract` (500 chars)", list_doc)
        self.assertIn("default: 10, capped at 25", recent_doc)
        self.assertIn("`limit=0` returns an empty result set", recent_doc)
        self.assertIn("metadata (`requested_limit`, `applied_limit`, `limit_cap`, `limit_capped`)", recent_doc)
        self.assertIn("`date_added`", recent_doc)
        self.assertIn("truncated `abstract` (500 chars)", recent_doc)

    def test_search_library_delegates_to_db(self):
        with patch.object(server.db, "search", return_value='{"items": []}') as db_mock:
            result = server.search_library(
                query="transformer attention",
                collection_key="COLL123",
                item_type="preprint",
                limit=5,
                include_attachments=True,
            )

        self.assertEqual(result, '{"items": []}')
        db_mock.assert_called_once_with(
            "transformer attention",
            collection_key="COLL123",
            item_type="preprint",
            limit=5,
            include_attachments=True,
        )

    def test_search_library_docstring_mentions_snippet_and_abstract_behavior(self):
        doc = server.search_library.__doc__ or ""
        normalized_doc = " ".join(doc.split())

        self.assertIn("abstract text truncated to 500 characters", doc)
        self.assertIn("include_attachments", doc)
        self.assertIn("invalid `collection_key` / `item_type` filters", doc)
        self.assertIn(
            "values not present in the current search index return no items plus a warning",
            normalized_doc,
        )
        self.assertIn("default: 10, capped at 25", normalized_doc)

    def test_search_within_item_delegates_to_db(self):
        with patch.object(server.db, "search_within_item", return_value='{"matches": []}') as db_mock:
            result = server.search_within_item(
                item_key="ITEM123",
                item_keys=["ITEM456"],
                query="transformer attention",
                limit=5,
            )

        self.assertEqual(result, '{"matches": []}')
        db_mock.assert_called_once_with(
            item_key="ITEM123",
            item_keys=["ITEM456"],
            query="transformer attention",
            limit=5,
        )

    def test_search_within_item_tool_description_mentions_attachment_chunk_fields(self):
        description = _get_registered_tool("search_within_item").description

        self.assertIn("attachment_key", description)
        self.assertIn("attachment_title", description)
        self.assertIn("attachment_filepath", description)
        self.assertIn("chunk_index", description)
        self.assertIn("char_start", description)
        self.assertIn("char_end", description)
        self.assertIn("returned_match_count", description)
        self.assertIn("top_score", description)
        self.assertIn("top_match_type", description)
        self.assertIn("requested_limit", description)
        self.assertIn("applied_limit", description)

    def test_response_shape_docstrings_reflect_canonical_keys(self):
        search_within_doc = " ".join(server.search_within_item.__doc__.split())
        self.assertIn("under `items`", server.search_library.__doc__)
        self.assertIn("`matches`", server.search_within_item.__doc__)
        self.assertIn("attachment_key", server.search_within_item.__doc__)
        self.assertIn("`returned_match_count`", server.search_within_item.__doc__)
        self.assertIn("`top_score`", server.search_within_item.__doc__)
        self.assertIn("`top_match_type`", server.search_within_item.__doc__)
        self.assertIn("`requested_limit`", server.search_within_item.__doc__)
        self.assertIn("include parent `key` only for multi-item calls", search_within_doc)
        self.assertIn("attachment_count", server.list_collection_items.__doc__)
        self.assertIn("attachment_count", server.get_recent_items.__doc__)
        self.assertIn("date_added", server.get_recent_items.__doc__)
        self.assertIn("Single-key requests return JSON", server.get_item.__doc__)
        self.assertIn("`item_keys`, `items`, `requested`, `total`", server.get_item.__doc__)

    def test_get_item_delegates_to_db(self):
        with patch.object(server.db, "get_item", return_value='{"key": "ITEM123"}') as db_mock:
            result = server.get_item(item_key="ITEM123", item_keys=["ITEM456"])

        self.assertEqual(result, '{"key": "ITEM123"}')
        db_mock.assert_called_once_with(item_key="ITEM123", item_keys=["ITEM456"])

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

    def test_get_bibtex_tool_description_mentions_required_key_inputs(self):
        description = _get_registered_tool("get_bibtex_and_citation_for_items").description

        self.assertIn("Provide at least one of `item_key` or `item_keys`.", description)

    def test_get_bibtex_tool_schema_requires_item_key_or_item_keys(self):
        schema = _get_registered_tool("get_bibtex_and_citation_for_items").inputSchema

        # anyOf is not allowed at the top level by the Claude API
        self.assertNotIn("anyOf", schema)
        self.assertEqual(
            schema["properties"]["item_key"]["description"],
            "A single Zotero item key. At least one of `item_key` or `item_keys` must be provided.",
        )
        self.assertIsNone(schema["properties"]["item_key"]["default"])
        self.assertEqual(
            schema["properties"]["item_keys"]["description"],
            "A list of Zotero item keys for batch export. At least one of `item_key` or `item_keys` must be provided.",
        )

    def test_search_library_tool_description_mentions_item_type_values_and_warning_behavior(self):
        description = _get_registered_tool("search_library").description

        self.assertIn("Canonical `item_type` values:", description)
        self.assertIn("artwork", description)
        self.assertIn("journalArticle", description)
        self.assertIn("webpage", description)
        self.assertIn("returns no items and a warning", description)

    def test_limit_parameter_schemas_describe_clamping_and_metadata(self):
        search_schema = _get_registered_tool("search_library").inputSchema
        list_schema = _get_registered_tool("list_collection_items").inputSchema
        recent_schema = _get_registered_tool("get_recent_items").inputSchema

        self.assertIn("Values below 0 are treated as 0, values above 25 are clamped to 25", search_schema["properties"]["limit"]["description"])
        self.assertIn("`requested_limit`, `applied_limit`, `limit_cap`, and `limit_capped`", search_schema["properties"]["limit"]["description"])
        self.assertIn("Values below 0 are treated as 0, values above 25 are clamped to 25", list_schema["properties"]["limit"]["description"])
        self.assertIn("`requested_limit`, `applied_limit`, `limit_cap`, and `limit_capped`", list_schema["properties"]["limit"]["description"])
        self.assertIn("Values below 0 are treated as 0, values above 25 are clamped to 25", recent_schema["properties"]["limit"]["description"])
        self.assertIn("`requested_limit`, `applied_limit`, `limit_cap`, and `limit_capped`", recent_schema["properties"]["limit"]["description"])

    def test_get_item_tool_description_mentions_batch_inputs_and_response_shape(self):
        description = _get_registered_tool("get_item").description
        normalized_description = " ".join(description.split())

        self.assertIn("`item_key` and `item_keys` can be combined", normalized_description)
        self.assertIn("Single-key requests keep the legacy single-item response shape.", normalized_description)
        self.assertIn("`item_keys`, `items`, `requested`, `total`", normalized_description)

    def test_add_paper_tool_description_mentions_required_inputs_and_precedence(self):
        description = _get_registered_tool("add_paper").description

        self.assertIn("Provide at least one of `arxiv_id` or `doi`.", description)
        self.assertIn("If both are provided,", description)
        self.assertIn("`arxiv_id` takes precedence.", description)
