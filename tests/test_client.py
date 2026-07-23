"""
Tests for UnityCatalogClient — pagination handling and the
fetch-then-append comment behavior used by `write-back`.
"""

from unittest.mock import patch

from odgs_databricks.client import UnityCatalogClient


def make_client():
    return UnityCatalogClient(workspace_url="https://example.databricks.com", token="test-token")


class TestPagination:
    def test_list_catalogs_follows_next_page_token(self):
        """Regression test: list endpoints never read next_page_token, so a
        workspace with more results than fit on one API page silently
        truncated with no warning."""
        client = make_client()
        pages = [
            {"catalogs": [{"name": "prod"}], "next_page_token": "tok-2"},
            {"catalogs": [{"name": "staging"}]},
        ]
        with patch.object(client, "_get", side_effect=pages) as mock_get:
            catalogs = client.list_catalogs()

        assert [c["name"] for c in catalogs] == ["prod", "staging"]
        assert mock_get.call_count == 2
        # second call must forward the page token
        _, kwargs = mock_get.call_args_list[1]
        assert kwargs["params"]["page_token"] == "tok-2"

    def test_list_tables_single_page_no_token(self):
        """A single-page response (no next_page_token) should not loop forever."""
        client = make_client()
        with patch.object(client, "_get", return_value={"tables": [{"full_name": "a.b.c", "name": "c"}]}) as mock_get:
            tables = client.list_tables("a", "b")

        assert len(tables) == 1
        assert mock_get.call_count == 1


class TestAppendOdgsComment:
    def test_appends_without_destroying_existing_comment(self):
        """Regression test: write-back used to fully replace the table comment,
        destroying any pre-existing human-authored description."""
        client = make_client()
        with patch.object(client, "get_table", return_value={"comment": "Human-authored description."}) as mock_get, \
             patch.object(client, "update_table_comment") as mock_update:
            client.append_odgs_comment("a.b.c", "**ODGS Enforcement: APPROVED**")

        mock_get.assert_called_once_with("a.b.c")
        new_comment = mock_update.call_args[0][1]
        assert "Human-authored description." in new_comment
        assert "**ODGS Enforcement: APPROVED**" in new_comment

    def test_repeated_writeback_does_not_grow_comment_unboundedly(self):
        """A second write-back run should replace the previous ODGS block,
        not stack a new one on top of it each time."""
        client = make_client()
        first_pass_comment = (
            f"Human-authored description.\n\n{UnityCatalogClient.ODGS_COMMENT_MARKER}\n"
            f"**ODGS Enforcement: BLOCKED**"
        )
        with patch.object(client, "get_table", return_value={"comment": first_pass_comment}), \
             patch.object(client, "update_table_comment") as mock_update:
            client.append_odgs_comment("a.b.c", "**ODGS Enforcement: APPROVED**")

        new_comment = mock_update.call_args[0][1]
        assert new_comment.count(UnityCatalogClient.ODGS_COMMENT_MARKER) == 1
        assert "Human-authored description." in new_comment
        assert "**ODGS Enforcement: APPROVED**" in new_comment
        assert "BLOCKED" not in new_comment

    def test_no_existing_comment(self):
        client = make_client()
        with patch.object(client, "get_table", return_value={"comment": None}), \
             patch.object(client, "update_table_comment") as mock_update:
            client.append_odgs_comment("a.b.c", "**ODGS Enforcement: APPROVED**")

        new_comment = mock_update.call_args[0][1]
        assert new_comment.startswith(UnityCatalogClient.ODGS_COMMENT_MARKER)
