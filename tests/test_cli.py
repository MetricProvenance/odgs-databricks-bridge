import os
import tempfile
import json
import pytest
from unittest.mock import patch
from typer.testing import CliRunner
from odgs_databricks.cli import app

runner = CliRunner()

def test_write_back_cli():
    # Create a mock audit log
    log_content = [
        "2026-03-11 09:50:40 - " + json.dumps({
            "event_id": "test-123",
            "execution_result": "BLOCKED",
            "applied_metadata": {"rule_1": {"databricks_table": "my_catalog.my_schema.my_table"}},
            "tri_partite_binding": {"payload_hash": "hash-abc"}
        })
    ]
    
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
        f.write("\n".join(log_content))
        log_path = f.name
        
    try:
        with patch("odgs_databricks.client.UnityCatalogClient") as MockClient:
            mock_instance = MockClient.return_value
            
            result = runner.invoke(app, [
                "write-back",
                "--log-path", log_path,
                "--url", "https://mock.databricks.com",
                "--token", "mock-token"
            ])
            
            assert result.exit_code == 0
            assert "Bi-Directional Sync Complete" in result.stdout
            assert mock_instance.update_table_comment.call_count == 1
    finally:
        os.remove(log_path)
