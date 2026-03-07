"""
ODGS Databricks Bridge
======================

Transforms Databricks Unity Catalog metadata into ODGS JSON schemas
for runtime enforcement via the Universal Interceptor.

Usage:
    from odgs_databricks import DatabricksBridge

    bridge = DatabricksBridge(
        workspace_url="https://adb-1234.azuredatabricks.net",
        token="dapi...",
        organization="acme_corp",
    )
    bridge.sync(catalog="production", output_dir="./schemas/custom/")
"""

__version__ = "0.1.0"

from odgs_databricks.bridge import DatabricksBridge
from odgs_databricks.client import UnityCatalogClient
from odgs_databricks.transformer import DatabricksTransformer

__all__ = ["DatabricksBridge", "UnityCatalogClient", "DatabricksTransformer"]
