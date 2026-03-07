"""
ODGS Databricks Bridge — Orchestrator
=======================================

High-level interface for syncing Unity Catalog table metadata
into ODGS schemas on the local filesystem.
"""

import json
import os
import logging
from typing import Optional

from odgs_databricks.client import UnityCatalogClient
from odgs_databricks.transformer import DatabricksTransformer

logger = logging.getLogger("odgs_databricks.bridge")


class DatabricksBridge:
    """
    High-level orchestrator that syncs Databricks Unity Catalog
    metadata into ODGS-compliant JSON schema files.

    Usage:
        bridge = DatabricksBridge(
            workspace_url="https://adb-1234.azuredatabricks.net",
            token="dapi...",
            organization="acme_corp",
        )
        bridge.sync(catalog="production", output_dir="./schemas/custom/")
    """

    def __init__(
        self,
        workspace_url: str,
        token: str,
        organization: str,
    ):
        self.client = UnityCatalogClient(
            workspace_url=workspace_url,
            token=token,
        )
        self.transformer = DatabricksTransformer(organization=organization)

    def sync(
        self,
        catalog: str,
        schema_filter: Optional[str] = None,
        output_dir: str = "./schemas/custom/",
        output_type: str = "metrics",
        severity: str = "WARNING",
    ) -> str:
        """
        Sync Unity Catalog tables to ODGS JSON schemas on disk.

        Args:
            catalog: Databricks catalog name to scan
            schema_filter: Optional schema name to limit scope
            output_dir: Directory to write ODGS JSON files
            output_type: "metrics" or "rules"
            severity: Rule severity level (HARD_STOP, WARNING, INFO)

        Returns:
            Absolute path to the generated schema file.
        """
        # Fetch all tables from the catalog
        tables = self.client.get_all_tables(
            catalog_name=catalog,
            schema_filter=schema_filter,
        )

        if not tables:
            logger.warning(
                f"No tables found in catalog '{catalog}'"
                + (f" schema '{schema_filter}'" if schema_filter else "")
            )
            return ""

        logger.info(
            f"Transforming {len(tables)} Unity Catalog tables → ODGS {output_type}"
        )

        # Transform to ODGS schema
        schema = self.transformer.transform_tables(
            tables=tables,
            output_type=output_type,
            severity=severity,
        )

        # Write to disk
        os.makedirs(output_dir, exist_ok=True)
        catalog_id = catalog.replace(".", "_").lower()
        filename = f"databricks_{self.transformer.organization}_{catalog_id}_{output_type}.json"
        filepath = os.path.join(output_dir, filename)

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(schema, f, indent=2, ensure_ascii=False)

        abs_path = os.path.abspath(filepath)
        logger.info(
            f"✅ Written ODGS schema: {abs_path} "
            f"({len(tables)} tables → {schema['metadata']['items_generated']} {output_type})"
        )
        return abs_path
