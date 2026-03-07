"""
Databricks Unity Catalog REST API Client
==========================================

Lightweight client for reading catalog, schema, table, and column
metadata from Databricks Unity Catalog.

Reference: https://docs.databricks.com/api/workspace/catalogs
"""

import logging
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field

import requests

logger = logging.getLogger("odgs_databricks.client")


@dataclass
class CatalogColumn:
    """Represents a column in a Unity Catalog table."""
    name: str
    data_type: str
    nullable: bool = True
    comment: str = ""
    partition_index: Optional[int] = None


@dataclass
class CatalogTable:
    """Represents a table or view in Unity Catalog."""
    full_name: str  # catalog.schema.table
    catalog_name: str
    schema_name: str
    table_name: str
    table_type: str  # MANAGED, EXTERNAL, VIEW
    data_source_format: str = ""
    comment: str = ""
    owner: str = ""
    columns: List[CatalogColumn] = field(default_factory=list)
    properties: Dict[str, str] = field(default_factory=dict)


class UnityCatalogClient:
    """
    REST API client for Databricks Unity Catalog.

    Reads catalogs, schemas, tables, and column metadata
    for transformation into ODGS enforcement schemas.

    Args:
        workspace_url: Databricks workspace URL
            (e.g., https://adb-1234567890.azuredatabricks.net)
        token: Personal access token or service principal token
        timeout: Request timeout in seconds
    """

    API_VERSION = "2.1"

    def __init__(
        self,
        workspace_url: str,
        token: str,
        timeout: int = 30,
    ):
        self.workspace_url = workspace_url.rstrip("/")
        self.timeout = timeout
        self._session = requests.Session()
        self._session.headers.update({
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        })

    def _get(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """Execute a GET request against the Unity Catalog API."""
        url = f"{self.workspace_url}/api/{self.API_VERSION}/unity-catalog/{endpoint}"
        logger.debug(f"GET {url} params={params}")

        response = self._session.get(url, params=params, timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def list_catalogs(self) -> List[Dict]:
        """List all catalogs in the metastore."""
        result = self._get("catalogs")
        return result.get("catalogs", [])

    def list_schemas(self, catalog_name: str) -> List[Dict]:
        """List all schemas within a catalog."""
        result = self._get("schemas", params={"catalog_name": catalog_name})
        return result.get("schemas", [])

    def list_tables(
        self,
        catalog_name: str,
        schema_name: str,
    ) -> List[CatalogTable]:
        """
        List all tables within a schema, including column metadata.

        Returns CatalogTable objects with basic metadata.
        """
        result = self._get(
            "tables",
            params={
                "catalog_name": catalog_name,
                "schema_name": schema_name,
            },
        )
        raw_tables = result.get("tables", [])

        tables = []
        for raw in raw_tables:
            columns = []
            for col in raw.get("columns", []):
                columns.append(CatalogColumn(
                    name=col.get("name", ""),
                    data_type=col.get("type_name", col.get("type_text", "STRING")),
                    nullable=col.get("nullable", True),
                    comment=col.get("comment", ""),
                    partition_index=col.get("partition_index"),
                ))

            table = CatalogTable(
                full_name=raw.get("full_name", ""),
                catalog_name=raw.get("catalog_name", catalog_name),
                schema_name=raw.get("schema_name", schema_name),
                table_name=raw.get("name", ""),
                table_type=raw.get("table_type", "MANAGED"),
                data_source_format=raw.get("data_source_format", ""),
                comment=raw.get("comment", ""),
                owner=raw.get("owner", ""),
                columns=columns,
                properties=raw.get("properties", {}),
            )
            tables.append(table)

        logger.info(
            f"Fetched {len(tables)} tables from "
            f"{catalog_name}.{schema_name}"
        )
        return tables

    def get_all_tables(
        self,
        catalog_name: str,
        schema_filter: Optional[str] = None,
    ) -> List[CatalogTable]:
        """
        Fetch all tables across all schemas in a catalog.

        Args:
            catalog_name: The catalog to scan
            schema_filter: Optional schema name to limit to

        Returns:
            List of CatalogTable objects with column metadata.
        """
        schemas = self.list_schemas(catalog_name)
        all_tables = []

        for schema in schemas:
            schema_name = schema.get("name", "")

            # Skip internal schemas
            if schema_name in ("information_schema", "__internal__"):
                continue

            if schema_filter and schema_name != schema_filter:
                continue

            tables = self.list_tables(catalog_name, schema_name)
            all_tables.extend(tables)

        logger.info(
            f"Total: {len(all_tables)} tables from catalog '{catalog_name}'"
        )
        return all_tables
