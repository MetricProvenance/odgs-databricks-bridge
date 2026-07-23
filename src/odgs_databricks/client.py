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

    def _get_paginated(self, endpoint: str, params: Optional[Dict] = None, items_key: str = "") -> List[Dict]:
        """GET all pages of a Unity Catalog list endpoint, following next_page_token.

        Without this, any workspace with more catalogs/schemas/tables than fit on
        a single API page would have results silently truncated with no warning.
        """
        all_items: List[Dict] = []
        page_params = dict(params or {})
        while True:
            result = self._get(endpoint, params=page_params)
            all_items.extend(result.get(items_key, []))
            next_token = result.get("next_page_token")
            if not next_token:
                break
            page_params = dict(params or {})
            page_params["page_token"] = next_token
        return all_items

    def _patch(self, endpoint: str, json_data: Dict) -> Dict:
        """Execute a PATCH request against the Unity Catalog API."""
        url = f"{self.workspace_url}/api/{self.API_VERSION}/unity-catalog/{endpoint}"
        logger.debug(f"PATCH {url} data={json_data}")

        response = self._session.patch(url, json=json_data, timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def update_table_comment(self, full_name: str, comment: str) -> Dict:
        """Replace the comment/description of a Unity Catalog table wholesale.

        Use append_odgs_comment() instead if you need to preserve any existing
        human-authored comment — this method overwrites it entirely.
        """
        payload = {"comment": comment}
        return self._patch(f"tables/{full_name}", json_data=payload)

    ODGS_COMMENT_MARKER = "<!-- odgs-enforcement-block -->"

    def get_table(self, full_name: str) -> Dict:
        """Fetch a single table's current metadata (including its comment)."""
        return self._get(f"tables/{full_name}")

    def append_odgs_comment(self, full_name: str, odgs_block: str) -> Dict:
        """Append an ODGS enforcement block to a table's comment without
        destroying any pre-existing human-authored content.

        Fetches the current comment, strips any previously-appended ODGS block
        (identified by ODGS_COMMENT_MARKER, so repeated write-back runs don't
        grow the comment unboundedly), then writes back preserved-human-text +
        marker + the new ODGS block.
        """
        current = self.get_table(full_name).get("comment") or ""
        marker_pos = current.find(self.ODGS_COMMENT_MARKER)
        preserved = (current[:marker_pos] if marker_pos != -1 else current).rstrip()

        if preserved:
            new_comment = f"{preserved}\n\n{self.ODGS_COMMENT_MARKER}\n{odgs_block}"
        else:
            new_comment = f"{self.ODGS_COMMENT_MARKER}\n{odgs_block}"

        return self.update_table_comment(full_name, new_comment)

    def list_catalogs(self) -> List[Dict]:
        """List all catalogs in the metastore."""
        return self._get_paginated("catalogs", items_key="catalogs")

    def list_schemas(self, catalog_name: str) -> List[Dict]:
        """List all schemas within a catalog."""
        return self._get_paginated("schemas", params={"catalog_name": catalog_name}, items_key="schemas")

    def list_tables(
        self,
        catalog_name: str,
        schema_name: str,
    ) -> List[CatalogTable]:
        """
        List all tables within a schema, including column metadata.

        Returns CatalogTable objects with basic metadata.
        """
        raw_tables = self._get_paginated(
            "tables",
            params={
                "catalog_name": catalog_name,
                "schema_name": schema_name,
            },
            items_key="tables",
        )

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
