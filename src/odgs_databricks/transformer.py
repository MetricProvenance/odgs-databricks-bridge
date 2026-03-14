"""
Databricks → ODGS Schema Transformer
=====================================

Transforms Unity Catalog table/column metadata into
ODGS-compliant JSON schemas for runtime enforcement.

Key difference from Collibra bridge:
- Collibra maps business terms → ODGS metrics
- Databricks maps table schemas → ODGS metrics + column-level DQ rules
"""

import hashlib
import json
import re
import datetime
import logging
from typing import Any, Dict, List, Optional

from odgs_databricks.client import CatalogTable, CatalogColumn

logger = logging.getLogger(__name__)


def _content_hash(data: Dict) -> str:
    """Generate SHA-256 content hash for immutability verification."""
    canonical = json.dumps(data, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _sanitize_urn(name: str) -> str:
    """Convert a name into a URN-safe identifier."""
    result = (
        name.lower()
        .replace(" ", "_")
        .replace("-", "_")
        .replace(".", "_")
        .replace("(", "")
        .replace(")", "")
        .replace("/", "_")
        .replace("&", "and")
    )
    result = re.sub(r"_+", "_", result).strip("_")
    return result


# Column-type to ODGS constraint mapping
TYPE_CONSTRAINTS = {
    "INT": {"type": "integer"},
    "BIGINT": {"type": "integer"},
    "LONG": {"type": "integer"},
    "SHORT": {"type": "integer"},
    "TINYINT": {"type": "integer"},
    "FLOAT": {"type": "numeric"},
    "DOUBLE": {"type": "numeric"},
    "DECIMAL": {"type": "numeric"},
    "STRING": {"type": "string"},
    "VARCHAR": {"type": "string"},
    "CHAR": {"type": "string"},
    "BOOLEAN": {"type": "boolean"},
    "DATE": {"type": "date"},
    "TIMESTAMP": {"type": "timestamp"},
    "TIMESTAMP_NTZ": {"type": "timestamp"},
}


class DatabricksTransformer:
    """
    Transforms Unity Catalog table metadata into ODGS JSON schemas.

    Generates two types of ODGS artifacts:
    - **Metric definitions** from table/column metadata
    - **Enforcement rules** from column types, nullability, and DQ properties

    Args:
        organization: Organization identifier for URN namespace
            (e.g., "acme_corp" → urn:odgs:custom:acme_corp:*)
    """

    def __init__(self, organization: str):
        self.organization = _sanitize_urn(organization)

    def table_to_metric(self, table: CatalogTable) -> Dict[str, Any]:
        """
        Transform a Unity Catalog table into an ODGS metric definition.

        Captures table-level metadata, column inventory, and data lineage.
        """
        table_urn = _sanitize_urn(table.full_name)
        urn = f"urn:odgs:custom:{self.organization}:{table_urn}"

        columns_spec = []
        for col in table.columns:
            col_spec = {
                "name": col.name,
                "data_type": col.data_type,
                "nullable": col.nullable,
            }
            if col.comment:
                col_spec["description"] = col.comment
            if col.partition_index is not None:
                col_spec["partition_index"] = col.partition_index
            columns_spec.append(col_spec)

        metric = {
            "metric_id": _sanitize_urn(table.table_name),
            "metric_urn": urn,
            "name": table.table_name,
            "description": table.comment or f"Table {table.full_name}",
            "domain": f"{table.catalog_name}.{table.schema_name}",
            "source_authority": f"databricks:{table.catalog_name}",
            "schema": {
                "full_name": table.full_name,
                "table_type": table.table_type,
                "data_source_format": table.data_source_format,
                "column_count": len(table.columns),
                "columns": columns_spec,
            },
            "compliance": {
                "owner": table.owner,
                "governance_catalog": table.catalog_name,
            },
            "provenance": {
                "bridge": "odgs-databricks-bridge",
                "bridge_version": "0.1.0",
                "synced_at": datetime.datetime.utcnow().isoformat() + "Z",
                "source_url": f"databricks://{table.full_name}",
            },
        }

        metric["content_hash"] = _content_hash(metric)
        return metric

    def table_to_rules(
        self,
        table: CatalogTable,
        severity: str = "WARNING",
    ) -> List[Dict[str, Any]]:
        """
        Generate ODGS enforcement rules from a table's column schema.

        Creates rules for:
        - NOT NULL constraints (non-nullable columns)
        - Type validation (expected data type)
        - Custom properties (if annotated in Unity Catalog)
        """
        rules = []
        table_id = _sanitize_urn(table.table_name)

        for col in table.columns:
            col_id = _sanitize_urn(col.name)

            # Rule 1: NOT NULL enforcement for non-nullable columns
            if not col.nullable:
                rule_urn = f"urn:odgs:custom:{self.organization}:rule:{table_id}_{col_id}_not_null"
                rules.append({
                    "rule_id": f"{table_id}_{col_id}_not_null",
                    "rule_urn": rule_urn,
                    "name": f"{table.table_name}.{col.name} NOT NULL",
                    "description": f"Column {col.name} must not be null",
                    "domain": f"{table.catalog_name}.{table.schema_name}",
                    "severity": severity,
                    "logic_expression": f"{col.name} != None",
                    "constraint_type": "NOT_NULL",
                    "target_column": col.name,
                    "target_table": table.full_name,
                    "source_authority": f"databricks:{table.catalog_name}",
                    "provenance": {
                        "bridge": "odgs-databricks-bridge",
                        "bridge_version": "0.1.0",
                        "synced_at": datetime.datetime.utcnow().isoformat() + "Z",
                        "source_url": f"databricks://{table.full_name}/{col.name}",
                    },
                })

            # Rule 2: Type constraint from column type
            type_upper = col.data_type.upper().split("(")[0]  # Handle DECIMAL(10,2)
            if type_upper in TYPE_CONSTRAINTS:
                rule_urn = f"urn:odgs:custom:{self.organization}:rule:{table_id}_{col_id}_type"
                rules.append({
                    "rule_id": f"{table_id}_{col_id}_type",
                    "rule_urn": rule_urn,
                    "name": f"{table.table_name}.{col.name} TYPE({col.data_type})",
                    "description": f"Column {col.name} must be {TYPE_CONSTRAINTS[type_upper]['type']}",
                    "domain": f"{table.catalog_name}.{table.schema_name}",
                    "severity": "INFO",
                    "logic_expression": f"type({col.name}) == '{TYPE_CONSTRAINTS[type_upper]['type']}'",
                    "constraint_type": "TYPE_CHECK",
                    "expected_type": TYPE_CONSTRAINTS[type_upper]["type"],
                    "target_column": col.name,
                    "target_table": table.full_name,
                    "source_authority": f"databricks:{table.catalog_name}",
                    "provenance": {
                        "bridge": "odgs-databricks-bridge",
                        "bridge_version": "0.1.0",
                        "synced_at": datetime.datetime.utcnow().isoformat() + "Z",
                        "source_url": f"databricks://{table.full_name}/{col.name}",
                    },
                })

        # Add content hashes
        for rule in rules:
            rule["content_hash"] = _content_hash(rule)

        return rules

    def transform_tables(
        self,
        tables: List[CatalogTable],
        output_type: str = "metrics",
        severity: str = "WARNING",
    ) -> Dict[str, Any]:
        """
        Transform a list of Unity Catalog tables into an ODGS schema pack.

        Args:
            tables: List of CatalogTable objects
            output_type: "metrics" or "rules"
            severity: Severity for rule output

        Returns:
            ODGS-compliant schema dictionary.
        """
        logger.warning("[ODGS Bridge] ⚠️ Compiling unsigned rules for ODGS Community Edition. Get Certified Sovereign Packs at https://platform.metricprovenance.com")
        items = []
        for table in tables:
            if output_type == "metrics":
                items.append(self.table_to_metric(table))
            elif output_type == "rules":
                items.extend(self.table_to_rules(table, severity=severity))

        schema = {
            "$schema": "https://metricprovenance.com/schemas/odgs/v4",
            "metadata": {
                "source": "databricks",
                "organization": self.organization,
                "bridge": "odgs-databricks-bridge",
                "bridge_version": "0.1.0",
                "generated_at": datetime.datetime.utcnow().isoformat() + "Z",
                "tables_processed": len(tables),
                "items_generated": len(items),
            },
            "items": items,
        }

        return schema
