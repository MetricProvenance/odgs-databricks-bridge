"""
Tests for the Databricks → ODGS transformer.

Uses mock Unity Catalog data to validate schema transformation.
"""

import json
import pytest
from odgs_databricks.client import CatalogTable, CatalogColumn
from odgs_databricks.transformer import DatabricksTransformer, _sanitize_urn


@pytest.fixture
def transformer():
    return DatabricksTransformer(organization="acme_corp")


@pytest.fixture
def sample_table():
    return CatalogTable(
        full_name="production.finance.revenue",
        catalog_name="production",
        schema_name="finance",
        table_name="revenue",
        table_type="MANAGED",
        data_source_format="DELTA",
        comment="Monthly revenue aggregation table",
        owner="data-team@acme.com",
        columns=[
            CatalogColumn(name="transaction_id", data_type="BIGINT", nullable=False),
            CatalogColumn(name="amount", data_type="DECIMAL(18,2)", nullable=False, comment="Revenue amount in EUR"),
            CatalogColumn(name="currency", data_type="STRING", nullable=True),
            CatalogColumn(name="transaction_date", data_type="DATE", nullable=False),
            CatalogColumn(name="region", data_type="STRING", nullable=True, partition_index=0),
        ],
    )


@pytest.fixture
def minimal_table():
    return CatalogTable(
        full_name="staging.raw.events",
        catalog_name="staging",
        schema_name="raw",
        table_name="events",
        table_type="EXTERNAL",
        columns=[
            CatalogColumn(name="event_id", data_type="STRING", nullable=False),
            CatalogColumn(name="payload", data_type="STRING", nullable=True),
        ],
    )


class TestMetricTransformation:
    def test_basic_metric_structure(self, transformer, sample_table):
        metric = transformer.table_to_metric(sample_table)

        assert metric["metric_urn"] == "urn:odgs:custom:acme_corp:production_finance_revenue"
        assert metric["name"] == "revenue"
        assert metric["domain"] == "production.finance"
        assert metric["source_authority"] == "databricks:production"
        assert metric["schema"]["column_count"] == 5
        assert metric["schema"]["table_type"] == "MANAGED"
        assert "content_hash" in metric
        assert len(metric["content_hash"]) == 64

    def test_column_metadata_preserved(self, transformer, sample_table):
        metric = transformer.table_to_metric(sample_table)
        columns = metric["schema"]["columns"]

        assert len(columns) == 5
        amount_col = next(c for c in columns if c["name"] == "amount")
        assert amount_col["data_type"] == "DECIMAL(18,2)"
        assert amount_col["nullable"] is False
        assert amount_col["description"] == "Revenue amount in EUR"

    def test_partition_index_captured(self, transformer, sample_table):
        metric = transformer.table_to_metric(sample_table)
        columns = metric["schema"]["columns"]
        region_col = next(c for c in columns if c["name"] == "region")
        assert region_col["partition_index"] == 0

    def test_provenance_tracking(self, transformer, sample_table):
        metric = transformer.table_to_metric(sample_table)
        prov = metric["provenance"]

        assert prov["bridge"] == "odgs-databricks-bridge"
        assert prov["source_url"] == "databricks://production.finance.revenue"
        assert prov["synced_at"].endswith("Z")


class TestRuleGeneration:
    def test_not_null_rules(self, transformer, sample_table):
        rules = transformer.table_to_rules(sample_table)

        not_null_rules = [r for r in rules if r["constraint_type"] == "NOT_NULL"]
        # 3 non-nullable columns: transaction_id, amount, transaction_date
        assert len(not_null_rules) == 3

        names = [r["target_column"] for r in not_null_rules]
        assert "transaction_id" in names
        assert "amount" in names
        assert "transaction_date" in names
        # nullable columns should NOT have NOT_NULL rules
        assert "currency" not in names
        assert "region" not in names

    def test_type_check_rules(self, transformer, sample_table):
        rules = transformer.table_to_rules(sample_table)

        type_rules = [r for r in rules if r["constraint_type"] == "TYPE_CHECK"]
        assert len(type_rules) > 0

        bigint_rule = next(
            (r for r in type_rules if r["target_column"] == "transaction_id"),
            None,
        )
        assert bigint_rule is not None
        assert bigint_rule["expected_type"] == "integer"

    def test_decimal_type_mapping(self, transformer, sample_table):
        rules = transformer.table_to_rules(sample_table)
        type_rules = [r for r in rules if r["constraint_type"] == "TYPE_CHECK"]

        amount_rule = next(
            (r for r in type_rules if r["target_column"] == "amount"), None
        )
        assert amount_rule is not None
        assert amount_rule["expected_type"] == "numeric"

    def test_rule_severity(self, transformer, sample_table):
        rules = transformer.table_to_rules(sample_table, severity="HARD_STOP")
        not_null_rules = [r for r in rules if r["constraint_type"] == "NOT_NULL"]
        assert all(r["severity"] == "HARD_STOP" for r in not_null_rules)

    def test_content_hashes(self, transformer, sample_table):
        rules = transformer.table_to_rules(sample_table)
        for rule in rules:
            assert "content_hash" in rule
            assert len(rule["content_hash"]) == 64


class TestSchemaPackOutput:
    def test_metrics_schema(self, transformer, sample_table, minimal_table):
        schema = transformer.transform_tables(
            [sample_table, minimal_table], output_type="metrics"
        )

        assert schema["$schema"] == "https://metricprovenance.com/schemas/odgs/v4"
        assert schema["metadata"]["source"] == "databricks"
        assert schema["metadata"]["tables_processed"] == 2
        assert schema["metadata"]["items_generated"] == 2
        assert len(schema["items"]) == 2

    def test_rules_schema(self, transformer, sample_table):
        schema = transformer.transform_tables(
            [sample_table], output_type="rules"
        )

        assert schema["metadata"]["tables_processed"] == 1
        # 3 NOT_NULL + at least 4 TYPE_CHECK rules
        assert schema["metadata"]["items_generated"] >= 7


class TestUrnSanitization:
    def test_dots_replaced(self):
        assert _sanitize_urn("production.finance.revenue") == "production_finance_revenue"

    def test_special_chars(self):
        result = _sanitize_urn("My Table (v2) & Friends / 2026")
        assert "(" not in result
        assert ")" not in result
        assert "&" not in result
        assert "/" not in result

    def test_consecutive_underscores_collapsed(self):
        result = _sanitize_urn("hello  /  world")
        assert "__" not in result
