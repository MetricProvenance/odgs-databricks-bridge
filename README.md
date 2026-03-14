# ODGS Databricks Bridge

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![ODGS](https://img.shields.io/badge/ODGS-v4.0.1-0055AA)](https://github.com/MetricProvenance/odgs-protocol)

**Transform your Databricks Unity Catalog into active ODGS runtime enforcement schemas.**

> Unity Catalog describes your data. ODGS enforces it.

---

## What It Does

The ODGS Databricks Bridge connects to your Databricks workspace and transforms Unity Catalog table/column metadata into ODGS-compliant JSON schemas that the [Universal Interceptor](https://github.com/MetricProvenance/odgs-protocol) can enforce at runtime.

```
Unity Catalog REST API            ODGS
┌──────────────┐     Bridge      ┌──────────────┐
│ Catalogs     │ ──────────────→ │ JSON Schema  │
│ Schemas      │   reads tables, │ + Interceptor│
│ Tables       │   outputs ODGS  │ = Enforcement│
│ Columns      │                 └──────────────┘
└──────────────┘
```

### What Gets Generated

| Input | Output Type | ODGS Artifact |
|---|---|---|
| Table metadata | `metrics` | Metric definitions with full column schemas |
| Non-nullable columns | `rules` | `NOT_NULL` enforcement rules |
| Column data types | `rules` | `TYPE_CHECK` validation rules |

## Install

```bash
pip install odgs-databricks-bridge
```

## Quick Start

### Python API

```python
from odgs_databricks import DatabricksBridge

bridge = DatabricksBridge(
    workspace_url="https://adb-1234567890.azuredatabricks.net",
    token="dapi...",
    organization="acme_corp",
)

# Sync all tables from a catalog → ODGS metric definitions
bridge.sync(
    catalog="production",
    output_dir="./schemas/custom/",
    output_type="metrics",
)

# Sync column constraints → ODGS enforcement rules
bridge.sync(
    catalog="production",
    schema_filter="finance",
    output_dir="./schemas/custom/",
    output_type="rules",
    severity="HARD_STOP",
)
```

### CLI

```bash
# Using environment variables (standard Databricks SDK convention)
export DATABRICKS_HOST=https://adb-1234567890.azuredatabricks.net
export DATABRICKS_TOKEN=dapi...

odgs-databricks sync \
    --org acme_corp \
    --catalog production \
    --schema finance \
    --type rules \
    --severity HARD_STOP \
    --output ./schemas/custom/
```

### Output

```json
{
  "$schema": "https://metricprovenance.com/schemas/odgs/v4",
  "metadata": {
    "source": "databricks",
    "organization": "acme_corp",
    "tables_processed": 12,
    "items_generated": 47
  },
  "items": [
    {
      "rule_urn": "urn:odgs:custom:acme_corp:rule:revenue_amount_not_null",
      "name": "revenue.amount NOT NULL",
      "severity": "HARD_STOP",
      "constraint_type": "NOT_NULL",
      "target_table": "production.finance.revenue",
      "content_hash": "a1b2c3..."
    }
  ]
}
```

## 🆕 v4.1.0: Bi-Directional Write-Backs

The ODGS Databricks bridge now supports **Bi-Directional Sync (Plane 4)**. It can parse your secure `sovereign_audit.log` offline and push compliance results back directly into your Unity Catalog table comments. 

This creates a seamless feedback loop for Data Stewards without compromising the Air-Gapped nature of the core ODGS protocol.

```bash
odgs-databricks write-back \
    --log-path ./sovereign_audit.log \
    --url https://adb-1234567890.azuredatabricks.net \
    --token dapi...
```

## Authentication

| Method | CLI Flag | Environment Variable |
|---|---|---|
| Personal Access Token | `--token` | `DATABRICKS_TOKEN` |
| Workspace URL | `--url` | `DATABRICKS_HOST` |

## Requirements

- Python ≥ 3.9
- `odgs` ≥ 4.0.0 (core protocol)
- Databricks workspace with Unity Catalog enabled

## Related

- [ODGS Protocol](https://github.com/MetricProvenance/odgs-protocol) — The core enforcement engine
- [ODGS Collibra Bridge](https://github.com/MetricProvenance/odgs-collibra-bridge) — Collibra integration
- [ODGS Snowflake Bridge](https://github.com/MetricProvenance/odgs-snowflake-bridge) — Snowflake integration (planned)

---

## License

Apache 2.0 — [Metric Provenance](https://metricprovenance.com) | The Hague, NL 🇳🇱
