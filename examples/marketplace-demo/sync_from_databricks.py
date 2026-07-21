#!/usr/bin/env python3
"""
SEGMENT 3 (Databricks) — Sync Unity Catalog metadata → ODGS enforcement schemas.

Uses the REAL `odgs-databricks-bridge` package (DatabricksBridge /
UnityCatalogClient / DatabricksTransformer, v0.4.0) against the mock Unity
Catalog REST API at http://localhost:8600.

Real API entrypoints exercised:
    DatabricksBridge(workspace_url=..., token=..., organization=...)
    bridge.client.list_catalogs()
    bridge.client.list_schemas(catalog_name)
    bridge.client.get_all_tables(catalog_name)     # -> real REST GETs
    bridge.sync(catalog=..., output_type="metrics"|"rules", severity="HARD_STOP")
"""
import logging

from odgs_databricks import DatabricksBridge

logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(name)s | %(message)s")

WORKSPACE_URL = "http://localhost:8600"
TOKEN = "dapi-demo-token"
CATALOG = "production"

bridge = DatabricksBridge(
    workspace_url=WORKSPACE_URL,
    token=TOKEN,
    organization="acme_finance",
)

print("=" * 74)
print("Connecting to Databricks Unity Catalog:", WORKSPACE_URL)
cats = bridge.client.list_catalogs()
for c in cats:
    print(f"Catalog:  {c['name']}")
    for s in bridge.client.list_schemas(c["name"]):
        print(f"  Schema: {s['name']}")

tables = bridge.client.get_all_tables(catalog_name=CATALOG)
print(f"\nUnity Catalog tables in '{CATALOG}':")
for t in tables:
    nn = sum(1 for col in t.columns if not col.nullable)
    print(f"  [{t.table_type:>8}] {t.full_name:<34} "
          f"{len(t.columns)} cols ({nn} NOT NULL)  owner={t.owner}")
print("=" * 74)

print("\n>>> bridge.sync(catalog='production', output_type='metrics')")
metrics_path = bridge.sync(
    catalog=CATALOG, output_dir="./schemas/custom/", output_type="metrics",
)

print("\n>>> bridge.sync(catalog='production', output_type='rules', severity='HARD_STOP')")
rules_path = bridge.sync(
    catalog=CATALOG, output_dir="./schemas/custom/", output_type="rules",
    severity="HARD_STOP",
)

print("\nGenerated ODGS schemas:")
print("  metrics:", metrics_path)
print("  rules:  ", rules_path)
