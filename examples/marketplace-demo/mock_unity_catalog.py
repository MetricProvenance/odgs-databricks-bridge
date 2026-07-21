#!/usr/bin/env python3
"""
Mock Databricks Unity Catalog — REST API 2.1
============================================

Local stand-in implementing exactly the endpoints the real
`odgs-databricks-bridge` client (odgs_databricks/client.py) calls:

    GET   /api/2.1/unity-catalog/catalogs
    GET   /api/2.1/unity-catalog/schemas   ?catalog_name=
    GET   /api/2.1/unity-catalog/tables    ?catalog_name=&schema_name=
    PATCH /api/2.1/unity-catalog/tables/<full_name>     (ODGS write-back)

Auth: Bearer token, expects "Authorization: Bearer <token>".

-----------------------------------------------------------------------
NOTE: ALL DATA BELOW IS SAMPLE DATA, invented for demo purposes only.
It models a believable Unity Catalog 'production' catalog with a finance
schema so the bridge has realistic table/column metadata to sync into
ODGS rules. It is not real company data.
-----------------------------------------------------------------------
"""
import os
from flask import Flask, request, jsonify

app = Flask(__name__)
PORT = 8600
API_TOKEN = "dapi-demo-token"

# ============================================================================
# SAMPLE Unity Catalog metadata (demo purposes only)
# ============================================================================
CATALOG = "production"

SCHEMAS = ["finance", "information_schema"]  # information_schema is skipped by client


def _col(name, type_name, nullable, comment="", partition_index=None):
    return {"name": name, "type_name": type_name, "nullable": nullable,
            "comment": comment, "partition_index": partition_index}


# One curated finance schema with two governed tables.
TABLES = {
    ("production", "finance"): [
        {
            "full_name": "production.finance.transactions",
            "catalog_name": "production", "schema_name": "finance",
            "name": "transactions", "table_type": "MANAGED",
            "data_source_format": "DELTA",
            "comment": "Ledger of customer financial transactions (SAMPLE).",
            "owner": "finance_data_eng@acme.demo",
            "columns": [
                _col("transaction_id", "STRING",  False, "Primary key", 0),
                _col("customer_id",    "STRING",  False, "FK to customers"),
                _col("amount",         "DECIMAL", False, "Transaction amount, 2dp"),
                _col("currency",       "STRING",  False, "ISO-4217 code"),
                _col("created_at",     "TIMESTAMP", False, "Event time"),
                _col("memo",           "STRING",  True,  "Optional free-text note"),
            ],
            "properties": {"delta.minReaderVersion": "2", "quality": "gold"},
        },
        {
            "full_name": "production.finance.customers",
            "catalog_name": "production", "schema_name": "finance",
            "name": "customers", "table_type": "MANAGED",
            "data_source_format": "DELTA",
            "comment": "Master customer records (SAMPLE).",
            "owner": "finance_data_eng@acme.demo",
            "columns": [
                _col("customer_id",  "STRING",  False, "Primary key", 0),
                _col("legal_name",   "STRING",  False, "Registered legal name"),
                _col("risk_tier",    "INT",     False, "1-5 risk rating"),
                _col("onboarded_at", "DATE",    True,  "KYC completion date"),
            ],
            "properties": {"quality": "gold"},
        },
    ],
}


def _authorized():
    return request.headers.get("Authorization", "") == f"Bearer {API_TOKEN}"


@app.before_request
def check_auth():
    if request.path.startswith("/api/2.1/unity-catalog/") and not _authorized():
        return jsonify({"error_code": "PERMISSION_DENIED",
                        "message": "Valid bearer token required."}), 401


@app.route("/api/2.1/unity-catalog/catalogs")
def catalogs():
    return jsonify({"catalogs": [
        {"name": CATALOG, "comment": "Production catalog (SAMPLE)", "owner": "platform@acme.demo"}
    ]})


@app.route("/api/2.1/unity-catalog/schemas")
def schemas():
    catalog_name = request.args.get("catalog_name", CATALOG)
    return jsonify({"schemas": [
        {"name": s, "catalog_name": catalog_name} for s in SCHEMAS
    ]})


@app.route("/api/2.1/unity-catalog/tables")
def tables():
    catalog_name = request.args.get("catalog_name", CATALOG)
    schema_name = request.args.get("schema_name", "")
    return jsonify({"tables": TABLES.get((catalog_name, schema_name), [])})


@app.route("/api/2.1/unity-catalog/tables/<path:full_name>", methods=["PATCH"])
def update_table(full_name):
    payload = request.get_json(force=True, silent=True) or {}
    return jsonify({"full_name": full_name, **payload})


@app.route("/")
def index():
    return jsonify({"service": "mock-unity-catalog", "api": "2.1",
                    "note": "SAMPLE demo instance — production.finance"})


if __name__ == "__main__":
    print(f"Mock Unity Catalog (SAMPLE data) listening on http://localhost:{PORT}")
    app.run(host="127.0.0.1", port=PORT)
