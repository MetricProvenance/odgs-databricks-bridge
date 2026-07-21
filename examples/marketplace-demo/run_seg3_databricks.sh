#!/usr/bin/env bash
# SEGMENT 3 (Databricks) — single-command runnable.
# Mock Unity Catalog REST -> real odgs-databricks-bridge sync -> ODGS schemas
# -> signed pack -> real engine enforces an OK row and a NULL-violating row.
set -e
cd "$(dirname "$0")"

echo "[seg3-databricks] starting mock Unity Catalog on :8600 and JWKS on :8601"
python3 mock_unity_catalog.py >/tmp/seg3_db_mock.log 2>&1 &
MOCK=$!
python3 jwks_server.py >/tmp/seg3_db_jwks.log 2>&1 &
JWKS=$!
trap "kill $MOCK $JWKS 2>/dev/null" EXIT
sleep 2

echo ""
echo '$ pip show odgs-databricks-bridge | grep -E "Name|Version"'
pip show odgs-databricks-bridge 2>/dev/null | grep -E "^(Name|Version):"

echo ""
echo '$ python3 sync_from_databricks.py'
python3 sync_from_databricks.py

echo ""
echo '$ python3 build_and_enforce.py rows/transactions_ok.json'
python3 build_and_enforce.py rows/transactions_ok.json

echo ""
echo '$ python3 build_and_enforce.py rows/transactions_bad.json'
python3 build_and_enforce.py rows/transactions_bad.json || echo "(exit 1 — NULL currency hard-stopped, as intended)"

echo ""
echo "[seg3-databricks] done."
