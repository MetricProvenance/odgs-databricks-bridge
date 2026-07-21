# Marketplace Demo — end-to-end without a Databricks workspace

Self-contained demonstration of the ODGS Databricks Bridge: a local mock of
the Unity Catalog REST API (`mock_unity_catalog.py`, port 8600) serves a
**sample** `production.finance` catalog — 2 managed tables (`transactions`,
`customers`), clearly labeled sample data. The real, unmodified
`odgs-databricks-bridge` package syncs from it; the real `odgs` engine then
enforces the generated rules.

## Run

```bash
pip install odgs-databricks-bridge odgs flask
bash run_seg3_databricks.sh
```

Starts the mock Unity Catalog (`:8600`) and a JWKS server (`:8601`), then:

1. `sync_from_databricks.py` — bridge fetches the `production.finance` schema
   over the Unity Catalog REST API and writes two ODGS schema files to
   `schemas/custom/`
2. `build_and_enforce.py rows/transactions_ok.json` — VERDICT: APPROVED
3. `build_and_enforce.py rows/transactions_bad.json` — VERDICT: BLOCKED
   (HARD_STOP, exit 1) — a `NULL` `currency` column

`seg3_databricks_output.txt` is a captured full run for reference.

## Honest accounting

- The bridge generates **18 rules** from the 2-table schema (column
  constraints, type assertions, nullability). Only the **5 `NOT NULL`
  HARD_STOP rules** on `transactions` are actually bound and enforced in this
  demo.
- The auto-generated `TYPE_CHECK` / `INFO` rules ship in the schema file but
  are **not bound** here — they use a `type()` check that falls outside the
  engine's `simpleeval` expression allowlist (a deliberate security
  restriction: the rule-evaluation sandbox only permits a constrained set of
  safe operations, not arbitrary Python builtins). Binding them would need a
  custom evaluator extension, which is out of scope for this demo.
- The rules pack signature is **ES256/JWS**, verified by the engine's own
  `CryptoResolver` against the mock's JWKS at load time. The **Ed25519 "audit
  seal"** is demo glue around the engine-generated audit record, the same
  pattern as the Collibra demo — not a built-in engine feature.
- Everything else — bridge code, API calls, transformation, enforcement,
  blocking — is the real published packages doing real work. Only the
  catalog data is sample data.

Same rig pattern as `odgs-collibra-bridge`'s `examples/marketplace-demo/`
and `odgs-snowflake-bridge`'s — mock API/DBAPI boundary + real bridge + real
engine.
