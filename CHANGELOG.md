## [v0.4.0] - 2026-04-13

### ✨ Added — ODGS v6.0 Compatibility

- **`SOFT_STOP` severity support:** CLI `--severity` flag and Python API now accept `SOFT_STOP`, the override-gated severity introduced in ODGS v6.0.0. Existing `HARD_STOP`, `WARNING`, and `INFO` remain unchanged.
- **Badge updated** to `v5.1+ | v6.0 Compatible` — signals forward compatibility with the ODGS Sovereign Validation Engine.
- **Architecture diagram** updated to include `SOFT_STOP` in the Universal Interceptor severity list.

### 🔗 Compatibility

- Requires `odgs>=5.1.0` — works with both v5.x and v6.0 engines.
- All changes are **additive, backward-compatible, and non-breaking**.

---

## [v0.3.0] - 2026-03-19

### ✨ Added

- **Legislative lineage fields (ODGS S-Cert v5.1.0):** Rules now include:
  - `legislative_source` — declares the source authority (defaults to `"BRIDGE_GENERATED_UNATTESTED"`; set explicitly in asset attributes to declare your legislative source)
  - `verbatim_source_text` — optional raw text from source (Collibra bridge reads from asset attributes)
  - `semantic_hash: "UNATTESTED"` — placeholder for Registry-attested SHA-256 hash; upgrade to Registry at https://platform.metricprovenance.com
  - `verdict_on_pass: "PASS"` — explicit pass verdict per ODGS S-Cert specification

- **Bridge version bumped to 0.3.0** in all generated schema packs.

### ⚠️ Migration Notes

All new fields are additive. Existing schemas continue to work. Rules without `legislative_source` set in source attributes will show `"BRIDGE_GENERATED_UNATTESTED"`.

---


# Changelog

All notable changes to this project will be documented in this file.
Format based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
adhering to [Semantic Versioning](https://semver.org/).

## [0.2.0] - 2026-03-19

### 🔧 Fixed
- **Python 3.14 Forward Compatibility:** Replaced deprecated `datetime.datetime.utcnow()` with
  `datetime.datetime.now(datetime.timezone.utc)`. Timestamp format changes from
  `"2026-03-19T12:00:00Z"` (naive UTC with `Z`) to `"2026-03-19T12:00:00+00:00"` (timezone-aware ISO 8601).
- **Schema Reference:** Updated `$schema` URL from `schemas/odgs/v4` → `schemas/odgs/v5` to reflect
  compatibility with the ODGS v5.0.x polymorphic execution engine.
- **Provenance Metadata:** `bridge_version` field in all generated ODGS schemas now correctly reports `0.2.0`.

### 🔗 Compatibility
- Requires `odgs>=5.1.0` — targets the ODGS v5.1.0 audit engine (S-Cert, LOG_ONLY, temporal bounds).

## [0.1.0] - 2026-03-07

### 🚀 Initial Release
- Databricks Unity Catalog → ODGS Schema transformation engine.
- Converts Unity Catalog tables and column metadata into ODGS-compliant JSON schemas for runtime enforcement.
- CLI: `odgs-databricks bridge` command for direct integration pipeline.
- SHA-256 content hashing for immutability verification.
- Optional `databricks-sdk` integration (`pip install odgs-databricks-bridge[sdk]`).