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
- Requires `odgs>=5.0.0` — fully compatible with the v5.0.1 audit log format (dual-field backward-compat aliases).

## [0.1.0] - 2026-03-07

### 🚀 Initial Release
- Databricks Unity Catalog → ODGS Schema transformation engine.
- Converts Unity Catalog tables and column metadata into ODGS-compliant JSON schemas for runtime enforcement.
- CLI: `odgs-databricks bridge` command for direct integration pipeline.
- SHA-256 content hashing for immutability verification.
- Optional `databricks-sdk` integration (`pip install odgs-databricks-bridge[sdk]`).
