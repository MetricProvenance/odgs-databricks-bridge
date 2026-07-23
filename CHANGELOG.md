## [v0.4.3] - 2026-07-23

### 🐛 Fixed

- **`write-back` destroyed any existing human-authored table comment.** It always did a full-replace PATCH instead of fetch-then-append. Added `client.get_table()` + `client.append_odgs_comment()`, which fetch the current comment, strip any previously-appended ODGS block (so repeated write-back runs don't grow the comment unboundedly), and append the new block after preserved human content. The mock demo server's `GET /tables/<full_name>` route and in-memory comment persistence were added to match.
- **No pagination handling** in `list_catalogs`/`list_schemas`/`list_tables` — a workspace with more results than fit on one Unity Catalog API page had results silently truncated with no warning. Added `_get_paginated()`, which follows `next_page_token` until exhausted.
- **`TYPE_CHECK` rules ignored the `--severity` flag**, always hardcoding `INFO`. Kept as always-`INFO` (now documented, not silent) rather than honoring an arbitrary severity: the generated `logic_expression` uses `type()`, which the ODGS engine's sandboxed evaluator doesn't allow, so these rules can't actually enforce anything today — inheriting a user-requested `HARD_STOP` would have falsely implied enforcement that isn't real.
- **`output_type`/`severity` were never validated** — a CLI typo silently produced a valid-looking but empty schema, with no error. Both now raise `ValueError` on an unrecognized value.
- **Audit-log parse errors in `write-back` were only logged at `DEBUG`** (invisible by default) and unparseable line counts weren't surfaced. Now logged at `INFO` with a summary count printed at the end of the run. Also fixed a related gap: a syntactically-valid-but-non-object JSON line (e.g. `null` or a list) would raise an uncaught `AttributeError` past the parse step — now validated explicitly.
- **`typer` was imported and used by the CLI but never declared as a dependency** — every clean `pip install odgs-databricks-bridge` followed by `odgs-databricks --help` failed unless `typer` happened to already be present from something else. Added `typer>=0.9.0` to `[project.dependencies]`.
- **Hardcoded `"0.4.2"` `bridge_version` literal** in four places in `transformer.py` instead of importing `odgs_databricks.__version__` — the same drift class of bug already fixed once in the Collibra bridge this release cycle.

### 📦 Packaging

- Removed `odgs>=5.1.0` and `pydantic>=2.0.0` from `[project.dependencies]` — neither is imported anywhere in this package; every install was pulling in the full `odgs` package for no runtime use.
- Removed the unused `sdk` extra (`databricks-sdk>=0.20.0`) — never referenced anywhere in the code; the actual client is a hand-rolled `requests` wrapper. `CHANGELOG.md`'s v0.1.0 entry describing "Optional `databricks-sdk` integration" described a capability that never existed in the shipped code.
- Fixed a stale sdist-exclude comment referencing a vendor PDF that was already deleted from the repo in v0.4.2.

### 📄 Docs

- README's "What Gets Generated" table showed a SQL-style NOT NULL example (`revenue.amount IS NOT NULL`) and a wrong type mapping (`type(amount) == 'decimal'`, should be `'numeric'`) that didn't match the actual generated `logic_expression`. Corrected to the literal generated strings, with an explicit note on the `TYPE_CHECK` severity scoping above.

Verified: full test suite passes (20 passed, up from 15 baseline — 5 new tests for pagination and fetch-then-append), plus a live fresh-install run of the full demo rig (mock server → sync → sign → enforce, both APPROVED and BLOCKED paths) and a direct GET/PATCH/GET round-trip confirming the mock server correctly persists appended comments.

## [v0.4.2] - 2026-07-20

### Docs

- Removed a committee/standards-body reference from the README that named specific external bodies — the standard and the software are two different things and this repo's docs should describe the latter, not standardization-process status.
- Removed a Databricks partner document that had been sitting in the repository (already excluded from the PyPI package build).

## [v0.4.1] - 2026-07-18

### 🔧 Fixed — Version unification

- **Unified version metadata:** `pyproject.toml`, `__version__`, and the `bridge_version` stamped into emitted ODGS schemas now all report `0.4.1`. Previously these disagreed (package 0.4.0, `__version__` 1.0.0, emitted `bridge_version` stale), which made provenance metadata in generated schemas unreliable.
- **Packaging:** the vendored Databricks best-practices PDF is now excluded from the published sdist (`[tool.hatch.build.targets.sdist] exclude`). The file remains in the repository.

No functional changes to transformation logic.

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
  - `semantic_hash: "UNATTESTED"` — placeholder for Registry-attested SHA-256 hash; upgrade to Registry at https://registry.metricprovenance.com
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