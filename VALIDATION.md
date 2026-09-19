# Release validation

Validation is in progress for the v0.1.0 candidate. This file will be finalized with exact measured results before the release is published.

Executed locally on Linux / Python 3.12.14: 108 passing tests, approximately 89% line coverage; Ruff and mypy pass. Fifty controlled-language corpus cases pass (20 accepted IR cases, 30 clarification/rejection cases). A real Airflow 3.1.8 DAG imported with exact graph assertions and executed through `DAG.test`, producing the expected two valid rows and one quarantined row.

Initial cross-platform CI passed on Linux and macOS with Python 3.11 and 3.14. Windows exposed a missing timezone-data dependency; this has been patched and awaits rerun. Initial local-model JSON-only inference returned invalid schemas and was rejected; `docs/live-model-initial.json` preserves that result. Schema-constrained inference is being validated separately. These incomplete gates are not represented as passed.
