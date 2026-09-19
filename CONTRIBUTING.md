# Contributing

Use Python 3.11+ and install `.[dev,parquet]` in a virtual environment. Run Ruff, mypy, pytest, and the semantic corpus. Install the tested Airflow extra to exercise import and deployment tests. Never interpret a skipped Airflow test as a passed Airflow gate.

A capability change needs a versioned closed parameter schema, explicit input/output contracts, an implementation, permission checks, exact-record tests, failure tests, and adversarial cases. Merely adding registry metadata must not make an unavailable adapter executable. Preserve deterministic IR normalization and compilation.

A provider change must retain endpoint/transmission policy, sanitized errors, bounded responses, and schema validation. Add controlled-wire tests and separately report any actual model evaluation. Do not tune a test provider to return the expected corpus answer and call that language accuracy.

Keep examples domain-neutral and synthetic. Do not commit credentials, production datasets, local approvals, generated runtime state, or proprietary material. Extend the manual and scope matrix whenever behavior changes. Regenerate the PDF with `pip install '.[docs]'` and `python scripts/build_manual.py`, then render and visually inspect every page.

Release candidates must pass the documented gates, including clean wheel/source installs and cross-platform CI. Do not claim production readiness outside the release's explicit tested scope.
