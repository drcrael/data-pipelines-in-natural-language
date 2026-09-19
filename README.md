# Data Pipelines in Natural Language

[![Validation](https://github.com/drcrael/data-pipelines-in-natural-language/actions/workflows/ci.yml/badge.svg)](https://github.com/drcrael/data-pipelines-in-natural-language/actions/workflows/ci.yml)

**Natural language → governed Pipeline IR → deterministic Airflow DAGs.**

`nlpipe` is a production-oriented, bounded-data MVP. Models propose specifications; registered capabilities execute them. The compiler never asks a model to write Python, and approval is checked separately from model output.

**[User manual (PDF)](docs/NLPIPE_User_Manual.pdf)** · [Manual (Markdown)](docs/USER_MANUAL.md) · [Validation evidence](VALIDATION.md) · [Scope and limitations](LIMITATIONS.md)

## Quick start

Python 3.11+ is required. Use a virtual environment.

```bash
git clone https://github.com/drcrael/data-pipelines-in-natural-language.git
cd data-pipelines-in-natural-language
python -m venv .venv
source .venv/bin/activate
python -m pip install ".[dev,parquet]"

nlpipe create "Load orders into analytics daily at 2 AM." \
  --catalog examples/catalog.yaml --out work/orders.yaml
nlpipe explain work/orders.yaml
nlpipe test examples/orders.yaml --catalog examples/catalog.yaml \
  --fixture-root examples/fixtures
```

On Windows, activate with `.venv\Scripts\Activate.ps1`. The default frontend works offline with a limited vocabulary. For broader language understanding, configure a local or explicitly authorized remote OpenAI-compatible model; see the manual. Mock-provider results are never presented as model accuracy.

Use a Python 3.12 environment to check the generated artifact with the tested Airflow version below. Airflow 3.1.8 requires Python below 3.14; the base CLI also supports Python 3.14.

```bash
python -m pip install "apache-airflow==3.1.8"
nlpipe test examples/orders.yaml --catalog examples/catalog.yaml \
  --airflow --fixture-root examples/fixtures
```

The fixture test copies synthetic inputs into a temporary sandbox. The example reads four orders, deduplicates to three, quarantines one missing customer ID, and writes two valid records. `run` is the explicit command for persistent local output; `deploy` is the explicit command for installing a DAG. Creation never deploys automatically.

## Architecture

```text
Natural language / conversational changes
  → structured intent → asset and capability resolution
  → versioned Pipeline IR → deterministic graph planning
  → structural, semantic, operational, security and quality validation
  → governance decisions → deterministic Airflow 3 compiler
  → verification → human approval where required → deployment
  → runtime evidence → recommendation → proposed IR change
                              ↖ revalidation and new approval ↙
```

The Airflow backend is replaceable; the IR and normalized observations do not depend on Airflow. DataOps integration is currently an evidence/proposal interface, not a live dependency on another repository. Future repair adapters must return IR proposals and re-enter validation and approval.

## Implemented MVP

- Closed Pydantic IR, JSON Schema, JSON/YAML serialization, explicit version gate, deterministic graph ordering, cycle detection, quality barriers and lineage.
- Catalog-only asset resolution, versioned capability schemas, clarification responses and fail-closed unsupported integrations.
- CSV, JSON, JSONL, Parquet, SQLite, and administrator-enabled HTTPS JSON reads; bounded table transformations and six quality checks.
- Exact-content, expiring HMAC approvals for production, destructive writes, external writes, and schema evolution. Models cannot grant or disable approval.
- Airflow 3 TaskFlow generation with schedules, timezone, dependencies, retries, timeouts, documentation and local notification callbacks. Hash-checked shared-storage references keep tables out of XCom.
- Local fixture execution, Airflow imports and semantic DAG checks, full Airflow local task-runner demonstration, normalized runtime observations and a conservative repair proposal.
- Fifty acceptance requests, 18 paraphrases across three canonical pipelines, property/adversarial tests, provider wire tests, and separate evaluation dimensions with machine-readable reports.

Registered interfaces for PostgreSQL, object storage, model enrichment, embeddings, entity resolution, arbitrary branching and lifecycle retention are deliberately unavailable in this release. They fail validation until an implementation is installed and reviewed. Notifications are local outbox events. Distributed storage, enterprise identity, automatic production repair and arbitrary generated code are outside the MVP. See [limitations](LIMITATIONS.md).

## Commands

`nlpipe create`, `explain`, `validate`, `compile`, `test`, `run`, `diff`, `approve`, `deploy`, `status`, `ask`, `observe`, `repair`, `schema`, and `evaluate` each expose `--help`. `pipeline` is an alias for `nlpipe`.

Exit 0 means the requested operation completed, exit 2 indicates invalid/denied/failed work, and exit 3 requests clarification. Inspect the report for the gates actually run. A quarantined pipeline can succeed while recording a failed quality threshold.

## Validation and development

```bash
ruff check .
mypy src
pytest --cov=nlpipe --cov-report=term-missing
nlpipe evaluate corpus/acceptance.json --catalog examples/catalog.yaml \
  --out work/evaluation
python scripts/airflow_e2e.py /tmp/nlpipe-airflow-demo
python scripts/benchmark.py
python -m build
```

Tests assert exact records, graph structure, source/destination selection, schedules, quality outcomes, governance, clarification, credential rejection, path confinement, and approval invalidation. Airflow tests skip only when the optional dependency is absent; a mandatory Linux Airflow CI job runs those gates. See [VALIDATION.md](VALIDATION.md) for actual results, platform coverage, live-model evidence and limitations.

[Apache Airflow's stable Task SDK](https://airflow.apache.org/docs/task-sdk/stable/) is the authoring interface used by the compiler. The project is MIT licensed; all bundled data is synthetic and domain-neutral.
