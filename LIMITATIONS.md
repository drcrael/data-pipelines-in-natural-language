# MVP boundaries

This release is a production-oriented reference implementation for bounded data pipelines in an administrator-controlled environment. It is not a distributed data engine, a multi-tenant sandbox, or a guarantee of arbitrary natural-language accuracy.

## Language and evaluation

The offline frontend implements a limited compositional vocabulary. It has no language model and no corpus lookup. The 50-case offline corpus contains 20 accepted cases with exact semantic checks and 30 clarification/rejection cases, including requests whose integrations are unavailable. Eighteen paraphrases across three canonical pipelines check normalization invariance. These results do not establish broad language understanding. Richer requests require an OpenAI-compatible model or explicit IR authoring and representative semantic evaluation. Model output can be structurally valid and semantically wrong; review remains necessary.

Mock providers test protocol and validation boundaries only. The separate live-model smoke audit has a narrow, explicitly reported scope. The complete corpus can be evaluated with any configured model using `nlpipe evaluate --config`; full cross-model quality is not established by a smoke test. Automatic model-output correction/retry is intentionally absent.

## Adapters

Implemented IO: local CSV/JSON/JSONL/Parquet, SQLite registered tables, and explicitly enabled HTTPS JSON-array reads. HTTP has no pagination and no API writes. PostgreSQL, object storage, raw SQL transforms, arbitrary Python callables, API outputs, embeddings, LLM enrichment, entity resolution, distribution/anomaly adapters, retention and arbitrary branch operators are extension entries that fail validation. There is no hidden no-op execution path for these entries.

The checkpoint capability verifies supplied upstream dataset count. It does not wait for files. Runtime parameters and persistent incremental checkpoints are rejected. Previous-calendar-day windows require an explicit timestamp field. Backfill scheduling is represented by Airflow catchup/start date; the offline frontend asks for more configuration rather than guessing.

## Runtime and deployment

Tables are materialized in memory and serialized artifacts have a 32 MiB limit. Default maximum rows: 100,000; configurable ceiling: 1,000,000. This is suitable for small datasets and verification, not bulk enterprise ETL. Local runs retain intermediate tables in memory. Airflow tasks exchange artifact references and require a shared, administrator-controlled filesystem.

The local runner executes once per task and detects elapsed-time overruns after return. Airflow provides actual scheduling, retry delays and task timeout enforcement. CPU/memory declarations are metadata; worker resource isolation must be configured in Airflow. By default, one active DAG run is configured. Increasing run concurrency or letting separate DAGs write the same destination requires administrative coordination to enforce a single writer. Cross-file fan-out is not an atomic distributed transaction.

Replace mode intentionally overwrites its registered output. Append is non-idempotent and cannot claim otherwise. Upsert merges by explicit keys. SQLite replacement owns and recreates the destination table; unmanaged table constraints, triggers and indexes are not preserved. Quarantine outputs are replaced, not retained historically. Schema-changing quality checks require an explicit expected schema.

Notifications are local JSON outbox records; external delivery is an adapter responsibility. DAG-folder deployment does not provision or start Airflow and does not prove scheduler pickup. Airflow is tested on Linux; authoring and local file runtime are tested across operating systems. Airflow callbacks and workers must receive the same trusted catalog, package and runtime configuration.

## Governance and observation

Approvals use a shared HMAC key and attributed actor labels. They are not enterprise authentication or separation of duties. Administrators with key/file access are trusted. Expiry is checked at execution, so scheduled runs may require renewed approvals. Pipeline/catalog edits invalidate approvals. Policies have a small fixed YAML-representable rule vocabulary; custom policies require trusted Python implementation and tests.

Airflow observations are imported from exported state, not continuously polled. `ask` summarizes supplied failure evidence and explicitly leaves causes unresolved when unavailable. Repair is a proposed bounded retry change, not a diagnosis or automatic remediation. The `agentic-data-ops` reference repository is not a runtime dependency; a live DataOps adapter remains future work.
