# MVP delivery map

| Requirement | Delivered behavior / evidence |
|---|---|
| Domain-neutral architecture | Synthetic retail, weather, inventory and support assets; no external dataset dependency |
| Typed, versioned IR | Closed Pydantic models, JSON Schema, JSON/YAML, explicit migration rejection, full IR goldens |
| Capability registry | Versioned metadata and closed parameter schemas; unavailable adapters fail explicitly |
| Natural-language compiler | Offline controlled vocabulary plus provider-neutral OpenAI-compatible structured generation |
| Clarification | Structured questions and exit 3; no artifact for unresolved requests |
| Catalog and resolution | Trusted local JSON/YAML, aliases, schemas, operation permissions, secret references |
| Planner | Stable topological order, reference/cycle checks, explicit quality barriers, derived lineage |
| Validation / governance | Structural, semantic, operational, quality and security checks; PASS/WARN/FAIL/approval decisions |
| Airflow | Deterministic TaskFlow generation, real imports/graph checks and a full DAG.test execution |
| Execution | Bounded file/SQLite/HTTPS reads, table transforms, local writes, quality/quarantine, local notification outbox |
| Human review | Expiring HMAC approvals bound to IR, catalog and environment; checked again by workers |
| Conversation and explanation | Previous-IR modifications, diffs, deterministic prose; actual-model round-trip audit reported separately |
| Observation / repair | Backend-neutral models, Airflow exported-state adapter, evidence-based summaries, draft retry proposal |
| Testing | Unit, integration, security, property, CLI acceptance, full IR goldens and real Airflow gates |
| Corpus and evaluation | 50 cases; 8 separately reported dimensions; 6 paraphrases; no mock-as-model accuracy claims |
| Packaging | Wheel/source distributions, clean installs, checksum manifest, cross-platform release audit |
| Manual | 14-page PDF plus editable Markdown source and repeatable PDF builder |

See LIMITATIONS.md for integrations intentionally represented as unavailable interfaces, deployment assumptions, and performance limits. The release does not claim implementation of every future adapter or general language accuracy.
