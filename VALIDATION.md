# Release validation

The v0.1.0 release is a bounded-data MVP. The following measurements distinguish deterministic software validation from actual model inference. The final live-model acceptance gate passed; scope and retained development failures are reported below.

## Software and runtime gates

- Linux / Python 3.12.14: **134 tests passed**, **90.44% line coverage** (1,758 statements). Ruff and mypy pass; 18 source files type-checked. Dependency consistency checks pass.
- **40 security tests** cover credential rejection, code/prompt injection, catalog permissions, path traversal and symlinks, untrusted responses, endpoint restrictions, and approval binding/invalidation.
- **50/50 controlled-language corpus outcomes** match expectations: 20 accepted pipelines and 30 clarification/rejection cases. Each scored accepted dimension passes 20/20. These are not 50 executable pipelines and are not LLM accuracy measurements. [Machine-readable results](docs/evaluation/eval-results.json) and [dimension report](docs/evaluation/eval-report.md).
- **18 paraphrases across three canonical pipelines** are compared against independent complete IR goldens. Property tests also check deterministic ordering, graph normalization and malformed inputs.
- A real **Airflow 3.1.8** import validates the emitted DAG and dependencies. A full `DAG.test` execution through the Airflow task runner produces two expected valid rows and quarantines the invalid row. [Execution evidence](docs/airflow-e2e.json). Generated semantic tests also run independently of the caller's working directory.
- [Cross-platform validation](https://github.com/drcrael/data-pipelines-in-natural-language/actions/runs/35476525015) passes all seven jobs: Linux, Windows and macOS on Python 3.11 and 3.14, plus the Linux/Python 3.12 real-Airflow job. Windows initially exposed missing timezone data; the explicit `tzdata` dependency fixes it.
- Clean wheel and source installations each pass **131 tests**, with **88.68% coverage**, fixture execution and 50/50 corpus outcomes outside the source checkout. Environments without Airflow skip its three optional tests; the dedicated Airflow gate above executes them.

## Performance observation

A local synthetic 100,000-row deduplication pipeline produces 50,000 rows in **4.84 seconds**, with **41.48 MiB peak Python-traced allocations**. This excludes native-library and interpreter memory and is not a distributed throughput claim. [Raw result](docs/benchmark.json); reproduce with `python scripts/benchmark.py`.

## Actual model evaluation

**PASS: Qwen2.5 3B**, using structured outputs, a 16,384-token Ollama context and a 900-second request budget. [Live workflow](https://github.com/drcrael/data-pipelines-in-natural-language/actions/runs/35476525178), [inference report](docs/live-model.json), [raw synthetic responses](docs/live-model-raw.json), and [execution replay](docs/live-model-execution.json). Both paraphrases passed every check. Both explanation round-trips preserved graph and effective execution settings, including zero retries, disabled backoff and nondefault 120/900-second timeouts. All four generated proposals produced the four exact expected records on both first execution and replacement rerun. CPU inference took 189.39 seconds for the first paraphrase and 38.85 seconds for the second; this is not an interactive latency claim.

Live inference uses a local Ollama service on a GitHub-hosted Linux runner and synthetic catalog metadata. No response fixtures replace inference. Two copy-pipeline paraphrases must pass source/destination, capability, write mode, schedule, validation and real Airflow import checks; two further inferences reconstruct default and nondefault explanations and must preserve semantics.

Development audits exposed real failures; all are retained rather than counted as successful inference:

| Evidence | Observed failure / resulting correction |
|---|---|
| [Initial JSON response](docs/live-model-initial.json) | Invalid response schemas; added constrained generation |
| [Graph endpoints](docs/live-model-graph-rejection.json) | Invalid dependency references; derive candidate edges from task inputs |
| [3B task roles](docs/live-model-small-model.json), [7B task roles](docs/live-model-role-rejection.json) | Invalid read/write roles; encoded existing role rules in the generation schema |
| [Missing outputs](docs/live-model-missing-output.json), [raw responses](docs/live-model-missing-output-raw.json) | Incomplete declarations and invented operations; added a generic worked example and required destination declarations |
| [Policy audit](docs/live-model-policy-rejection.json) | Unrequested policy changes; preserved explicit defaults and strengthened operational round-trip checks |
| [7B CPU audit](docs/live-model-large-timeout.json) | A request exceeded the 900-second budget; the complete audit failed |
| [Retry drift](docs/live-model-retry-drift.json) | A nondefault retry policy was omitted on round-trip; made generated execution settings explicit and required |
| [Explanation screen](docs/live-model-explanation-rejection.json) | Both paraphrases passed, but a harmless explanation label triggered the credential screen before inference; renamed the label and added a regression test |

No validator or execution approval gate was relaxed to accept these outputs. The earlier round-trip checks covered fewer semantic dimensions; the final audit also compares effective execution settings and replays exact records twice. These are development acceptance cases, not a held-out accuracy benchmark. A narrow passing smoke test cannot establish general language accuracy or qualify arbitrary models. The passing final run is linked above. No broader model accuracy or 7B qualification is claimed.

## Reproduction and release artifacts

Use the commands in README.md. The repository includes all synthetic fixtures, corpus cases, independent goldens, benchmark and Airflow scripts. `docs/dependencies-linux.txt` records the local validation environment. The release includes a wheel, source distribution, manual and SHA-256 manifest. The release-artifact workflow verifies checksums, installs the exact published wheel and source archive separately, and runs tests, fixture execution and corpus evaluation on three operating systems and two Python versions (12 jobs).

These gates establish the documented MVP behavior, not general natural-language accuracy or readiness for arbitrary production infrastructure. See LIMITATIONS.md for unavailable adapters, bounded memory/storage, shared-filesystem deployment, approval identity assumptions and the DataOps integration boundary.
