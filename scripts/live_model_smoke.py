"""Optional real model check; never substitute canned responses for inference."""

import argparse
import json
import time
from pathlib import Path

from nlpipe.catalog import load_catalog
from nlpipe.intent import OpenAICompatibleProvider, ProviderConfig, explain, interpret
from nlpipe.validation import validate
from nlpipe.verification import verify

parser = argparse.ArgumentParser()
parser.add_argument("--model", default="qwen2.5:3b")
parser.add_argument("--base-url", default="http://127.0.0.1:11434/v1")
parser.add_argument("--out", type=Path, required=True)
args = parser.parse_args()
catalog = load_catalog(Path("examples/catalog.yaml"))
# A focused catalog is an administrator scope restriction, not an expected answer.
catalog.assets = [a for a in catalog.assets if a.identifier in {"orders", "analytics"}]
provider = OpenAICompatibleProvider(
    ProviderConfig(base_url=args.base_url, model=args.model, json_schema=True, timeout_seconds=900)
)
requests = [
    "Create an ad-hoc development pipeline named orders_copy, pipeline_id orders_copy. Read all orders records and write them to analytics, replacing its contents. No transformations or quality checks. Use UTC and no catchup.",
    "Make a development pipeline, orders_copy, that copies the complete orders dataset to analytics on demand. Replace the destination; leave the rows unchanged. UTC, catchup false.",
]
results = []
first_spec = None
for index, prompt in enumerate(requests):
    print(f"Running live model case {index + 1}", flush=True)
    started = time.monotonic()
    result = interpret(prompt, catalog, provider)
    checks = {"ready": result.status == "ready"}
    if result.spec:
        spec = result.spec
        checks.update(
            sources=[s.asset for s in spec.sources] == ["orders"],
            destinations=[d.asset for d in spec.destinations] == ["analytics"],
            schedule=spec.schedule.cron is None,
            capabilities=sorted(t.capability for t in spec.tasks)
            == ["ingest.csv@1", "output.jsonl@1"],
            mode=spec.destinations[0].mode == "replace",
            validation=validate(spec, catalog).valid,
            airflow=verify(spec, catalog, airflow=True)["passed"],
        )
        if first_spec is None:
            first_spec = spec
    results.append(
        {
            "case": index + 1,
            "checks": checks,
            "elapsed_seconds": time.monotonic() - started,
            "result": result.model_dump(mode="json"),
        }
    )
    print(json.dumps({"case": index + 1, "checks": checks, "errors": result.errors}), flush=True)
if first_spec:
    print("Running live explanation round-trip", flush=True)
    result = interpret(
        "Reconstruct the pipeline described below. Preserve every stated semantic requirement.\n"
        + explain(first_spec),
        catalog,
        provider,
    )
    checks = {"ready": result.status == "ready"}
    if result.spec:
        from nlpipe.evaluation import properties

        expected = properties(
            type("Result", (), {"spec": first_spec, "status": "ready", "questions": []})(), catalog
        )
        actual = properties(result, catalog)
        checks["semantic_round_trip"] = all(
            expected[k] == actual[k]
            for k in ["assets", "capabilities", "schedule", "quality", "policy", "graph"]
        )
    results.append(
        {"case": "round_trip", "checks": checks, "result": result.model_dump(mode="json")}
    )
report = {
    "provider": provider.name,
    "model": args.model,
    "cases": results,
    "passed": len(results) == 3 and all(all(r["checks"].values()) for r in results),
    "scope": "Two live paraphrases plus one explanation round-trip; not a broad LLM accuracy benchmark.",
}
args.out.parent.mkdir(parents=True, exist_ok=True)
args.out.write_text(json.dumps(report, indent=2) + "\n")
print(json.dumps({"passed": report["passed"], "checks": [r["checks"] for r in results]}, indent=2))
raise SystemExit(0 if report["passed"] else 1)
