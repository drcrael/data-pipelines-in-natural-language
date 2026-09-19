"""Execute accepted live copy-audit proposals against independent exact-record fixtures."""

import argparse
import json
import shutil
import tempfile
from pathlib import Path

from nlpipe.catalog import load_catalog
from nlpipe.ir import PipelineSpec
from nlpipe.runtime import run_pipeline

parser = argparse.ArgumentParser()
parser.add_argument("report", type=Path)
parser.add_argument("--out", type=Path, required=True)
args = parser.parse_args()
report = json.loads(args.report.read_text())
if not report["passed"]:
    raise SystemExit("Cannot certify a failed model audit")
catalog = load_catalog(Path("examples/catalog.yaml"))
catalog.assets = [a for a in catalog.assets if a.identifier in {"orders", "analytics"}]
expected = [
    {"order_id": 1, "customer_id": 10, "amount": 12.5, "region": "west"},
    {"order_id": 1, "customer_id": 10, "amount": 12.5, "region": "west"},
    {"order_id": 2, "customer_id": 20, "amount": 20.0, "region": "east"},
    {"order_id": 3, "customer_id": None, "amount": 4.5, "region": "west"},
]
results = []
for case in report["cases"]:
    spec = PipelineSpec.model_validate(case["result"]["spec"])
    with tempfile.TemporaryDirectory(prefix="nlpipe-live-replay-") as directory:
        root = Path(directory)
        shutil.copytree("examples/fixtures", root, dirs_exist_ok=True)
        attempts = []
        for _ in range(2):
            run = run_pipeline(spec, catalog, root)
            output = root / "output/analytics.jsonl"
            rows = (
                [json.loads(line) for line in output.read_text().splitlines()]
                if output.exists()
                else []
            )
            attempts.append(run.status.value == "success" and rows == expected)
        results.append(
            {"case": case["case"], "first_execution": attempts[0], "replace_on_rerun": attempts[1]}
        )

payload = {
    "model": report["model"],
    "cases": results,
    "passed": len(results) == 3
    and all(r["first_execution"] and r["replace_on_rerun"] for r in results),
}
args.out.parent.mkdir(parents=True, exist_ok=True)
args.out.write_text(json.dumps(payload, indent=2) + "\n")
print(json.dumps(payload, indent=2))
raise SystemExit(0 if payload["passed"] else 1)
