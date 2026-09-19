"""Repeatable synthetic small-data runtime benchmark; not a production SLA."""

import csv
import json
import tempfile
import time
import tracemalloc
from pathlib import Path

from nlpipe.catalog import load_catalog
from nlpipe.ir import load
from nlpipe.runtime import run_pipeline

repo = Path(__file__).resolve().parents[1]
catalog = load_catalog(repo / "examples/catalog.yaml")
spec = load(repo / "examples/orders.yaml")
with tempfile.TemporaryDirectory() as directory:
    root = Path(directory)
    with (root / "orders.csv").open("w", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(["order_id", "customer_id", "amount", "region"])
        for i in range(100000):
            writer.writerow([i % 50000, i % 100, 1.5, "west"])
    tracemalloc.start()
    start = time.perf_counter()
    run = run_pipeline(spec, catalog, root)
    elapsed = time.perf_counter() - start
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    assert run.status == "success"
    assert run.tasks[-1].output_rows == 50000
    report = {
        "input_rows": 100000,
        "output_rows": 50000,
        "elapsed_seconds": elapsed,
        "peak_python_traced_mib": peak / (1024**2),
        "memory_scope": "Python traced allocations; excludes native-library and interpreter memory",
    }
    (repo / "docs/benchmark.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))
