"""Run the generated DAG through Airflow's real local task runner on synthetic data."""

import json
import os
import runpy
import shutil
import subprocess
import sys
from pathlib import Path

import pendulum

from nlpipe.catalog import load_catalog
from nlpipe.compiler import compile_airflow
from nlpipe.ir import load

root = Path(sys.argv[1]).resolve()
root.mkdir(parents=True, exist_ok=False)
repository = Path(__file__).resolve().parents[1]
shutil.copytree(repository / "examples/fixtures", root / "data")
os.environ["AIRFLOW_HOME"] = str(root / "airflow")
os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = str(root / "dags")
(root / "dags").mkdir()
os.environ["NLPIPE_DATA_ROOT"] = str(root / "data")
os.environ["NLPIPE_CATALOG"] = str(repository / "examples/catalog.yaml")
subprocess.run([sys.executable, "-m", "airflow", "db", "migrate"], check=True)
spec = load(repository / "examples/orders.yaml")
dag_path = root / "dags/orders_daily.py"
dag_path.write_text(compile_airflow(spec, load_catalog(repository / "examples/catalog.yaml")))
dag = runpy.run_path(str(dag_path))["dag"]
run = dag.test(logical_date=pendulum.datetime(2025, 1, 2, 2, tz="UTC"))
rows = [
    json.loads(line) for line in (root / "data/output/analytics.jsonl").read_text().splitlines()
]
assert run.state == "success", run.state
assert [row["order_id"] for row in rows] == [1, 2]
(root / "airflow-e2e.json").write_text(
    json.dumps(
        {
            "state": run.state,
            "output_rows": len(rows),
            "order_ids": [row["order_id"] for row in rows],
        },
        indent=2,
    )
)
print("AIRFLOW_E2E_PASS")
