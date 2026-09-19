"""Layered artifact verification. Missing Airflow is a failed requested gate, never a pass."""

import ast
import importlib.metadata
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

from nlpipe.catalog import Catalog
from nlpipe.compiler import compile_airflow
from nlpipe.ir import PipelineSpec, digest
from nlpipe.validation import validate


def verify(spec: PipelineSpec, catalog: Catalog, airflow: bool = False) -> dict:
    report = validate(spec, catalog)
    checks = {"ir_graph_policy": report.valid}
    details = list(report.errors)
    if report.valid:
        code = compile_airflow(spec, catalog)
        checks["python_syntax"] = isinstance(ast.parse(code), ast.Module)
        checks["deterministic_compiler"] = code == compile_airflow(spec, catalog)
        for package in ("pydantic", "PyYAML", "httpx", "croniter", "jsonschema"):
            try:
                importlib.metadata.version(package)
                checks["dependency:" + package] = True
            except importlib.metadata.PackageNotFoundError:
                checks["dependency:" + package] = False
        if airflow:
            with tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "pipeline.py"
                path.write_text(code)
                inspection = """import json, runpy, sys
ns = runpy.run_path(sys.argv[1])
dag = ns["dag"]
assert dag.dag_id == sys.argv[2]
print("NLPIPE_RESULT=" + json.dumps({"tasks": sorted(dag.task_ids), "edges": sorted([list(e) for e in dag.edge_info]) if False else sorted([[t.task_id, downstream] for t in dag.tasks for downstream in t.downstream_task_ids])}))
"""
                env = dict(os.environ, AIRFLOW_HOME=directory, AIRFLOW__CORE__LOAD_EXAMPLES="False")
                try:
                    process = subprocess.run(
                        [sys.executable, "-c", inspection, str(path), spec.pipeline_id],
                        capture_output=True,
                        text=True,
                        timeout=60,
                        env=env,
                    )
                    lines = [
                        line
                        for line in process.stdout.splitlines()
                        if line.startswith("NLPIPE_RESULT=")
                    ]
                    if process.returncode or not lines:
                        checks["airflow_import"] = False
                        details.append(
                            "Airflow import failed; install the tested Airflow extra and inspect the artifact locally"
                        )
                    else:
                        structure = json.loads(lines[-1].split("=", 1)[1])
                        normalized = report.normalized
                        assert normalized is not None
                        expected_edges = sorted(
                            [
                                [upstream, t.id]
                                for t in normalized.tasks
                                for upstream in t.dependencies
                            ]
                        )
                        checks["airflow_import"] = True
                        checks["airflow_semantics"] = structure == {
                            "tasks": sorted(t.id for t in normalized.tasks),
                            "edges": expected_edges,
                        }
                except subprocess.TimeoutExpired:
                    checks["airflow_import"] = False
                    details.append("Airflow import timed out")
    return {
        "passed": all(checks.values()),
        "checks": checks,
        "details": details,
        "airflow_requested": airflow,
        "spec_hash": digest(report.normalized) if report.normalized else None,
    }


def generated_test(spec: PipelineSpec, catalog: Catalog) -> str:
    """Generate a standalone semantic DAG regression test for a reviewed normalized IR."""
    normalized = validate(spec, catalog).normalized
    if normalized is None:
        raise ValueError("Cannot generate tests for invalid IR")
    edges = sorted((d, t.id) for t in normalized.tasks for d in t.dependencies)
    return f"""import runpy

def test_generated_dag():
    dag = runpy.run_path({spec.pipeline_id + ".py"!r})['dag']
    assert dag.dag_id == {spec.pipeline_id!r}
    assert sorted(dag.task_ids) == {sorted(t.id for t in normalized.tasks)!r}
    assert sorted((t.task_id, d) for t in dag.tasks for d in t.downstream_task_ids) == {edges!r}
"""
