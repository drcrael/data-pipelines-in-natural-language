import json
from datetime import UTC, datetime

import pytest

from nlpipe.catalog import Asset
from nlpipe.ir import QualityRule, TaskSpec
from nlpipe.observability import RunStatus, diagnose, from_airflow, propose_retry
from nlpipe.planner import normalize
from nlpipe.runtime import check_rows, execute_task, run_pipeline
from nlpipe.runtime.storage import read, rows_checked, write

pytestmark = pytest.mark.integration


def test_full_pipeline_exact_results(spec, catalog, data_root):
    result = run_pipeline(spec, catalog, data_root)
    assert result.status == RunStatus.SUCCESS
    assert [t.output_rows for t in result.tasks] == [4, 3, 2, 2]
    output = [
        json.loads(line) for line in (data_root / "output/analytics.jsonl").read_text().splitlines()
    ]
    assert output == [
        {"order_id": 1, "customer_id": 10, "amount": 12.5, "region": "west"},
        {"order_id": 2, "customer_id": 20, "amount": 20.0, "region": "east"},
    ]
    assert json.loads((data_root / "output/quarantine.jsonl").read_text())["order_id"] == 3
    assert result.quality[0].invalid == 1
    assert len(list((data_root / ".nlpipe/runs/orders_daily").glob("*.json"))) == 1
    before = (data_root / "output/analytics.jsonl").read_bytes()
    run_pipeline(spec, catalog, data_root)
    assert (data_root / "output/analytics.jsonl").read_bytes() == before


def test_quality_failure_blocks_output(spec, catalog, data_root):
    spec.quality_rules[0].action = "fail"
    spec.quality_rules[0].quarantine_asset = None
    result = run_pipeline(spec, catalog, data_root)
    assert result.status == RunStatus.FAILED
    assert result.tasks[-1].status == RunStatus.SKIPPED
    assert not (data_root / "output/analytics.jsonl").exists()
    assert "QualityFailure" in diagnose(result, "why did it fail?")
    proposal = propose_retry(normalize(spec), result)
    assert proposal.proposed.retry_policy.retries == 3
    assert proposal.status == "PROPOSED"
    with pytest.raises(ValueError):
        propose_retry(spec, result)


@pytest.mark.parametrize(
    "kind,field,params,rows,invalid",
    [
        ("not_null", "id", {}, [{"id": None}, {"id": 1}], 1),
        ("unique", "id", {}, [{"id": 1}, {"id": 1}, {"id": 2}], 1),
        ("range", "x", {"min": 0, "max": 10}, [{"x": -1}, {"x": True}, {"x": 5}], 2),
        (
            "freshness",
            "ts",
            {"max_age_seconds": 3600},
            [{"ts": "2025-01-01T00:30:00+00:00"}, {"ts": "bad"}],
            1,
        ),
        ("row_count", None, {"min": 1, "max": 3}, [], 0),
        ("schema", None, {}, [{"id": "wrong"}, {"id": 1}], 1),
    ],
)
def test_quality_ground_truth(kind, field, params, rows, invalid):
    rule = QualityRule(id="rule", task="read", kind=kind, field=field, parameters=params)
    bad, result = check_rows(rule, rows, {"id": "integer"}, datetime(2025, 1, 1, 1, tzinfo=UTC))
    assert len(bad) == invalid
    assert not result.passed


@pytest.mark.parametrize("kind", ["csv", "json", "jsonl", "parquet", "sqlite"])
def test_storage_roundtrip(kind, tmp_path):
    asset = Asset(
        identifier="dataset",
        type=kind,
        location="data." + kind,
        schema={"id": "integer", "name": "string"},
        allowed_operations=["read", "write"],
        table="records",
    )
    rows = [{"id": 1, "name": "A"}, {"id": 2, "name": "B"}]
    write(asset, tmp_path, rows)
    assert read(asset, tmp_path, 100) == rows
    write(asset, tmp_path, [{"id": 2, "name": "changed"}], mode="upsert", keys=["id"])
    assert read(asset, tmp_path, 100) == [rows[0], {"id": 2, "name": "changed"}]
    with pytest.raises(ValueError):
        read(asset, tmp_path, 1)


@pytest.mark.parametrize(
    "cap,params,inputs,expected",
    [
        (
            "transform.filter",
            {"field": "x", "operator": "gt", "value": 1},
            {"a": [{"x": 1}, {"x": 2}]},
            [{"x": 2}],
        ),
        (
            "transform.filter",
            {"field": "x", "operator": "not_null"},
            {"a": [{"x": None}, {"x": 2}]},
            [{"x": 2}],
        ),
        (
            "transform.normalize",
            {"fields": ["name"], "operation": "strip"},
            {"a": [{"name": " Ada "}]},
            [{"name": "Ada"}],
        ),
        ("transform.map", {"mapping": {"x": "y"}}, {"a": [{"x": 2}]}, [{"y": 2}]),
        (
            "transform.aggregate",
            {"group_by": ["region"], "field": "amount", "operation": "sum", "output": "total"},
            {"a": [{"region": "west", "amount": 2}, {"region": "west", "amount": 3}]},
            [{"region": "west", "total": 5}],
        ),
        ("transform.union", {}, {"a": [{"id": 1}], "b": [{"id": 2}]}, [{"id": 1}, {"id": 2}]),
        (
            "transform.join",
            {"left_key": "id", "right_key": "id", "how": "inner"},
            {"a": [{"id": 1, "x": 2}, {"id": 2, "x": 3}], "b": [{"id": 1, "y": 4}]},
            [{"id": 1, "x": 2, "y": 4}],
        ),
        (
            "enrich.lookup",
            {"left_key": "id", "right_key": "id", "how": "left"},
            {"a": [{"id": 2, "x": 3}], "b": [{"id": 1, "y": 4}]},
            [{"id": 2, "x": 3}],
        ),
        (
            "ops.checkpoint",
            {"expected_count": 2},
            {"a": [{"id": 1}], "b": [{"id": 2}]},
            [{"id": 1}, {"id": 2}],
        ),
    ],
)
def test_transform_results(cap, params, inputs, expected, spec, catalog, data_root):
    family = (
        "enrichment"
        if cap.startswith("enrich")
        else "operations"
        if cap.startswith("ops")
        else "transformation"
    )
    task = TaskSpec(
        id="transform",
        type=family,
        capability=cap + "@1",
        inputs=["task:" + key for key in inputs],
        parameters=params,
    )
    assert (
        execute_task(spec, catalog, task, inputs, data_root, "run", datetime.now(UTC), [])
        == expected
    )


def test_runtime_limits(spec, catalog, data_root):
    spec.resources.max_rows = 2
    for task in spec.tasks:
        task.resources.max_rows = 2
    result = run_pipeline(spec, catalog, data_root)
    assert result.status == RunStatus.FAILED
    assert result.tasks[0].status == RunStatus.FAILED
    with pytest.raises(ValueError):
        rows_checked([{"x": float("nan")}], 100)


def test_airflow_observation(spec):
    run = from_airflow(
        {
            "dag_id": spec.pipeline_id,
            "dag_run_id": "one",
            "state": "success",
            "task_instances": [{"task_id": "read_orders", "state": "success", "try_number": 1}],
        },
        spec,
    )
    assert run.status == RunStatus.SUCCESS
    assert "unresolved" in diagnose(run, "why?")
    with pytest.raises(ValueError):
        from_airflow({"dag_id": "wrong", "state": "success"}, spec)
    with pytest.raises(ValueError):
        from_airflow({"dag_id": spec.pipeline_id, "state": "unknown"}, spec)
    with pytest.raises(ValueError):
        propose_retry(spec, run)


def test_notifications(spec, catalog, data_root):
    from nlpipe.ir import NotificationPolicy

    spec.notifications = [
        NotificationPolicy(asset="data_team", on="quality", threshold=0.01),
        NotificationPolicy(asset="data_team", on="success"),
    ]
    run = run_pipeline(spec, catalog, data_root)
    assert run.status == RunStatus.SUCCESS
    events = [json.loads(p.read_text())["event"] for p in (data_root / "outbox").glob("*.json")]
    assert sorted(events) == ["quality", "success"]


def test_multisource_join_aggregate_fanout(catalog, data_root):
    from pathlib import Path

    from nlpipe.ir import load

    spec = load(Path(__file__).parents[1] / "examples/region_revenue.yaml")
    run = run_pipeline(spec, catalog, data_root)
    assert run.status == RunStatus.SUCCESS
    expected = [{"region": "east", "revenue": 20.0}, {"region": "west", "revenue": 12.5}]
    assert [
        json.loads(line) for line in (data_root / "output/analytics.jsonl").read_text().splitlines()
    ] == expected
    import pyarrow.parquet as pq

    assert pq.read_table(data_root / "output/data.parquet").to_pylist() == expected
