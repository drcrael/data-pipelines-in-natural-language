import json
from datetime import UTC, datetime

import pytest

from nlpipe.ir import digest
from nlpipe.planner import normalize
from nlpipe.runtime import airflow_execute, airflow_notify
from nlpipe.runtime.storage import read_artifact

pytestmark = pytest.mark.integration


def test_worker_execution_exact_artifacts(spec, catalog, data_root, tmp_path, monkeypatch):
    path = tmp_path / "catalog.json"
    path.write_text(catalog.model_dump_json(by_alias=True))
    monkeypatch.setenv("NLPIPE_CATALOG", str(path))
    monkeypatch.setenv("NLPIPE_DATA_ROOT", str(data_root))
    spec = normalize(spec)
    outputs = {}
    for task in spec.tasks:
        upstream = {r[5:]: outputs[r[5:]] for r in task.inputs if r.startswith("task:")}
        outputs[task.id] = airflow_execute(
            spec.model_dump(mode="json"),
            digest(catalog),
            task.id,
            upstream,
            "scheduled__2025-01-01",
            "2025-01-02T00:00:00+00:00",
        )
    rows = read_artifact(data_root, outputs["write_analytics"], 100)
    assert [r["order_id"] for r in rows] == [1, 2]
    assert all(len(json.dumps(ref)) < 500 for ref in outputs.values())
    with pytest.raises(ValueError):
        airflow_execute(
            spec.model_dump(mode="json"),
            "wrong",
            "read_orders",
            {},
            "one",
            "2025-01-02T00:00:00+00:00",
        )
    with pytest.raises(ValueError):
        airflow_notify(spec.model_dump(mode="json"), "wrong", "failure", {})


def test_worker_notifications(spec, catalog, data_root, tmp_path, monkeypatch):
    from nlpipe.ir import NotificationPolicy

    spec.notifications = [NotificationPolicy(asset="data_team", on="failure")]
    path = tmp_path / "catalog.json"
    path.write_text(catalog.model_dump_json(by_alias=True))
    monkeypatch.setenv("NLPIPE_CATALOG", str(path))
    monkeypatch.setenv("NLPIPE_DATA_ROOT", str(data_root))
    airflow_notify(spec.model_dump(mode="json"), digest(catalog), "failure", {"run_id": "safe"})
    event = json.loads(next((data_root / "outbox").glob("*.json")).read_text())
    assert event["event"] == "failure"


def test_window_read(spec, catalog, data_root):
    from nlpipe.catalog import Asset
    from nlpipe.ir import SourceSpec, TaskSpec
    from nlpipe.runtime import execute_task

    asset = Asset(
        identifier="timed",
        type="json",
        location="timed.json",
        schema={"ts": "datetime"},
        allowed_operations=["read"],
    )
    catalog.assets.append(asset)
    (data_root / "timed.json").write_text(
        json.dumps([{"ts": "2025-01-01T00:00:00+00:00"}, {"ts": "2025-01-02T00:00:00+00:00"}])
    )
    spec.sources = [SourceSpec(asset="timed", incremental_field="ts", window="previous_day")]
    task = TaskSpec(id="read", type="ingestion", capability="ingest.json@1", inputs=["asset:timed"])
    rows = execute_task(
        spec, catalog, task, {}, data_root, "run", datetime(2025, 1, 2, tzinfo=UTC), []
    )
    assert rows == [{"ts": "2025-01-01T00:00:00+00:00"}]
