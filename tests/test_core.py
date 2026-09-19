import ast
from datetime import datetime

import pytest
from hypothesis import given
from hypothesis import strategies as st
from pydantic import ValidationError

from nlpipe.capabilities import Capability, builtin_registry, schema
from nlpipe.compiler import compile_airflow
from nlpipe.ir import (
    PipelineSpec,
    ScheduleSpec,
    TaskSpec,
    canonical,
    diff,
    digest,
    load,
    migrate,
    save,
)
from nlpipe.planner import normalize, order
from nlpipe.validation import validate
from nlpipe.verification import generated_test, verify

pytestmark = pytest.mark.unit


def test_roundtrip_json_yaml(spec, tmp_path):
    for extension in ("json", "yaml"):
        path = tmp_path / ("pipeline." + extension)
        save(spec, path)
        assert load(path) == spec
    assert PipelineSpec.model_validate_json(canonical(spec)) == spec
    assert PipelineSpec.model_json_schema()["additionalProperties"] is False


@pytest.mark.parametrize("schedule", ["* * *", "61 * * * *", "* * * * * *", "banana", "0 24 * * *"])
def test_bad_schedules(schedule):
    with pytest.raises(ValidationError):
        ScheduleSpec(cron=schedule)


@pytest.mark.parametrize(
    "updates",
    [
        dict(version="2"),
        dict(pipeline_id="../evil"),
        dict(concurrency=0),
        dict(start_date=datetime(2025, 1, 1)),
        dict(timezone="Bogus/Time"),
        dict(approved=True),
    ],
)
def test_closed_ir(spec, updates):
    with pytest.raises((ValueError, KeyError)):
        PipelineSpec.model_validate(spec.model_dump() | updates)


def test_version_hook():
    with pytest.raises(ValueError):
        migrate({"version": "2.0"})
    assert migrate({"version": "1.0"}) == {"version": "1.0"}


def test_normalization(spec, catalog):
    normalized = normalize(spec)
    assert normalize(normalized) == normalized
    assert [t.id for t in normalized.tasks] == [
        "read_orders",
        "deduplicate",
        "q_deduplicate",
        "write_analytics",
    ]
    assert normalized.tasks[-1].inputs == ["task:q_deduplicate"]
    assert normalized.tasks[-1].dependencies == ["q_deduplicate"]
    assert ("task:q_deduplicate", "asset:quarantine") in {
        (e.source, e.destination) for e in normalized.lineage
    }
    assert validate(spec, catalog).valid


def test_determinism_order_independent(spec, catalog):
    other = spec.model_copy(deep=True)
    other.tasks.reverse()
    assert compile_airflow(spec, catalog) == compile_airflow(other, catalog)
    assert ast.parse(compile_airflow(spec, catalog))
    assert "get_current_context" in compile_airflow(spec, catalog)
    assert "dag.task_ids" in generated_test(spec, catalog)


@pytest.mark.parametrize(
    "kind",
    [
        "cycle",
        "missing",
        "duplicate",
        "unknown_capability",
        "mismatch",
        "unknown_asset",
        "parameters",
        "barrier_collision",
        "missing_output",
        "multiple_writers",
    ],
)
def test_invalid_graph_semantics(spec, catalog, kind):
    if kind == "cycle":
        spec.tasks[0].dependencies = ["write_analytics"]
    if kind == "missing":
        spec.tasks[0].dependencies = ["missing"]
    if kind == "duplicate":
        spec.tasks.append(spec.tasks[0])
    if kind == "unknown_capability":
        spec.tasks[0].capability = "ingest.unknown@1"
    if kind == "mismatch":
        spec.tasks[0].type = "output"
    if kind == "unknown_asset":
        spec.sources[0].asset = "absent"
    if kind == "parameters":
        spec.tasks[1].parameters = {"keys": 42}
    if kind == "barrier_collision":
        spec.tasks.append(TaskSpec(id="q_deduplicate", type="ingestion", capability="ingest.csv@1"))
    if kind == "missing_output":
        spec.tasks[-1].outputs = []
    if kind == "multiple_writers":
        spec.tasks.append(spec.tasks[-1].model_copy(update={"id": "write_again"}))
    report = validate(spec, catalog)
    assert not report.valid
    with pytest.raises(ValueError):
        compile_airflow(spec, catalog)


@pytest.mark.parametrize(
    "changes",
    [
        dict(field="absent"),
        dict(kind="range", parameters={"min": 5, "max": 1}),
        dict(kind="freshness", parameters={}),
        dict(kind="row_count", parameters={"min": -1}),
        dict(kind="distribution"),
        dict(retention_days=30),
    ],
)
def test_quality_validation(spec, catalog, changes):
    spec.quality_rules[0] = spec.quality_rules[0].model_copy(update=changes)
    assert not validate(spec, catalog).valid


def test_registry():
    registry = builtin_registry()
    assert len(registry.context()) >= 40
    assert not registry.get("enrich.embeddings@1").implemented
    with pytest.raises(ValueError):
        registry.get("invented@1")
    with pytest.raises(ValueError):
        registry.register(registry.get("ingest.csv@1"))
    registry.register(Capability("custom.read", "ingestion", "Example", schema()))
    assert registry.get("custom.read@1").deterministic


@given(st.lists(st.integers(min_value=0, max_value=9), min_size=1, max_size=30))
@pytest.mark.property
def test_topological_property(numbers):
    tasks = [
        TaskSpec(
            id=f"t{i}",
            type="transformation",
            capability="transform.union@1",
            dependencies=[f"t{j}" for j in range(i) if j in numbers],
        )
        for i in range(10)
    ]
    sequence = order(tasks)
    position = {v: i for i, v in enumerate(sequence)}
    assert all(position[d] < position[t.id] for t in tasks for d in t.dependencies)


@given(st.text(min_size=1, max_size=80))
@pytest.mark.property
def test_unknown_capability_never_executable(name):
    if name not in builtin_registry().entries:
        with pytest.raises(ValueError):
            builtin_registry().get(name)


def test_diff(spec):
    other = spec.model_copy(deep=True)
    other.schedule.cron = "0 3 * * *"
    assert diff(spec, other) == {
        "schedule": {"before": {"cron": "0 2 * * *"}, "after": {"cron": "0 3 * * *"}}
    }
    assert digest(spec) != digest(other)


@pytest.mark.airflow
def test_real_airflow_import(spec, catalog):
    result = verify(spec, catalog, airflow=True)
    assert result["passed"], result
    assert result["checks"]["airflow_semantics"]


def test_load_limits(tmp_path):
    p = tmp_path / "bad.yaml"
    p.write_text("[]")
    with pytest.raises(ValueError):
        load(p)
    p.write_text("x" * 2_000_001)
    with pytest.raises(ValueError):
        load(p)


def test_operational_validation(spec, catalog):
    spec.destinations[0].mode = "append"
    assert not validate(spec, catalog).valid
    spec.execution_policy.idempotent = False
    assert validate(spec, catalog).valid
    spec.destinations[0].mode = "upsert"
    assert not validate(spec, catalog).valid
