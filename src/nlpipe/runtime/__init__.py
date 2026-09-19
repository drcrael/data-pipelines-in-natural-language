"""Small-data reference runtime. Airflow schedules/retries tasks; rows stay in shared storage."""

from __future__ import annotations

import json
import operator
import os
import time
import uuid
from collections import defaultdict
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from nlpipe.catalog import Catalog, load_catalog
from nlpipe.deployment import Approval, atomic_write, check_approval
from nlpipe.ir import PipelineSpec, digest
from nlpipe.observability import (
    FailureEvent,
    LineageEvent,
    PipelineRun,
    QualityResult,
    RunStatus,
    TaskRun,
)
from nlpipe.runtime.storage import artifact, confined, read, read_artifact, rows_checked, write
from nlpipe.validation import require_valid


class QualityFailure(ValueError):
    pass


def check_rows(rule, rows, schema, now):
    invalid = []
    seen = set()
    for index, row in enumerate(rows):
        value = row.get(rule.field)
        good = True
        if rule.kind == "not_null":
            good = value is not None and value != ""
        elif rule.kind == "unique":
            key = json.dumps(value, sort_keys=True)
            good = value is not None and key not in seen
            seen.add(key)
        elif rule.kind == "range":
            good = (
                isinstance(value, (float, int))
                and not isinstance(value, bool)
                and rule.parameters["min"] <= value <= rule.parameters["max"]
            )
        elif rule.kind == "schema":
            types: dict[str, Any] = {
                "string": str,
                "integer": int,
                "number": (int, float),
                "boolean": bool,
                "datetime": str,
            }
            good = set(row) == set(schema) and all(
                row[k] is None
                or (
                    isinstance(row[k], types[v])
                    and not (v in {"integer", "number"} and isinstance(row[k], bool))
                )
                for k, v in schema.items()
            )
        elif rule.kind == "freshness":
            try:
                parsed = datetime.fromisoformat(value)
                good = (
                    parsed.tzinfo is not None
                    and 0 <= (now - parsed).total_seconds() <= rule.parameters["max_age_seconds"]
                )
            except (ValueError, TypeError):
                good = False
        elif rule.kind == "row_count":
            good = rule.parameters.get("min", 0) <= len(rows) <= rule.parameters.get("max", 1000000)
        else:
            raise ValueError("Quality adapter unavailable")
        if not good:
            invalid.append(index)
    count_ok = not (
        rule.kind == "row_count"
        and not rule.parameters.get("min", 0) <= len(rows) <= rule.parameters.get("max", 1000000)
    )
    rate = len(invalid) / len(rows) if rows else (0 if count_ok else 1)
    result = QualityResult(
        rule_id=rule.id,
        total=len(rows),
        invalid=len(invalid),
        invalid_rate=rate,
        passed=count_ok and rate <= rule.max_invalid_rate,
        quarantine_asset=rule.quarantine_asset,
    )
    return invalid, result


def _notify(spec, catalog, root, event, run_id, details):
    for policy in spec.notifications:
        if policy.on != event or (
            event == "quality" and details.get("invalid_rate", 0) <= policy.threshold
        ):
            continue
        asset = catalog.get(policy.asset)
        # MVP notifications are durable local outbox events, delivered by an administrator adapter.
        path = confined(root, asset.location) / f"{run_id}-{event}-{uuid.uuid4().hex}.json"
        atomic_write(
            path,
            json.dumps(
                {
                    "pipeline_id": spec.pipeline_id,
                    "run_id": run_id,
                    "event": event,
                    "details": details,
                }
            ),
        )


def execute_task(spec, catalog, task, inputs, root, run_id, now, quality_results, allow_http=False):
    limit = min(spec.resources.max_rows, task.resources.max_rows)
    name = task.capability.split("@")[0]
    p = task.parameters
    tables = [inputs[r[5:]] for r in task.inputs if r.startswith("task:")]
    rows = tables[0] if tables else []
    if task.type == "ingestion":
        asset = catalog.get(task.inputs[0][6:])
        rows = read(asset, root, limit, allow_http)
        source = next(s for s in spec.sources if s.asset == asset.identifier)
        if source.window == "previous_day":
            field = source.incremental_field
            from zoneinfo import ZoneInfo

            local = now.astimezone(ZoneInfo(spec.timezone))
            end = local.replace(hour=0, minute=0, second=0, microsecond=0)
            start = end - timedelta(days=1)
            filtered = []
            for row in rows:
                stamp = datetime.fromisoformat(row[field])
                if stamp.tzinfo is None:
                    raise ValueError("Window field must contain timezone-aware timestamps")
                if start <= stamp < end:
                    filtered.append(row)
            rows = filtered
        elif source.incremental_field:
            raise ValueError("Persistent incremental checkpoints require an adapter")
    elif name == "transform.deduplicate":
        seen, kept = set(), []
        for row in rows:
            key = json.dumps([row[k] for k in p["keys"]], sort_keys=True)
            if key not in seen:
                seen.add(key)
                kept.append(row)
        rows = kept
    elif name == "transform.normalize":
        rows = [
            {
                **r,
                **{
                    f: getattr(r[f], p["operation"])() if isinstance(r.get(f), str) else r.get(f)
                    for f in p["fields"]
                },
            }
            for r in rows
        ]
    elif name == "transform.filter":
        ops = {
            "eq": operator.eq,
            "ne": operator.ne,
            "gt": operator.gt,
            "ge": operator.ge,
            "lt": operator.lt,
            "le": operator.le,
        }
        rows = [
            r
            for r in rows
            if (
                r.get(p["field"]) is not None
                if p["operator"] == "not_null"
                else ops[p["operator"]](r.get(p["field"]), p.get("value"))
            )
        ]
    elif name in {"transform.map", "transform.schema_mapping"}:
        rows = [{target: r[source] for source, target in p["mapping"].items()} for r in rows]
    elif name == "transform.aggregate":
        groups = defaultdict(list)
        for row in rows:
            groups[tuple(row[k] for k in p["group_by"])].append(row[p["field"]])
        functions: dict[str, Callable] = {
            "sum": sum,
            "count": len,
            "mean": lambda x: sum(x) / len(x),
            "min": min,
            "max": max,
        }
        rows = [
            {
                **dict(zip(p["group_by"], keys, strict=True)),
                p["output"]: functions[p["operation"]](values),
            }
            for keys, values in sorted(groups.items(), key=lambda item: repr(item[0]))
        ]
    elif name in {"transform.join", "enrich.lookup"}:
        right_index = defaultdict(list)
        for row in tables[1]:
            if row.get(p["right_key"]) is not None:
                right_index[row[p["right_key"]]].append(row)
        joined = []
        for left in rows:
            matches = right_index.get(left.get(p["left_key"]), [])
            if not matches and p["how"] == "left":
                matches = [{}]
            for right in matches:
                merged = dict(left)
                for key, value in right.items():
                    merged[
                        key
                        if key not in merged or key == p["right_key"] == p["left_key"]
                        else "right_" + key
                    ] = value
                joined.append(merged)
                if len(joined) > limit:
                    raise ValueError("Join exceeds row limit")
        rows = joined
    elif name == "transform.union":
        rows = [row for table in tables for row in table]
    elif name == "quality.check":
        reject = set()
        rules = [r for r in spec.quality_rules if r.id in p["rule_ids"]]
        for rule in rules:
            # Schema comes from the immediate lineage where possible; explicit schema override is supported.
            schema = rule.parameters.get("schema")
            if schema is None and rule.kind == "schema":
                if len(spec.sources) != 1:
                    raise ValueError(
                        "Schema quality checks on multiple sources require explicit schema"
                    )
                schema = catalog.get(spec.sources[0].asset).schema_fields
            invalid, result = check_rows(rule, rows, schema or {}, now)
            quality_results.append(result)
            _notify(spec, catalog, root, "quality", run_id, result.model_dump(mode="json"))
            if rule.action == "quarantine":
                bad = [rows[i] for i in invalid]
                write(catalog.get(rule.quarantine_asset), root, bad)
                reject.update(invalid)
            elif not result.passed:
                raise QualityFailure(f"Quality rule failed: {rule.id}")
        rows = [r for i, r in enumerate(rows) if i not in reject]
    elif task.type == "output":
        destination = next(d for d in spec.destinations if "asset:" + d.asset == task.outputs[0])
        write(catalog.get(destination.asset), root, rows, destination.mode, destination.key)
    elif name == "ops.checkpoint":
        if len(tables) != p["expected_count"]:
            raise ValueError("Expected inputs have not all arrived")
        rows = [r for table in tables for r in table]
    elif name == "ops.notification":
        asset = catalog.get(p["asset"])
        if asset.type != "notification" or "notify" not in asset.allowed_operations:
            raise ValueError("Notification not permitted")
        atomic_write(
            confined(root, asset.location) / (run_id + ".json"),
            json.dumps({"pipeline_id": spec.pipeline_id, "rows": len(rows)}),
        )
    else:
        raise ValueError("Runtime capability unavailable")
    return rows_checked(rows, limit)


def run_pipeline(
    spec: PipelineSpec,
    catalog: Catalog,
    root: Path,
    approval: Approval | None = None,
    now: datetime | None = None,
    allow_http=False,
) -> PipelineRun:
    spec = require_valid(spec, catalog)
    check_approval(spec, catalog, approval)
    now = now or datetime.now(UTC)
    if now.tzinfo is None:
        raise ValueError("Runtime clock must be timezone aware")
    run_id = uuid.uuid4().hex
    run = PipelineRun(
        pipeline_id=spec.pipeline_id,
        run_id=run_id,
        spec_hash=digest(spec),
        status=RunStatus.RUNNING,
    )
    outputs: dict[str, list[dict]] = {}
    started = time.monotonic()
    for task in spec.tasks:
        step_started = time.monotonic()
        try:
            outputs[task.id] = execute_task(
                spec, catalog, task, outputs, root, run_id, now, run.quality, allow_http
            )
            elapsed = time.monotonic() - step_started
            if (
                elapsed > (task.timeout or spec.timeout_policy.task_seconds)
                or time.monotonic() - started > spec.timeout_policy.pipeline_seconds
            ):
                raise TimeoutError("Local verification time budget exceeded")
            run.tasks.append(
                TaskRun(
                    task_id=task.id,
                    status=RunStatus.SUCCESS,
                    output_rows=len(outputs[task.id]),
                    duration_seconds=elapsed,
                )
            )
        except Exception as exc:
            # Arbitrary IO exceptions may contain secret URLs or data values; persist category only.
            run.tasks.append(
                TaskRun(
                    task_id=task.id,
                    status=RunStatus.FAILED,
                    duration_seconds=time.monotonic() - step_started,
                )
            )
            run.failures.append(
                FailureEvent(
                    task_id=task.id,
                    category=type(exc).__name__,
                    message="Task failed; inspect protected local logs and input configuration",
                )
            )
            run.status = RunStatus.FAILED
            _notify(
                spec,
                catalog,
                root,
                "failure",
                run_id,
                {"task_id": task.id, "category": type(exc).__name__},
            )
            break
    else:
        run.status = RunStatus.SUCCESS
        _notify(spec, catalog, root, "success", run_id, {})
    completed = {t.task_id for t in run.tasks}
    run.tasks.extend(
        TaskRun(task_id=t.id, status=RunStatus.SKIPPED, attempts=0)
        for t in spec.tasks
        if t.id not in completed
    )
    run.finished_at = datetime.now(UTC)
    run.lineage = [
        LineageEvent(source=e.source, destination=e.destination, run_id=run_id)
        for e in spec.lineage
    ]
    atomic_write(
        confined(root, f".nlpipe/runs/{spec.pipeline_id}/{run_id}.json"),
        run.model_dump_json(indent=2),
    )
    return run


def airflow_execute(
    spec_data: dict, catalog_hash: str, task_id: str, upstream: dict, run_id: str, interval_end: str
):
    """Called by generated trusted TaskFlow tasks, with only artifact references in XCom."""
    spec = PipelineSpec.model_validate(spec_data)
    root = Path(os.environ["NLPIPE_DATA_ROOT"])
    catalog = load_catalog(Path(os.environ["NLPIPE_CATALOG"]))
    if digest(catalog) != catalog_hash:
        raise ValueError("Runtime catalog differs from compiled catalog")
    spec = require_valid(spec, catalog)
    approval_file = os.environ.get("NLPIPE_APPROVAL_FILE")
    approval = (
        Approval.model_validate_json(Path(approval_file).read_text()) if approval_file else None
    )
    check_approval(spec, catalog, approval)
    task = next(t for t in spec.tasks if t.id == task_id)
    safe_run_id = __import__("hashlib").sha256(run_id.encode()).hexdigest()[:32]
    inputs = {
        key: read_artifact(root, ref, spec.resources.max_rows) for key, ref in upstream.items()
    }
    quality: list[QualityResult] = []
    rows = execute_task(
        spec,
        catalog,
        task,
        inputs,
        root,
        safe_run_id,
        datetime.fromisoformat(interval_end),
        quality,
        allow_http=os.environ.get("NLPIPE_ALLOW_HTTP") == "1",
    )
    ref = artifact(root, f".nlpipe/artifacts/{spec.pipeline_id}/{safe_run_id}/{task_id}.json", rows)
    atomic_write(
        confined(
            root, f".nlpipe/artifacts/{spec.pipeline_id}/{safe_run_id}/{task_id}.quality.json"
        ),
        json.dumps([q.model_dump() for q in quality]),
    )
    return ref


def airflow_notify(spec_data, catalog_hash, event, context):
    spec = PipelineSpec.model_validate(spec_data)
    catalog = load_catalog(Path(os.environ["NLPIPE_CATALOG"]))
    if digest(catalog) != catalog_hash:
        raise ValueError("Notification catalog mismatch")
    run_id = (
        __import__("hashlib")
        .sha256(str(context.get("run_id", "unknown")).encode())
        .hexdigest()[:32]
    )
    _notify(spec, catalog, Path(os.environ["NLPIPE_DATA_ROOT"]), event, run_id, {"state": event})
