"""Structural, semantic, operational, quality, and security validation."""

from typing import Any

from jsonschema import Draft202012Validator
from pydantic import Field

from nlpipe.capabilities import Registry, builtin_registry, schema
from nlpipe.catalog import Catalog
from nlpipe.ir import Model, PipelineSpec
from nlpipe.planner import normalize
from nlpipe.policy import Decision, evaluate


class ValidationReport(Model):
    valid: bool
    errors: list[str] = Field(default_factory=list)
    decisions: list[Decision] = Field(default_factory=list)
    normalized: PipelineSpec | None = None

    @property
    def requires_approval(self):
        return any(d.status == "REQUIRES_APPROVAL" for d in self.decisions)


def validate(
    spec: PipelineSpec, catalog: Catalog, registry: Registry | None = None
) -> ValidationReport:
    registry = registry or builtin_registry()
    errors = []
    try:
        spec = PipelineSpec.model_validate(spec.model_dump(mode="json"))
        rule_targets = {r.id: r.task for r in spec.quality_rules}
        for task in spec.tasks:
            if any(rule_targets.get(q) != task.id for q in task.quality_checks):
                raise ValueError("Unknown or incorrectly bound task quality reference")
        spec = normalize(spec)
    except (ValueError, TypeError) as exc:
        return ValidationReport(valid=False, errors=[str(exc)])
    ids = {t.id for t in spec.tasks}
    source_ids = {s.asset for s in spec.sources}
    dest_ids = {d.asset for d in spec.destinations}
    if len(source_ids) != len(spec.sources) or len(dest_ids) != len(spec.destinations):
        errors.append("Duplicate source or destination")
    rule_ids = {r.id for r in spec.quality_rules}
    if len(rule_ids) != len(spec.quality_rules):
        errors.append("Duplicate quality rule IDs")
    used_sources, used_destinations = set(), set()
    if spec.parameters:
        errors.append(
            "Runtime parameter substitution is not implemented; resolve parameters into reviewed IR"
        )
    if any(s.incremental_field and s.window == "all" for s in spec.sources):
        errors.append("Incremental checkpoints require an installed adapter")
    writers = [ref for t in spec.tasks for ref in t.outputs]
    if len(writers) != len(set(writers)):
        errors.append("Multiple writers to one destination are prohibited")
    schemas: dict[str, dict] = {}
    for source in spec.sources:
        try:
            asset = catalog.get(source.asset)
            if "read" not in asset.allowed_operations:
                errors.append(f"Read forbidden: {source.asset}")
            if source.incremental_field or source.window != "all":
                if (
                    not source.incremental_field
                    or source.incremental_field not in asset.schema_fields
                ):
                    errors.append("Incremental/windowed reads require a registered timestamp field")
            schemas["asset:" + source.asset] = asset.schema_fields
        except ValueError as exc:
            errors.append(str(exc))
    for destination in spec.destinations:
        try:
            asset = catalog.get(destination.asset)
            if "write" not in asset.allowed_operations:
                errors.append(f"Write forbidden: {destination.asset}")
            if destination.mode == "rebuild" and "delete" not in asset.allowed_operations:
                errors.append(f"Delete forbidden: {destination.asset}")
            if destination.mode == "upsert" and not destination.key:
                errors.append("Upsert requires keys")
        except ValueError as exc:
            errors.append(str(exc))
    for task in spec.tasks:
        try:
            cap = registry.get(task.capability)
            if not cap.implemented:
                errors.append(
                    f"Capability requires an installed, approved adapter: {cap.reference}"
                )
            if cap.family != task.type:
                errors.append(f"Task family mismatch: {task.id}")
            if not cap.min_inputs <= len(task.inputs) <= cap.max_inputs:
                errors.append(f"Wrong input count: {task.id}")
            parameter_errors = list(
                Draft202012Validator(cap.parameter_schema).iter_errors(task.parameters)
            )
            if parameter_errors:
                errors.extend(
                    f"{task.id}: invalid capability parameters at {list(e.path)}"
                    for e in parameter_errors
                )
                continue
            for ref in task.inputs:
                if ref.startswith("asset:") and ref[6:] in source_ids:
                    used_sources.add(ref[6:])
                    if task.type != "ingestion":
                        errors.append("Assets must be read through an ingestion capability")
                elif not (ref.startswith("task:") and ref[5:] in ids):
                    errors.append(f"Unresolved input: {ref}")
            for ref in task.outputs:
                if ref.startswith("asset:") and ref[6:] in dest_ids and task.type == "output":
                    used_destinations.add(ref[6:])
                else:
                    errors.append(f"Unresolved or non-output destination: {ref}")
            if task.type == "output" and len(task.outputs) != 1:
                errors.append("Output task requires exactly one destination")
            if task.type != "output" and task.outputs:
                errors.append("Only output tasks may write destinations")
            if task.type == "ingestion":
                asset = catalog.get(task.inputs[0].removeprefix("asset:"))
                expected = {"sqlite": "sql"}.get(asset.type, asset.type)
                if cap.name not in {"ingest." + expected, "ingest.filesystem"}:
                    errors.append(f"Ingestion format mismatch: {task.id}")
                if cap.name == "ingest.filesystem" and asset.type not in {
                    "csv",
                    "json",
                    "jsonl",
                    "parquet",
                }:
                    errors.append("Filesystem capability requires a file asset")
            if task.type == "output" and task.outputs:
                asset = catalog.get(task.outputs[0][6:])
                expected = {"sqlite": "sql"}.get(asset.type, asset.type)
                if cap.name not in {"output." + expected, "output.filesystem"}:
                    errors.append(f"Output format mismatch: {task.id}")
                if cap.name == "output.filesystem" and asset.type not in {
                    "csv",
                    "json",
                    "jsonl",
                    "parquet",
                }:
                    errors.append("Filesystem output requires a file asset")
            current = dict(schemas.get(task.inputs[0], {})) if task.inputs else {}
            params: dict[str, Any] = dict(task.parameters)
            fields = (
                list(params.get("keys", []))
                + list(params.get("fields", []))
                + list(params.get("group_by", []))
            )
            if "field" in params:
                fields.append(params["field"])
            if current:
                for name in fields:
                    if name not in current:
                        errors.append(f"Unknown field {name} in {task.id}")
            if cap.name in {"transform.join", "enrich.lookup"} and len(task.inputs) == 2:
                right = schemas.get(task.inputs[1], {})
                left_key, right_key = params.get("left_key"), params.get("right_key")
                if left_key not in current or right_key not in right:
                    errors.append("Join keys must exist in both schemas")
                elif current[left_key] != right[right_key]:
                    errors.append("Incompatible join key types")
                current.update(
                    {
                        k if k not in current or k == right_key == left_key else "right_" + k: v
                        for k, v in right.items()
                    }
                )
            if cap.name in {"transform.map", "transform.schema_mapping"}:
                mapping = params.get("mapping", {})
                if any(k not in current for k in mapping) or len(set(mapping.values())) != len(
                    mapping
                ):
                    errors.append("Invalid schema mapping")
                current = {str(v): current.get(k, "string") for k, v in mapping.items()}
            if cap.name == "transform.aggregate" and all(
                k in params for k in ("group_by", "output")
            ):
                current = {str(k): current.get(k, "string") for k in params["group_by"]} | {
                    str(params["output"]): "number"
                }
            if cap.name == "quality.check" and not set(params.get("rule_ids", [])).issubset(
                rule_ids
            ):
                errors.append("Unknown quality rule reference")
            if task.type == "output" and task.outputs:
                target_schema = catalog.get(task.outputs[0][6:]).schema_fields
                if target_schema and current:
                    if set(target_schema) != set(current) or any(
                        current[k] != target_schema[k]
                        and not (current[k] == "integer" and target_schema[k] == "number")
                        for k in current
                        if k in target_schema
                    ):
                        errors.append(f"Destination schema mismatch: {task.id}")
            if task.failure_behavior == "notify_and_stop" and not any(
                n.on == "failure" for n in spec.notifications
            ):
                errors.append("notify_and_stop requires a failure notification destination")
            schemas["task:" + task.id] = current
            if task.resources.max_rows > spec.resources.max_rows:
                errors.append("Task row limit exceeds pipeline limit")
        except (ValueError, IndexError, TypeError) as exc:
            errors.append(str(exc))
    if source_ids != used_sources:
        errors.append("Every declared source must be read")
    if dest_ids != used_destinations:
        errors.append("Every declared destination must be written")
    for rule in spec.quality_rules:
        contracts = {
            "not_null": schema(),
            "unique": schema(),
            "range": schema({"min": {"type": "number"}, "max": {"type": "number"}}, ["min", "max"]),
            "row_count": schema(
                {"min": {"type": "integer", "minimum": 0}, "max": {"type": "integer", "minimum": 0}}
            ),
            "freshness": schema(
                {"max_age_seconds": {"type": "number", "exclusiveMinimum": 0}}, ["max_age_seconds"]
            ),
            "schema": schema(
                {
                    "schema": {
                        "type": "object",
                        "additionalProperties": {
                            "enum": ["string", "integer", "number", "boolean", "datetime"]
                        },
                    }
                }
            ),
        }
        if rule.kind in contracts and list(
            Draft202012Validator(contracts[rule.kind]).iter_errors(rule.parameters)
        ):
            errors.append(f"Invalid quality parameters: {rule.id}")
        if rule.kind == "schema" and "schema" not in rule.parameters:
            if len(spec.sources) != 1 or any(
                t.capability
                in {
                    "transform.join@1",
                    "transform.aggregate@1",
                    "transform.map@1",
                    "transform.schema_mapping@1",
                }
                for t in spec.tasks
            ):
                errors.append("Schema checks after shape changes require an explicit schema")
        rule_fields = schemas.get("task:" + rule.task, {})
        if rule.field and rule_fields and rule.field not in rule_fields:
            errors.append(f"Quality field absent: {rule.field}")
        if rule.kind == "range":
            lo, hi = rule.parameters.get("min"), rule.parameters.get("max")
            if not isinstance(lo, (int, float)) or not isinstance(hi, (int, float)) or lo > hi:
                errors.append("Range requires numeric min <= max")
        elif rule.kind == "row_count":
            lo, hi = rule.parameters.get("min", 0), rule.parameters.get("max", 1000000)
            if not isinstance(lo, int) or not isinstance(hi, int) or lo < 0 or hi < lo:
                errors.append("Invalid row count thresholds")
        elif rule.kind == "freshness":
            max_age = rule.parameters.get("max_age_seconds")
            if not isinstance(max_age, (int, float)) or max_age <= 0:
                errors.append("Freshness requires positive max_age_seconds")
        elif rule.kind in {"referential", "distribution"}:
            errors.append(f"Quality adapter not implemented: {rule.kind}")
        if rule.retention_days:
            errors.append("Retention requires an approved external lifecycle adapter")
        if rule.quarantine_asset:
            try:
                if "write" not in catalog.get(rule.quarantine_asset).allowed_operations:
                    errors.append("Quarantine write forbidden")
                if catalog.get(rule.quarantine_asset).external:
                    errors.append("External quarantine not supported by local runtime")
            except ValueError as exc:
                errors.append(str(exc))
    for notification in spec.notifications:
        try:
            asset = catalog.get(notification.asset)
            if asset.type != "notification" or "notify" not in asset.allowed_operations:
                errors.append("Notification destination not permitted")
        except ValueError as exc:
            errors.append(str(exc))
    try:
        decisions = evaluate(spec, catalog)
    except ValueError as exc:
        errors.append(str(exc))
        decisions = []
    errors.extend(d.explanation for d in decisions if d.status == "FAIL")
    return ValidationReport(
        valid=not errors, errors=sorted(set(errors)), decisions=decisions, normalized=spec
    )


def require_valid(spec: PipelineSpec, catalog: Catalog) -> PipelineSpec:
    report = validate(spec, catalog)
    if not report.valid:
        raise ValueError("; ".join(report.errors))
    assert report.normalized is not None
    return report.normalized
