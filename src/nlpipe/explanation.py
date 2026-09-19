"""Deterministic plain-language explanations from reviewed IR."""

import json

from nlpipe.ir import PipelineSpec


def explain(spec: PipelineSpec) -> str:
    """Readable, fully specified explanation; JSON parameters preserve exact semantics."""
    lines = [
        f"Pipeline {spec.pipeline_id}: {spec.name}.",
        f"Schedule: {spec.schedule.cron or 'ad-hoc'} in {spec.timezone}; catchup={spec.catchup}.",
        f"Sources: {', '.join(s.asset for s in spec.sources) or 'none'}.",
        f"Destinations: {', '.join(d.asset + ' (' + d.mode + ')' for d in spec.destinations) or 'none'}.",
    ]
    for task in spec.tasks:
        lines.append(
            f"Task {task.id} uses {task.capability}, reads {task.inputs}, writes {task.outputs}, "
            f"after {task.dependencies}, with {json.dumps(task.parameters, sort_keys=True)}."
        )
    for rule in spec.quality_rules:
        lines.append(
            f"Quality: {rule.kind} on {rule.task}.{rule.field}, threshold {rule.max_invalid_rate}; "
            f"{rule.action}, quarantine={rule.quarantine_asset}, parameters={rule.parameters}."
        )
    lines.extend(
        [
            f"Retries: {spec.retry_policy.retries}; task timeout: {spec.timeout_policy.task_seconds}s.",
            f"Notifications: {[n.model_dump() for n in spec.notifications]}.",
            f"Governance: environment={spec.execution_policy.environment}; owners={spec.owners}; "
            "production, destructive writes and external writes require human approval.",
        ]
    )
    return "\n".join(lines)
