"""Deterministic plain-language explanations from reviewed IR."""

import json

from nlpipe.ir import PipelineSpec


def explain(spec: PipelineSpec) -> str:
    """Readable, fully specified explanation; JSON parameters preserve exact semantics."""
    lines = [
        f"Pipeline {spec.pipeline_id}: {spec.name}. Specification version {spec.version}.",
        f"Description: {spec.description or 'none'}. Tags: {spec.tags}.",
        f"Schedule: {spec.schedule.cron or 'ad-hoc'} in {spec.timezone}; catchup={spec.catchup}.",
        f"Sources: {', '.join(s.asset for s in spec.sources) or 'none'}.",
        f"Destinations: {', '.join(d.asset + ' (' + d.mode + ')' for d in spec.destinations) or 'none'}.",
    ]
    lines.append(
        f"Start date: {spec.start_date.isoformat()}; at most {spec.concurrency} concurrent runs."
    )
    for source in spec.sources:
        lines.append(
            f"Source {source.asset} uses window {source.window}; incremental field {source.incremental_field}."
        )
    for destination in spec.destinations:
        lines.append(
            f"Destination {destination.asset} uses {destination.mode} mode; keys {destination.key}."
        )
    for task in spec.tasks:
        lines.append(
            f"Task {task.id} uses {task.capability}, reads {task.inputs}, writes {task.outputs}, "
            f"after {task.dependencies}, with {json.dumps(task.parameters, sort_keys=True)}."
        )
        lines.append(
            f"Task {task.id} retry override: {task.retries}; timeout override: {task.timeout}; "
            f"CPU {task.resources.cpu}, memory {task.resources.memory_mb} MB, row limit {task.resources.max_rows}; "
            f"failure behavior {task.failure_behavior}; quality references {task.quality_checks}."
        )
    for rule in spec.quality_rules:
        lines.append(
            f"Quality: {rule.id}; {rule.kind} on {rule.task}.{rule.field}, threshold {rule.max_invalid_rate}; "
            f"{rule.action}, quarantine={rule.quarantine_asset}, retention_days={rule.retention_days}, parameters={rule.parameters}."
        )
    lines.extend(
        [
            f"Retries: {spec.retry_policy.retries}, delay {spec.retry_policy.delay_seconds}s, "
            f"exponential backoff {spec.retry_policy.exponential_backoff}; task timeout "
            f"{spec.timeout_policy.task_seconds}s; pipeline timeout {spec.timeout_policy.pipeline_seconds}s.",
            f"Pipeline resources: CPU {spec.resources.cpu}, memory {spec.resources.memory_mb} MB, "
            f"row limit {spec.resources.max_rows}.",
            f"Execution policy: environment={spec.execution_policy.environment}; "
            f"idempotent={spec.execution_policy.idempotent}; schema_evolution={spec.execution_policy.schema_evolution}.",
            f"Approval requirement declarations: production={spec.approval_requirements.production}, "
            f"external_writes={spec.approval_requirements.external_writes}, "
            f"destructive_writes={spec.approval_requirements.destructive_writes}. Mandatory gates still apply.",
            f"Governance rules: {[r.model_dump() for r in spec.governance_rules]}.",
            f"SLAs: {[s.model_dump() for s in spec.slas]}. Runtime parameters: {spec.parameters}.",
            f"Credential references (not values): {[s.model_dump() for s in spec.secrets]}.",
            f"Notifications: {[n.model_dump() for n in spec.notifications]}.",
            f"Governance: environment={spec.execution_policy.environment}; owners={spec.owners}; "
            "production, destructive writes and external writes require human approval.",
        ]
    )
    return "\n".join(lines)
