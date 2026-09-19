"""Policy decisions are evidence; approvals are separately issued, hash-bound records."""

import re
from typing import Literal

from nlpipe.catalog import Catalog
from nlpipe.ir import Model, PipelineSpec, canonical


class Decision(Model):
    rule: str
    status: Literal["PASS", "WARN", "FAIL", "REQUIRES_APPROVAL"]
    explanation: str


# Reject before model transmission, and scan model responses/IR as a separate boundary.
SECRET = re.compile(
    r"(?i)(?:\b(?:password|passwd|api[_ -]?key|access[_ -]?token|secret)\b[\s:=]+[^\s,;]+|\bsk-[A-Za-z0-9_-]{12,}|://[^/\s:]+:[^/\s@]+@)"
)
ATTACK = re.compile(
    r"(?i)(ignore\s+(?:all\s+|the\s+)?(?:previous|security|system)\s+(?:rules|instructions)|(?:execute|run)\s+(?:this\s+)?(?:python|shell|code)|__import__|\beval\s*\(|\bexec\s*\(|\$\(|\b(?:rm\s+-rf|curl\s+https?://)|;\s*(?:drop|delete)\s+|\.\./)"
)


def screen_prompt(prompt: str):
    if len(prompt) > 20000:
        raise ValueError("Prompt exceeds 20,000 characters")
    if SECRET.search(prompt):
        raise ValueError("Plaintext credentials prohibited; use a registered secret reference")
    if ATTACK.search(prompt):
        raise ValueError("Unsafe executable content or policy-bypass instruction rejected")
    if re.search(r"(?i)retry\s+(?:forever|indefinitely)", prompt):
        raise ValueError("Unbounded retries are prohibited")


def scan_ir(spec: PipelineSpec):
    raw = spec.model_dump(mode="json")
    # References contain names, never values. Do not exempt arbitrary parameter values.
    raw.pop("secrets")
    text = canonical(raw)
    if re.search(
        r'(?i)"(?:password|passwd|api_key|access_token|secret)"\s*:', text
    ) or SECRET.search(text):
        raise ValueError("Plaintext credential fields are prohibited")
    if ATTACK.search(text):
        raise ValueError("Unsafe content in IR")


def evaluate(spec: PipelineSpec, catalog: Catalog) -> list[Decision]:
    decisions = []

    def emit(rule, status, explanation):
        decisions.append(Decision(rule=rule, status=status, explanation=explanation))

    try:
        scan_ir(spec)
        emit("safe_ir", "PASS", "No executable payloads or plaintext credentials detected")
    except ValueError as exc:
        emit("safe_ir", "FAIL", str(exc))
    if spec.execution_policy.environment == "production":
        emit("owner", "PASS" if spec.owners else "FAIL", "Production requires an accountable owner")
        emit("production", "REQUIRES_APPROVAL", "Production deployment requires human approval")
    for destination in spec.destinations:
        asset = catalog.get(destination.asset)
        if destination.mode == "rebuild":
            emit(
                "destructive_write",
                "REQUIRES_APPROVAL",
                f"Rebuild of {asset.identifier} requires approval",
            )
        if asset.external:
            emit(
                "external_write",
                "REQUIRES_APPROVAL",
                f"External write to {asset.identifier} requires approval",
            )
        if destination.mode == "append" and spec.execution_policy.idempotent:
            emit("idempotency", "FAIL", "Append cannot claim idempotency; use upsert or replace")
    if spec.execution_policy.schema_evolution == "approved":
        emit("schema_change", "REQUIRES_APPROVAL", "Schema evolution requires human review")
    for rule in spec.governance_rules:
        if rule.name == "require_owner":
            emit(rule.name, "PASS" if spec.owners else "FAIL", rule.reason or "Owner required")
        elif rule.name == "require_approval":
            emit(rule.name, "REQUIRES_APPROVAL", rule.reason or "Configured approval required")
        elif rule.name == "deny_external_write":
            emit(
                rule.name,
                "FAIL" if any(catalog.get(d.asset).external for d in spec.destinations) else "PASS",
                rule.reason or "External writes denied",
            )
    if not spec.owners:
        emit("development_owner", "WARN", "Assign an owner before production")
    return decisions
