"""Versioned, closed IR. No executable strings or authority fields are accepted."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Annotated, Any, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import yaml
from croniter import croniter
from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator, model_validator

Identifier = Annotated[str, Field(pattern=r"^[a-z][a-z0-9_]{0,62}$")]


class Model(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True, allow_inf_nan=False)


class ScheduleSpec(Model):
    cron: str | None = None

    @field_validator("cron")
    @classmethod
    def valid_cron(cls, value):
        if value is not None and (len(value.split()) != 5 or not croniter.is_valid(value)):
            raise ValueError("Schedule must be a valid five-field cron, or null for ad-hoc")
        return value


class RetryPolicy(Model):
    retries: int = Field(default=2, ge=0, le=10)
    delay_seconds: int = Field(default=60, ge=1, le=86400)
    exponential_backoff: bool = True


class TimeoutPolicy(Model):
    task_seconds: int = Field(default=300, ge=1, le=86400)
    pipeline_seconds: int = Field(default=3600, ge=1, le=604800)


class ResourceSpec(Model):
    cpu: float = Field(default=1, gt=0, le=64)
    memory_mb: int = Field(default=512, ge=64, le=131072)
    max_rows: int = Field(default=100000, ge=1, le=1000000)


class SecretReference(Model):
    name: Identifier
    provider: Literal["environment", "airflow_connection"] = "environment"
    key: str = Field(pattern=r"^[A-Za-z][A-Za-z0-9_]{0,127}$")


class SourceSpec(Model):
    asset: Identifier
    incremental_field: str | None = None
    window: Literal["all", "previous_day"] = "all"


class DestinationSpec(Model):
    asset: Identifier
    mode: Literal["replace", "append", "upsert", "rebuild"] = "replace"
    key: list[str] = Field(default_factory=list)


class QualityRule(Model):
    id: Identifier
    task: Identifier
    kind: Literal[
        "not_null",
        "unique",
        "range",
        "schema",
        "row_count",
        "freshness",
        "referential",
        "distribution",
    ]
    field: str | None = None
    parameters: dict[str, JsonValue] = Field(default_factory=dict)
    max_invalid_rate: float = Field(default=0, ge=0, le=1)
    action: Literal["fail", "quarantine"] = "fail"
    quarantine_asset: Identifier | None = None
    retention_days: int | None = Field(default=None, ge=1, le=3650)

    @model_validator(mode="after")
    def check_quarantine(self):
        if self.action == "quarantine" and self.quarantine_asset is None:
            raise ValueError("Quarantine requires a registered destination")
        if (
            self.kind in {"not_null", "unique", "range", "freshness", "referential", "distribution"}
            and not self.field
        ):
            raise ValueError("This rule requires a field")
        return self


class GovernanceRule(Model):
    name: Literal["require_owner", "require_approval", "deny_external_write"]
    reason: str = ""


class NotificationPolicy(Model):
    asset: Identifier
    on: Literal["failure", "success", "quality"] = "failure"
    threshold: float = Field(default=0, ge=0, le=1)


class ApprovalPolicy(Model):
    production: bool = True
    external_writes: bool = True
    destructive_writes: bool = True


class ExecutionPolicy(Model):
    environment: Literal["dev", "production"] = "dev"
    idempotent: bool = True
    schema_evolution: Literal["reject", "approved"] = "reject"


class SLASpec(Model):
    max_duration_seconds: int = Field(ge=1, le=604800)


class LineageEdge(Model):
    source: str
    destination: str


class TaskSpec(Model):
    id: Identifier
    type: Literal["ingestion", "transformation", "quality", "enrichment", "output", "operations"]
    capability: str = Field(pattern=r"^[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*@1$")
    inputs: list[Annotated[str, Field(pattern=r"^(asset|task):[a-z][a-z0-9_]{0,62}$")]] = Field(
        default_factory=list
    )
    outputs: list[Annotated[str, Field(pattern=r"^asset:[a-z][a-z0-9_]{0,62}$")]] = Field(
        default_factory=list
    )
    parameters: dict[str, JsonValue] = Field(default_factory=dict)
    dependencies: list[Identifier] = Field(default_factory=list)
    retries: int | None = Field(default=None, ge=0, le=10)
    timeout: int | None = Field(default=None, ge=1, le=86400)
    resources: ResourceSpec = Field(default_factory=ResourceSpec)
    failure_behavior: Literal["stop", "notify_and_stop"] = "stop"
    quality_checks: list[Identifier] = Field(default_factory=list)


class Dependency(Model):
    upstream: Identifier
    downstream: Identifier


class PipelineSpec(Model):
    version: Literal["1.0"] = "1.0"
    pipeline_id: Identifier
    name: str = Field(min_length=1, max_length=200)
    description: str = Field(default="", max_length=5000)
    owners: list[str] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)
    schedule: ScheduleSpec = Field(default_factory=ScheduleSpec)
    timezone: str = "UTC"
    start_date: datetime = datetime.fromisoformat("2025-01-01T00:00:00+00:00")
    catchup: bool = False
    concurrency: int = Field(default=1, ge=1, le=64)
    retry_policy: RetryPolicy = Field(default_factory=RetryPolicy)
    timeout_policy: TimeoutPolicy = Field(default_factory=TimeoutPolicy)
    parameters: dict[str, JsonValue] = Field(default_factory=dict)
    sources: list[SourceSpec] = Field(min_length=1)
    destinations: list[DestinationSpec] = Field(default_factory=list)
    tasks: list[TaskSpec] = Field(min_length=1, max_length=200)
    dependencies: list[Dependency] = Field(default_factory=list)
    quality_rules: list[QualityRule] = Field(default_factory=list)
    governance_rules: list[GovernanceRule] = Field(default_factory=list)
    notifications: list[NotificationPolicy] = Field(default_factory=list)
    slas: list[SLASpec] = Field(default_factory=list)
    lineage: list[LineageEdge] = Field(default_factory=list)
    resources: ResourceSpec = Field(default_factory=ResourceSpec)
    secrets: list[SecretReference] = Field(default_factory=list)
    execution_policy: ExecutionPolicy = Field(default_factory=ExecutionPolicy)
    approval_requirements: ApprovalPolicy = Field(default_factory=ApprovalPolicy)

    @field_validator("timezone")
    @classmethod
    def valid_timezone(cls, value):
        try:
            ZoneInfo(value)
        except (ZoneInfoNotFoundError, ValueError) as exc:
            raise ValueError("Unknown timezone") from exc
        return value

    @field_validator("start_date")
    @classmethod
    def aware_start(cls, value):
        if value.tzinfo is None:
            raise ValueError("start_date must be timezone aware")
        return value


def canonical(value: BaseModel | dict | list) -> str:
    obj = value.model_dump(mode="json") if isinstance(value, BaseModel) else value
    return json.dumps(
        obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    )


def digest(value: BaseModel | dict | list) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def migrate(raw: dict[str, Any]) -> dict[str, Any]:
    if raw.get("version") != "1.0":
        raise ValueError("Unsupported IR version; register an explicit migration first")
    return raw


def load(path: Path) -> PipelineSpec:
    if path.stat().st_size > 2_000_000:
        raise ValueError("IR exceeds 2 MB limit")
    raw = yaml.safe_load(path.read_text())
    if not isinstance(raw, dict):
        raise ValueError("IR must be an object")
    return PipelineSpec.model_validate(migrate(raw))


def save(spec: PipelineSpec, path: Path) -> None:
    data = spec.model_dump(mode="json")
    path.write_text(
        json.dumps(data, indent=2) + "\n"
        if path.suffix == ".json"
        else yaml.safe_dump(data, sort_keys=True)
    )


def diff(old: PipelineSpec, new: PipelineSpec) -> dict:
    a, b = old.model_dump(mode="json"), new.model_dump(mode="json")
    return {key: {"before": a[key], "after": b[key]} for key in sorted(a) if a[key] != b[key]}
