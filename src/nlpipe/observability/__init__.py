"""Backend-neutral runtime evidence and conservative repair proposals."""

from datetime import UTC, datetime
from enum import StrEnum
from typing import Literal

from pydantic import Field

from nlpipe.ir import Model, PipelineSpec, digest


class RunStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


class QualityResult(Model):
    rule_id: str
    total: int
    invalid: int
    invalid_rate: float
    passed: bool
    quarantine_asset: str | None = None


class FailureEvent(Model):
    task_id: str
    category: str
    message: str


class TaskRun(Model):
    task_id: str
    status: RunStatus
    attempts: int = 1
    duration_seconds: float = 0
    output_rows: int | None = None


class LineageEvent(Model):
    source: str
    destination: str
    run_id: str


class PipelineRun(Model):
    pipeline_id: str
    run_id: str
    spec_hash: str
    status: RunStatus
    started_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    finished_at: datetime | None = None
    tasks: list[TaskRun] = Field(default_factory=list)
    failures: list[FailureEvent] = Field(default_factory=list)
    quality: list[QualityResult] = Field(default_factory=list)
    lineage: list[LineageEvent] = Field(default_factory=list)


def from_airflow(payload: dict, spec: PipelineSpec) -> PipelineRun:
    states = {
        "success": RunStatus.SUCCESS,
        "failed": RunStatus.FAILED,
        "upstream_failed": RunStatus.FAILED,
        "running": RunStatus.RUNNING,
        "queued": RunStatus.QUEUED,
        "scheduled": RunStatus.QUEUED,
        "skipped": RunStatus.SKIPPED,
        "up_for_retry": RunStatus.QUEUED,
        "deferred": RunStatus.QUEUED,
    }
    state = payload.get("state")
    if state not in states:
        raise ValueError("Unknown Airflow state")
    if payload.get("dag_id") != spec.pipeline_id:
        raise ValueError("Airflow DAG identity does not match IR")
    known = {t.id for t in spec.tasks}
    tasks = []
    for task in payload.get("task_instances", []):
        if task.get("task_id") not in known or task.get("state") not in states:
            raise ValueError("Unknown Airflow task or state")
        tasks.append(
            TaskRun(
                task_id=task["task_id"],
                status=states[task["state"]],
                attempts=task.get("try_number", 1),
            )
        )
    return PipelineRun(
        pipeline_id=spec.pipeline_id,
        run_id=payload["dag_run_id"],
        spec_hash=digest(spec),
        status=states[state],
        tasks=tasks,
    )


class RepairProposal(Model):
    base_hash: str
    proposed: PipelineSpec
    rationale: str
    status: Literal["PROPOSED"] = "PROPOSED"


def propose_retry(spec: PipelineSpec, run: PipelineRun) -> RepairProposal:
    if run.spec_hash != digest(spec) or run.pipeline_id != spec.pipeline_id:
        raise ValueError("Runtime evidence does not match this IR")
    if run.status != RunStatus.FAILED:
        raise ValueError("No failed run to repair")
    proposed = spec.model_copy(deep=True)
    proposed.retry_policy.retries = min(10, proposed.retry_policy.retries + 1)
    return RepairProposal(
        base_hash=digest(spec),
        proposed=proposed,
        rationale="Consider one additional bounded retry after checking the failure cause. Revalidate and obtain new approval before deployment.",
    )


def diagnose(run: PipelineRun, question: str) -> str:
    if run.failures:
        return "Observed failures: " + "; ".join(
            f"{f.task_id}: {f.category} — {f.message}" for f in run.failures
        )
    return f"Run {run.run_id} is {run.status.value}. No failure details were supplied; root cause is unresolved."
