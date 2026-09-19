"""Local Airflow DAG-folder deployment with immutable, signed approval evidence."""

import hashlib
import hmac
import json
import os
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal

from nlpipe.catalog import Catalog
from nlpipe.ir import Model, PipelineSpec, canonical, digest
from nlpipe.validation import require_valid, validate


class Approval(Model):
    spec_hash: str
    catalog_hash: str
    environment: Literal["dev", "production"]
    actor: str
    issued_at: datetime
    expires_at: datetime
    signature: str


def _key() -> bytes:
    value = os.environ.get("NLPIPE_APPROVAL_KEY", "")
    if len(value) < 32:
        raise ValueError(
            "NLPIPE_APPROVAL_KEY must contain at least 32 characters, managed outside the IR"
        )
    return value.encode()


def sign(spec: PipelineSpec, catalog: Catalog, actor: str, hours: int = 24) -> Approval:
    report = validate(spec, catalog)
    if not report.valid or report.normalized is None:
        raise ValueError("Cannot approve invalid IR")
    if not actor.strip() or not 1 <= hours <= 168:
        raise ValueError("Approval requires actor and expiry of 1–168 hours")
    now = datetime.now(UTC)
    record = Approval(
        spec_hash=digest(report.normalized),
        catalog_hash=digest(catalog),
        environment=spec.execution_policy.environment,
        actor=actor,
        issued_at=now,
        expires_at=now + timedelta(hours=hours),
        signature="",
    )
    record.signature = hmac.new(_key(), canonical(record).encode(), hashlib.sha256).hexdigest()
    return record


def check_approval(spec: PipelineSpec, catalog: Catalog, approval: Approval | None):
    report = validate(spec, catalog)
    if not report.valid:
        raise ValueError("Invalid pipeline: " + "; ".join(report.errors))
    assert report.normalized is not None
    if not report.requires_approval:
        return
    if approval is None:
        raise ValueError("Human approval required before execution or deployment")
    data = approval.model_copy(update={"signature": ""})
    expected = hmac.new(_key(), canonical(data).encode(), hashlib.sha256).hexdigest()
    now = datetime.now(UTC)
    if (
        not hmac.compare_digest(expected, approval.signature)
        or approval.spec_hash != digest(report.normalized)
        or approval.catalog_hash != digest(catalog)
        or approval.environment != spec.execution_policy.environment
        or approval.expires_at.tzinfo is None
        or approval.issued_at.tzinfo is None
        or not approval.issued_at <= now < approval.expires_at
    ):
        raise ValueError("Approval is invalid, expired, or bound to different content")


def atomic_write(path: Path, content: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=".nlpipe-", dir=path.parent)
    try:
        with os.fdopen(fd, "w") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def deploy(
    spec: PipelineSpec, catalog: Catalog, dag_folder: Path, approval: Approval | None = None
) -> Path:
    from nlpipe.compiler import compile_airflow
    from nlpipe.verification import verify

    check_approval(spec, catalog, approval)
    result = verify(spec, catalog, airflow=True)
    if not result["passed"]:
        raise ValueError("Deployment verification failed: " + json.dumps(result))
    destination = dag_folder / (spec.pipeline_id + ".py")
    code = compile_airflow(spec, catalog)
    atomic_write(destination, code)
    atomic_write(
        dag_folder / (spec.pipeline_id + ".manifest.json"),
        json.dumps(
            {
                "state": "DEPLOYED",
                "spec_hash": digest(require_valid(spec, catalog)),
                "catalog_hash": digest(catalog),
                "artifact_sha256": hashlib.sha256(code.encode()).hexdigest(),
                "approval": approval.model_dump(mode="json") if approval else None,
                "note": "Installed in DAG folder; scheduler pickup and task success are separate observations",
            },
            indent=2,
        ),
    )
    return destination
