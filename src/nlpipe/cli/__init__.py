"""CLI: commands emit structured results and nonzero exits for unresolved or failed work."""

import json
from pathlib import Path
from typing import Annotated

import typer
import yaml

from nlpipe.catalog import load_catalog
from nlpipe.compiler import compile_airflow
from nlpipe.deployment import Approval, atomic_write, sign
from nlpipe.deployment import deploy as deploy_artifact
from nlpipe.intent import OpenAICompatibleProvider, ProviderConfig, interpret
from nlpipe.intent import explain as describe
from nlpipe.intent.controlled import ControlledProvider
from nlpipe.ir import PipelineSpec, load, save
from nlpipe.ir import diff as ir_diff
from nlpipe.observability import PipelineRun, diagnose, from_airflow, propose_retry
from nlpipe.runtime import run_pipeline
from nlpipe.validation import validate as validate_spec
from nlpipe.verification import generated_test, verify

app = typer.Typer(
    no_args_is_help=True,
    help="Compile natural-language data pipelines through governed, versioned IR.",
)
CatalogOption = Annotated[Path, typer.Option("--catalog", "-c", exists=True, dir_okay=False)]


def output(value):
    typer.echo(json.dumps(value, indent=2, default=str))


def provider(config: Path | None):
    return (
        OpenAICompatibleProvider(ProviderConfig.model_validate(yaml.safe_load(config.read_text())))
        if config
        else ControlledProvider()
    )


def fail(exc):
    # Never echo Pydantic input values or arbitrary IO exception messages.
    from pydantic import ValidationError

    output(
        {
            "status": "error",
            "error": "Invalid configuration or IR"
            if isinstance(exc, ValidationError)
            else type(exc).__name__,
        }
    )
    raise typer.Exit(2)


@app.command()
def create(
    prompt: str,
    catalog: CatalogOption,
    out: Annotated[Path, typer.Option("--out", "-o")] = Path("pipeline.yaml"),
    config: Annotated[Path | None, typer.Option(exists=True)] = None,
    previous: Annotated[Path | None, typer.Option(exists=True)] = None,
):
    """Interpret a request or modify prior IR. Never deploy implicitly."""
    try:
        assets = load_catalog(catalog)
        old = load(previous) if previous else None
        result = interpret(prompt, assets, provider(config), old)
        payload = result.model_dump(mode="json")
        if result.spec:
            if out.exists() or out.with_suffix(".py").exists():
                raise FileExistsError("Choose a new output path")
            out.parent.mkdir(parents=True, exist_ok=True)
            save(result.spec, out)
            dag_path = out.with_suffix(".py")
            atomic_write(dag_path, compile_airflow(result.spec, assets))
            payload["validation"] = validate_spec(result.spec, assets).model_dump(
                mode="json", exclude={"normalized"}
            )
            payload["generated_dag"] = str(dag_path)
            payload["state"] = "VALIDATED"
            if old:
                payload["diff"] = ir_diff(old, result.spec)
        output(payload)
        if result.status != "ready":
            raise typer.Exit(3 if result.status == "needs_clarification" else 2)
    except (ValueError, OSError, KeyError) as exc:
        fail(exc)


@app.command()
def explain(path: Path):
    """Explain an IR without invoking a model."""
    try:
        typer.echo(describe(load(path)))
    except (ValueError, OSError) as exc:
        fail(exc)


@app.command()
def validate(path: Path, catalog: CatalogOption):
    try:
        report = validate_spec(load(path), load_catalog(catalog))
        output(report.model_dump(mode="json", exclude={"normalized"}))
        if not report.valid:
            raise typer.Exit(2)
    except (ValueError, OSError) as exc:
        fail(exc)


@app.command("compile")
def compile_command(
    path: Path,
    catalog: CatalogOption,
    out: Annotated[Path, typer.Option("--out", "-o")],
    target: str = "airflow",
):
    try:
        if target != "airflow":
            raise ValueError("Unsupported compiler target")
        spec, assets = load(path), load_catalog(catalog)
        atomic_write(out, compile_airflow(spec, assets))
        atomic_write(
            out.with_name("test_" + out.stem + ".py"),
            generated_test(spec, assets).replace(repr(spec.pipeline_id + ".py"), repr(out.name)),
        )
        output({"artifact": str(out), "state": "VALIDATED"})
    except (ValueError, OSError) as exc:
        fail(exc)


@app.command("test")
def test_command(
    path: Path,
    catalog: CatalogOption,
    airflow: bool = False,
    fixture_root: Annotated[Path | None, typer.Option(exists=True, file_okay=False)] = None,
):
    """Verify IR and artifact; optionally execute a copied synthetic fixture sandbox."""
    try:
        spec, assets = load(path), load_catalog(catalog)
        result = verify(spec, assets, airflow)
        if fixture_root and result["passed"]:
            import shutil
            import tempfile

            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory) / "fixtures"
                shutil.copytree(fixture_root, root, symlinks=True)
                run = run_pipeline(spec, assets, root)
                result["fixture_run"] = run.model_dump(mode="json")
                result["passed"] &= run.status.value == "success"
        output(result)
        if not result["passed"]:
            raise typer.Exit(2)
    except (ValueError, OSError) as exc:
        fail(exc)


@app.command()
def run(
    path: Path,
    catalog: CatalogOption,
    data_root: Path,
    approval: Annotated[Path | None, typer.Option(exists=True)] = None,
):
    """Execute locally against the explicitly selected root (writes real outputs)."""
    try:
        approved = Approval.model_validate_json(approval.read_text()) if approval else None
        result = run_pipeline(load(path), load_catalog(catalog), data_root, approved)
        output(result.model_dump(mode="json"))
        if result.status.value != "success":
            raise typer.Exit(2)
    except (ValueError, OSError) as exc:
        fail(exc)


@app.command()
def diff(old: Path, new: Path):
    try:
        output(ir_diff(load(old), load(new)))
    except (ValueError, OSError) as exc:
        fail(exc)


@app.command()
def approve(
    path: Path,
    catalog: CatalogOption,
    actor: str,
    out: Annotated[Path, typer.Option("--out", "-o")],
    hours: int = 24,
):
    """Sign exact validated IR/catalog; requires an administrator-managed approval key."""
    try:
        record = sign(load(path), load_catalog(catalog), actor, hours)
        atomic_write(out, record.model_dump_json(indent=2))
        output({"state": "APPROVED", "approval": str(out), "expires_at": record.expires_at})
    except (ValueError, OSError) as exc:
        fail(exc)


@app.command()
def deploy(
    path: Path,
    catalog: CatalogOption,
    dag_folder: Path,
    environment: str = "dev",
    approval: Annotated[Path | None, typer.Option(exists=True)] = None,
):
    try:
        spec = load(path)
        if spec.execution_policy.environment != environment:
            raise ValueError("Deployment environment must match reviewed IR")
        record = Approval.model_validate_json(approval.read_text()) if approval else None
        result = deploy_artifact(spec, load_catalog(catalog), dag_folder, record)
        output({"state": "DEPLOYED", "artifact": str(result), "scheduler_status": "unobserved"})
    except (ValueError, OSError) as exc:
        fail(exc)


def latest(pipeline_id, data_root):
    # Prevent input being interpreted as a path.
    import re

    from nlpipe.runtime.storage import confined

    if not re.fullmatch(r"[a-z][a-z0-9_]{0,62}", pipeline_id):
        raise ValueError("Invalid pipeline ID")
    files = list(confined(data_root, f".nlpipe/runs/{pipeline_id}").glob("*.json"))
    runs = [PipelineRun.model_validate_json(p.read_text()) for p in files]
    if not runs:
        raise ValueError("No runtime observations recorded")
    return max(runs, key=lambda r: r.started_at)


@app.command()
def status(pipeline_id: str, data_root: Path):
    try:
        output(latest(pipeline_id, data_root).model_dump(mode="json"))
    except (ValueError, OSError) as exc:
        fail(exc)


@app.command()
def ask(pipeline_id: str, question: str, data_root: Path):
    try:
        typer.echo(diagnose(latest(pipeline_id, data_root), question))
    except (ValueError, OSError) as exc:
        fail(exc)


@app.command()
def observe(path: Path, airflow_state: Path, out: Annotated[Path, typer.Option("--out", "-o")]):
    try:
        run = from_airflow(json.loads(airflow_state.read_text()), load(path))
        atomic_write(out, run.model_dump_json(indent=2))
        output({"observation": str(out)})
    except (ValueError, OSError, KeyError) as exc:
        fail(exc)


@app.command()
def repair(path: Path, run_file: Path, out: Annotated[Path, typer.Option("--out", "-o")]):
    """Produce a reviewable IR proposal; never execute or deploy it."""
    try:
        proposal = propose_retry(load(path), PipelineRun.model_validate_json(run_file.read_text()))
        atomic_write(out, proposal.model_dump_json(indent=2))
        output({"state": "DRAFT", "proposal": str(out)})
    except (ValueError, OSError) as exc:
        fail(exc)


@app.command()
def schema(out: Annotated[Path, typer.Option("--out", "-o")]):
    atomic_write(out, json.dumps(PipelineSpec.model_json_schema(), indent=2))


@app.command()
def evaluate(
    corpus: Path,
    catalog: CatalogOption,
    out: Annotated[Path, typer.Option("--out", "-o")],
    config: Path | None = None,
):
    from nlpipe.evaluation import evaluate_corpus

    try:
        result = evaluate_corpus(
            json.loads(corpus.read_text()), load_catalog(catalog), provider(config), out
        )
        output(result["summary"])
        if not result["summary"]["all_cases_pass"]:
            raise typer.Exit(2)
    except (ValueError, OSError) as exc:
        fail(exc)
