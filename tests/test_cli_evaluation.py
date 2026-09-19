import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from nlpipe.cli import app
from nlpipe.evaluation import evaluate_corpus
from nlpipe.intent import explain, interpret
from nlpipe.intent.controlled import ControlledProvider
from nlpipe.ir import canonical

ROOT = Path(__file__).resolve().parents[1]
runner = CliRunner()
pytestmark = pytest.mark.acceptance


def test_corpus_semantics(catalog, tmp_path):
    cases = json.loads((ROOT / "corpus/acceptance.json").read_text())
    assert len(cases) >= 40
    report = evaluate_corpus(cases, catalog, ControlledProvider(), tmp_path)
    assert report["summary"]["all_cases_pass"], [r for r in report["results"] if not r["passed"]]
    assert report["summary"]["assets"]["scored"] == 20
    assert (tmp_path / "eval-results.json").is_file()
    assert (tmp_path / "eval-report.md").is_file()


def test_paraphrase_invariance(catalog):
    groups = json.loads((ROOT / "corpus/paraphrases.json").read_text())
    for group in groups:
        normalized = []
        for prompt in group["requests"]:
            result = interpret(prompt, catalog, ControlledProvider())
            assert result.status == "ready", result
            normalized.append(canonical(result.spec))
        assert len(set(normalized)) == 1


def test_cli_complete_workflow(tmp_path, data_root):
    catalog = str(ROOT / "examples/catalog.yaml")
    out = tmp_path / "pipeline.yaml"
    commands = [
        ["--help"],
        ["create", "Load orders into analytics daily at 2 AM.", "-c", catalog, "-o", str(out)],
        ["validate", str(out), "-c", catalog],
        ["explain", str(out)],
        ["compile", str(out), "-c", catalog, "-o", str(tmp_path / "dag.py")],
        ["test", str(out), "-c", catalog, "--fixture-root", str(data_root)],
        ["run", str(out), "-c", catalog, str(data_root)],
        ["status", "pipeline", str(data_root)],
        ["ask", "pipeline", "Why did it fail?", str(data_root)],
        ["schema", "-o", str(tmp_path / "schema.json")],
        ["diff", str(out), str(out)],
    ]
    for command in commands:
        result = runner.invoke(app, command)
        assert result.exit_code == 0, (command, result.output, result.exception)
    assert (tmp_path / "dag.py").is_file()
    assert (tmp_path / "test_dag.py").is_file()
    result = runner.invoke(
        app, ["create", "Load orders into analytics daily at 2 AM.", "-c", catalog, "-o", str(out)]
    )
    assert result.exit_code == 2


def test_cli_clarify_and_reject(tmp_path):
    catalog = str(ROOT / "examples/catalog.yaml")
    for prompt, expected in [("Move the data over there.", 3), ("Use password hunter2", 2)]:
        result = runner.invoke(
            app, ["create", prompt, "-c", catalog, "-o", str(tmp_path / "no.yaml")]
        )
        assert result.exit_code == expected
        assert "hunter2" not in result.output
        assert not (tmp_path / "no.yaml").exists()


def test_cli_bad_inputs(tmp_path):
    bad = tmp_path / "bad.yaml"
    bad.write_text("version: bad")
    catalog = str(ROOT / "examples/catalog.yaml")
    for command in [
        ["explain", str(bad)],
        ["validate", str(bad), "-c", catalog],
        ["compile", str(bad), "-c", catalog, "-o", str(tmp_path / "out.py")],
        ["test", str(bad), "-c", catalog],
        ["run", str(bad), "-c", catalog, str(tmp_path)],
        ["diff", str(bad), str(bad)],
        ["status", "../escape", str(tmp_path)],
        ["ask", "missing", "why", str(tmp_path)],
    ]:
        assert runner.invoke(app, command).exit_code == 2


def test_cli_conversational_modification(tmp_path):
    result = runner.invoke(
        app,
        [
            "create",
            "Run it at 4 AM.",
            "-c",
            str(ROOT / "examples/catalog.yaml"),
            "--previous",
            str(ROOT / "examples/orders.yaml"),
            "-o",
            str(tmp_path / "modified.yaml"),
        ],
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["spec"]["schedule"]["cron"] == "0 4 * * *"
    assert payload["spec"]["pipeline_id"] == "orders_daily"
    assert "schedule" in payload["diff"]


@pytest.mark.airflow
def test_cli_deploy(tmp_path):
    result = runner.invoke(
        app,
        [
            "deploy",
            str(ROOT / "examples/orders.yaml"),
            "-c",
            str(ROOT / "examples/catalog.yaml"),
            str(tmp_path / "dags"),
        ],
    )
    assert result.exit_code == 0, result.output
    manifest = json.loads((tmp_path / "dags/orders_daily.manifest.json").read_text())
    assert manifest["state"] == "DEPLOYED"
    assert manifest["approval"] is None


def test_cli_approval(spec, catalog, tmp_path, monkeypatch):
    from nlpipe.ir import save

    monkeypatch.setenv("NLPIPE_APPROVAL_KEY", "test-only-key-" * 4)
    spec.execution_policy.environment = "production"
    path = tmp_path / "prod.yaml"
    save(spec, path)
    approval = tmp_path / "approval.json"
    result = runner.invoke(
        app,
        [
            "approve",
            str(path),
            "-c",
            str(ROOT / "examples/catalog.yaml"),
            "operator",
            "-o",
            str(approval),
        ],
    )
    assert result.exit_code == 0, result.output
    assert json.loads(approval.read_text())["actor"] == "operator"


def test_evaluation_detects_semantic_error(catalog, tmp_path):
    cases = [
        {
            "id": "wrong",
            "request": "Load orders into analytics daily at 2 AM.",
            "expected": {"intent": "ready", "schedule": {"cron": "0 3 * * *", "timezone": "UTC"}},
            "canonical": {"schedule.cron": "0 3 * * *"},
        }
    ]
    result = evaluate_corpus(cases, catalog, ControlledProvider(), tmp_path)
    assert not result["summary"]["all_cases_pass"]
    assert result["summary"]["schedule"]["correct"] == 0


def test_explanation(spec):
    text = explain(spec)
    for phrase in [
        "Schedule:",
        "Sources:",
        "Destinations:",
        "Quality:",
        "Retries:",
        "Notifications:",
        "Governance:",
    ]:
        assert phrase in text


@pytest.mark.parametrize(
    "fixture,prompt",
    [
        ("copy", "Load orders into analytics daily at 2 AM."),
        (
            "deduplicate",
            "Read orders, deduplicate using order_id, and write analytics daily at 2 AM.",
        ),
        ("quarantine", "Load orders into analytics; quarantine records without customer_id."),
    ],
)
def test_full_ir_goldens(fixture, prompt, catalog):
    from nlpipe.ir import load

    expected = load(ROOT / "corpus/golden" / f"{fixture}.json")
    result = interpret(prompt, catalog, ControlledProvider())
    assert result.status == "ready"
    assert result.spec == expected


@pytest.mark.parametrize(
    "path,value",
    [
        ("concurrency", 2),
        ("retry_policy.delay_seconds", 90),
        ("retry_policy.exponential_backoff", False),
        ("timeout_policy.pipeline_seconds", 7200),
        ("resources.memory_mb", 1024),
        ("execution_policy.idempotent", False),
        ("execution_policy.schema_evolution", "approved"),
        ("tasks.0.retries", 4),
        ("tasks.0.resources.memory_mb", 1024),
        ("destinations.0.key", ["order_id"]),
    ],
)
def test_explanation_preserves_operational_distinctions(spec, path, value):
    before = explain(spec)
    parts = path.split(".")
    target = spec
    for part in parts[:-1]:
        target = target[int(part)] if part.isdigit() else getattr(target, part)
    setattr(target, parts[-1], value)
    assert explain(spec) != before
