"""Independent semantic dimensions; absent expected dimensions are not scored as passes."""

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from nlpipe.catalog import Catalog
from nlpipe.deployment import atomic_write
from nlpipe.intent import Provider, interpret
from nlpipe.ir import PipelineSpec
from nlpipe.validation import validate

DIMENSIONS = (
    "intent",
    "assets",
    "capabilities",
    "graph",
    "schedule",
    "quality",
    "policy",
    "clarification",
)


def properties(result, catalog):
    spec = result.spec
    report = validate(spec, catalog) if spec else None
    return {
        "intent": result.status,
        "assets": {
            "sources": sorted(s.asset for s in spec.sources),
            "destinations": sorted(d.asset for d in spec.destinations),
        }
        if spec
        else None,
        "capabilities": [t.capability for t in spec.tasks] if spec else None,
        "graph": sorted([[dep, t.id] for t in spec.tasks for dep in t.dependencies])
        if spec
        else None,
        "schedule": {"cron": spec.schedule.cron, "timezone": spec.timezone} if spec else None,
        "quality": [
            {
                "kind": q.kind,
                "field": q.field,
                "action": q.action,
                "max_invalid_rate": q.max_invalid_rate,
            }
            for q in spec.quality_rules
        ]
        if spec
        else None,
        "policy": sorted(set(d.status for d in report.decisions)) if report else None,
        "clarification": sorted(q.field for q in result.questions),
    }


def evaluate_corpus(cases: list[dict], catalog: Catalog, provider: Provider, out: Path) -> dict:
    results = []
    for case in cases:
        previous = PipelineSpec.model_validate(case["previous"]) if case.get("previous") else None
        result = interpret(case["request"], catalog, provider, previous)
        actual = properties(result, catalog)
        scores = {
            name: actual[name] == case["expected"][name]
            for name in DIMENSIONS
            if name in case["expected"]
        }
        # Canonical properties use dotted field paths and compare exact values.
        canonical_checks = {}
        value: Any
        for path, expected in case.get("canonical", {}).items():
            value = result.spec.model_dump(mode="json") if result.spec else None
            try:
                for part in path.split("."):
                    value = value[int(part)] if isinstance(value, list) else value[part]
            except (KeyError, IndexError, TypeError, ValueError):
                value = None
            canonical_checks[path] = value == expected
        results.append(
            {
                "id": case["id"],
                "scores": scores,
                "canonical": canonical_checks,
                "passed": all(scores.values()) and all(canonical_checks.values()),
                "actual": actual,
                "errors": result.errors,
            }
        )
    summary: dict[str, Any] = {
        name: {
            "correct": sum(r["scores"].get(name) is True for r in results),
            "scored": sum(name in r["scores"] for r in results),
        }
        for name in DIMENSIONS
    }
    for metric in summary.values():
        metric["accuracy"] = metric["correct"] / metric["scored"] if metric["scored"] else None
    summary.update(
        {
            "cases": len(results),
            "passed": sum(r["passed"] for r in results),
            "all_cases_pass": all(r["passed"] for r in results),
        }
    )
    payload = {
        "provider": provider.name,
        "model": getattr(getattr(provider, "config", None), "model", None),
        "created_at": datetime.now(UTC).isoformat(),
        "summary": summary,
        "results": results,
    }
    atomic_write(out / "eval-results.json", json.dumps(payload, indent=2))
    lines = [
        "# Semantic evaluation",
        "",
        f"Provider: {provider.name}. Cases: {summary['passed']}/{len(results)}.",
        "",
        "| Dimension | Correct | Scored |",
        "|---|---:|---:|",
    ]
    lines += [
        f"| {name} | {summary[name]['correct']} | {summary[name]['scored']} |"
        for name in DIMENSIONS
    ]
    lines += [
        "",
        "Intent accuracy here measures the expected disposition (ready/clarify/reject), not a free-text similarity score.",
        "Unscored dimensions are excluded, never counted as successes. Controlled/mock results do not establish LLM accuracy.",
        "",
        "## Case results",
        "",
    ]
    lines += [f"- {r['id']}: {'PASS' if r['passed'] else 'FAIL'}" for r in results]
    atomic_write(out / "eval-report.md", "\n".join(lines) + "\n")
    return payload
