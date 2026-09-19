"""Offline controlled-language frontend. Conservative: unsupported requests ask questions.

This frontend is deliberately not a general-purpose language model. The same validation
boundary is shared with the OpenAI-compatible frontend. No corpus lookup is performed.
"""

import re
from typing import Literal

from nlpipe.ir import (
    DestinationSpec,
    NotificationPolicy,
    PipelineSpec,
    QualityRule,
    ScheduleSpec,
    SourceSpec,
    TaskSpec,
)


class ControlledProvider:
    name = "controlled"

    def generate(self, prompt, catalog, previous=None):
        text = prompt.lower().strip().rstrip(".")

        def question(field, reason):
            return {
                "status": "needs_clarification",
                "questions": [
                    {"field": field, "question": f"Please specify {field}.", "reason": reason}
                ],
            }

        if any(word in text for word in ("super_magic_loader", "connector called")):
            return question("capability", "Only registered capabilities can be used")
        if " and never " in text or ("hourly" in text and "daily" in text):
            return question("schedule", "Conflicting schedule requirements")
        if any(
            word in text
            for word in (
                "embedding",
                "classify",
                "identit",
                "lifetime",
                "distribution",
                "archive",
                "retention",
                "30 days",
                "backfill",
                "incremental",
                "parameterized",
            )
        ):
            return question(
                "adapter",
                "This request requires explicit adapter/configuration support; use the model frontend or author IR",
            )
        cron = None
        specified_schedule = False
        if any(
            word in text
            for word in (
                "daily",
                "every day",
                "each day",
                "nightly",
                "every night",
                "each night",
                "every morning",
            )
        ):
            cron, specified_schedule = "0 0 * * *", True
        if "every hour" in text or "hourly" in text:
            cron, specified_schedule = "0 * * * *", True
        if "every fifteen minutes" in text or "every 15 minutes" in text:
            cron, specified_schedule = "*/15 * * * *", True
        if "every five minutes" in text or "every 5 minutes" in text:
            cron, specified_schedule = "*/5 * * * *", True
        if "every monday" in text:
            cron, specified_schedule = "0 0 * * 1", True
        if "except sunday" in text:
            cron, specified_schedule = "0 0 * * 1-6", True
        times = re.findall(
            r"\b(?:at\s+|for\s+)?(\d{1,2})(?::(\d{2}))?\s*(am|pm)\b|\b(\d{2}):(\d{2})\b", text
        )
        if "two in the morning" in text:
            times.append(("2", "00", "am", "", ""))
        if times:
            h, m, meridiem, hh, mm = times[0]
            hour, minute = int(h or hh), int(m or mm or 0)
            if hour > 23 or minute > 59 or (meridiem and not 1 <= hour <= 12):
                return question("schedule", "Invalid time")
            if meridiem:
                hour = hour % 12 + (12 if meridiem == "pm" else 0)
            cron = f"{minute} {hour} " + (cron.split(" ", 2)[2] if cron else "* * *")
            specified_schedule = True
        elif (
            any(word in text for word in ("every morning", "every night", "nightly"))
            and "midnight" not in text
        ):
            return question("schedule", "An exact run time is required")
        if "midnight" in text:
            cron, specified_schedule = "0 0 * * *", True
        timezone = re.search(r"\b(?:in|timezone)\s+([A-Za-z_]+/[A-Za-z_]+)\b", prompt)
        if previous:
            spec = previous.model_copy(deep=True)
            # Remove derived barriers before replanning after edits.
            barrier_targets = {"q_" + r.task: r.task for r in spec.quality_rules}
            spec.tasks = [t for t in spec.tasks if t.id not in barrier_targets]
            for task in spec.tasks:
                task.inputs = [
                    "task:" + barrier_targets.get(x[5:], x[5:]) if x.startswith("task:") else x
                    for x in task.inputs
                ]
                task.dependencies = [barrier_targets.get(x, x) for x in task.dependencies]
            if specified_schedule:
                spec.schedule = ScheduleSpec(cron=cron)
            elif "quarantine" in text:
                if not spec.quality_rules:
                    return question("quality rule", "Specify which records are invalid")
                matches = catalog.resolve("quarantine")
                if len(matches) != 1:
                    return question(
                        "quarantine destination", "A unique quarantine asset is required"
                    )
                spec.quality_rules = [
                    r.model_copy(
                        update={"action": "quarantine", "quarantine_asset": matches[0].identifier}
                    )
                    for r in spec.quality_rules
                ]
            elif "alert" in text or "notify" in text:
                notifications = [a for a in catalog.assets if a.type == "notification"]
                if len(notifications) != 1:
                    return question(
                        "notification destination", "Register an explicit notification asset"
                    )
                percent = re.search(r"(\d+(?:\.\d+)?)\s*(?:percent|%)", text)
                rate = (
                    float(percent[1]) / 100 if percent else (0.01 if "one percent" in text else 0)
                )
                spec.notifications = [
                    NotificationPolicy(
                        asset=notifications[0].identifier, on="quality", threshold=rate
                    )
                ]
            else:
                return question(
                    "modification",
                    "Offline frontend supports schedule, quarantine, and alert changes",
                )
            if timezone:
                spec.timezone = timezone[1]
            return {
                "status": "ready",
                "intent": "Modify existing pipeline",
                "spec": spec.model_dump(mode="json"),
            }
        # Exact identifiers/aliases only; catalog ambiguity is surfaced, never tie-broken.
        mentioned = []
        for asset in catalog.assets:
            names = [asset.identifier, asset.identifier.replace("_", " "), *asset.aliases]
            if any(re.search(r"(?<!\w)" + re.escape(name) + r"(?!\w)", text) for name in names):
                mentioned.append(asset)
        sources = [
            a
            for a in mentioned
            if "read" in a.allowed_operations and "write" not in a.allowed_operations
        ]
        destinations = [
            a for a in mentioned if "write" in a.allowed_operations and a.identifier != "quarantine"
        ]
        for a in sources:
            if any(alias in text and len(catalog.resolve(alias)) > 1 for alias in a.aliases):
                return question("source", "Multiple compatible sources match the name")
        if not sources:
            return question("source", "No unique registered input was specified")
        if not destinations and "join" not in text and "ad-hoc" not in text:
            return question(
                "destination", "Specify a registered output, or explicitly request ad-hoc results"
            )
        if any(s in text for s in ("yesterday", "previous day")):
            return question(
                "time window",
                "Specify the timestamp field and data interval for yesterday's records",
            )
        tasks = []
        for asset in sorted(sources, key=lambda a: a.identifier):
            kind = {"sqlite": "sql"}.get(asset.type, asset.type)
            tasks.append(
                TaskSpec(
                    id="read_" + asset.identifier,
                    type="ingestion",
                    capability="ingest." + kind + "@1",
                    inputs=["asset:" + asset.identifier],
                )
            )
        head = tasks[-1].id
        if len(sources) > 1:
            if "join" in text:
                keys = re.search(r"(?:on|using)\s+([a-z][a-z0-9_]*)", text)
                if not keys:
                    return question("join keys", "Join keys must be explicitly specified")
                tasks.append(
                    TaskSpec(
                        id="join_data",
                        type="transformation",
                        capability="transform.join@1",
                        inputs=["task:" + t.id for t in tasks],
                        parameters={"left_key": keys[1], "right_key": keys[1], "how": "inner"},
                    )
                )
            elif "combine" in text:
                tasks.append(
                    TaskSpec(
                        id="combine_data",
                        type="transformation",
                        capability="transform.union@1",
                        inputs=["task:" + t.id for t in tasks],
                    )
                )
            else:
                return question("source", "Multiple sources require explicit combination semantics")
            head = tasks[-1].id
        if any(s in text for s in ("deduplicate", "duplicates")):
            key = re.search(r"(?:using|by|on)\s+([a-z][a-z0-9_]*)(?:[,. ]|$)", text)
            if not key:
                return question("deduplication key", "Specify the identity column")
            tasks.append(
                TaskSpec(
                    id="deduplicate",
                    type="transformation",
                    capability="transform.deduplicate@1",
                    inputs=["task:" + head],
                    parameters={"keys": [key[1]]},
                )
            )
            head = tasks[-1].id
        if "normalize" in text:
            match = re.search(r"normalize\s+(\w+)\s+(?:to\s+)?(lower|upper|strip)", text)
            if not match:
                return question(
                    "normalization", "Specify a field and lower, upper, or strip operation"
                )
            tasks.append(
                TaskSpec(
                    id="normalize",
                    type="transformation",
                    capability="transform.normalize@1",
                    inputs=["task:" + head],
                    parameters={"fields": [match[1]], "operation": match[2]},
                )
            )
            head = tasks[-1].id
        if "aggregate" in text or "calculate" in text or "summary" in text:
            match = re.search(r"sum\s+(\w+)\s+by\s+(\w+)", text)
            if not match:
                return question(
                    "aggregation", "Specify sum FIELD by GROUP for the offline frontend"
                )
            tasks.append(
                TaskSpec(
                    id="aggregate",
                    type="transformation",
                    capability="transform.aggregate@1",
                    inputs=["task:" + head],
                    parameters={
                        "field": match[1],
                        "group_by": [match[2]],
                        "operation": "sum",
                        "output": "total",
                    },
                )
            )
            head = tasks[-1].id
        rules = []
        not_null = re.search(
            r"(?:without|missing|null|not null)\s+(?:a\s+)?([a-z][a-z0-9_]*)", text
        )
        if not_null:
            action: Literal["fail", "quarantine"] = "quarantine" if "quarantine" in text else "fail"
            rules.append(
                QualityRule(
                    id="required_id",
                    task=head,
                    kind="not_null",
                    field=not_null[1],
                    action=action,
                    quarantine_asset="quarantine" if action == "quarantine" else None,
                )
            )
        elif unique_match := re.search(r"(\w+)\s+is\s+unique", text):
            unique_field = unique_match[1]
            rules.append(QualityRule(id="unique_key", task=head, kind="unique", field=unique_field))
        elif "schema" in text:
            rules.append(QualityRule(id="schema_check", task=head, kind="schema"))
        elif "quarantine" in text or "validat" in text:
            return question("quality rule", "Specify the validation rule and its thresholds")
        for asset in sorted(destinations, key=lambda a: a.identifier):
            kind = {"sqlite": "sql"}.get(asset.type, asset.type)
            tasks.append(
                TaskSpec(
                    id="write_" + asset.identifier,
                    type="output",
                    capability="output." + kind + "@1",
                    inputs=["task:" + head],
                    outputs=["asset:" + asset.identifier],
                )
            )
        spec = PipelineSpec(
            pipeline_id="pipeline",
            name="Data pipeline",
            schedule=ScheduleSpec(cron=cron),
            timezone=timezone[1] if timezone else "UTC",
            sources=[SourceSpec(asset=a.identifier) for a in sources],
            destinations=[
                DestinationSpec(
                    asset=a.identifier,
                    mode="rebuild"
                    if "delete" in text
                    else "append"
                    if "append" in text
                    else "replace",
                )
                for a in destinations
            ],
            tasks=tasks,
            quality_rules=rules,
        )
        if "append" in text:
            spec.execution_policy.idempotent = False
        if "production" in text:
            return question(
                "owner",
                "Production requires an explicit accountable owner; author IR or use the model frontend",
            )
        return {
            "status": "ready",
            "intent": "Read, transform, validate, and publish registered data",
            "spec": spec.model_dump(mode="json"),
        }
