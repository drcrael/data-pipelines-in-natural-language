"""Deterministic graph normalization and explicit quality barriers."""

from graphlib import CycleError, TopologicalSorter

from nlpipe.ir import LineageEdge, PipelineSpec, TaskSpec


def order(tasks: list[TaskSpec]) -> list[str]:
    ids = [t.id for t in tasks]
    if len(ids) != len(set(ids)):
        raise ValueError("Duplicate task IDs")
    graph = {t.id: set(t.dependencies) for t in tasks}
    for task in tasks:
        for ref in task.inputs:
            if ref.startswith("task:"):
                graph[task.id].add(ref[5:])
        if not graph[task.id].issubset(ids):
            raise ValueError(f"Unknown task dependency for {task.id}")
    try:
        sorter = TopologicalSorter(graph)
        sorter.prepare()
        result = []
        while sorter.is_active():
            ready = sorted(sorter.get_ready())
            result.extend(ready)
            sorter.done(*ready)
        return result
    except CycleError as exc:
        raise ValueError("Dependency graph contains a cycle") from exc


def normalize(spec: PipelineSpec) -> PipelineSpec:
    result = spec.model_copy(deep=True)
    by_id = {t.id: t for t in result.tasks}
    for edge in result.dependencies:
        if edge.downstream not in by_id or edge.upstream not in by_id:
            raise ValueError("Unknown dependency endpoint")
        by_id[edge.downstream].dependencies.append(edge.upstream)
    result.dependencies = []
    for rule in sorted(result.quality_rules, key=lambda r: r.id):
        if rule.task not in by_id:
            raise ValueError(f"Quality rule refers to missing task: {rule.task}")
    # One quality barrier per target; existing generated barriers must match their rules.
    for target in sorted({r.task for r in result.quality_rules}):
        rule_ids = sorted(r.id for r in result.quality_rules if r.task == target)
        check_id = f"q_{target}"
        if len(check_id) > 63:
            raise ValueError("Task ID too long for quality barrier")
        if check_id in by_id:
            existing = by_id[check_id]
            if (
                existing.capability != "quality.check@1"
                or existing.inputs != [f"task:{target}"]
                or existing.parameters != {"rule_ids": rule_ids}
            ):
                raise ValueError("Quality barrier collision or stale rule binding")
        else:
            check = TaskSpec(
                id=check_id,
                type="quality",
                capability="quality.check@1",
                inputs=[f"task:{target}"],
                dependencies=[target],
                parameters={"rule_ids": list(rule_ids)},
                resources=by_id[target].resources.model_copy(deep=True),
            )
            result.tasks.append(check)
            by_id[check_id] = check
        for task in result.tasks:
            if task.id not in {target, check_id}:
                task.inputs = [
                    f"task:{check_id}" if ref == f"task:{target}" else ref for ref in task.inputs
                ]
                task.dependencies = [
                    check_id if dep == target else dep for dep in task.dependencies
                ]
    for task in result.tasks:
        task.dependencies = sorted(
            set(task.dependencies) | {r[5:] for r in task.inputs if r.startswith("task:")}
        )
        task.quality_checks = sorted(r.id for r in result.quality_rules if r.task == task.id)
    sequence = order(result.tasks)
    result.tasks = [by_id[name] for name in sequence]
    result.sources.sort(key=lambda x: x.asset)
    result.destinations.sort(key=lambda x: x.asset)
    result.quality_rules.sort(key=lambda x: x.id)
    result.tags = sorted(set(result.tags))
    edges = set()
    for task in result.tasks:
        for ref in task.inputs:
            edges.add((ref, f"task:{task.id}"))
        for ref in task.outputs:
            edges.add((f"task:{task.id}", ref))
    for rule in result.quality_rules:
        if rule.quarantine_asset:
            edges.add((f"task:q_{rule.task}", f"asset:{rule.quarantine_asset}"))
    result.lineage = [LineageEdge(source=a, destination=b) for a, b in sorted(edges)]
    return result
