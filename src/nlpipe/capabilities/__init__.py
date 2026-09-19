"""Closed, versioned capability registry and JSON Schema parameter contracts."""

from dataclasses import dataclass, field

from jsonschema import Draft202012Validator


def schema(properties=None, required=()):
    return {
        "type": "object",
        "properties": properties or {},
        "required": list(required),
        "additionalProperties": False,
    }


STRING = {"type": "string", "minLength": 1, "maxLength": 200}
FIELDS = {"type": "array", "items": STRING, "minItems": 1, "uniqueItems": True}
NUMBER = {"type": "number"}


@dataclass(frozen=True)
class Capability:
    name: str
    family: str
    description: str
    parameter_schema: dict = field(default_factory=schema)
    accepted_inputs: tuple[str, ...] = ("table",)
    produced_outputs: str = "table"
    security_classification: str = "internal"
    deterministic: bool = True
    side_effect: bool = False
    required_secrets: tuple[str, ...] = ()
    resource_requirements: dict = field(default_factory=lambda: {"max_rows": 100000})
    implemented: bool = True
    min_inputs: int = 1
    max_inputs: int = 1
    version: str = "1"

    @property
    def reference(self):
        return f"{self.name}@{self.version}"


class Registry:
    def __init__(self):
        self.entries: dict[str, Capability] = {}

    def register(self, capability: Capability):
        if capability.reference in self.entries:
            raise ValueError("Capability already registered")
        Draft202012Validator.check_schema(capability.parameter_schema)
        self.entries[capability.reference] = capability

    def get(self, reference: str) -> Capability:
        if reference not in self.entries:
            raise ValueError(f"Unknown capability: {reference}")
        return self.entries[reference]

    def context(self):
        return [
            {
                "reference": c.reference,
                "type": c.family,
                "parameters": c.parameter_schema,
                "implemented": c.implemented,
                "min_inputs": c.min_inputs,
                "max_inputs": c.max_inputs,
            }
            for c in sorted(self.entries.values(), key=lambda c: c.reference)
        ]


def builtin_registry() -> Registry:
    registry = Registry()

    def add(name, family, parameters=None, **kwargs):
        registry.register(
            Capability(name, family, name.replace(".", " "), parameters or schema(), **kwargs)
        )

    for kind in (
        "csv",
        "json",
        "jsonl",
        "parquet",
        "filesystem",
        "http",
        "sql",
        "postgresql",
        "object_storage",
    ):
        add(
            "ingest." + kind,
            "ingestion",
            min_inputs=1,
            implemented=kind not in {"postgresql", "object_storage"},
        )
    add("transform.deduplicate", "transformation", schema({"keys": FIELDS}, ["keys"]))
    add(
        "transform.normalize",
        "transformation",
        schema(
            {"fields": FIELDS, "operation": {"enum": ["lower", "upper", "strip"]}},
            ["fields", "operation"],
        ),
    )
    add(
        "transform.filter",
        "transformation",
        schema(
            {
                "field": STRING,
                "operator": {"enum": ["eq", "ne", "gt", "ge", "lt", "le", "not_null"]},
                "value": {},
            },
            ["field", "operator"],
        ),
    )
    add(
        "transform.map",
        "transformation",
        schema(
            {"mapping": {"type": "object", "additionalProperties": STRING, "minProperties": 1}},
            ["mapping"],
        ),
    )
    add(
        "transform.schema_mapping",
        "transformation",
        schema(
            {"mapping": {"type": "object", "additionalProperties": STRING, "minProperties": 1}},
            ["mapping"],
        ),
    )
    add(
        "transform.aggregate",
        "transformation",
        schema(
            {
                "group_by": FIELDS,
                "field": STRING,
                "operation": {"enum": ["sum", "count", "mean", "min", "max"]},
                "output": STRING,
            },
            ["group_by", "field", "operation", "output"],
        ),
    )
    add(
        "transform.join",
        "transformation",
        schema(
            {"left_key": STRING, "right_key": STRING, "how": {"enum": ["inner", "left"]}},
            ["left_key", "right_key", "how"],
        ),
        min_inputs=2,
        max_inputs=2,
    )
    add("transform.union", "transformation", min_inputs=2, max_inputs=20)
    for kind in ("sql", "python_callable"):
        add("transform." + kind, "transformation", implemented=False)
    add("quality.check", "quality", schema({"rule_ids": FIELDS}, ["rule_ids"]))
    for kind in (
        "schema",
        "null",
        "uniqueness",
        "range",
        "referential",
        "freshness",
        "row_count",
        "distribution",
        "anomaly",
    ):
        add("quality." + kind, "quality", implemented=False)
    add(
        "enrich.lookup",
        "enrichment",
        schema(
            {"left_key": STRING, "right_key": STRING, "how": {"enum": ["inner", "left"]}},
            ["left_key", "right_key", "how"],
        ),
        min_inputs=2,
        max_inputs=2,
    )
    for kind in ("entity_resolution", "embeddings", "llm"):
        add("enrich." + kind, "enrichment", implemented=False, deterministic=False)
    for kind in (
        "csv",
        "json",
        "jsonl",
        "parquet",
        "filesystem",
        "sql",
        "postgresql",
        "object_storage",
        "api",
    ):
        add(
            "output." + kind,
            "output",
            side_effect=True,
            implemented=kind not in {"postgresql", "object_storage", "api"},
        )
    add(
        "ops.checkpoint",
        "operations",
        schema({"expected_count": {"type": "integer", "minimum": 1}}, ["expected_count"]),
        min_inputs=1,
        max_inputs=20,
    )
    add("ops.notification", "operations", schema({"asset": STRING}, ["asset"]), side_effect=True)
    for kind in ("quarantine", "retry", "branch", "conditional"):
        add("ops." + kind, "operations", implemented=False)
    return registry
