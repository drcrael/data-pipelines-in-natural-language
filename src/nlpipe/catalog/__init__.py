"""Trusted administrator-owned asset declarations; model output only references IDs."""

from pathlib import Path
from typing import Literal

import yaml
from pydantic import Field, model_validator

from nlpipe.ir import Identifier, Model, SecretReference


class Asset(Model):
    identifier: Identifier
    type: Literal[
        "csv",
        "json",
        "jsonl",
        "parquet",
        "http",
        "sqlite",
        "postgresql",
        "object_storage",
        "notification",
    ]
    location: str
    schema_fields: dict[str, Literal["string", "integer", "number", "boolean", "datetime"]] = Field(
        default_factory=dict, alias="schema"
    )
    owner: str = ""
    tags: list[str] = Field(default_factory=list)
    description: str = ""
    freshness_seconds: int | None = Field(default=None, gt=0)
    quality_metadata: dict = Field(default_factory=dict)
    lineage: list[str] = Field(default_factory=list)
    allowed_operations: list[Literal["read", "write", "delete", "notify"]] = Field(default=["read"])
    credential_reference: SecretReference | None = None
    table: Identifier | None = None
    aliases: list[str] = Field(default_factory=list)
    external: bool = False


class Catalog(Model):
    assets: list[Asset]

    @model_validator(mode="after")
    def unique_ids(self):
        if len({a.identifier for a in self.assets}) != len(self.assets):
            raise ValueError("Duplicate asset identifiers")
        return self

    def get(self, identifier: str) -> Asset:
        for asset in self.assets:
            if asset.identifier == identifier:
                return asset
        raise ValueError(f"Unknown asset: {identifier}")

    def resolve(self, name: str) -> list[Asset]:
        normalized = name.lower().strip().replace(" ", "_")
        exact = [a for a in self.assets if a.identifier == normalized]
        return exact or [a for a in self.assets if name.lower().strip() in a.aliases]

    def context(self) -> list[dict]:
        # Never send physical locations, credentials, or data to a model.
        return [
            {
                "identifier": a.identifier,
                "type": a.type,
                "schema": a.schema_fields,
                "allowed_operations": a.allowed_operations,
                "aliases": a.aliases,
            }
            for a in self.assets
        ]


def load_catalog(path: Path) -> Catalog:
    if path.stat().st_size > 2_000_000:
        raise ValueError("Catalog exceeds size limit")
    return Catalog.model_validate(yaml.safe_load(path.read_text()))
