"""Bounded IO through catalog declarations. No model-supplied paths or SQL."""

import csv
import hashlib
import json
import os
import re
import sqlite3
import tempfile
from pathlib import Path
from urllib.parse import urlparse

import httpx

from nlpipe.catalog import Asset
from nlpipe.deployment import atomic_write

MAX_BYTES = 32 * 1024 * 1024


def confined(root: Path, location: str) -> Path:
    if Path(location).is_absolute() or ".." in Path(location).parts:
        raise ValueError("Asset location must be relative and contain no traversal")
    path = (root / location).resolve()
    if not path.is_relative_to(root.resolve()):
        raise ValueError("Asset path escapes runtime root")
    return path


def rows_checked(rows, max_rows):
    if (
        not isinstance(rows, list)
        or len(rows) > max_rows
        or any(not isinstance(r, dict) for r in rows)
    ):
        raise ValueError("Expected bounded array of records")
    if len(json.dumps(rows, allow_nan=False).encode()) > MAX_BYTES:
        raise ValueError("Dataset exceeds byte limit")
    return rows


def read(asset: Asset, root: Path, max_rows: int, allow_http: bool = False):
    if "read" not in asset.allowed_operations:
        raise ValueError("Read forbidden")
    if asset.type == "http":
        endpoint = urlparse(asset.location)
        if (
            not allow_http
            or endpoint.scheme != "https"
            or endpoint.username
            or endpoint.password
            or endpoint.query
        ):
            raise ValueError(
                "HTTP reads require administrator network opt-in and a credential-free HTTPS endpoint"
            )
        headers = {}
        if asset.credential_reference:
            ref = asset.credential_reference
            if ref.provider != "environment" or not os.environ.get(ref.key):
                raise ValueError("Missing supported credential reference")
            headers["Authorization"] = "Bearer " + os.environ[ref.key]
        with httpx.Client(timeout=30, trust_env=False, follow_redirects=False) as client:
            with client.stream("GET", asset.location, headers=headers) as response:
                response.raise_for_status()
                data = bytearray()
                for chunk in response.iter_bytes():
                    data.extend(chunk)
                    if len(data) > MAX_BYTES:
                        raise ValueError("HTTP response too large")
        return rows_checked(json.loads(data), max_rows)
    path = confined(root, asset.location)
    if not path.is_file() or path.stat().st_size > MAX_BYTES:
        raise ValueError("Missing or oversized source")
    if asset.type == "csv":
        with path.open(newline="") as stream:
            reader = csv.DictReader(stream)
            if not reader.fieldnames or len(set(reader.fieldnames)) != len(reader.fieldnames):
                raise ValueError("Missing or duplicate CSV headers")
            rows: list[dict] = []
            for row in reader:
                if len(rows) >= max_rows or None in row:
                    raise ValueError("CSV row limit or shape violation")
                for key, value in row.items():
                    if value == "":
                        row[key] = None
                    elif asset.schema_fields.get(key) == "boolean" and value in {"true", "false"}:
                        row[key] = value == "true"
                    elif asset.schema_fields.get(key) in {"integer", "number"}:
                        try:
                            row[key] = (
                                int(value)
                                if asset.schema_fields[key] == "integer"
                                else float(value)
                            )
                        except (TypeError, ValueError):
                            pass  # Preserve invalid values for explicit quality checks.
                rows.append(row)
    elif asset.type == "json":
        rows = json.loads(path.read_text())
    elif asset.type == "jsonl":
        rows = []
        with path.open() as stream:
            for line in stream:
                if len(rows) >= max_rows:
                    raise ValueError("JSONL row limit exceeded")
                rows.append(json.loads(line))
    elif asset.type == "parquet":
        import pyarrow.parquet as pq

        file = pq.ParquetFile(path)
        if file.metadata.num_rows > max_rows:
            raise ValueError("Parquet row limit exceeded")
        rows = file.read().to_pylist()
    elif asset.type == "sqlite":
        if not asset.table:
            raise ValueError("SQL asset requires a registered table")
        with sqlite3.connect(path.as_uri() + "?mode=ro", uri=True) as connection:
            connection.row_factory = sqlite3.Row
            rows = [
                dict(r)
                for r in connection.execute(
                    f'SELECT * FROM "{asset.table}" LIMIT ?', (max_rows + 1,)
                )
            ]
    else:
        raise ValueError("Storage adapter is not implemented")
    return rows_checked(rows, max_rows)


def write(asset: Asset, root: Path, rows: list[dict], mode="replace", keys=()):
    if "write" not in asset.allowed_operations:
        raise ValueError("Write forbidden")
    path = confined(root, asset.location)
    if asset.type not in {"csv", "json", "jsonl", "parquet", "sqlite"}:
        raise ValueError("Output adapter is not implemented")
    if mode == "rebuild" and "delete" not in asset.allowed_operations:
        raise ValueError("Destructive operation forbidden")
    if mode in {"append", "upsert"} and path.exists():
        readable = asset.model_copy(update={"allowed_operations": ["read"]})
        old = read(readable, root, 1000000)
        if mode == "append":
            rows = old + rows
        else:
            if not keys:
                raise ValueError("Upsert requires keys")
            merged = {}
            for row in old + rows:
                key = tuple(row[k] for k in keys)
                if any(v is None for v in key):
                    raise ValueError("Null upsert key")
                merged[key] = row
            rows = list(merged.values())
    rows_checked(rows, 1000000)
    path.parent.mkdir(parents=True, exist_ok=True)
    if asset.type == "json":
        atomic_write(path, json.dumps(rows, sort_keys=True, allow_nan=False) + "\n")
    elif asset.type == "jsonl":
        atomic_write(
            path, "".join(json.dumps(r, sort_keys=True, allow_nan=False) + "\n" for r in rows)
        )
    elif asset.type == "csv":
        import io

        output = io.StringIO(newline="")
        columns = list(asset.schema_fields) or sorted({k for r in rows for k in r})
        writer = csv.DictWriter(output, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)
        atomic_write(path, output.getvalue())
    elif asset.type == "parquet":
        import pyarrow as pa
        import pyarrow.parquet as pq

        fd, temporary = tempfile.mkstemp(dir=path.parent, suffix=".parquet")
        os.close(fd)
        try:
            pq.write_table(pa.Table.from_pylist(rows), temporary)
            os.replace(temporary, path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
    else:
        if not asset.table or not asset.schema_fields:
            raise ValueError("SQL destination requires a registered table and schema")
        columns = list(asset.schema_fields)
        if any(not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", c) for c in columns):
            raise ValueError("Invalid SQL column identifier")
        types = {
            "integer": "INTEGER",
            "number": "REAL",
            "boolean": "INTEGER",
            "string": "TEXT",
            "datetime": "TEXT",
        }
        with sqlite3.connect(path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(f'DROP TABLE IF EXISTS "{asset.table}"')
            definition = ", ".join(f'"{c}" {types[asset.schema_fields[c]]}' for c in columns)
            connection.execute(f'CREATE TABLE "{asset.table}" ({definition})')
            placeholders = ",".join("?" for _ in columns)
            connection.executemany(
                f'INSERT INTO "{asset.table}" VALUES ({placeholders})',
                [tuple(r.get(c) for c in columns) for r in rows],
            )


def artifact(root: Path, relative: str, rows: list[dict]) -> dict:
    content = json.dumps(rows, sort_keys=True, allow_nan=False)
    atomic_write(confined(root, relative), content)
    return {
        "path": relative,
        "sha256": hashlib.sha256(content.encode()).hexdigest(),
        "rows": len(rows),
    }


def read_artifact(root: Path, ref: dict, max_rows: int):
    path = confined(root, ref["path"])
    if path.stat().st_size > MAX_BYTES:
        raise ValueError("Oversized runtime artifact")
    content = path.read_bytes()
    if hashlib.sha256(content).hexdigest() != ref["sha256"]:
        raise ValueError("Runtime artifact hash mismatch")
    return rows_checked(json.loads(content), max_rows)
