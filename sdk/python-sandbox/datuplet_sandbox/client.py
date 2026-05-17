"""HTTP client for the TableGateway resolve-tables API."""

from __future__ import annotations

import os
from dataclasses import dataclass, field

import requests

DEFAULT_GATEWAY_URL = "http://tablegateway:8080"


@dataclass
class TableAccess:
    type: str
    ttl_seconds: int
    permissions: list[str]
    data_files: list[str]


@dataclass
class ResolvedTable:
    format: str = ""
    snapshot_id: int = 0
    schema: dict | None = None
    total_rows: int = 0
    total_bytes: int = 0
    access: TableAccess | None = None
    error: str | None = None


@dataclass
class ResolveTablesResponse:
    tables: dict[str, ResolvedTable] = field(default_factory=dict)


def resolve_tables(
    tables: list[str],
    *,
    gateway_url: str | None = None,
    principal: str = "sandbox",
) -> ResolveTablesResponse:
    """Resolve logical table names to metadata and pre-signed data file URLs.

    Args:
        tables: List of logical table names (e.g., ["sales.orders"]).
        gateway_url: TableGateway HTTP URL. Defaults to DATUPLET_GATEWAY_URL
            env var or http://tablegateway:8080.
        principal: Caller identity for audit/auth.

    Returns:
        ResolveTablesResponse with per-table metadata and pre-signed URLs.

    Raises:
        requests.HTTPError: If the gateway returns a non-2xx response.
    """
    url = gateway_url or os.environ.get("DATUPLET_GATEWAY_URL", DEFAULT_GATEWAY_URL)

    resp = requests.post(
        f"{url.rstrip('/')}/v1/resolve-tables",
        json={"principal": principal, "tables": tables, "mode": "read"},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()

    result = ResolveTablesResponse()
    for name, table_data in data.get("tables", {}).items():
        access_data = table_data.get("access")
        access = None
        if access_data:
            access = TableAccess(
                type=access_data.get("type", ""),
                ttl_seconds=access_data.get("ttl_seconds", 0),
                permissions=access_data.get("permissions", []),
                data_files=access_data.get("data_files", []),
            )
        result.tables[name] = ResolvedTable(
            format=table_data.get("format", ""),
            snapshot_id=table_data.get("snapshot_id", 0),
            schema=table_data.get("schema"),
            total_rows=table_data.get("total_rows", 0),
            total_bytes=table_data.get("total_bytes", 0),
            access=access,
            error=table_data.get("error"),
        )

    return result
