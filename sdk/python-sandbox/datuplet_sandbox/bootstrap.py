"""DuckDB bootstrap: resolves tables and creates views backed by pre-signed URLs."""

from __future__ import annotations

import duckdb

from datuplet_sandbox.client import resolve_tables


def connect(
    tables: list[str],
    *,
    gateway_url: str | None = None,
    principal: str = "sandbox",
    database: str = ":memory:",
) -> duckdb.DuckDBPyConnection:
    """Resolve tables and return a DuckDB connection with views ready to query.

    Each table is mapped to a DuckDB view under its logical namespace.
    For example, "sales.orders" becomes a view queryable as ``sales.orders``.

    Pre-signed HTTPS URLs are used to read Parquet files - no S3 credentials needed.

    Args:
        tables: Logical table names (e.g., ["sales.orders", "raw.products"]).
        gateway_url: TableGateway HTTP URL (defaults to env or http://tablegateway:8080).
        principal: Caller identity for audit/auth.
        database: DuckDB database path (default: in-memory).

    Returns:
        A DuckDB connection with views for all resolved tables.

    Raises:
        RuntimeError: If a table fails to resolve.
    """
    response = resolve_tables(tables, gateway_url=gateway_url, principal=principal)

    con = duckdb.connect(database)
    con.execute("INSTALL httpfs; LOAD httpfs;")

    for name, table in response.tables.items():
        if table.error:
            raise RuntimeError(f"failed to resolve table '{name}': {table.error}")

        if not table.access or not table.access.data_files:
            raise RuntimeError(f"table '{name}' has no data files")

        # Split logical name into schema.table for DuckDB
        parts = name.split(".", 1)
        if len(parts) != 2:
            raise RuntimeError(f"invalid table name '{name}': expected 'namespace.table'")

        schema, table_name = parts

        con.execute(f"CREATE SCHEMA IF NOT EXISTS \"{schema}\"")

        # Build view from pre-signed URLs
        urls = table.access.data_files
        url_list = ", ".join(f"'{u}'" for u in urls)
        con.execute(
            f'CREATE VIEW "{schema}"."{table_name}" AS '
            f"SELECT * FROM read_parquet([{url_list}])"
        )

    return con
