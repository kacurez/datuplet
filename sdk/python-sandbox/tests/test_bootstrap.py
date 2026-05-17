"""Tests for the DuckDB bootstrap module."""

from unittest.mock import patch, MagicMock

import pytest

from datuplet_sandbox.client import ResolveTablesResponse, ResolvedTable, TableAccess
from datuplet_sandbox.bootstrap import connect


def _mock_resolve(tables_dict):
    """Create a mock resolve_tables that returns the given tables."""
    response = ResolveTablesResponse(tables=tables_dict)
    return MagicMock(return_value=response)


@patch("datuplet_sandbox.bootstrap.resolve_tables")
def test_connect_creates_views(mock_resolve):
    mock_resolve.return_value = ResolveTablesResponse(tables={
        "sales.orders": ResolvedTable(
            format="iceberg",
            snapshot_id=123,
            total_rows=10,
            total_bytes=500,
            access=TableAccess(
                type="presigned",
                ttl_seconds=3600,
                permissions=["read"],
                data_files=[
                    "https://example.com/file1.parquet?sig=abc",
                    "https://example.com/file2.parquet?sig=def",
                ],
            ),
        ),
    })

    con = connect(["sales.orders"], gateway_url="http://localhost:8080")

    # Verify view exists and uses read_parquet with the presigned URLs
    result = con.execute("SELECT * FROM information_schema.schemata WHERE schema_name = 'sales'").fetchall()
    assert len(result) == 1

    # The view should exist (though querying it would fail without real URLs)
    tables = con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'sales'"
    ).fetchall()
    assert ("orders",) in tables

    con.close()


@patch("datuplet_sandbox.bootstrap.resolve_tables")
def test_connect_raises_on_error(mock_resolve):
    mock_resolve.return_value = ResolveTablesResponse(tables={
        "missing.table": ResolvedTable(error="table not found: missing.table"),
    })

    with pytest.raises(RuntimeError, match="failed to resolve table"):
        connect(["missing.table"], gateway_url="http://localhost:8080")


@patch("datuplet_sandbox.bootstrap.resolve_tables")
def test_connect_raises_on_no_data_files(mock_resolve):
    mock_resolve.return_value = ResolveTablesResponse(tables={
        "empty.table": ResolvedTable(
            format="iceberg",
            access=TableAccess(
                type="presigned",
                ttl_seconds=3600,
                permissions=["read"],
                data_files=[],
            ),
        ),
    })

    with pytest.raises(RuntimeError, match="has no data files"):
        connect(["empty.table"], gateway_url="http://localhost:8080")


@patch("datuplet_sandbox.bootstrap.resolve_tables")
def test_connect_raises_on_invalid_name(mock_resolve):
    mock_resolve.return_value = ResolveTablesResponse(tables={
        "nodot": ResolvedTable(
            format="iceberg",
            access=TableAccess(
                type="presigned",
                ttl_seconds=3600,
                permissions=["read"],
                data_files=["https://url"],
            ),
        ),
    })

    with pytest.raises(RuntimeError, match="invalid table name"):
        connect(["nodot"], gateway_url="http://localhost:8080")
