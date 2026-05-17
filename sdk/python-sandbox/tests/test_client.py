"""Tests for the TableGateway HTTP client."""

from unittest.mock import MagicMock, patch

from datuplet_sandbox.client import resolve_tables


def _mock_response(json_data, status_code=200):
    resp = MagicMock()
    resp.json.return_value = json_data
    resp.status_code = status_code
    resp.raise_for_status.return_value = None
    return resp


@patch("datuplet_sandbox.client.requests.post")
def test_resolve_tables_single(mock_post):
    mock_post.return_value = _mock_response({
        "tables": {
            "sales.orders": {
                "format": "iceberg",
                "snapshot_id": 123,
                "schema": {"fields": []},
                "total_rows": 1000,
                "total_bytes": 50000,
                "access": {
                    "type": "presigned",
                    "ttl_seconds": 3600,
                    "permissions": ["read"],
                    "data_files": [
                        "https://minio:9000/bucket/data/file1.parquet?X-Amz-Signature=abc",
                    ],
                },
            }
        }
    })

    result = resolve_tables(["sales.orders"], gateway_url="http://localhost:8080")

    assert "sales.orders" in result.tables
    table = result.tables["sales.orders"]
    assert table.format == "iceberg"
    assert table.snapshot_id == 123
    assert table.total_rows == 1000
    assert table.access is not None
    assert table.access.type == "presigned"
    assert len(table.access.data_files) == 1
    assert "X-Amz-Signature" in table.access.data_files[0]

    mock_post.assert_called_once()
    call_args = mock_post.call_args
    assert call_args[1]["json"]["tables"] == ["sales.orders"]
    assert call_args[1]["json"]["mode"] == "read"


@patch("datuplet_sandbox.client.requests.post")
def test_resolve_tables_error(mock_post):
    mock_post.return_value = _mock_response({
        "tables": {
            "missing.table": {
                "error": "table not found: missing.table",
            }
        }
    })

    result = resolve_tables(["missing.table"], gateway_url="http://localhost:8080")

    table = result.tables["missing.table"]
    assert table.error == "table not found: missing.table"
    assert table.access is None


@patch("datuplet_sandbox.client.requests.post")
def test_resolve_tables_multiple(mock_post):
    mock_post.return_value = _mock_response({
        "tables": {
            "sales.orders": {
                "format": "iceberg",
                "snapshot_id": 1,
                "total_rows": 100,
                "total_bytes": 5000,
                "access": {
                    "type": "presigned",
                    "ttl_seconds": 3600,
                    "permissions": ["read"],
                    "data_files": ["https://url1"],
                },
            },
            "raw.products": {
                "format": "iceberg",
                "snapshot_id": 2,
                "total_rows": 50,
                "total_bytes": 2500,
                "access": {
                    "type": "presigned",
                    "ttl_seconds": 3600,
                    "permissions": ["read"],
                    "data_files": ["https://url2"],
                },
            },
        }
    })

    result = resolve_tables(
        ["sales.orders", "raw.products"], gateway_url="http://localhost:8080"
    )

    assert len(result.tables) == 2
    assert "sales.orders" in result.tables
    assert "raw.products" in result.tables
