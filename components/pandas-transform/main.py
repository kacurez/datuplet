#!/usr/bin/env python3
"""Pandas Transform component - applies pandas operations to data via Data gateway.

Supports operations:
- filter: Filter rows based on column conditions
- select: Select specific columns
- sort: Sort by columns
- rename: Rename columns
- drop: Drop columns
- fillna: Fill missing values
"""

import io
import sys
from typing import Any

import pandas as pd

# Add SDK to path (when running in container, SDK is copied to /app/sdk)
sys.path.insert(0, "/app/sdk/python")

from client import Client
from gateway.v2 import gateway_pb2 as pb
from status import exit_app_error, exit_user_error, status_message


def main():
    # Connect to gateway
    client = Client()

    config = client.config
    client.log("INFO", f"Pandas Transform started: execution={config.execution_id}")

    # Parse component config
    comp_config = client.parse_config()
    operations = comp_config.get("operations", [])

    # Get input table (bucket-based API)
    if not config.input_tables:
        exit_user_error("no input tables configured")

    # Get first input table
    input_table = config.input_tables[0]
    client.log("INFO", f"Reading input: {input_table.bucket}.{input_table.table}")

    # Read all data from input (request CSV format)
    reader = client.open_reader(
        input_table.bucket,
        input_table.table,
        output_format=pb.DataFormat.FORMAT_CSV,
    )
    data = b"".join(chunk.data for chunk in reader)
    reader.close()

    if not data:
        client.log("WARN", "No data received from input")
        # Still commit to mark success
        result = client.commit()
        client.close()
        return

    # Parse as CSV (most common format from other components)
    try:
        df = pd.read_csv(io.BytesIO(data))
    except Exception as e:
        exit_user_error(f"failed to parse CSV: {e}")

    client.log("INFO", f"Loaded {len(df)} rows, columns: {list(df.columns)}")

    # Apply operations
    for op in operations:
        df = apply_operation(df, op, client)

    client.log("INFO", f"After transform: {len(df)} rows")

    # Get output table name from config (or default to input table name)
    output_table = comp_config.get("output_table", input_table.table)
    client.log("INFO", f"Writing to output table: {output_table}")

    # Open writer with CSV format (uses defaultBucket from config)
    writer = client.open_writer(output_table, input_format=pb.DataFormat.FORMAT_CSV)

    # Convert back to CSV
    output_buffer = io.BytesIO()
    df.to_csv(output_buffer, index=False)
    output_data = output_buffer.getvalue()

    writer.write(output_data)

    # Close writer and get stats
    close_result = writer.close()
    client.log("INFO", f"Wrote {close_result.total_rows} rows to {writer.bucket}.{writer.table}")

    # Commit all outputs
    result = client.commit()

    if not result.success:
        exit_app_error("commit failed")

    for b in result.buckets:
        for t in b.tables:
            client.log("INFO", f"Committed {t.bucket}.{t.table}: files={t.files_added}, rows={t.rows_added}")

    client.log("INFO", "Pandas transformation completed successfully")
    status_message(f"transformed {len(df)} rows")
    client.close()


def apply_operation(df: pd.DataFrame, op: dict[str, Any], client: Client) -> pd.DataFrame:
    """Apply a single operation to the DataFrame."""
    op_type = op.get("type", "").lower()

    if op_type == "filter":
        return apply_filter(df, op, client)
    elif op_type == "select":
        return apply_select(df, op, client)
    elif op_type == "sort":
        return apply_sort(df, op, client)
    elif op_type == "rename":
        return apply_rename(df, op, client)
    elif op_type == "drop":
        return apply_drop(df, op, client)
    elif op_type == "fillna":
        return apply_fillna(df, op, client)
    else:
        client.log("WARN", f"Unknown operation type: {op_type}")
        return df


def apply_filter(df: pd.DataFrame, op: dict[str, Any], client: Client) -> pd.DataFrame:
    """Filter rows based on column condition."""
    column = op.get("column")
    operator = op.get("op", "==")
    value = op.get("value")

    if not column or column not in df.columns:
        client.log("WARN", f"Filter: column '{column}' not found")
        return df

    client.log("INFO", f"Filter: {column} {operator} {value}")

    if operator == ">":
        return df[df[column] > value]
    elif operator == ">=":
        return df[df[column] >= value]
    elif operator == "<":
        return df[df[column] < value]
    elif operator == "<=":
        return df[df[column] <= value]
    elif operator == "==":
        return df[df[column] == value]
    elif operator == "!=":
        return df[df[column] != value]
    elif operator == "in":
        return df[df[column].isin(value)]
    elif operator == "contains":
        return df[df[column].astype(str).str.contains(str(value), case=False, na=False)]
    else:
        client.log("WARN", f"Filter: unknown operator '{operator}'")
        return df


def apply_select(df: pd.DataFrame, op: dict[str, Any], client: Client) -> pd.DataFrame:
    """Select specific columns."""
    columns = op.get("columns", [])

    if not columns:
        return df

    # Filter to existing columns
    existing = [c for c in columns if c in df.columns]
    missing = [c for c in columns if c not in df.columns]

    if missing:
        client.log("WARN", f"Select: columns not found: {missing}")

    if existing:
        client.log("INFO", f"Select: {existing}")
        return df[existing]

    return df


def apply_sort(df: pd.DataFrame, op: dict[str, Any], client: Client) -> pd.DataFrame:
    """Sort by columns."""
    by = op.get("by", [])
    ascending = op.get("ascending", True)

    if not by:
        return df

    if isinstance(by, str):
        by = [by]

    # Filter to existing columns
    existing = [c for c in by if c in df.columns]

    if existing:
        client.log("INFO", f"Sort: by {existing}, ascending={ascending}")
        return df.sort_values(by=existing, ascending=ascending)

    return df


def apply_rename(df: pd.DataFrame, op: dict[str, Any], client: Client) -> pd.DataFrame:
    """Rename columns."""
    columns = op.get("columns", {})

    if not columns:
        return df

    # Filter to existing columns
    existing = {k: v for k, v in columns.items() if k in df.columns}

    if existing:
        client.log("INFO", f"Rename: {existing}")
        return df.rename(columns=existing)

    return df


def apply_drop(df: pd.DataFrame, op: dict[str, Any], client: Client) -> pd.DataFrame:
    """Drop columns."""
    columns = op.get("columns", [])

    if not columns:
        return df

    if isinstance(columns, str):
        columns = [columns]

    # Filter to existing columns
    existing = [c for c in columns if c in df.columns]

    if existing:
        client.log("INFO", f"Drop: {existing}")
        return df.drop(columns=existing)

    return df


def apply_fillna(df: pd.DataFrame, op: dict[str, Any], client: Client) -> pd.DataFrame:
    """Fill missing values."""
    value = op.get("value")
    column = op.get("column")

    if column:
        if column in df.columns:
            client.log("INFO", f"Fillna: {column} with {value}")
            df[column] = df[column].fillna(value)
    else:
        client.log("INFO", f"Fillna: all columns with {value}")
        df = df.fillna(value)

    return df


if __name__ == "__main__":
    main()
