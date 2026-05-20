"""Thin Python SDK for Datuplet components.

Communicates with the Data Gateway sidecar via gRPC using v2 protocol.
Uses HTTP for bulk data transfer to avoid gRPC message size limits.
"""

import json
import os
from dataclasses import dataclass
from typing import Any, Iterator, Optional
from urllib.parse import urlparse, urlunparse

import grpc
import requests

from gateway.v2 import gateway_pb2 as pb
from gateway.v2 import gateway_pb2_grpc


# Content-Type mapping for HTTP data transfer
_FORMAT_TO_CONTENT_TYPE = {
    pb.DataFormat.FORMAT_CSV: "text/csv",
    pb.DataFormat.FORMAT_JSON: "application/json",
    pb.DataFormat.FORMAT_JSONL: "application/x-ndjson",
    pb.DataFormat.FORMAT_PARQUET: "application/vnd.apache.parquet",
    pb.DataFormat.FORMAT_ARROW_IPC: "application/vnd.apache.arrow.stream",
}


def _format_to_content_type(fmt: int) -> str:
    """Convert DataFormat to HTTP Content-Type."""
    return _FORMAT_TO_CONTENT_TYPE.get(fmt, "application/octet-stream")


@dataclass
class TableRef:
    """Reference to a table by bucket and name."""

    bucket: str
    table: str


@dataclass
class Config:
    """Component configuration."""

    execution_id: str
    default_bucket: str  # Default bucket for writes (if configured)
    input_buckets: list[str]  # Buckets available for reading
    output_buckets: list[str]  # Buckets available for writing
    input_tables: list[TableRef]  # Specific input tables
    raw: bytes

    # Legacy fields (deprecated)
    inputs: list[str]
    outputs: list[str]


@dataclass
class Chunk:
    """A chunk of data."""

    data: bytes
    format: int  # pb.DataFormat enum value
    rows: int
    is_last: bool


@dataclass
class WriteResult:
    """Result of a write operation."""

    rows_accepted: int
    buffer_size: int
    inferred_schema: Optional[Any]  # pb.Schema


@dataclass
class CloseResult:
    """Result of closing a writer."""

    total_rows: int
    total_bytes: int
    files_written: int


@dataclass
class TableResult:
    """Result for a single table commit."""

    bucket: str
    table: str
    success: bool
    snapshot_id: int
    files_added: int
    rows_added: int
    bytes_added: int
    error: str


@dataclass
class BucketResult:
    """Result for a single bucket commit."""

    bucket: str
    success: bool
    tables: list[TableResult]
    error: str


@dataclass
class CommitResult:
    """Result of a commit operation."""

    success: bool
    error: str
    buckets: list[BucketResult]


@dataclass
class DeltaInfo:
    """Metadata about an incremental (delta) read."""

    base_snapshot_id: int
    target_snapshot_id: int
    kind: str  # "APPEND_ONLY" or "REQUIRES_FULL_REFRESH"
    snapshots_in_range: int


@dataclass
class SampleResult:
    """Sample data from an input."""

    schema: Any  # pb.Schema
    rows: list[bytes]
    total_estimate: int


class Reader:
    """Chunked reader for an input."""

    def __init__(
        self,
        stub: gateway_pb2_grpc.DataGatewayStub,
        http_session: requests.Session,
        reader_id: str,
        http_endpoint: str,
        bucket: str,
        table: str,
        schema: Optional[Any],
        delta_info: Optional[DeltaInfo] = None,
    ):
        self._stub = stub
        self._http_session = http_session
        self._reader_id = reader_id
        self._http_endpoint = http_endpoint  # Empty string = use gRPC
        self._bucket = bucket
        self._table = table
        self._schema = schema  # pb.Schema
        self._delta_info = delta_info
        self._stream: Optional[Iterator] = None
        self._is_last = False

    @property
    def bucket(self) -> str:
        """Bucket name."""
        return self._bucket

    @property
    def table(self) -> str:
        """Table name."""
        return self._table

    @property
    def schema(self) -> Optional[Any]:
        """Data schema (pb.Schema)."""
        return self._schema

    @property
    def delta_info(self) -> Optional[DeltaInfo]:
        """Delta info for incremental reads, or None for full reads."""
        return self._delta_info

    @property
    def column_names(self) -> list[str]:
        """Column names for convenience."""
        if self._schema is None:
            return []
        return [col.name for col in self._schema.columns]

    def __iter__(self) -> Iterator[Chunk]:
        """Iterate over chunks."""
        if self._http_endpoint:
            yield from self._iter_http()
        else:
            yield from self._iter_grpc()

    def _iter_http(self) -> Iterator[Chunk]:
        """Iterate over chunks via HTTP."""
        while not self._is_last:
            resp = self._http_session.get(self._http_endpoint)
            resp.raise_for_status()

            is_last = resp.headers.get("X-Datuplet-Is-Last", "false").lower() == "true"
            rows = int(resp.headers.get("X-Datuplet-Rows", "0"))

            if is_last:
                self._is_last = True

            # Skip empty last chunk
            if len(resp.content) == 0 and is_last:
                return

            yield Chunk(
                data=resp.content,
                format=pb.DataFormat.FORMAT_UNSPECIFIED,  # Not in headers
                rows=rows,
                is_last=is_last,
            )

    def _iter_grpc(self) -> Iterator[Chunk]:
        """Iterate over chunks via gRPC."""
        if self._stream is None:
            self._stream = self._stub.ReadChunk(pb.ReadChunkRequest(reader_id=self._reader_id))

        for chunk in self._stream:
            yield Chunk(
                data=chunk.data,
                format=chunk.format,
                rows=chunk.rows_in_chunk,
                is_last=chunk.is_last,
            )

    def close(self) -> None:
        """Close the reader."""
        self._stub.CloseReader(pb.CloseReaderRequest(reader_id=self._reader_id))


# Default Write-call accumulator threshold (bytes). See Writer.write for semantics.
# Mirrors the Go SDK's defaultBatchSize so behavior is consistent across languages.
DEFAULT_BATCH_SIZE = 1024 * 1024  # 1 MiB


class Writer:
    """Chunked writer for an output."""

    def __init__(
        self,
        stub: gateway_pb2_grpc.DataGatewayStub,
        http_session: requests.Session,
        writer_id: str,
        http_endpoint: str,
        bucket: str,
        table: str,
        input_format: int,
        inferred_schema: Optional[Any],
        batch_size: int = DEFAULT_BATCH_SIZE,
    ):
        self._stub = stub
        self._http_session = http_session
        self._writer_id = writer_id
        self._http_endpoint = http_endpoint  # Empty string = use gRPC
        self._bucket = bucket
        self._table = table
        self._input_format = input_format
        self._inferred_schema = inferred_schema  # pb.Schema

        # Write-call batching state. See write() for the contract.
        #   > 0: batching ON at this byte threshold
        #   == 0: batching ON at DEFAULT_BATCH_SIZE
        #   < 0: batching OFF (legacy semantics — every write -> immediate write_chunk)
        if batch_size == 0:
            self._batch_threshold = DEFAULT_BATCH_SIZE
        else:
            self._batch_threshold = batch_size
        self._batch_buffer: bytearray = bytearray()

    @property
    def bucket(self) -> str:
        """Resolved bucket name."""
        return self._bucket

    @property
    def table(self) -> str:
        """Table name."""
        return self._table

    @property
    def schema(self) -> Optional[Any]:
        """Schema (inferred or provided) - returns pb.Schema."""
        return self._inferred_schema

    def write_chunk(self, data: bytes) -> WriteResult:
        """Write a chunk of data immediately and return the gateway's result.

        If a write() batch is pending, it is flushed first so the gateway
        sees chunks in submission order. The returned WriteResult describes
        only THIS write_chunk's data, not the pending-batch flush.

        Uses HTTP if endpoint available (no size limit), falls back to gRPC.

        Args:
            data: Data bytes to write.

        Returns:
            WriteResult with rows accepted and buffer info.
        """
        # Preserve call order: drain any pending write() batch first.
        self._flush_batch()
        return self._write_chunk_immediate(data)

    def _write_chunk_immediate(self, data: bytes) -> WriteResult:
        """Send data without consulting the batch buffer.

        Used by write_chunk (after flushing) and by _flush_batch itself.
        """
        if self._http_endpoint:
            return self._write_chunk_http(data)
        return self._write_chunk_grpc(data)

    def _write_chunk_http(self, data: bytes) -> WriteResult:
        """Write chunk via HTTP POST (no size limit)."""
        content_type = _format_to_content_type(self._input_format)

        resp = self._http_session.post(
            self._http_endpoint,
            data=data,
            headers={"Content-Type": content_type},
        )
        resp.raise_for_status()

        result = resp.json()

        # Update inferred schema if provided
        if result.get("inferred_schema") and self._inferred_schema is None:
            # Note: HTTP returns JSON schema, not pb.Schema
            self._inferred_schema = result["inferred_schema"]

        return WriteResult(
            rows_accepted=result.get("rows_accepted", 0),
            buffer_size=result.get("buffer_size_bytes", 0),
            inferred_schema=result.get("inferred_schema"),
        )

    def _write_chunk_grpc(self, data: bytes) -> WriteResult:
        """Write chunk via gRPC (4MB limit)."""
        resp = self._stub.WriteChunk(
            pb.WriteChunkRequest(
                writer_id=self._writer_id,
                data=data,
            )
        )

        # Update inferred schema if provided
        if resp.inferred_schema and self._inferred_schema is None:
            self._inferred_schema = resp.inferred_schema

        return WriteResult(
            rows_accepted=resp.rows_accepted,
            buffer_size=resp.buffer_size_bytes,
            inferred_schema=resp.inferred_schema,
        )

    def write(self, data: bytes) -> None:
        """Row-at-a-time / streaming convenience method.

        Does not return per-call results; callers that need them should use
        write_chunk.

        By default write() batches: bytes are appended to a per-writer
        accumulator and the gateway only sees a write_chunk when the
        accumulator reaches the batch threshold (1 MiB by default). This
        collapses row-at-a-time HTTP traffic by ~1000x for the common case
        of one row per write call. To opt out, open the writer with
        batch_size=-1 — every write() becomes one immediate write_chunk
        (the legacy behavior).

        The accumulator is drained on every:
          * threshold cross (this method)
          * write_chunk call (to preserve call order)
          * flush call (explicit)
          * close call (final flush before commit)

        Args:
            data: Data bytes to write.
        """
        if self._batch_threshold <= 0:
            # Batching disabled — legacy semantics.
            self._write_chunk_immediate(data)
            return

        self._batch_buffer.extend(data)
        if len(self._batch_buffer) >= self._batch_threshold:
            self._flush_batch()

    def flush(self) -> None:
        """Force any pending write()-batched data to the gateway immediately.

        No-op when batching is disabled or the accumulator is empty. Useful
        for callers that want to checkpoint progress without closing the
        writer.
        """
        self._flush_batch()

    def _flush_batch(self) -> None:
        """Send the currently-buffered write() payload as a single write_chunk."""
        if not self._batch_buffer:
            return
        # bytes() copies; the underlying bytearray is then cleared in place.
        # This is the equivalent of slicing-to-zero-length in the Go SDK.
        payload = bytes(self._batch_buffer)
        self._batch_buffer.clear()
        self._write_chunk_immediate(payload)

    def close(self) -> CloseResult:
        """Close the writer and finalize output.

        Drains any pending write()-batched data before closing — otherwise
        the gateway would commit without seeing the tail of the stream.

        Returns:
            CloseResult with stats.
        """
        self._flush_batch()
        resp = self._stub.CloseWriter(pb.CloseWriterRequest(writer_id=self._writer_id))
        return CloseResult(
            total_rows=resp.total_rows,
            total_bytes=resp.total_bytes,
            files_written=resp.files_written,
        )


class Client:
    """Datuplet client for components."""

    def __init__(self, addr: Optional[str] = None):
        """Connect to the Data Gateway.

        Args:
            addr: Gateway address. Defaults to DATUPLET_GATEWAY_ADDR env var or localhost:50051.
        """
        if addr is None:
            addr = os.getenv("DATUPLET_GATEWAY_ADDR", "localhost:50051")

        # Extract host from address (for HTTP endpoint rewriting)
        # Address format: "host:port" or just "host"
        if ":" in addr:
            self._gateway_host = addr.rsplit(":", 1)[0]
        else:
            self._gateway_host = addr

        self._channel = grpc.insecure_channel(addr)
        self._stub = gateway_pb2_grpc.DataGatewayStub(self._channel)
        self._http_session = requests.Session()

        # Fetch config immediately
        self._config = self._stub.GetConfig(pb.GetConfigRequest())

    @property
    def config(self) -> Config:
        """Component configuration."""
        # Build input tables from proto
        input_tables = [
            TableRef(bucket=t.bucket, table=t.table)
            for t in self._config.input_tables
        ]

        # Get default bucket from output config
        default_bucket = ""
        if self._config.output_config:
            default_bucket = self._config.output_config.default_bucket

        return Config(
            execution_id=self._config.execution_id,
            default_bucket=default_bucket,
            input_buckets=list(self._config.input_buckets),
            output_buckets=list(self._config.output_buckets),
            input_tables=input_tables,
            raw=self._config.config,
            # Legacy fields
            inputs=list(self._config.inputs.keys()),
            outputs=list(self._config.outputs.keys()),
        )

    def parse_config(self, cls: type = dict) -> Any:
        """Parse component config as JSON.

        Args:
            cls: Type to parse into. Default is dict.

        Returns:
            Parsed config.
        """
        data = json.loads(self._config.config)
        if cls is dict:
            return data
        return cls(**data)

    def _fix_http_endpoint(self, endpoint: str) -> str:
        """Rewrite HTTP endpoint to use the correct gateway host.

        The gateway returns "http://localhost:50052/..." but in Docker the component
        needs to connect to the gateway container's hostname.
        """
        if not endpoint:
            return ""

        parsed = urlparse(endpoint)
        # Replace host with gateway host, keeping the port
        port = parsed.port or 50052
        new_netloc = f"{self._gateway_host}:{port}"
        return urlunparse(parsed._replace(netloc=new_netloc))

    def open_reader(
        self,
        bucket: str,
        table: str,
        output_format: int = pb.DataFormat.FORMAT_CSV,
        chunk_size: int = 0,
        transforms: Optional[Any] = None,
        since_snapshot: Optional[int] = None,
        since_time: Optional[int] = None,
    ) -> Reader:
        """Open a reader for a table.

        Args:
            bucket: Bucket name (required).
            table: Table name (required).
            output_format: Desired output format (pb.DataFormat enum). Default: CSV.
            chunk_size: Target chunk size in bytes (0 = default).
            transforms: Optional pb.TransformSpec for read-time transforms.
            since_snapshot: Read only data added after this snapshot ID (incremental read).
            since_time: Read only data added after this timestamp in ms (incremental read).

        Returns:
            Reader for the table.
        """
        # Build incremental read spec if requested
        incremental = None
        if since_snapshot is not None:
            incremental = pb.IncrementalReadSpec(from_snapshot_id=since_snapshot)
        elif since_time is not None:
            incremental = pb.IncrementalReadSpec(from_timestamp_ms=since_time)

        resp = self._stub.OpenReader(
            pb.OpenReaderRequest(
                bucket=bucket,
                table=table,
                output_format=output_format,
                chunk_size_bytes=chunk_size,
                transforms=transforms,
                incremental=incremental,
            )
        )

        # Build DeltaInfo from response if present
        di = None
        if resp.delta_info and resp.delta_info.base_snapshot_id:
            di = DeltaInfo(
                base_snapshot_id=resp.delta_info.base_snapshot_id,
                target_snapshot_id=resp.delta_info.target_snapshot_id,
                kind=resp.delta_info.kind,
                snapshots_in_range=resp.delta_info.snapshots_in_range,
            )

        return Reader(
            self._stub,
            self._http_session,
            resp.reader_id,
            self._fix_http_endpoint(resp.http_endpoint),
            resp.bucket,
            resp.table,
            resp.schema,
            delta_info=di,
        )

    def open_writer(
        self,
        table: str,
        bucket: str = "",
        input_format: int = pb.DataFormat.FORMAT_CSV,
        schema: Optional[Any] = None,
        transforms: Optional[Any] = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> Writer:
        """Open a writer for a table.

        If bucket is empty, uses the defaultBucket from config.

        Args:
            table: Table name (required).
            bucket: Bucket name (optional - uses defaultBucket if not set).
            input_format: Format of data chunks to send (pb.DataFormat enum). Default: CSV.
            schema: Optional pb.Schema. If not provided, inferred from first chunk.
            transforms: Optional pb.TransformSpec for write-time transforms.
            batch_size: SDK-side write() accumulator threshold in bytes.
                Defaults to 1 MiB. Pass 0 to use the default explicitly,
                or a negative value to disable batching (every write becomes
                one immediate write_chunk — legacy v0.2.x behavior). Has no
                effect on write_chunk(), which always sends immediately.

        Returns:
            Writer for the table.
        """
        resp = self._stub.OpenWriter(
            pb.OpenWriterRequest(
                bucket=bucket,
                table=table,
                input_format=input_format,
                schema=schema,
                transforms=transforms,
            )
        )
        return Writer(
            self._stub,
            self._http_session,
            resp.writer_id,
            self._fix_http_endpoint(resp.http_endpoint),
            resp.bucket,
            resp.table,
            input_format,
            resp.inferred_schema,
            batch_size=batch_size,
        )

    def commit(self, best_effort: bool = False) -> CommitResult:
        """Commit all outputs atomically (per-bucket).

        Args:
            best_effort: Continue on individual bucket failures.

        Returns:
            Commit result with status for each bucket and table.
        """
        resp = self._stub.Commit(pb.CommitRequest(best_effort=best_effort))

        buckets = []
        for b in resp.buckets:
            tables = []
            for t in b.tables:
                tables.append(
                    TableResult(
                        bucket=t.bucket,
                        table=t.table,
                        success=t.status == pb.TableCommitResult.STATUS_COMMITTED,
                        snapshot_id=t.snapshot_id,
                        files_added=t.files_added,
                        rows_added=t.rows_added,
                        bytes_added=t.bytes_added,
                        error=t.error,
                    )
                )
            buckets.append(
                BucketResult(
                    bucket=b.bucket,
                    success=b.status == pb.BucketCommitResult.STATUS_COMMITTED,
                    tables=tables,
                    error=b.error,
                )
            )

        return CommitResult(success=resp.success, error=resp.error, buckets=buckets)

    def get_schema(self, bucket: str, table: str) -> Any:
        """Get schema for a table.

        Args:
            bucket: Bucket name.
            table: Table name.

        Returns:
            pb.Schema for the table.
        """
        resp = self._stub.GetSchema(pb.GetSchemaRequest(bucket=bucket, table=table))
        return resp.schema

    def get_sample(
        self,
        bucket: str,
        table: str,
        limit: int = 10,
        format: int = pb.DataFormat.FORMAT_JSON,
    ) -> SampleResult:
        """Get sample rows from a table.

        Args:
            bucket: Bucket name.
            table: Table name.
            limit: Number of sample rows.
            format: Format for sample rows (pb.DataFormat enum). Default: JSON.

        Returns:
            SampleResult with schema and sample rows.
        """
        resp = self._stub.GetSample(
            pb.GetSampleRequest(
                bucket=bucket,
                table=table,
                limit=limit,
                format=format,
            )
        )
        return SampleResult(
            schema=resp.schema,
            rows=list(resp.rows),
            total_estimate=resp.total_estimate,
        )

    def log(self, level: str, message: str, **fields: str) -> None:
        """Send a log message to the gateway.

        Args:
            level: Log level (DEBUG, INFO, WARN, ERROR).
            message: Log message.
            **fields: Additional fields.
        """
        self._stub.Log(pb.LogRequest(level=level, message=message, fields=fields))

    def debug(self, message: str) -> None:
        """Log a debug message."""
        self.log("DEBUG", message)

    def info(self, message: str) -> None:
        """Log an info message."""
        self.log("INFO", message)

    def warn(self, message: str) -> None:
        """Log a warning message."""
        self.log("WARN", message)

    def error(self, message: str) -> None:
        """Log an error message."""
        self.log("ERROR", message)

    def close(self) -> None:
        """Close the client connection."""
        self._http_session.close()
        self._channel.close()

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, *args) -> None:
        self.close()
