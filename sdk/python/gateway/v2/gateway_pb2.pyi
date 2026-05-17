from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class DataFormat(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    FORMAT_UNSPECIFIED: _ClassVar[DataFormat]
    FORMAT_CSV: _ClassVar[DataFormat]
    FORMAT_JSON: _ClassVar[DataFormat]
    FORMAT_JSONL: _ClassVar[DataFormat]
    FORMAT_PARQUET: _ClassVar[DataFormat]
    FORMAT_ARROW_IPC: _ClassVar[DataFormat]
FORMAT_UNSPECIFIED: DataFormat
FORMAT_CSV: DataFormat
FORMAT_JSON: DataFormat
FORMAT_JSONL: DataFormat
FORMAT_PARQUET: DataFormat
FORMAT_ARROW_IPC: DataFormat

class Schema(_message.Message):
    __slots__ = ("columns",)
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    columns: _containers.RepeatedCompositeFieldContainer[ColumnDef]
    def __init__(self, columns: _Optional[_Iterable[_Union[ColumnDef, _Mapping]]] = ...) -> None: ...

class ColumnDef(_message.Message):
    __slots__ = ("name", "type", "nullable")
    NAME_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    NULLABLE_FIELD_NUMBER: _ClassVar[int]
    name: str
    type: str
    nullable: bool
    def __init__(self, name: _Optional[str] = ..., type: _Optional[str] = ..., nullable: bool = ...) -> None: ...

class GetConfigRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ComponentConfig(_message.Message):
    __slots__ = ("execution_id", "component_name", "inputs", "outputs", "config", "chunk_size")
    class InputsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    class OutputsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    EXECUTION_ID_FIELD_NUMBER: _ClassVar[int]
    COMPONENT_NAME_FIELD_NUMBER: _ClassVar[int]
    INPUTS_FIELD_NUMBER: _ClassVar[int]
    OUTPUTS_FIELD_NUMBER: _ClassVar[int]
    CONFIG_FIELD_NUMBER: _ClassVar[int]
    CHUNK_SIZE_FIELD_NUMBER: _ClassVar[int]
    execution_id: str
    component_name: str
    inputs: _containers.ScalarMap[str, str]
    outputs: _containers.ScalarMap[str, str]
    config: bytes
    chunk_size: int
    def __init__(self, execution_id: _Optional[str] = ..., component_name: _Optional[str] = ..., inputs: _Optional[_Mapping[str, str]] = ..., outputs: _Optional[_Mapping[str, str]] = ..., config: _Optional[bytes] = ..., chunk_size: _Optional[int] = ...) -> None: ...

class OpenWriterRequest(_message.Message):
    __slots__ = ("output_name", "input_format", "schema", "transforms")
    OUTPUT_NAME_FIELD_NUMBER: _ClassVar[int]
    INPUT_FORMAT_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    TRANSFORMS_FIELD_NUMBER: _ClassVar[int]
    output_name: str
    input_format: DataFormat
    schema: Schema
    transforms: TransformSpec
    def __init__(self, output_name: _Optional[str] = ..., input_format: _Optional[_Union[DataFormat, str]] = ..., schema: _Optional[_Union[Schema, _Mapping]] = ..., transforms: _Optional[_Union[TransformSpec, _Mapping]] = ...) -> None: ...

class OpenWriterResponse(_message.Message):
    __slots__ = ("writer_id", "inferred_schema", "http_endpoint")
    WRITER_ID_FIELD_NUMBER: _ClassVar[int]
    INFERRED_SCHEMA_FIELD_NUMBER: _ClassVar[int]
    HTTP_ENDPOINT_FIELD_NUMBER: _ClassVar[int]
    writer_id: str
    inferred_schema: Schema
    http_endpoint: str
    def __init__(self, writer_id: _Optional[str] = ..., inferred_schema: _Optional[_Union[Schema, _Mapping]] = ..., http_endpoint: _Optional[str] = ...) -> None: ...

class WriteChunkRequest(_message.Message):
    __slots__ = ("writer_id", "data")
    WRITER_ID_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    writer_id: str
    data: bytes
    def __init__(self, writer_id: _Optional[str] = ..., data: _Optional[bytes] = ...) -> None: ...

class WriteChunkResponse(_message.Message):
    __slots__ = ("rows_accepted", "buffer_size_bytes", "inferred_schema")
    ROWS_ACCEPTED_FIELD_NUMBER: _ClassVar[int]
    BUFFER_SIZE_BYTES_FIELD_NUMBER: _ClassVar[int]
    INFERRED_SCHEMA_FIELD_NUMBER: _ClassVar[int]
    rows_accepted: int
    buffer_size_bytes: int
    inferred_schema: Schema
    def __init__(self, rows_accepted: _Optional[int] = ..., buffer_size_bytes: _Optional[int] = ..., inferred_schema: _Optional[_Union[Schema, _Mapping]] = ...) -> None: ...

class CloseWriterRequest(_message.Message):
    __slots__ = ("writer_id",)
    WRITER_ID_FIELD_NUMBER: _ClassVar[int]
    writer_id: str
    def __init__(self, writer_id: _Optional[str] = ...) -> None: ...

class CloseWriterResponse(_message.Message):
    __slots__ = ("total_rows", "total_bytes", "files_written")
    TOTAL_ROWS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_BYTES_FIELD_NUMBER: _ClassVar[int]
    FILES_WRITTEN_FIELD_NUMBER: _ClassVar[int]
    total_rows: int
    total_bytes: int
    files_written: int
    def __init__(self, total_rows: _Optional[int] = ..., total_bytes: _Optional[int] = ..., files_written: _Optional[int] = ...) -> None: ...

class OpenReaderRequest(_message.Message):
    __slots__ = ("input_name", "output_format", "chunk_size_bytes", "transforms")
    INPUT_NAME_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_FORMAT_FIELD_NUMBER: _ClassVar[int]
    CHUNK_SIZE_BYTES_FIELD_NUMBER: _ClassVar[int]
    TRANSFORMS_FIELD_NUMBER: _ClassVar[int]
    input_name: str
    output_format: DataFormat
    chunk_size_bytes: int
    transforms: TransformSpec
    def __init__(self, input_name: _Optional[str] = ..., output_format: _Optional[_Union[DataFormat, str]] = ..., chunk_size_bytes: _Optional[int] = ..., transforms: _Optional[_Union[TransformSpec, _Mapping]] = ...) -> None: ...

class OpenReaderResponse(_message.Message):
    __slots__ = ("reader_id", "schema", "total_rows_estimate", "total_size_estimate", "http_endpoint")
    READER_ID_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    TOTAL_ROWS_ESTIMATE_FIELD_NUMBER: _ClassVar[int]
    TOTAL_SIZE_ESTIMATE_FIELD_NUMBER: _ClassVar[int]
    HTTP_ENDPOINT_FIELD_NUMBER: _ClassVar[int]
    reader_id: str
    schema: Schema
    total_rows_estimate: int
    total_size_estimate: int
    http_endpoint: str
    def __init__(self, reader_id: _Optional[str] = ..., schema: _Optional[_Union[Schema, _Mapping]] = ..., total_rows_estimate: _Optional[int] = ..., total_size_estimate: _Optional[int] = ..., http_endpoint: _Optional[str] = ...) -> None: ...

class ReadChunkRequest(_message.Message):
    __slots__ = ("reader_id",)
    READER_ID_FIELD_NUMBER: _ClassVar[int]
    reader_id: str
    def __init__(self, reader_id: _Optional[str] = ...) -> None: ...

class DataChunk(_message.Message):
    __slots__ = ("data", "format", "rows_in_chunk", "is_last", "metadata")
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    DATA_FIELD_NUMBER: _ClassVar[int]
    FORMAT_FIELD_NUMBER: _ClassVar[int]
    ROWS_IN_CHUNK_FIELD_NUMBER: _ClassVar[int]
    IS_LAST_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    data: bytes
    format: DataFormat
    rows_in_chunk: int
    is_last: bool
    metadata: _containers.ScalarMap[str, str]
    def __init__(self, data: _Optional[bytes] = ..., format: _Optional[_Union[DataFormat, str]] = ..., rows_in_chunk: _Optional[int] = ..., is_last: bool = ..., metadata: _Optional[_Mapping[str, str]] = ...) -> None: ...

class CloseReaderRequest(_message.Message):
    __slots__ = ("reader_id",)
    READER_ID_FIELD_NUMBER: _ClassVar[int]
    reader_id: str
    def __init__(self, reader_id: _Optional[str] = ...) -> None: ...

class CloseReaderResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class TransformSpec(_message.Message):
    __slots__ = ("operations",)
    OPERATIONS_FIELD_NUMBER: _ClassVar[int]
    operations: _containers.RepeatedCompositeFieldContainer[TransformOp]
    def __init__(self, operations: _Optional[_Iterable[_Union[TransformOp, _Mapping]]] = ...) -> None: ...

class TransformOp(_message.Message):
    __slots__ = ("filter", "select", "drop", "rename", "compute", "cast", "reorder")
    FILTER_FIELD_NUMBER: _ClassVar[int]
    SELECT_FIELD_NUMBER: _ClassVar[int]
    DROP_FIELD_NUMBER: _ClassVar[int]
    RENAME_FIELD_NUMBER: _ClassVar[int]
    COMPUTE_FIELD_NUMBER: _ClassVar[int]
    CAST_FIELD_NUMBER: _ClassVar[int]
    REORDER_FIELD_NUMBER: _ClassVar[int]
    filter: FilterOp
    select: SelectOp
    drop: DropOp
    rename: RenameOp
    compute: ComputeOp
    cast: CastOp
    reorder: ReorderOp
    def __init__(self, filter: _Optional[_Union[FilterOp, _Mapping]] = ..., select: _Optional[_Union[SelectOp, _Mapping]] = ..., drop: _Optional[_Union[DropOp, _Mapping]] = ..., rename: _Optional[_Union[RenameOp, _Mapping]] = ..., compute: _Optional[_Union[ComputeOp, _Mapping]] = ..., cast: _Optional[_Union[CastOp, _Mapping]] = ..., reorder: _Optional[_Union[ReorderOp, _Mapping]] = ...) -> None: ...

class FilterOp(_message.Message):
    __slots__ = ("condition",)
    CONDITION_FIELD_NUMBER: _ClassVar[int]
    condition: str
    def __init__(self, condition: _Optional[str] = ...) -> None: ...

class SelectOp(_message.Message):
    __slots__ = ("columns",)
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    columns: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, columns: _Optional[_Iterable[str]] = ...) -> None: ...

class DropOp(_message.Message):
    __slots__ = ("columns",)
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    columns: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, columns: _Optional[_Iterable[str]] = ...) -> None: ...

class RenameOp(_message.Message):
    __slots__ = ("mapping",)
    class MappingEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    MAPPING_FIELD_NUMBER: _ClassVar[int]
    mapping: _containers.ScalarMap[str, str]
    def __init__(self, mapping: _Optional[_Mapping[str, str]] = ...) -> None: ...

class ComputeOp(_message.Message):
    __slots__ = ("column", "expression", "replace")
    COLUMN_FIELD_NUMBER: _ClassVar[int]
    EXPRESSION_FIELD_NUMBER: _ClassVar[int]
    REPLACE_FIELD_NUMBER: _ClassVar[int]
    column: str
    expression: str
    replace: bool
    def __init__(self, column: _Optional[str] = ..., expression: _Optional[str] = ..., replace: bool = ...) -> None: ...

class CastOp(_message.Message):
    __slots__ = ("column", "target_type")
    COLUMN_FIELD_NUMBER: _ClassVar[int]
    TARGET_TYPE_FIELD_NUMBER: _ClassVar[int]
    column: str
    target_type: str
    def __init__(self, column: _Optional[str] = ..., target_type: _Optional[str] = ...) -> None: ...

class ReorderOp(_message.Message):
    __slots__ = ("columns",)
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    columns: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, columns: _Optional[_Iterable[str]] = ...) -> None: ...

class CommitRequest(_message.Message):
    __slots__ = ("best_effort",)
    BEST_EFFORT_FIELD_NUMBER: _ClassVar[int]
    best_effort: bool
    def __init__(self, best_effort: bool = ...) -> None: ...

class CommitResponse(_message.Message):
    __slots__ = ("success", "error", "tables")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    TABLES_FIELD_NUMBER: _ClassVar[int]
    success: bool
    error: str
    tables: _containers.RepeatedCompositeFieldContainer[TableCommitResult]
    def __init__(self, success: bool = ..., error: _Optional[str] = ..., tables: _Optional[_Iterable[_Union[TableCommitResult, _Mapping]]] = ...) -> None: ...

class TableCommitResult(_message.Message):
    __slots__ = ("output_name", "table_path", "status", "snapshot_id", "files_added", "rows_added", "bytes_added", "error")
    class Status(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        STATUS_UNSPECIFIED: _ClassVar[TableCommitResult.Status]
        STATUS_COMMITTED: _ClassVar[TableCommitResult.Status]
        STATUS_FAILED: _ClassVar[TableCommitResult.Status]
        STATUS_SKIPPED: _ClassVar[TableCommitResult.Status]
    STATUS_UNSPECIFIED: TableCommitResult.Status
    STATUS_COMMITTED: TableCommitResult.Status
    STATUS_FAILED: TableCommitResult.Status
    STATUS_SKIPPED: TableCommitResult.Status
    OUTPUT_NAME_FIELD_NUMBER: _ClassVar[int]
    TABLE_PATH_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_ID_FIELD_NUMBER: _ClassVar[int]
    FILES_ADDED_FIELD_NUMBER: _ClassVar[int]
    ROWS_ADDED_FIELD_NUMBER: _ClassVar[int]
    BYTES_ADDED_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    output_name: str
    table_path: str
    status: TableCommitResult.Status
    snapshot_id: int
    files_added: int
    rows_added: int
    bytes_added: int
    error: str
    def __init__(self, output_name: _Optional[str] = ..., table_path: _Optional[str] = ..., status: _Optional[_Union[TableCommitResult.Status, str]] = ..., snapshot_id: _Optional[int] = ..., files_added: _Optional[int] = ..., rows_added: _Optional[int] = ..., bytes_added: _Optional[int] = ..., error: _Optional[str] = ...) -> None: ...

class GetSchemaRequest(_message.Message):
    __slots__ = ("input_name",)
    INPUT_NAME_FIELD_NUMBER: _ClassVar[int]
    input_name: str
    def __init__(self, input_name: _Optional[str] = ...) -> None: ...

class SchemaResponse(_message.Message):
    __slots__ = ("schema",)
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    schema: Schema
    def __init__(self, schema: _Optional[_Union[Schema, _Mapping]] = ...) -> None: ...

class GetSampleRequest(_message.Message):
    __slots__ = ("input_name", "limit", "format")
    INPUT_NAME_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    FORMAT_FIELD_NUMBER: _ClassVar[int]
    input_name: str
    limit: int
    format: DataFormat
    def __init__(self, input_name: _Optional[str] = ..., limit: _Optional[int] = ..., format: _Optional[_Union[DataFormat, str]] = ...) -> None: ...

class SampleResponse(_message.Message):
    __slots__ = ("schema", "rows", "total_estimate")
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    ROWS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_ESTIMATE_FIELD_NUMBER: _ClassVar[int]
    schema: Schema
    rows: _containers.RepeatedScalarFieldContainer[bytes]
    total_estimate: int
    def __init__(self, schema: _Optional[_Union[Schema, _Mapping]] = ..., rows: _Optional[_Iterable[bytes]] = ..., total_estimate: _Optional[int] = ...) -> None: ...

class LogRequest(_message.Message):
    __slots__ = ("level", "message", "fields")
    class FieldsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    LEVEL_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    FIELDS_FIELD_NUMBER: _ClassVar[int]
    level: str
    message: str
    fields: _containers.ScalarMap[str, str]
    def __init__(self, level: _Optional[str] = ..., message: _Optional[str] = ..., fields: _Optional[_Mapping[str, str]] = ...) -> None: ...

class LogResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ShutdownRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ShutdownResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
