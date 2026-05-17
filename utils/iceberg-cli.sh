#!/bin/bash

# DuckDB Iceberg Preview Script
# Usage: ./iceberg-cli.sh [options] [sql_query]

# ── Storage mode ──────────────────────────────────────────────────────
# --local   Local filesystem (default)
# --minio   MinIO/S3
STORAGE_MODE="local"

LOCAL_WAREHOUSE="/Users/tomaskacur/mydevel/datuplet/tmp/warehouse"
MINIO_WAREHOUSE="s3://datuplet/orgs/myorg/projects/myproject/tables"

show_help() {
  echo "DuckDB Iceberg Preview Script"
  echo ""
  echo "Usage: $0 [options] [sql_query]"
  echo ""
  echo "Storage mode (pick one):"
  echo "  --local                 Local filesystem (default)"
  echo "  --minio                 MinIO/S3"
  echo ""
  echo "Options:"
  echo "  -l, --list [TABLE]      List tables with stats, or show table summary"
  echo "  -la, --list-all TABLE   Show full table details (summary + schema + sample)"
  echo "  -f, --from TABLE        Specify table path (simplifies query syntax)"
  echo "  --raw                   Use raw parquet glob (bypass Iceberg metadata)"
  echo "  -h, --help              Show this help message"
  echo ""
  echo "Note: TABLE path is relative to warehouse (e.g., 'raw/products')"
  echo ""
  echo "Examples:"
  echo "  $0 -l                                      # List all tables (local)"
  echo "  $0 --minio -l                              # List all tables (MinIO)"
  echo "  $0 -l raw/products                         # Show summary for table"
  echo "  $0 -la raw/products                        # Show full details"
  echo "  $0 \"SELECT *\" -f raw/products"
}

LIST_TABLES=false
LIST_ALL=false
LIST_TABLE_NAME=""
TABLE_PATH=""
RAW_MODE=false

# Parse arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --local)
      STORAGE_MODE="local"
      shift
      ;;
    --minio)
      STORAGE_MODE="minio"
      shift
      ;;
    -la|--list-all)
      LIST_TABLES=true
      LIST_ALL=true
      shift
      if [[ $# -gt 0 && ! "$1" =~ ^- ]]; then
        LIST_TABLE_NAME="$1"
        shift
      fi
      ;;
    -l|--list)
      LIST_TABLES=true
      shift
      if [[ $# -gt 0 && ! "$1" =~ ^- ]]; then
        LIST_TABLE_NAME="$1"
        shift
      fi
      ;;
    -f|--from)
      TABLE_PATH="$2"
      shift 2
      ;;
    --raw)
      RAW_MODE=true
      shift
      ;;
    -h|--help)
      show_help
      exit 0
      ;;
    *)
      SQL_QUERY="$1"
      shift
      ;;
  esac
done

# ── Resolve warehouse path ────────────────────────────────────────────
if [ "$STORAGE_MODE" = "minio" ]; then
  WAREHOUSE="$MINIO_WAREHOUSE"
else
  # Local: find orgs/*/projects/*/tables under warehouse root
  TABLES_ROOT=$(find "$LOCAL_WAREHOUSE" -type d -name "tables" -path "*/orgs/*/projects/*/tables" 2>/dev/null | head -1)
  WAREHOUSE="${TABLES_ROOT:-$LOCAL_WAREHOUSE}"
fi

DATA_GLOB="data/*.parquet"

# In local mode, metadata contains Docker container paths (file:///data/warehouse/...)
# that don't exist on the host. allow_moved_paths resolves paths relative to metadata file.
if [ "$STORAGE_MODE" = "local" ]; then
  ICEBERG_OPTS=", allow_moved_paths = true"
else
  ICEBERG_OPTS=""
fi

# ── DuckDB preamble ──────────────────────────────────────────────────
build_preamble() {
  echo "INSTALL iceberg;"
  echo "LOAD iceberg;"
  if [ "$STORAGE_MODE" = "minio" ]; then
    echo "INSTALL httpfs;"
    echo "LOAD httpfs;"
    echo "SET s3_endpoint='localhost:30900';"
    echo "SET s3_access_key_id='minioadmin';"
    echo "SET s3_secret_access_key='minioadmin';"
    echo "SET s3_use_ssl=false;"
    echo "SET force_download=true;"
    echo "SET s3_url_style='path';"
  fi
}

build_preamble_raw() {
  if [ "$STORAGE_MODE" = "minio" ]; then
    echo "INSTALL httpfs;"
    echo "LOAD httpfs;"
    echo "SET s3_endpoint='localhost:30900';"
    echo "SET s3_access_key_id='minioadmin';"
    echo "SET s3_secret_access_key='minioadmin';"
    echo "SET s3_use_ssl=false;"
    echo "SET force_download=true;"
    echo "SET s3_url_style='path';"
  fi
}

# ── Table discovery ──────────────────────────────────────────────────
discover_tables() {
  if [ "$STORAGE_MODE" = "minio" ]; then
    duckdb -noheader -csv << EOF
$(build_preamble_raw)
SELECT DISTINCT
  regexp_replace(file, '$(echo "$WAREHOUSE" | sed 's/[&/\]/\\&/g')/(.+)/metadata/.*', '\1') as table_path
FROM glob('${WAREHOUSE}/**/metadata/*.metadata.json')
ORDER BY table_path;
EOF
  else
    find "$WAREHOUSE" -path "*/metadata/*.metadata.json" -type f 2>/dev/null \
      | sed "s|${WAREHOUSE}/||" \
      | sed 's|/metadata/.*||' \
      | sort -u
  fi
}

# ── Build query from --from flag ─────────────────────────────────────
if [ -n "$TABLE_PATH" ] && [ -n "$SQL_QUERY" ]; then
  if [ "$RAW_MODE" = true ]; then
    SQL_QUERY="$SQL_QUERY FROM read_parquet('${WAREHOUSE}/${TABLE_PATH}/${DATA_GLOB}')"
  else
    SQL_QUERY="$SQL_QUERY FROM iceberg_scan('${WAREHOUSE}/${TABLE_PATH}'${ICEBERG_OPTS})"
  fi
fi

# ── Main logic ───────────────────────────────────────────────────────
if [ "$LIST_TABLES" = true ]; then
  if [ -n "$LIST_TABLE_NAME" ]; then
    if [ "$LIST_ALL" = true ]; then
      if [ "$RAW_MODE" = true ]; then
        duckdb << EOF
$(build_preamble_raw)
-- Table: ${LIST_TABLE_NAME} (RAW PARQUET)
.echo on
SELECT COUNT(*) as row_count FROM read_parquet('${WAREHOUSE}/${LIST_TABLE_NAME}/${DATA_GLOB}');
SELECT
  COUNT(DISTINCT file_name) as file_count,
  SUM(total_compressed_size) as total_bytes,
  printf('%.2f MB', SUM(total_compressed_size) / 1024.0 / 1024.0) as total_size
FROM parquet_metadata('${WAREHOUSE}/${LIST_TABLE_NAME}/${DATA_GLOB}');
DESCRIBE SELECT * FROM read_parquet('${WAREHOUSE}/${LIST_TABLE_NAME}/${DATA_GLOB}');
SELECT * FROM read_parquet('${WAREHOUSE}/${LIST_TABLE_NAME}/${DATA_GLOB}') LIMIT 5;
EOF
      else
        duckdb << EOF
$(build_preamble)
-- Table: ${LIST_TABLE_NAME} (Iceberg)
.echo on
SELECT COUNT(*) as row_count FROM iceberg_scan('${WAREHOUSE}/${LIST_TABLE_NAME}'${ICEBERG_OPTS});
SELECT * FROM iceberg_metadata('${WAREHOUSE}/${LIST_TABLE_NAME}'${ICEBERG_OPTS});
SELECT * FROM iceberg_snapshots('${WAREHOUSE}/${LIST_TABLE_NAME}');
DESCRIBE SELECT * FROM iceberg_scan('${WAREHOUSE}/${LIST_TABLE_NAME}'${ICEBERG_OPTS});
SELECT * FROM iceberg_scan('${WAREHOUSE}/${LIST_TABLE_NAME}'${ICEBERG_OPTS}) LIMIT 5;
EOF
      fi
    else
      if [ "$RAW_MODE" = true ]; then
        duckdb << EOF
$(build_preamble_raw)
SELECT
  COUNT(*) as row_count,
  (SELECT COUNT(DISTINCT file_name) FROM parquet_metadata('${WAREHOUSE}/${LIST_TABLE_NAME}/${DATA_GLOB}')) as file_count,
  (SELECT printf('%.2f MB', SUM(total_compressed_size) / 1024.0 / 1024.0) FROM parquet_metadata('${WAREHOUSE}/${LIST_TABLE_NAME}/${DATA_GLOB}')) as total_size
FROM read_parquet('${WAREHOUSE}/${LIST_TABLE_NAME}/${DATA_GLOB}');
EOF
      else
        duckdb << EOF
$(build_preamble)
SELECT COUNT(*) as row_count FROM iceberg_scan('${WAREHOUSE}/${LIST_TABLE_NAME}'${ICEBERG_OPTS});
EOF
      fi
    fi
  else
    # List all tables
    TABLES=$(discover_tables)

    if [ -z "$TABLES" ]; then
      echo "No Iceberg tables found in: $WAREHOUSE"
      exit 0
    fi

    echo "[$STORAGE_MODE] $WAREHOUSE"
    echo ""

    if [ "$RAW_MODE" = true ]; then
      printf "%-40s %12s %12s %12s\n" "table_path" "raw_rows" "file_count" "total_size"
      printf "%-40s %12s %12s %12s\n" "----------------------------------------" "------------" "------------" "------------"

      for TABLE in $TABLES; do
        STATS=$(duckdb -noheader -csv 2>/dev/null << EOF
$(build_preamble_raw)
SELECT
  COUNT(*) as row_count,
  (SELECT COUNT(DISTINCT file_name) FROM parquet_metadata('${WAREHOUSE}/${TABLE}/${DATA_GLOB}')) as file_count,
  (SELECT printf('%.2f MB', SUM(total_compressed_size) / 1024.0 / 1024.0) FROM parquet_metadata('${WAREHOUSE}/${TABLE}/${DATA_GLOB}')) as total_size
FROM read_parquet('${WAREHOUSE}/${TABLE}/${DATA_GLOB}');
EOF
)
        if [ -n "$STATS" ]; then
          ROW_COUNT=$(echo "$STATS" | cut -d',' -f1)
          FILE_COUNT=$(echo "$STATS" | cut -d',' -f2)
          TOTAL_SIZE=$(echo "$STATS" | cut -d',' -f3)
          printf "%-40s %12s %12s %12s\n" "$TABLE" "$ROW_COUNT" "$FILE_COUNT" "$TOTAL_SIZE"
        else
          printf "%-40s %12s %12s %12s\n" "$TABLE" "(empty)" "-" "-"
        fi
      done
    else
      printf "%-40s %12s %12s\n" "table_path" "iceberg_rows" "snapshots"
      printf "%-40s %12s %12s\n" "----------------------------------------" "------------" "------------"

      for TABLE in $TABLES; do
        STATS=$(duckdb -noheader -csv 2>/dev/null << EOF
$(build_preamble)
SELECT
  (SELECT COUNT(*) FROM iceberg_scan('${WAREHOUSE}/${TABLE}'${ICEBERG_OPTS})) as row_count,
  (SELECT COUNT(*) FROM iceberg_snapshots('${WAREHOUSE}/${TABLE}')) as snapshot_count;
EOF
)
        if [ -n "$STATS" ]; then
          ROW_COUNT=$(echo "$STATS" | cut -d',' -f1)
          SNAPSHOT_COUNT=$(echo "$STATS" | cut -d',' -f2)
          printf "%-40s %12s %12s\n" "$TABLE" "$ROW_COUNT" "$SNAPSHOT_COUNT"
        else
          printf "%-40s %12s %12s\n" "$TABLE" "(error)" "-"
        fi
      done
    fi
  fi
elif [ -z "$SQL_QUERY" ]; then
  echo "Error: SQL query required"
  echo "Usage: $0 [options] <sql_query>"
  echo "       $0 -l (to list tables)"
  echo "Use -h or --help for more information"
  exit 1
else
  duckdb << EOF
$(build_preamble)
$SQL_QUERY;
EOF
fi
