//go:build duckdb_arrow

package main

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	sdk "github.com/datuplet/datuplet/sdk/go"
)

// openTestConn opens an in-memory engine the way duckdb.go does (one pinned
// connection) without the component's resource SETs.
func openTestConn(t *testing.T) *sql.Conn {
	t.Helper()
	ctx := context.Background()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() { db.Close() })
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("db.Conn: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func TestNullTypedColumns(t *testing.T) {
	ctx := context.Background()
	conn := openTestConn(t)

	cases := []struct {
		name  string
		table string
		query string
		want  []string
	}{
		{"untyped NULL", "t1", "SELECT 1 AS id, NULL AS probe_null", []string{"probe_null"}},
		{"cast NULL", "t2", "SELECT 1 AS id, CAST(NULL AS VARCHAR) AS v", nil},
		{"list of NULL", "t3", "SELECT [NULL] AS arr", []string{"arr"}},
		{"struct with NULL", "t4", "SELECT {'a': NULL} AS s", []string{"s"}},
		{"map with NULL value", "t7", "SELECT MAP {'k': NULL} AS m", []string{"m"}},
		{"name contains null", "t5", "SELECT 1 AS nullable_id", nil},
		{"two hits keep column order", "t6", "SELECT NULL AS a, 1 AS b, NULL AS c", []string{"a", "c"}},
		{"struct field named null", "t8", "SELECT {'null': 1} AS s", nil},
		{"struct field named NULL", "t9", "SELECT {'NULL': 1} AS s", nil},
		{"struct with NULL field then typed field", "t10", "SELECT {'a': NULL, 'b': 1} AS s", []string{"s"}},
		{"struct field named NULL with list type", "t11", "SELECT {'NULL': [1]} AS s", nil},
		{"reserved name tables", "tables", "SELECT 1 AS id, NULL AS n", []string{"n"}},
		{"reserved name databases, no NULL", "databases", "SELECT 1 AS d", nil},
		{"reserved name schemas", "schemas", "SELECT NULL AS s, [NULL] AS l", []string{"s", "l"}},
		{"reserved name variables", "variables", "SELECT NULL AS v", []string{"v"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := conn.ExecContext(ctx, "CREATE TABLE "+tc.table+" AS "+tc.query); err != nil {
				t.Fatalf("ddl: %v", err)
			}
			got, err := nullTypedColumns(ctx, conn, tc.table)
			if err != nil {
				t.Fatalf("nullTypedColumns(%s): %v", tc.table, err)
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("nullTypedColumns(%s) = %v, want %v", tc.table, got, tc.want)
			}
		})
	}
}

func TestNullTypedColumnsResolvesLikeCopy(t *testing.T) {
	ctx := context.Background()
	conn := openTestConn(t)
	for _, ddl := range []string{
		"CREATE TEMP TABLE tmp_out AS SELECT NULL AS x, 1 AS y",
		"CREATE VIEW v_out AS SELECT NULL AS z",
	} {
		if _, err := conn.ExecContext(ctx, ddl); err != nil {
			t.Fatalf("ddl %q: %v", ddl, err)
		}
	}
	for table, want := range map[string][]string{"tmp_out": {"x"}, "v_out": {"z"}} {
		got, err := nullTypedColumns(ctx, conn, table)
		if err != nil {
			t.Fatalf("nullTypedColumns(%s): %v", table, err)
		}
		if !slices.Equal(got, want) {
			t.Errorf("nullTypedColumns(%s) = %v, want %v", table, got, want)
		}
	}
}

func TestWriteOutputRejectsNullTypedColumnAsUserError(t *testing.T) {
	ctx := context.Background()
	conn := openTestConn(t)
	if _, err := conn.ExecContext(ctx, "CREATE TABLE t1 AS SELECT 1 AS id, NULL AS probe_null"); err != nil {
		t.Fatalf("ddl: %v", err)
	}

	workdir := t.TempDir()
	// A nil client is deliberate: the check must fire before any SDK use.
	_, err := writeOutput(ctx, conn, nil, workdir, sdk.OutputTableRef{Name: "t1", Bucket: "etl", Table: "null_probe"})
	if err == nil {
		t.Fatal("writeOutput returned nil error for a NULL-typed column")
	}
	var ue *userErr
	if !errors.As(err, &ue) {
		t.Fatalf("error is not a *userErr (would exit 20, not 1): %v", err)
	}
	for _, want := range []string{"probe_null", "type NULL", `CAST(NULL AS VARCHAR) AS "probe_null"`} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err.Error(), want)
		}
	}
	staged, _ := filepath.Glob(filepath.Join(workdir, "*.parquet"))
	if len(staged) != 0 {
		t.Errorf("parquet staged despite pre-flight rejection: %v", staged)
	}
}
