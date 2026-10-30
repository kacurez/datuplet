//go:build duckdb_arrow

package main

import (
	"context"
	"database/sql"
	"errors"
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

func nullColNames(cols []nullCol) []string {
	var names []string
	for _, c := range cols {
		names = append(names, c.name)
	}
	return names
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
		{"fixed-size array of NULL", "t12", "SELECT array_value(NULL, NULL) AS a", []string{"a"}},
		{"struct with fixed-size array of NULL", "t13", "SELECT {'a': array_value(NULL)} AS s", []string{"s"}},
		// Known false positive, documented: a struct field name ending in
		// `null)` looks like a NULL type to the regex.
		{"struct field name ending in null) (known false positive)", "t14", "SELECT {'value (null)': 1} AS s", []string{"s"}},
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
			if !slices.Equal(nullColNames(got), tc.want) {
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
		if !slices.Equal(nullColNames(got), want) {
			t.Errorf("nullTypedColumns(%s) = %v, want %v", table, got, want)
		}
	}
}

func TestCheckOutputTypesRejectsNullTypedColumnAsUserError(t *testing.T) {
	ctx := context.Background()
	conn := openTestConn(t)
	for _, ddl := range []string{
		"CREATE TABLE t1 AS SELECT 1 AS id, NULL AS probe_null",
		"CREATE TABLE clean AS SELECT 1 AS id",
		"CREATE TABLE bad2 AS SELECT NULL AS n",
		`CREATE TABLE quoted AS SELECT NULL AS "a""b"`,
		"CREATE TABLE nested AS SELECT 1 AS id, [NULL] AS arr, NULL AS other",
	} {
		if _, err := conn.ExecContext(ctx, ddl); err != nil {
			t.Fatalf("ddl: %v", err)
		}
	}

	check := func(t *testing.T, outs ...sdk.OutputTableRef) error {
		t.Helper()
		err := checkOutputTypes(ctx, conn, outs)
		if err == nil {
			t.Fatal("checkOutputTypes returned nil error for a NULL-typed column")
		}
		var ue *userErr
		if !errors.As(err, &ue) {
			t.Fatalf("error is not a *userErr (would exit 20, not 1): %v", err)
		}
		return err
	}
	wantAll := func(t *testing.T, err error, wants ...string) {
		t.Helper()
		for _, want := range wants {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q does not contain %q", err.Error(), want)
			}
		}
	}

	t.Run("top-level NULL", func(t *testing.T) {
		err := check(t, sdk.OutputTableRef{Name: "t1", Bucket: "etl", Table: "null_probe"})
		wantAll(t, err, "probe_null", "type NULL", `CAST(NULL AS VARCHAR) AS "probe_null"`)
	})
	t.Run("clean first, NULL second names the second", func(t *testing.T) {
		err := check(t,
			sdk.OutputTableRef{Name: "clean", Bucket: "etl", Table: "first"},
			sdk.OutputTableRef{Name: "bad2", Bucket: "etl", Table: "second"})
		wantAll(t, err, "etl.second", `column "n"`)
		if strings.Contains(err.Error(), "etl.first") {
			t.Errorf("error names the clean output: %v", err)
		}
	})
	t.Run("identifier is quoted SQL-style", func(t *testing.T) {
		err := check(t, sdk.OutputTableRef{Name: "quoted", Bucket: "etl", Table: "q"})
		wantAll(t, err, `AS "a""b"`)
	})
	t.Run("nested type gets no cast hint", func(t *testing.T) {
		err := check(t, sdk.OutputTableRef{Name: "nested", Bucket: "etl", Table: "n"})
		wantAll(t, err, `"arr"`, `"NULL"[]`, "not supported", "(also: ")
		if strings.Contains(err.Error(), "CAST(NULL AS VARCHAR)") {
			t.Errorf("nested message suggests the cast: %v", err)
		}
	})
	t.Run("no NULL columns is nil", func(t *testing.T) {
		if err := checkOutputTypes(ctx, conn, []sdk.OutputTableRef{{Name: "clean", Bucket: "etl", Table: "c"}}); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})
}
