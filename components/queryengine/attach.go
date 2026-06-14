//go:build duckdb_arrow

package queryengine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// jwtSegment matches a single base64url JWT segment (no padding). A
// structurally valid JWT is exactly three of these joined by dots.
var jwtSegment = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// validateJWTShape checks that s is structurally a JWT: non-empty, exactly
// three dot-separated segments, each non-empty base64url (no padding). It does
// NOT verify the signature or claims — lakekeeper does that — it only proves
// the token is parser-safe for embedding as a SQL literal.
//
// SECURITY: this guard is the load-bearing defence against transitive token
// leakage. A well-formed JWT contains only [A-Za-z0-9_-.] characters, none of
// which is a SQL metacharacter, so it cannot trigger a DuckDB parse error;
// therefore the parser can never echo a token fragment into an error message.
// This makes parser-safety a guarantee rather than an assumption. The error
// NEVER includes s.
func validateJWTShape(s string) error {
	segs := strings.Split(s, ".")
	if len(segs) != 3 {
		return errors.New("attachCatalog: CatalogJWT is not a valid JWT shape")
	}
	for _, seg := range segs {
		if !jwtSegment.MatchString(seg) {
			return errors.New("attachCatalog: CatalogJWT is not a valid JWT shape")
		}
	}
	return nil
}

// catalogAlias is the DuckDB ATTACH alias for the lakekeeper iceberg-REST
// catalog. Every name reference in user SQL resolves through it
// (lk.<ns>.<table>); the post-attach USE makes a chosen namespace current so
// bare and two-part names resolve too (see attachCatalog).
const catalogAlias = "lk"

// catalogSecretName is the DuckDB secret holding the catalog bearer JWT. It is
// referenced by name from the ATTACH statement (SECRET <name>).
const catalogSecretName = "lk_tok"

// attachCatalog wires the embedded DuckDB engine to a lakekeeper iceberg-REST
// catalog using the canonical form proven by RFC 022 Spike 0.1 (lakekeeper
// v0.12.1 + DuckDB 1.5.3 iceberg ext). It MUST run after openEngine and BEFORE
// lock(): ATTACH needs mutable config, and the first secret op initializes
// DuckDB's secret manager via the local stored-secrets dir — which lock()'s
// disabled_filesystems would block.
//
// SECURITY: the catalog JWT is embedded as a SQL literal in CREATE SECRET.
// No error or log line below ever includes a statement string that carries the
// token; the CREATE SECRET error is wrapped with a fixed redacted description.
// All interpolated values (token, endpoint, warehouse) pass through escapeSQL
// and sit inside single quotes.
func attachCatalog(ctx context.Context, e *engine, r Request) error {
	// SECURITY: validate the JWT shape BEFORE any SQL is built. A shape-valid
	// token (base64url segments, no SQL metacharacters) cannot break the DuckDB
	// parser, so the parser can never echo token fragments into an error — see
	// validateJWTShape. The error never includes the token value.
	if err := validateJWTShape(r.CatalogJWT); err != nil {
		return err
	}

	// ATTACH + secret-manager init need mutable config; once lock() has run
	// (disabled_filesystems, lock_configuration) attaching is both impossible
	// and a programming error. Catch it before any SQL touches the connection.
	if e.locked {
		return fmt.Errorf("attachCatalog: engine already locked; ATTACH must precede lock()")
	}

	// Iceberg + httpfs are NOT statically linked in duckdb-go, so they must be
	// installed and loaded before any secret/ATTACH op. In production images
	// the extensions are pre-baked into the local extension dir, making INSTALL
	// a no-op; on dev laptops INSTALL downloads on first use. Separate
	// ExecContext calls (no multi-statement smuggling) — these are constant
	// strings with no interpolation.
	for _, s := range []string{
		"INSTALL iceberg",
		"LOAD iceberg",
		"INSTALL httpfs",
		"LOAD httpfs",
	} {
		if _, err := e.conn.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("extension setup %q: %w", s, err)
		}
	}

	// CREATE SECRET carries the bearer JWT as a single-quoted literal. NEVER
	// surface the statement text on error — the redacted wrap below is the only
	// permitted error shape for this op.
	createSecret := fmt.Sprintf(
		"CREATE OR REPLACE SECRET %s (TYPE ICEBERG, TOKEN '%s')",
		catalogSecretName, escapeSQL(r.CatalogJWT),
	)
	if _, err := e.conn.ExecContext(ctx, createSecret); err != nil {
		// SECURITY: do not include createSecret (it contains the token). This
		// wrap is the only permitted error shape for this op:
		//   - the format string is a constant (no %v of createSecret), so the
		//     statement text — and the token in it — can never reach the error;
		//   - the token is shape-validated above, so it carries no SQL
		//     metacharacter that could break the parser and make DuckDB echo a
		//     token fragment back through %w.
		// Together these make a token leak through this path impossible, not
		// merely unlikely.
		return fmt.Errorf("CREATE SECRET %s: %w", catalogSecretName, err)
	}

	// ATTACH '<project-id>/<warehouse>'. The warehouse arg is opaque to us:
	// callers pass the already-project-qualified string via Request.Warehouse
	// (Spike 0.1 §1 — the extension's /v1/config handshake sends no
	// x-project-id header, so a bare name resolves against lakekeeper's
	// nil-UUID default project → 401/403). We pass it verbatim.
	attach := fmt.Sprintf(
		"ATTACH '%s' AS %s (TYPE ICEBERG, ENDPOINT '%s', SECRET %s, ACCESS_DELEGATION_MODE 'vended_credentials')",
		escapeSQL(r.Warehouse), catalogAlias, escapeSQL(r.LakekeeperURL), catalogSecretName,
	)
	if _, err := e.conn.ExecContext(ctx, attach); err != nil {
		// The ATTACH text carries no secret (the JWT lives in the secret, not
		// here), but the underlying error CAN echo the endpoint/warehouse,
		// which is fine to surface. Keep the statement text out anyway to avoid
		// any future drift where a literal becomes sensitive.
		return fmt.Errorf("ATTACH catalog %s: %w", catalogAlias, err)
	}

	// Name resolution (Spike 0.1 §4): `USE lk` alone FAILS — an iceberg catalog
	// has no default schema. Pick the first existing namespace and run
	// `USE lk."<schema>"` so bare <table> and two-part <ns>.<table> both
	// resolve. If the warehouse has NO namespaces, skip the USE entirely —
	// there is nothing to resolve, and fully-qualified lk.<ns>.<table> always
	// works regardless.
	//
	// duckdb_schemas() exposes the owning catalog as `database_name` in this
	// DuckDB build (1.5.3); `catalog_name` from the spike note is not a column
	// here. Filter on database_name == the attach alias.
	var schema string
	// catalogAlias is a package constant ("lk"), not caller input — this concat
	// is injection-safe by construction.
	row := e.conn.QueryRowContext(ctx,
		"SELECT schema_name FROM duckdb_schemas() WHERE database_name = '"+catalogAlias+"' ORDER BY schema_name LIMIT 1",
	)
	switch err := row.Scan(&schema); {
	case err == nil:
		// Quote the schema name: namespaces may contain characters (e.g. a
		// hyphen) that are not bare identifiers.
		use := fmt.Sprintf(`USE %s."%s"`, catalogAlias, escapeSQLIdent(schema))
		if _, err := e.conn.ExecContext(ctx, use); err != nil {
			return fmt.Errorf("USE %s.<namespace>: %w", catalogAlias, err)
		}
	case errors.Is(err, sql.ErrNoRows):
		// Empty warehouse with no namespaces. Skip the USE — nothing to
		// resolve; fully-qualified lk.<ns>.<table> always works regardless.
	default:
		return fmt.Errorf("list catalog %s namespaces: %w", catalogAlias, err)
	}

	return nil
}

// escapeSQLIdent escapes a value for embedding inside a double-quoted SQL
// identifier (e.g. a schema name in USE lk."<schema>"). DuckDB doubles an
// embedded double quote to escape it, mirroring escapeSQL's single-quote rule.
func escapeSQLIdent(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '"' {
			out = append(out, '"', '"')
			continue
		}
		out = append(out, s[i])
	}
	return string(out)
}
