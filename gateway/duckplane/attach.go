//go:build duckdb_arrow

package duckplane

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/datuplet/datuplet/gateway/duck"
)

// catalogAlias is the name the Lakekeeper catalog is attached under.
const catalogAlias = "lk"

// catalogSecretName holds the run JWT between CREATE SECRET and ATTACH only.
const catalogSecretName = "dg_lk_tok"

var (
	errNoCatalog = errors.New("duckplane: reads require a catalog")
	// errCatalogConfig, errTokenShape and errAttach are the only errors a
	// failed attach produces: neither a statement nor DuckDB's message (which
	// can echo the token or the statement) is surfaced.
	errCatalogConfig = errors.New("duckplane: incomplete catalog configuration")
	errTokenShape    = errors.New("duckplane: catalog token is not a valid JWT shape")
	errAttach        = errors.New("duckplane: attaching the catalog failed")
)

// jwtSegment is one base64url JWT segment without padding.
var jwtSegment = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// validateJWTShape checks that s is three non-empty base64url segments joined
// by dots (copied from components/queryengine/attach.go). It does not verify
// the token; it proves the token holds no SQL metacharacter, so the parser can
// never echo part of it. The error never includes s.
func validateJWTShape(s string) error {
	segs := strings.Split(s, ".")
	if len(segs) != 3 {
		return errTokenShape
	}
	for _, seg := range segs {
		if !jwtSegment.MatchString(seg) {
			return errTokenShape
		}
	}
	return nil
}

// attach ATTACHes the Lakekeeper catalog READ_ONLY as lk, once per plane. A
// result is cached for the plane's lifetime unless the attempt was cut short
// by ctx or a plane termination, in which case the next call retries. ctx
// bounds the wait and the attach.
func (p *Plane) attach(ctx context.Context) error {
	select {
	case p.attachOp <- struct{}{}:
	case <-ctx.Done():
		return fmt.Errorf("duckplane: waiting for the catalog attach: %w", ctx.Err())
	}
	defer func() { <-p.attachOp }()
	if p.attachDone {
		return p.attachErr
	}

	actx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	stop := context.AfterFunc(p.ctx, func() { cancel(context.Cause(p.ctx)) })
	defer stop()
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return errPlaneClosed
	}
	p.attachCancel = cancel
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		p.attachCancel = nil
		p.mu.Unlock()
	}()

	err := p.attachCatalog(actx)
	if err != nil && actx.Err() != nil { // cut short: not cached
		if ctx.Err() != nil {
			return fmt.Errorf("duckplane: catalog attach: %w", ctx.Err())
		}
		return context.Cause(actx) // Interrupt or plane Close
	}
	p.attachDone, p.attachErr = true, err
	return err
}

// attachCatalog runs CREATE SECRET, ATTACH and DROP SECRET on a connection of
// its own. The JWT shape is validated before any SQL is built.
func (p *Plane) attachCatalog(ctx context.Context) error {
	cat := p.cfg.Catalog
	if err := validateJWTShape(cat.Token); err != nil {
		return err
	}
	if cat.ProjectID == "" || cat.Warehouse == "" {
		return errCatalogConfig
	}
	c, err := p.db.Connect()
	if err != nil {
		return fmt.Errorf("duckplane: %w", err)
	}
	defer c.Close()
	if p.attachHook != nil {
		return p.attachHook(ctx, c)
	}

	warehouse, err1 := duck.LiteralChecked(cat.ProjectID + "/" + cat.Warehouse)
	uri, err2 := duck.LiteralChecked(cat.URL)
	if err1 != nil || err2 != nil {
		return errCatalogConfig
	}
	// Carries the token: never log it or put it into an error.
	createSecret := "CREATE OR REPLACE SECRET " + duck.Ident(catalogSecretName) +
		" (TYPE ICEBERG, TOKEN " + duck.Literal(cat.Token) + ")"
	if err := c.ExecRedacted(ctx, createSecret, "create catalog secret"); err != nil {
		return attachFailure(ctx)
	}
	attachSQL := "ATTACH " + warehouse + " AS " + duck.Ident(catalogAlias) +
		" (TYPE ICEBERG, URI " + uri + ", SECRET " + duck.Ident(catalogSecretName) +
		", ACCESS_DELEGATION_MODE 'vended_credentials', READ_ONLY)"
	attachErr := c.ExecRedacted(ctx, attachSQL, "attach catalog")
	// The attached catalog keeps its own copy of the token; the named secret
	// is not needed past ATTACH. Best effort on a failed ATTACH.
	dropErr := c.ExecRedacted(context.Background(), "DROP TEMPORARY SECRET IF EXISTS "+duck.Ident(catalogSecretName), "drop catalog secret")
	if attachErr != nil {
		return attachFailure(ctx)
	}
	if dropErr != nil {
		return errors.New("duckplane: dropping the catalog secret failed")
	}
	return nil
}

// attachFailure is the error of a failed secret or ATTACH statement: the
// cancellation cause when the attempt was cut short, else the fixed errAttach.
func attachFailure(ctx context.Context) error {
	if ctx.Err() != nil {
		return fmt.Errorf("%w: %w", errAttach, context.Cause(ctx))
	}
	return errAttach
}
