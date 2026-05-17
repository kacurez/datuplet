package authz

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	openfga "github.com/openfga/go-sdk"
	"github.com/openfga/go-sdk/client"
	"github.com/openfga/go-sdk/credentials"
)

// OpenFGAAuthorizer is an Authorizer backed by an OpenFGA HTTP endpoint.
// It wraps the OpenFGA Go SDK's client.OpenFgaClient.
//
// Every Check / Write / Delete call is wrapped in a deadline-bounded child
// context. On deadline exceeded, ErrAuthzUnavailable is returned so handlers
// can emit HTTP 503 rather than 403.
//
// Correction #6 (briefing): pipeline-api uses HTTP (port 8080), not gRPC
// (port 8081). The apiURL parameter is opaque — no port assumptions here.
type OpenFGAAuthorizer struct {
	fga      *client.OpenFgaClient
	modelID  string
	deadline time.Duration
}

// NewOpenFGAAuthorizer creates an OpenFGAAuthorizer that wraps the OpenFGA
// SDK's client.OpenFgaClient.
//
//   - apiURL:   base URL of the OpenFGA HTTP API (e.g. "http://localhost:8080")
//   - storeID:  OpenFGA store ULID (must be non-empty)
//   - modelID:  authorization model ULID (must be non-empty)
//   - apiKey:   preshared API key for OpenFGA's authn.method=preshared; empty
//     means no Authorization header is attached (existing behaviour).
//   - deadline: per-call timeout; on expiry the call returns ErrAuthzUnavailable
func NewOpenFGAAuthorizer(apiURL, storeID, modelID, apiKey string, deadline time.Duration) (*OpenFGAAuthorizer, error) {
	if apiURL == "" {
		return nil, errors.New("authz: apiURL is required")
	}
	if storeID == "" {
		return nil, errors.New("authz: storeID is required")
	}
	if modelID == "" {
		return nil, errors.New("authz: modelID is required")
	}
	if deadline <= 0 {
		return nil, errors.New("authz: deadline must be positive")
	}

	clientCfg := &client.ClientConfiguration{
		ApiUrl:               apiURL,
		StoreId:              storeID,
		AuthorizationModelId: modelID,
	}
	if apiKey != "" {
		clientCfg.Credentials = &credentials.Credentials{
			Method: credentials.CredentialsMethodApiToken,
			Config: &credentials.Config{
				ApiToken: apiKey,
			},
		}
	}
	fgaClient, err := client.NewSdkClient(clientCfg)
	if err != nil {
		return nil, fmt.Errorf("authz: create OpenFGA client: %w", err)
	}

	return &OpenFGAAuthorizer{
		fga:      fgaClient,
		modelID:  modelID,
		deadline: deadline,
	}, nil
}

// Check implements Authorizer.
func (a *OpenFGAAuthorizer) Check(ctx context.Context, user string, relation string, obj Object) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, a.deadline)
	defer cancel()

	resp, err := a.fga.Check(ctx).Body(client.ClientCheckRequest{
		User:     user,
		Relation: relation,
		Object:   obj.String(),
	}).Execute()
	if err != nil {
		return false, a.wrapErr(ctx, err)
	}
	return resp.GetAllowed(), nil
}

// BatchCheck issues N parallel Check calls via the OpenFGA SDK's
// ClientBatchCheck (goroutine pool, default max 10 concurrent HTTP requests).
// Returns per-query results; errs[i] holds any per-query error.
//
// NOTE: This uses client-side parallel fan-out, NOT OpenFGA's server-side
// /batch-check single-call endpoint. The server-side endpoint is ~14× faster
// (3.06ms vs 42.95ms for 20 keys, single HTTP call); this implementation
// is N HTTP calls in parallel — faster than sequential, but will not
// reach that speedup for batches >10 since the goroutine pool caps
// concurrency.
//
// TODO(slice-006.6): if storage-browse perf budget needs the full 14×
// speedup, switch to server-side BatchCheck via the SDK's
// fga.BatchCheck(ctx) call. The OpenFGA server v1.15.0+ supports the
// /batch-check endpoint and the SDK exposes it natively.
func (a *OpenFGAAuthorizer) BatchCheck(ctx context.Context, queries []CheckQuery) ([]bool, []error) {
	if len(queries) == 0 {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(ctx, a.deadline)
	defer cancel()

	checks := make(client.ClientBatchCheckClientBody, len(queries))
	for i, q := range queries {
		checks[i] = client.ClientCheckRequest{
			User:     q.User,
			Relation: q.Relation,
			Object:   q.Object.String(),
		}
	}

	resp, err := a.fga.ClientBatchCheck(ctx).Body(checks).Execute()
	if err != nil {
		wrapped := a.wrapErr(ctx, err)
		errs := make([]error, len(queries))
		for i := range errs {
			errs[i] = wrapped
		}
		return make([]bool, len(queries)), errs
	}

	results := make([]bool, len(queries))
	errs := make([]error, len(queries))
	for i, r := range *resp {
		if r.Error != nil {
			errs[i] = r.Error
		} else {
			results[i] = r.GetAllowed()
		}
	}
	return results, errs
}

// ListObjects implements Authorizer.
func (a *OpenFGAAuthorizer) ListObjects(ctx context.Context, user string, relation string, objType ObjectType) ([]Object, error) {
	ctx, cancel := context.WithTimeout(ctx, a.deadline)
	defer cancel()

	resp, err := a.fga.ListObjects(ctx).Body(client.ClientListObjectsRequest{
		User:     user,
		Relation: relation,
		Type:     string(objType),
	}).Execute()
	if err != nil {
		return nil, a.wrapErr(ctx, err)
	}

	objs := resp.GetObjects()
	out := make([]Object, 0, len(objs))
	prefix := string(objType) + ":"
	for _, s := range objs {
		id, ok := strings.CutPrefix(s, prefix)
		if !ok {
			// Unexpected type in response — skip rather than panic.
			continue
		}
		out = append(out, Object{kind: string(objType), id: id})
	}
	return out, nil
}

// WriteTuples implements Authorizer.
//
// Idempotency: OpenFGA's Write API is transactional and returns
// `write_failed_due_to_invalid_input` with a "tuple to be written
// already existed" message when ANY tuple in the batch is already
// present. For single-tuple writes (the only production caller shape —
// adminGrant, WriteAdminTuple, run-trigger paths each pass exactly one
// tuple) we treat that as success: re-writing a content-addressed
// (user, relation, object) triple is semantically a no-op, and
// callers genuinely want "ensure this tuple exists" rather than
// "fail if already there". For multi-tuple batches the error is still
// surfaced — silently swallowing would mask a missed write among
// already-existing peers, which we cannot disambiguate from the
// error message alone.
func (a *OpenFGAAuthorizer) WriteTuples(ctx context.Context, tuples []Tuple) error {
	if len(tuples) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, a.deadline)
	defer cancel()

	body := make(client.ClientWriteTuplesBody, len(tuples))
	for i, t := range tuples {
		body[i] = openfga.TupleKey{
			User:     t.User,
			Relation: t.Relation,
			Object:   t.Object.String(),
		}
	}
	_, err := a.fga.WriteTuples(ctx).Body(body).Execute()
	if err != nil {
		if len(tuples) == 1 && isTupleAlreadyExistsErr(err) {
			return nil
		}
		return a.wrapErr(ctx, err)
	}
	return nil
}

// isTupleAlreadyExistsErr detects OpenFGA's "tuple already exists"
// signal. The Go SDK surfaces it as a generic error whose Error()
// string contains the canonical OpenFGA message text. We also accept
// the alternate phrasing emitted by some OpenFGA versions.
func isTupleAlreadyExistsErr(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "tuple to be written already existed") ||
		strings.Contains(s, "cannot write a tuple which already exists")
}

// DeleteTuples implements Authorizer.
func (a *OpenFGAAuthorizer) DeleteTuples(ctx context.Context, tuples []Tuple) error {
	if len(tuples) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, a.deadline)
	defer cancel()

	body := make(client.ClientDeleteTuplesBody, len(tuples))
	for i, t := range tuples {
		body[i] = openfga.TupleKeyWithoutCondition{
			User:     t.User,
			Relation: t.Relation,
			Object:   t.Object.String(),
		}
	}
	_, err := a.fga.DeleteTuples(ctx).Body(body).Execute()
	if err != nil {
		return a.wrapErr(ctx, err)
	}
	return nil
}

// wrapErr converts a context deadline/cancellation error or a network-level
// error into ErrAuthzUnavailable, preserving other errors as-is.
//
// Handlers check errors.Is(err, ErrAuthzUnavailable) to emit HTTP 503 instead
// of 500. Without this, connection-refused / DNS failures would propagate as
// raw *url.Error / *net.OpError and be misclassified as unexpected 500s.
func (a *OpenFGAAuthorizer) wrapErr(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return fmt.Errorf("%w: %w", ErrAuthzUnavailable, err)
	}
	// Also catch the case where the context attached to the *request* is done
	// (the outer ctx passed by the caller), not just the inner deadline ctx.
	if ctx.Err() != nil {
		return fmt.Errorf("%w: %w", ErrAuthzUnavailable, ctx.Err())
	}
	// Network-unreachable errors (connection refused, DNS failure, TCP reset)
	// arrive as *url.Error wrapping *net.OpError. They must also map to
	// ErrAuthzUnavailable so callers emit 503 rather than 500.
	var urlErr *url.Error
	var netErr *net.OpError
	if errors.As(err, &urlErr) || errors.As(err, &netErr) {
		return fmt.Errorf("%w: %w", ErrAuthzUnavailable, err)
	}
	return err
}
