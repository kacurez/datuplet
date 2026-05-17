package icebergjob

import (
	"github.com/apache/iceberg-go"
	"github.com/golang-jwt/jwt/v5"
)

// BuildSnapshotSummary parses rawToken (unverified — lakekeeper has already
// verified the signature on every REST call; here we only read claims for
// audit purposes) and returns an iceberg.Properties map suitable for the
// snapshotProps argument of txn.AddFiles / txn.ReplaceDataFiles.
//
// Keys written:
//
//	datuplet.actor       — JWT "actor" claim (the triggering user UUID)
//	datuplet.run-id      — runID (from caller, authoritative)
//	datuplet.run-mode    — derived via RunModeFromTokenKind from "token_kind" claim
//	datuplet.pipeline-api — JWT "iss" claim (the issuing pipeline-api instance)
//
// Returns nil (no audit keys) when:
//   - rawToken is empty
//   - rawToken cannot be parsed as a JWT (malformed — corrupt tokens do not abort a commit)
//
// Unforgeability invariant: run-mode MUST NOT be sourced from env vars
// or any env-controllable input — it is derived exclusively from the
// RS256-signed token_kind JWT claim. A caller with shell access on the pod
// cannot forge a "cluster" badge by setting RUN_MODE or any other env var,
// and cannot inject a datuplet.run-mode JWT claim because this function reads
// only token_kind, not any datuplet.* claim present in the token body.
func BuildSnapshotSummary(rawToken string, runID string) iceberg.Properties {
	if rawToken == "" {
		return nil
	}

	parser := jwt.NewParser()
	tok, _, err := parser.ParseUnverified(rawToken, jwt.MapClaims{})
	if err != nil {
		return nil
	}

	claims, ok := tok.Claims.(jwt.MapClaims)
	if !ok {
		return nil
	}

	actor, _ := claims["actor"].(string)
	iss, _ := claims["iss"].(string)
	tokenKind, _ := claims["token_kind"].(string)
	runMode := RunModeFromTokenKind(tokenKind)
	if runMode == "" {
		return nil
	}

	props := iceberg.Properties{
		"datuplet.actor":        actor,
		"datuplet.run-id":       runID,
		"datuplet.run-mode":     runMode,
		"datuplet.pipeline-api": iss,
	}
	return props
}

// tokenKindRun and tokenKindLocalCLI are the canonical token_kind JWT claim
// values produced by pkg/pipelineapi/tokens. They are intentionally
// duplicated here rather than imported from that package to avoid a layering
// inversion (pkg/icebergjob is downstream of the catalog binary;
// pkg/pipelineapi is the issuer). The JWT spec is the integration contract —
// if these values change, both sides must update in lockstep.
const (
	tokenKindRun      = "run"
	tokenKindLocalCLI = "local-cli"
)

// RunModeFromTokenKind maps a JWT token_kind claim value to the
// datuplet.run-mode audit string.
//
// Mapping:
//
//	"run"       → "cluster"   (K8s-operator-triggered run token)
//	"local-cli" → "local-cli" (laptop CLI token)
//	anything else → ""        (unknown; no run-mode key written)
func RunModeFromTokenKind(kind string) string {
	switch kind {
	case tokenKindRun:
		return "cluster"
	case tokenKindLocalCLI:
		return "local-cli"
	default:
		return ""
	}
}
