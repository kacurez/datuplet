package projectgatetest

import (
	"context"

	"github.com/google/uuid"

	"github.com/datuplet/datuplet/pkg/pipelineapi/authz"
	"github.com/datuplet/datuplet/pkg/pipelineapi/projectgate"
)

// FakeAuthorizer implements only Check; the interface embed satisfies the
// other methods (BatchCheck etc. — they panic if called, fine for tests).
// NOTE the real signature takes authz.Object, not string
// (pkg/pipelineapi/authz/authorizer.go:22).
type FakeAuthorizer struct {
	authz.Authorizer
	Allow bool
	Err   error
}

func (f FakeAuthorizer) Check(_ context.Context, _, _ string, _ authz.Object) (bool, error) {
	return f.Allow, f.Err
}

// AllowAll returns a Gate that authorizes any caller for any project and
// resolves the given lakekeeper project + bare warehouse name.
func AllowAll(lkPID, warehouse string) *projectgate.Gate {
	return &projectgate.Gate{
		LakekeeperProjectIDFor: func(_ context.Context, _ uuid.UUID) (string, error) { return lkPID, nil },
		Authorizer:             FakeAuthorizer{Allow: true},
		WarehouseFor:           func(_ context.Context, _ string) (string, error) { return warehouse, nil },
	}
}
