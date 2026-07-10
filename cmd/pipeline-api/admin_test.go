package main

import (
	"context"
	"testing"

	"github.com/datuplet/datuplet/pkg/pipelineapi/authz"
	"github.com/datuplet/datuplet/pkg/pipelineapi/authz/authztest"
)

// TestValidateGrantFlags covers the --superadmin / --role / --project / --user
// flag combinations. --superadmin targets the server object, so it must reject
// a project role or an explicit project.
func TestValidateGrantFlags(t *testing.T) {
	tests := []struct {
		name       string
		email      string
		project    string
		role       string
		superadmin bool
		wantErr    bool
	}{
		{name: "project grant happy", email: "a@b.c", project: "proj", role: "editor"},
		{name: "project grant missing project", email: "a@b.c", role: "editor", wantErr: true},
		{name: "project grant missing user", project: "proj", role: "editor", wantErr: true},
		{name: "superadmin happy (default role, no project)", email: "a@b.c", role: "editor", superadmin: true},
		{name: "superadmin missing user", role: "editor", superadmin: true, wantErr: true},
		{name: "superadmin with non-default role", email: "a@b.c", role: "admin", superadmin: true, wantErr: true},
		{name: "superadmin with project", email: "a@b.c", project: "proj", role: "editor", superadmin: true, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateGrantFlags(tt.email, tt.project, tt.role, tt.superadmin)
			if (err != nil) != tt.wantErr {
				t.Fatalf("validateGrantFlags(%q,%q,%q,%v) err=%v, wantErr=%v",
					tt.email, tt.project, tt.role, tt.superadmin, err, tt.wantErr)
			}
		})
	}
}

// TestWriteSuperadminTuple asserts the written tuple is
// (user:oidc~<uuid>, admin, server:<uuid>) and that the authorizer captured it.
// discoverServerObjectFn is stubbed so no live FGA /changes call is made.
func TestWriteSuperadminTuple(t *testing.T) {
	const serverWire = "server:11111111-2222-3333-4444-555555555555"
	const userUUID = "99999999-8888-7777-6666-555555555555"

	orig := discoverServerObjectFn
	t.Cleanup(func() { discoverServerObjectFn = orig })
	discoverServerObjectFn = func(_ context.Context, _, _, _ string) (string, error) {
		return serverWire, nil
	}

	fake := authztest.New()
	tuple, err := writeSuperadminTuple(context.Background(), fake, "http://fga", "", "store-uuid", userUUID)
	if err != nil {
		t.Fatalf("writeSuperadminTuple: %v", err)
	}

	wantUser := authz.UserObject(userUUID).String() // user:oidc~<uuid>
	if tuple.User != wantUser {
		t.Errorf("tuple.User = %q, want %q", tuple.User, wantUser)
	}
	if tuple.Relation != "admin" {
		t.Errorf("tuple.Relation = %q, want %q", tuple.Relation, "admin")
	}
	if tuple.Object.String() != serverWire {
		t.Errorf("tuple.Object = %q, want %q", tuple.Object.String(), serverWire)
	}

	// The fake authorizer captured the write.
	serverObj, err := authz.ParseObject(serverWire)
	if err != nil {
		t.Fatalf("ParseObject: %v", err)
	}
	ok, err := fake.Check(context.Background(), wantUser, "admin", serverObj)
	if err != nil || !ok {
		t.Errorf("fake did not capture the superadmin tuple: ok=%v err=%v", ok, err)
	}
}
