//go:build integration

package goplane

import (
	"context"
	"io/fs"
	"testing"
	"time"

	"github.com/apache/iceberg-go/catalog"
	iceio "github.com/apache/iceberg-go/io"

	"github.com/datuplet/datuplet/pkg/catalogwriter"
)

// TestVendedCreds_ListAndDelete is the gate for iceberg-maintenance: the
// credentials Lakekeeper vends for a table must allow listing and deleting
// objects under the table location.
func TestVendedCreds_ListAndDelete(t *testing.T) {
	fx := sharedFixture(t)
	p := New(Config{})
	defer p.Close()
	r := resolver(t, fx)
	sch, inputs := allTypesSchema(t)
	const ns, tbl = "gw_it", "maint_creds"
	target := createTable(t, r, ns, tbl, sch)

	// Written but never committed: an orphan.
	files := writeRun(t, p, target, "part-orphan00001-w1", sch, inputs, 1, target.VendedCreds, 0)
	if len(files) == 0 {
		t.Fatal("no file written")
	}
	orphan := files[0].Path

	ctx := context.Background()
	cat, err := r.Catalog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	tb, err := cat.LoadTable(ctx, catalog.ToIdentifier(ns, tbl))
	if err != nil {
		t.Fatal(err)
	}
	// Which kind of credential did Lakekeeper vend: STS (session token) or static?
	vc, err := target.VendedCreds.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if s3c, ok := vc.(catalogwriter.S3Creds); ok {
		t.Logf("vended S3 creds: session token present=%v (true = STS), key=%.4s..., expires=%s",
			s3c.SessionToken != "", s3c.AccessKeyID, s3c.Expires.Format(time.RFC3339))
	} else {
		t.Logf("vended creds type %T", vc)
	}

	fsys, err := tb.FS(ctx)
	if err != nil {
		t.Fatal(err)
	}
	lio, ok := fsys.(iceio.ListableIO)
	if !ok {
		t.Fatalf("%T is not iceio.ListableIO", fsys)
	}

	walk := func() map[string]bool {
		seen := map[string]bool{}
		err := lio.WalkDir(tb.Location(), func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !d.IsDir() {
				seen[path] = true
			}
			return nil
		})
		if err != nil {
			t.Fatalf("WalkDir(%s): %v", tb.Location(), err)
		}
		return seen
	}

	before := walk()
	if !before[orphan] {
		t.Fatalf("WalkDir(%s) missing %s; got %v", tb.Location(), orphan, before)
	}
	if err := fsys.Remove(orphan); err != nil {
		t.Fatalf("Remove(%s): %v", orphan, err)
	}
	if after := walk(); after[orphan] {
		t.Fatalf("%s still listed after Remove", orphan)
	}
}
