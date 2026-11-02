//go:build integration

package goplane

import (
	"context"
	"crypto/rand"
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go/catalog"
	"github.com/minio/minio-go/v7"

	"github.com/datuplet/datuplet/pkg/datagateway"
	"github.com/datuplet/datuplet/pkg/datagateway/lakekeeper"
	dgschema "github.com/datuplet/datuplet/pkg/datagateway/schema"
	"github.com/datuplet/datuplet/pkg/icebergjob"
)

// TestIntegrationPartitionedUploadSmallParts writes ~16 MiB of incompressible
// data into one partition: silo accepts the 5 MiB multipart parts (the
// object's ETag counts ceil(size / 5 MiB) parts, not the 8 MiB default's),
// and the file commits and reads back.
func TestIntegrationPartitionedUploadSmallParts(t *testing.T) {
	fx := sharedFixture(t)
	p := New(Config{})
	defer p.Close()
	r := resolver(t, fx)
	sch := arrow.NewSchema([]arrow.Field{
		{Name: "k", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "blob", Type: arrow.BinaryTypes.Binary, Nullable: true},
	}, nil)
	const ns, tbl = "gw_it", "small_parts"
	gw, err := dgschema.NewSchemaFromArrow(sch)
	if err != nil {
		t.Fatal(err)
	}
	target, err := r.LoadOrCreateForWrite(context.Background(), ns, tbl,
		func(context.Context) (*dgschema.Schema, error) { return gw, nil },
		[]lakekeeper.PartitionField{{SourceColumn: "k", Transform: "identity"}})
	if err != nil {
		t.Fatalf("LoadOrCreateForWrite: %v", err)
	}
	w, err := p.OpenTableWriter(context.Background(), datagateway.WriterSpec{
		DataPrefix: target.BasePath, FilePrefix: "part-small-w1", Schema: sch, Creds: target.VendedCreds,
		Table: ns + "." + tbl, PartitionSpec: &target.Spec, TableSchema: target.Schema,
		RowGroupRows: 1024, // 4 MiB row groups, streamed as they fill
	})
	if err != nil {
		t.Fatal(err)
	}
	const batches, rows = 4, 1024
	mem := memory.NewGoAllocator()
	val := make([]byte, 4096)
	for b := 0; b < batches; b++ {
		kb := array.NewStringBuilder(mem)
		bb := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
		for i := 0; i < rows; i++ {
			kb.Append("a")
			_, _ = rand.Read(val)
			bb.Append(val)
		}
		ka, ba := kb.NewArray(), bb.NewArray()
		kb.Release()
		bb.Release()
		rec := array.NewRecordBatch(sch, []arrow.Array{ka, ba}, rows)
		ka.Release()
		ba.Release()
		err := w.Write(context.Background(), rec)
		rec.Release()
		if err != nil {
			w.Abort()
			t.Fatalf("Write: %v", err)
		}
	}
	files, err := w.Close(context.Background())
	if err != nil {
		t.Fatalf("Close: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("files = %d, want 1", len(files))
	}

	key := strings.TrimPrefix(files[0].Path, "s3://"+siloBucket+"/")
	info, err := fx.siloCore(t).StatObject(context.Background(), siloBucket, key, minio.StatObjectOptions{})
	if err != nil {
		t.Fatalf("StatObject: %v", err)
	}
	const part = 5 << 20
	want := (info.Size + part - 1) / part
	if !strings.HasSuffix(strings.Trim(info.ETag, `"`), fmt.Sprintf("-%d", want)) {
		t.Fatalf("object of %d bytes has ETag %q, want a %d-part multipart upload (5 MiB parts)", info.Size, info.ETag, want)
	}
	t.Logf("object %d bytes, ETag %s", info.Size, info.ETag)

	cat, err := r.Catalog(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	part2 := &icebergjob.PartitionedFiles{SpecID: files[0].SpecID, Tuples: []map[int]any{files[0].Partition}}
	if _, err := icebergjob.CommitTableFiles(context.Background(), cat, catalog.ToIdentifier(ns, tbl), paths(files), part2,
		icebergjob.WriteModeAppend, nil, "small-parts-1"); err != nil {
		t.Fatalf("CommitTableFiles: %v", err)
	}
	if n, _ := scanRows(t, r, ns, tbl); n != batches*rows {
		t.Fatalf("committed %d rows, want %d", n, batches*rows)
	}
}
