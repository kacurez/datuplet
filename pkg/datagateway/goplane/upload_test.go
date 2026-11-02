package goplane

import (
	"context"
	"io"
	"sync"
	"testing"

	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/io/gocloud/blobfs"
	"gocloud.dev/blob"
	"gocloud.dev/blob/driver"
	"gocloud.dev/gcerrors"

	"github.com/datuplet/datuplet/pkg/datagateway"
)

// spyBucket is a gocloud blob driver that records the options of each
// writer it is asked for and accepts the writes.
type spyBucket struct {
	driver.Bucket // unused methods panic
	mu            sync.Mutex
	opts          []driver.WriterOptions
}

func (b *spyBucket) NewTypedWriter(_ context.Context, _ string, _ string, opts *driver.WriterOptions) (driver.Writer, error) {
	b.mu.Lock()
	b.opts = append(b.opts, *opts)
	b.mu.Unlock()
	if opts.BeforeWrite != nil {
		if err := opts.BeforeWrite(func(any) bool { return false }); err != nil {
			return nil, err
		}
	}
	return nopDriverWriter{}, nil
}

func (b *spyBucket) ErrorCode(error) gcerrors.ErrorCode { return gcerrors.Unknown }
func (b *spyBucket) Close() error                       { return nil }

type nopDriverWriter struct{}

func (nopDriverWriter) Write(p []byte) (int, error) { return len(p), nil }
func (nopDriverWriter) Close() error                { return nil }

func TestPartitionedUploadUsesSmallParts(t *testing.T) {
	// The writer: partitioned files ask for 5 MiB x 1, unpartitioned for the
	// IO's defaults.
	spec := nameSpec(t)
	for _, tc := range []struct {
		name string
		ws   datagateway.WriterSpec
		want *blob.WriterOptions
	}{
		{"partitioned", datagateway.WriterSpec{DataPrefix: "file:///unused/", PartitionSpec: &spec, TableSchema: readSchema},
			&blob.WriterOptions{BufferSize: 5 << 20, MaxConcurrency: 1}},
		{"unpartitioned", datagateway.WriterSpec{DataPrefix: "file:///unused/"}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := New(Config{})
			defer p.Close()
			var mu sync.Mutex
			var got []*blob.WriterOptions
			p.openSink = func(_ context.Context, _ map[string]string, _ string, opts *blob.WriterOptions) (io.WriteCloser, error) {
				mu.Lock()
				got = append(got, opts)
				mu.Unlock()
				return nopSink{}, nil
			}
			w := openWriter(t, p, tc.ws)
			write(t, w, nameBatch(0, "a", "b"))
			if _, err := w.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			defer mu.Unlock()
			if len(got) == 0 {
				t.Fatal("no file opened")
			}
			for _, o := range got {
				if (o == nil) != (tc.want == nil) || (o != nil && (o.BufferSize != tc.want.BufferSize || o.MaxConcurrency != tc.want.MaxConcurrency)) {
					t.Errorf("upload options %+v, want %+v", o, tc.want)
				}
			}
		})
	}

	// The opener: an S3 path on a gocloud blob IO passes the options to the
	// bucket; nil options, or a non-S3 path (gs://), keep the defaults.
	for _, tc := range []struct {
		name      string
		path      string
		opts      *blob.WriterOptions
		buf, conc int
	}{
		{"s3 small parts", "s3://bkt/t/data/f.parquet", partitionUploadOptions(), 5 << 20, 1},
		{"s3 defaults", "s3://bkt/t/data/f.parquet", nil, 0, 0},
		{"gcs defaults", "gs://bkt/t/data/f.parquet", partitionUploadOptions(), 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spy := &spyBucket{}
			ctx := context.Background()
			var fsys iceio.IO = blobfs.New(ctx, blob.NewBucket(spy), blobfs.DefaultObjectLocationExtractor("bkt", "s3", "gs"))
			wc, err := createUpload(ctx, fsys, tc.path, tc.opts)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := wc.Write([]byte("PAR1")); err != nil {
				t.Fatal(err)
			}
			if err := wc.Close(); err != nil {
				t.Fatal(err)
			}
			if len(spy.opts) != 1 {
				t.Fatalf("bucket writers = %d, want 1", len(spy.opts))
			}
			if o := spy.opts[0]; o.BufferSize != tc.buf || o.MaxConcurrency != tc.conc {
				t.Errorf("bucket got BufferSize %d, MaxConcurrency %d; want %d, %d", o.BufferSize, o.MaxConcurrency, tc.buf, tc.conc)
			}
		})
	}
}

type nopSink struct{}

func (nopSink) Write(p []byte) (int, error) { return len(p), nil }
func (nopSink) Close() error                { return nil }
