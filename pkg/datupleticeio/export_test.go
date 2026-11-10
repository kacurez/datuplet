package datupleticeio

import (
	"context"

	iceio "github.com/apache/iceberg-go/io"
	"gocloud.dev/blob"
)

// NewGCSIOForTest exposes gcsIO over any gocloud bucket to the external test
// package (which may import icebergjob without an import cycle).
func NewGCSIOForTest(b *blob.Bucket, bucketName string) iceio.IO {
	return &gcsIO{bucket: b, bucketName: bucketName, ctx: context.Background()}
}
