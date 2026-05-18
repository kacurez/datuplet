package datupleticeio

import (
	// Blank-import the upstream gocloud subpackage so its init() runs
	// first (registering the default gs:// + s3:// factories). We then
	// unregister + re-register `gs` with our own implementation.
	_ "github.com/apache/iceberg-go/io/gocloud"

	iceio "github.com/apache/iceberg-go/io"
)

func init() {
	// iceio.Register panics on duplicate registration (verified against
	// pinned iceberg-go in RFC 019 §4.5.1, and confirmed by Slice A0
	// probe 1). Unregister first to make the override deterministic
	// regardless of import order.
	iceio.Unregister("gs")
	iceio.Register("gs", datupletGCSFactory)
}
