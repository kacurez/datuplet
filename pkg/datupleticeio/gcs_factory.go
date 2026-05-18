package datupleticeio

import (
	"context"
	"fmt"
	"net/url"

	iceio "github.com/apache/iceberg-go/io"
)

// datupletGCSFactory is the iceberg-go SchemeFactory we register for `gs`.
// D.1 stub: fails on missing token. D.2 fills in the full factory body.
//
// Signature per real iceberg-go API (verified by Slice A0 probe 1):
//
//	func(ctx, parsed *url.URL, props map[string]string) (IO, error)
func datupletGCSFactory(ctx context.Context, parsed *url.URL, props map[string]string) (iceio.IO, error) {
	tok := props["gcs.oauth2.token"]
	if tok == "" {
		return nil, fmt.Errorf("datupletGCSFactory: missing gcs.oauth2.token in props for %q", parsed.String())
	}
	// D.1: stub — real implementation lands in D.2.
	return nil, fmt.Errorf("datupletGCSFactory: NOT YET IMPLEMENTED (Slice D.2)")
}
