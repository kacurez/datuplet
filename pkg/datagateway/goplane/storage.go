package goplane

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"strconv"
	"strings"
	"time"

	iceio "github.com/apache/iceberg-go/io"
	icebergs3 "github.com/apache/iceberg-go/io/gocloud/s3"
	icebergutils "github.com/apache/iceberg-go/utils"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/middleware"

	"github.com/datuplet/datuplet/pkg/catalogwriter"
	"github.com/datuplet/datuplet/pkg/datagateway"
)

// minCredsValidity is the floor of the validity a file needs at its start
// (spec, Write path 7).
const minCredsValidity = 2 * time.Minute

var errCredsExpireTooSoon = errors.New("goplane: vended credentials expire too soon")

// sinkOpener opens the upload sink of one data file. ctx is the file's own
// upload context: cancelling it before Close aborts the upload.
type sinkOpener func(ctx context.Context, props map[string]string, path string) (io.WriteCloser, error)

// openIOSink loads the iceberg-go IO for path with ctx (gocloud blob writers
// keep ctx and abort the upload when it is cancelled before Close) and
// creates the file.
//
// For S3 the AWS config is built here (iceberg-go's own ParseAWSConfig, read
// back from ctx by its S3 factory) to add one middleware: the S3 transfer
// manager aborts a failed multipart upload with the upload's context, which
// is already cancelled when the writer aborts on purpose, so its
// AbortMultipartUpload would fail and leave the upload open. The middleware
// runs AbortMultipartUpload on an uncancelled context bounded by
// uploadAbortTimeout.
func openIOSink(ctx context.Context, props map[string]string, path string) (io.WriteCloser, error) {
	if strings.HasPrefix(path, "s3://") {
		cfg, err := icebergs3.ParseAWSConfig(ctx, props)
		if err != nil {
			return nil, fmt.Errorf("goplane: S3 config for %s: %w", path, err)
		}
		cfg.HTTPClient = nil // iceberg-go then installs its tuned S3 transport
		cfg.APIOptions = append(cfg.APIOptions, abortWithoutCancel)
		ctx = icebergutils.WithAwsConfig(ctx, cfg)
	}
	fsys, err := iceio.LoadFS(ctx, props, path)
	if err != nil {
		return nil, fmt.Errorf("goplane: load IO for %s: %w", path, err)
	}
	wfs, ok := fsys.(iceio.WriteFileIO)
	if !ok {
		return nil, fmt.Errorf("goplane: IO %T for %s is not writable", fsys, path)
	}
	fw, err := wfs.Create(path)
	if err != nil {
		return nil, fmt.Errorf("goplane: create %s: %w", path, err)
	}
	return fw, nil
}

// removeLocal deletes a partial local file; a missing file is not an error.
func removeLocal(path string) error {
	if err := (iceio.LocalFS{}).Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

// uploadAbortTimeout bounds AbortMultipartUpload once the upload context is
// cancelled.
const uploadAbortTimeout = 30 * time.Second

// abortWithoutCancel detaches AbortMultipartUpload from its caller's
// cancellation (see openIOSink).
func abortWithoutCancel(stack *middleware.Stack) error {
	return stack.Initialize.Add(middleware.InitializeMiddlewareFunc("goplaneAbortWithoutCancel",
		func(ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler) (
			middleware.InitializeOutput, middleware.Metadata, error,
		) {
			if _, ok := in.Parameters.(*s3.AbortMultipartUploadInput); ok && ctx.Err() != nil {
				actx, cancel := context.WithTimeout(context.WithoutCancel(ctx), uploadAbortTimeout)
				defer cancel()
				return next.HandleInitialize(actx, in)
			}
			return next.HandleInitialize(ctx, in)
		}), middleware.Before)
}

// validFor reports whether c stays valid long enough for one file:
// max(2 min, 25 % of its issued TTL). Credentials without an expiry pass.
func validFor(c catalogwriter.Creds, now time.Time) bool {
	exp := c.ExpiresAt()
	if exp.IsZero() {
		return true
	}
	need := minCredsValidity
	if iss := c.IssuedAt(); !iss.IsZero() {
		need = max(need, exp.Sub(iss)/4)
	}
	return exp.Sub(now) >= need
}

// currentCreds returns credentials that outlive the next file: one Get and,
// if those expire too soon, Invalidate and one more Get.
func currentCreds(ctx context.Context, src datagateway.CredsSource) (catalogwriter.Creds, error) {
	c, err := src.Get(ctx)
	if err != nil {
		return nil, fmt.Errorf("goplane: vended credentials: %w", err)
	}
	if c != nil && validFor(c, time.Now()) {
		return c, nil
	}
	src.Invalidate()
	if c, err = src.Get(ctx); err != nil {
		return nil, fmt.Errorf("goplane: vended credentials: %w", err)
	}
	if c != nil && validFor(c, time.Now()) {
		return c, nil
	}
	return nil, errCredsExpireTooSoon
}

// ioProps turns vended credentials into iceberg-go IO properties (spec, Write
// path 6). The result carries secrets: never log it or put it into an error,
// and no error returned here contains a credential value.
func ioProps(c catalogwriter.Creds) (map[string]string, error) {
	switch c := c.(type) {
	case catalogwriter.S3Creds:
		if c.AccessKeyID == "" || c.SecretAccessKey == "" {
			return nil, errors.New("goplane: S3 credentials without an access key")
		}
		region := c.Region
		if region == "" {
			region = "us-east-1"
		}
		props := map[string]string{
			iceio.S3AccessKeyID:     c.AccessKeyID,
			iceio.S3SecretAccessKey: c.SecretAccessKey,
			iceio.S3SessionToken:    c.SessionToken,
			iceio.S3Region:          region,
		}
		// No endpoint means the AWS default endpoint (an amazonaws.com host).
		virtual := true
		if c.Endpoint != "" {
			host, err := endpointHost(c.Endpoint)
			if err != nil {
				return nil, err
			}
			virtual = host == "amazonaws.com" || strings.HasSuffix(host, ".amazonaws.com")
			props[iceio.S3EndpointURL] = c.Endpoint
		}
		// Always explicit, so addressing never depends on iceberg-go's default.
		props[iceio.S3ForceVirtualAddressing] = strconv.FormatBool(virtual)
		return props, nil
	case catalogwriter.GCSCreds:
		if c.OAuthToken == "" {
			return nil, errors.New("goplane: GCS credentials without an OAuth token")
		}
		props := map[string]string{iceio.GCSOAuthToken: c.OAuthToken}
		if !c.Expires.IsZero() {
			props[iceio.GCSOAuthExpiresAt] = strconv.FormatInt(c.Expires.UnixMilli(), 10)
		}
		if c.GCPProjectID != "" {
			props["gcs.project-id"] = c.GCPProjectID
		}
		return props, nil
	case nil:
		return nil, errors.New("goplane: no vended credentials")
	default:
		return nil, fmt.Errorf("goplane: unsupported credential type %q", c.Type())
	}
}

// endpointHost returns the lower-cased host of a vended S3 endpoint
// ("scheme://host[:port]" or a bare "host[:port]").
func endpointHost(endpoint string) (string, error) {
	raw := endpoint
	if !strings.Contains(raw, "://") {
		raw = "//" + raw
	}
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" {
		return "", fmt.Errorf("goplane: invalid S3 endpoint %q", endpoint)
	}
	return strings.ToLower(strings.TrimSuffix(u.Hostname(), ".")), nil
}
