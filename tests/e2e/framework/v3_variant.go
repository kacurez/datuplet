// Package framework — helpers for the v3 + VARIANT scenario: a merge-on-read
// delete issued from the host, and a gateway read issued through a live run's
// Data Gateway sidecar.
package framework

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	icebergio "github.com/apache/iceberg-go/io"
	icebergtable "github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/datuplet/datuplet/pkg/catalogwriter"
	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
)

// e2eWarehouseRegion is the region lakekeeper's manager gives the e2e
// warehouse (pkg/pipelineapi/lakekeeper S3WarehouseProfile default).
const e2eWarehouseRegion = "local-01"

// hostIOCatalog loads tables with IO against the host-reachable MinIO
// endpoint: lakekeeper vends the in-cluster endpoint, which the host cannot
// reach. Commits go to lakekeeper unchanged.
type hostIOCatalog struct {
	cat catalog.Catalog
	fsF icebergtable.FSysF
}

func (c hostIOCatalog) LoadTable(ctx context.Context, ident icebergtable.Identifier) (*icebergtable.Table, error) {
	t, err := c.cat.LoadTable(ctx, ident)
	if err != nil {
		return nil, err
	}
	return icebergtable.New(ident, t.Metadata(), t.MetadataLocation(), c.fsF, c), nil
}

func (c hostIOCatalog) CommitTable(ctx context.Context, ident icebergtable.Identifier, reqs []icebergtable.Requirement, updates []icebergtable.Update) (icebergtable.Metadata, string, error) {
	return c.cat.CommitTable(ctx, ident, reqs, updates)
}

// hostIOLoader returns a function that loads (namespace, table) from
// lakekeeper as alice, with IO against the host-reachable MinIO endpoint.
func hostIOLoader(ctx context.Context, h *FGAHarness, clientName, namespace, table string) (func() (*icebergtable.Table, error), error) {
	cli, err := catalogwriter.NewClient(ctx, catalogwriter.ClientConfig{
		Name:      clientName,
		URI:       h.CatalogURI(),
		Warehouse: h.WarehouseName,
		ProjectID: h.LakekeeperProjectID,
		TokenProvider: func(ctx context.Context) (string, error) {
			tok, err := MintTestUserImpersonation(ctx, h.Signer, AliceID)
			if err != nil {
				return "", err
			}
			return tok.Reveal(), nil
		},
	})
	if err != nil {
		return nil, fmt.Errorf("open lakekeeper catalog: %w", err)
	}
	props := map[string]string{
		icebergio.S3EndpointURL:            "http://" + minioLocalAddress,
		icebergio.S3AccessKeyID:            minioAccessKey,
		icebergio.S3SecretAccessKey:        minioSecretKey,
		icebergio.S3Region:                 e2eWarehouseRegion,
		icebergio.S3ForceVirtualAddressing: "false",
	}
	cat := hostIOCatalog{cat: cli.Catalog}
	ident := catalog.ToIdentifier(namespace, table)
	return func() (*icebergtable.Table, error) {
		t, err := cli.Catalog.LoadTable(ctx, ident)
		if err != nil {
			return nil, fmt.Errorf("LoadTable %s.%s: %w", namespace, table, err)
		}
		cat.fsF = icebergio.LoadFSFunc(props, t.Location())
		return cat.LoadTable(ctx, ident)
	}, nil
}

// DeleteResult describes a table's scan plan after a delete.
type DeleteResult struct {
	DataFiles       int // data files in the current snapshot
	FilesWithDV     int // data files carrying a deletion vector
	FilesWithDelete int // data files carrying any other delete file
}

// DeleteRowsMergeOnRead switches (namespace, table) to merge-on-read and
// deletes the rows matching filter with iceberg-go, as alice. On a v3 table
// this writes a deletion vector on each affected data file. It returns the
// scan plan's file counts after the delete.
func DeleteRowsMergeOnRead(ctx context.Context, h *FGAHarness, namespace, table string, filter iceberg.BooleanExpression) (DeleteResult, error) {
	load, err := hostIOLoader(ctx, h, "datuplet-e2e-delete", namespace, table)
	if err != nil {
		return DeleteResult{}, err
	}

	tbl, err := load()
	if err != nil {
		return DeleteResult{}, err
	}
	txn := tbl.NewTransaction()
	if err := txn.SetProperties(iceberg.Properties{icebergtable.WriteDeleteModeKey: icebergtable.WriteModeMergeOnRead}); err != nil {
		return DeleteResult{}, fmt.Errorf("set %s: %w", icebergtable.WriteDeleteModeKey, err)
	}
	if _, err := txn.Commit(ctx); err != nil {
		return DeleteResult{}, fmt.Errorf("commit %s: %w", icebergtable.WriteDeleteModeKey, err)
	}

	if tbl, err = load(); err != nil {
		return DeleteResult{}, err
	}
	txn = tbl.NewTransaction()
	if err := txn.Delete(ctx, filter, nil); err != nil {
		return DeleteResult{}, fmt.Errorf("delete: %w", err)
	}
	if _, err := txn.Commit(ctx); err != nil {
		return DeleteResult{}, fmt.Errorf("commit delete: %w", err)
	}

	if tbl, err = load(); err != nil {
		return DeleteResult{}, err
	}
	tasks, err := tbl.Scan().PlanFiles(ctx)
	if err != nil {
		return DeleteResult{}, fmt.Errorf("plan files: %w", err)
	}
	res := DeleteResult{DataFiles: len(tasks)}
	for _, task := range tasks {
		if len(task.DeletionVectorFiles) > 0 {
			res.FilesWithDV++
		}
		if len(task.DeleteFiles) > 0 || len(task.EqualityDeleteFiles) > 0 {
			res.FilesWithDelete++
		}
	}
	return res, nil
}

// GatewayRead is what ReadThroughRunGateway got back.
type GatewayRead struct {
	Chunks  [][]byte // one entry per DataChunk
	Summary string   // the gateway's "read summary:" log line for the read
}

// ReadThroughRunGateway runs holderPipeline (a rendered PipelineDoc whose only
// component is named holderComponent and runs long enough to be attached to),
// port-forwards to that run's Data Gateway sidecar, and reads (bucket, table)
// in format over gRPC, the way a component's SDK would. The run's identity is
// an editor on the project, so the sidecar can read any project table. It
// waits for the holder run to finish and fails if it did not succeed.
func ReadThroughRunGateway(ctx context.Context, kb *K8sBackend, holderPipeline, holderComponent, bucket, table string, format pb.DataFormat) (*GatewayRead, error) {
	type outcome struct {
		res *RunResult
		err error
	}
	done := make(chan outcome, 1)
	runIDCh := make(chan uuid.UUID, 1)
	go func() {
		res, err := kb.RunPipeline(ctx, holderPipeline, RunOpts{
			StorageType: "s3",
			Timeout:     5 * time.Minute,
			OnRunID:     func(id uuid.UUID) { runIDCh <- id },
		})
		done <- outcome{res, err}
	}()

	var runID uuid.UUID
	select {
	case runID = <-runIDCh:
	case oc := <-done:
		if oc.err != nil {
			return nil, fmt.Errorf("holder run: %w", oc.err)
		}
		return nil, errors.New("holder run finished before its run id was observed")
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	read, readErr := readThroughPod(ctx, kb.Namespace, runID, holderComponent, bucket, table, format)

	select {
	case oc := <-done:
		if oc.err != nil {
			return read, errors.Join(readErr, fmt.Errorf("holder run: %w", oc.err))
		}
		if !oc.res.Success {
			return read, errors.Join(readErr, fmt.Errorf("holder run ended %s (exit %d): %s", oc.res.FailureType, oc.res.ExitCode, oc.res.StatusMessage))
		}
	case <-ctx.Done():
		return read, errors.Join(readErr, ctx.Err())
	}
	return read, readErr
}

func readThroughPod(ctx context.Context, ns string, runID uuid.UUID, component, bucket, table string, format pb.DataFormat) (*GatewayRead, error) {
	pod, err := waitRunningPod(ctx, ns, "datuplet.io/run-id="+runID.String()+",datuplet.io/component="+component, 3*time.Minute)
	if err != nil {
		return nil, err
	}

	port, err := freeLocalPort()
	if err != nil {
		return nil, err
	}
	addr := "127.0.0.1:" + strconv.Itoa(port)
	pf := exec.CommandContext(ctx, "kubectl", "port-forward", "-n", ns, "pod/"+pod, strconv.Itoa(port)+":50051")
	pf.Stdout, pf.Stderr = io.Discard, os.Stderr
	if err := pf.Start(); err != nil {
		return nil, fmt.Errorf("kubectl port-forward: %w", err)
	}
	defer func() {
		_ = pf.Process.Kill()
		_ = pf.Wait()
	}()
	if err := waitDial(ctx, addr, 30*time.Second); err != nil {
		return nil, fmt.Errorf("port-forward to %s/%s: %w", ns, pod, err)
	}

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64<<20)))
	if err != nil {
		return nil, fmt.Errorf("gRPC client: %w", err)
	}
	defer conn.Close()
	gw := pb.NewDataGatewayClient(conn)

	// The sidecar holds calls until boot ends; retry briefly in case the
	// port-forward came up before the listener did.
	var open *pb.OpenReaderResponse
	deadline := time.Now().Add(60 * time.Second)
	for {
		callCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		open, err = gw.OpenReader(callCtx, &pb.OpenReaderRequest{Bucket: bucket, Table: table, OutputFormat: format})
		cancel()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("OpenReader %s.%s: %w", bucket, table, err)
		}
		time.Sleep(2 * time.Second)
	}

	read := &GatewayRead{}
	stream, err := gw.ReadChunk(ctx, &pb.ReadChunkRequest{ReaderId: open.ReaderId})
	if err != nil {
		return nil, fmt.Errorf("ReadChunk: %w", err)
	}
	for {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("ReadChunk recv: %w", err)
		}
		read.Chunks = append(read.Chunks, chunk.Data)
		if chunk.IsLast {
			break
		}
	}
	if _, err := gw.CloseReader(ctx, &pb.CloseReaderRequest{ReaderId: open.ReaderId}); err != nil {
		return nil, fmt.Errorf("CloseReader: %w", err)
	}

	logs, err := exec.CommandContext(ctx, "kubectl", "logs", "-n", ns, pod, "-c", "gateway").Output()
	if err != nil {
		return read, fmt.Errorf("gateway logs of %s/%s: %w", ns, pod, err)
	}
	read.Summary = FindReadSummary(string(logs), bucket, table)
	return read, nil
}

func waitRunningPod(ctx context.Context, ns, selector string, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	for {
		out, err := exec.CommandContext(ctx, "kubectl", "get", "pods", "-n", ns, "-l", selector,
			"-o", "jsonpath={range .items[*]}{.metadata.name} {.status.phase}{\"\\n\"}{end}").Output()
		if err == nil {
			for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
				if name, phase, ok := strings.Cut(line, " "); ok && phase == "Running" {
					return name, nil
				}
			}
		}
		if time.Now().After(deadline) {
			return "", fmt.Errorf("no Running pod matching %q in %s after %s", selector, ns, timeout)
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

func freeLocalPort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("pick a local port: %w", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

func waitDial(ctx context.Context, addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			return conn.Close()
		}
		if time.Now().After(deadline) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// FindReadSummary returns the last gateway "read summary:" line for
// bucket.table in logs, or "" when there is none.
func FindReadSummary(logs, bucket, table string) string {
	var last string
	for _, line := range strings.Split(logs, "\n") {
		if i := strings.Index(line, "read summary: table="+bucket+"."+table+" "); i >= 0 {
			last = line[i:]
		}
	}
	return last
}

var summaryField = regexp.MustCompile(`\b(format|raw_files|decoded_files|rows|outcome)=(\S+)`)

// ParseReadSummary returns the format, raw_files, decoded_files, rows and
// outcome fields of a read summary line.
func ParseReadSummary(line string) map[string]string {
	out := map[string]string{}
	for _, m := range summaryField.FindAllStringSubmatch(line, -1) {
		out[m[1]] = m[2]
	}
	return out
}
