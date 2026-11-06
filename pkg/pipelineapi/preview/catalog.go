package preview

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	iceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	icebergtable "github.com/apache/iceberg-go/table"

	"github.com/datuplet/datuplet/pkg/catalogwriter"
)

// Catalog is the slice of the Iceberg catalog previews need. Missing tables
// and namespaces are not errors.
type Catalog interface {
	ListTables(ctx context.Context, ns string) ([]string, error) // missing namespace → nil, nil
	LoadTable(ctx context.Context, ns, table string) (*TableInfo, error)
	PurgeTable(ctx context.Context, ns, table string) error // not found → nil
	DropNamespace(ctx context.Context, ns string) error     // not found → nil
}

// TableInfo summarises a scratch table.
type TableInfo struct {
	Columns        []Column
	TotalRecords   int64
	VariantColumns []string
}

// Column is one top-level field; Type is the Iceberg type string.
type Column struct {
	Name, Type string
	Nullable   bool
}

// CatalogFactory opens a Catalog for a lakekeeper project + warehouse.
type CatalogFactory func(ctx context.Context, tp catalogwriter.TokenProvider, lakekeeperProjectID, warehouse string) (Catalog, error)

// Sampler reads the first rows of a scratch table. qualifiedWarehouse is
// lakekeeper_project_id + "/" + warehouse.
type Sampler interface {
	Sample(ctx context.Context, qualifiedWarehouse, ns, table string, variantCols []string, rows int) (*Sample, error)
}

// Sample is a table's first rows.
type Sample struct {
	Columns   []SampleColumn `json:"columns"`
	Rows      [][]any        `json:"rows"`
	Truncated bool           `json:"truncated"`
}

type SampleColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// LogTailer returns the last lines of a Job container's log.
type LogTailer interface {
	Tail(ctx context.Context, namespace, jobName, container string, lines int64) (string, error)
}

// NewRESTCatalogFactory opens lakekeeper REST catalogs at lakekeeperURL.
func NewRESTCatalogFactory(lakekeeperURL string) CatalogFactory {
	return func(ctx context.Context, tp catalogwriter.TokenProvider, lk, warehouse string) (Catalog, error) {
		cli, err := catalogwriter.NewClient(ctx, catalogwriter.ClientConfig{
			Name:          "datuplet-pipeline-api-preview",
			URI:           lakekeeperURL,
			Warehouse:     warehouse,
			ProjectID:     lk,
			TokenProvider: tp,
		})
		if err != nil {
			return nil, err
		}
		return &restCatalog{cli: cli}, nil
	}
}

type restCatalog struct{ cli *catalogwriter.Client }

func (r *restCatalog) ListTables(ctx context.Context, ns string) ([]string, error) {
	var out []string
	for ident, err := range r.cli.Catalog.ListTables(ctx, icebergtable.Identifier{ns}) {
		if errors.Is(err, catalog.ErrNoSuchNamespace) {
			return nil, nil
		}
		if err != nil {
			return nil, fmt.Errorf("list tables in %s: %w", ns, err)
		}
		out = append(out, ident[len(ident)-1])
	}
	return out, nil
}

func (r *restCatalog) LoadTable(ctx context.Context, ns, table string) (*TableInfo, error) {
	tbl, err := r.cli.Catalog.LoadTable(ctx, icebergtable.Identifier{ns, table})
	if err != nil {
		return nil, fmt.Errorf("load %s.%s: %w", ns, table, err)
	}
	info := &TableInfo{}
	for _, f := range tbl.Schema().Fields() {
		info.Columns = append(info.Columns, Column{Name: f.Name, Type: f.Type.String(), Nullable: !f.Required})
		if _, ok := f.Type.(iceberg.VariantType); ok {
			info.VariantColumns = append(info.VariantColumns, f.Name)
		}
	}
	if snap := tbl.CurrentSnapshot(); snap != nil && snap.Summary != nil {
		if n, err := strconv.ParseInt(snap.Summary.Properties["total-records"], 10, 64); err == nil {
			info.TotalRecords = n
		}
	}
	return info, nil
}

func (r *restCatalog) PurgeTable(ctx context.Context, ns, table string) error {
	err := r.cli.PurgeTable(ctx, []string{ns, table})
	if err == nil || errors.Is(err, catalog.ErrNoSuchTable) || errors.Is(err, catalog.ErrNoSuchNamespace) {
		return nil
	}
	return fmt.Errorf("purge %s.%s: %w", ns, table, err)
}

func (r *restCatalog) DropNamespace(ctx context.Context, ns string) error {
	err := r.cli.DropNamespace(ctx, []string{ns})
	if err == nil || errors.Is(err, catalog.ErrNoSuchNamespace) {
		return nil
	}
	return fmt.Errorf("drop namespace %s: %w", ns, err)
}
