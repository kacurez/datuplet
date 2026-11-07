package preview

import (
	"context"
	"errors"

	"github.com/datuplet/datuplet/pkg/pipelineapi/auth"
	"github.com/datuplet/datuplet/pkg/pipelineapi/queryproxy"
)

const sampleMaxBytes = 1 << 20

// NewQuerySampler samples through the query worker as the ctx user, like the
// storage preview handler. A nil core (query worker off) yields a nil Sampler.
func NewQuerySampler(core *queryproxy.Core) Sampler {
	if core == nil {
		return nil
	}
	return &querySampler{core: core}
}

type querySampler struct{ core *queryproxy.Core }

// Sample returns a *queryproxy.QueryError when the query worker refuses.
func (q *querySampler) Sample(ctx context.Context, qualifiedWarehouse, ns, table string, variantCols []string, rows int) (*Sample, error) {
	u, ok := auth.UserFromContext(ctx)
	if !ok {
		return nil, errors.New("no authenticated user")
	}
	res, qerr := q.core.Preview(ctx, u.ID.String(), qualifiedWarehouse, ns, table, variantCols,
		queryproxy.PreviewLimits{TimeoutS: 30, MaxRows: rows, MaxBytes: sampleMaxBytes})
	if qerr != nil {
		return nil, qerr
	}
	s := &Sample{Columns: make([]SampleColumn, len(res.Schema)), Rows: res.Rows, Truncated: res.Truncated}
	for i, c := range res.Schema {
		s.Columns[i] = SampleColumn{Name: c.Name, Type: c.Type}
	}
	if s.Rows == nil {
		s.Rows = [][]any{}
	}
	return s, nil
}
