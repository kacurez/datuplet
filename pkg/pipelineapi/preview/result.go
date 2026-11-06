package preview

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	"github.com/datuplet/datuplet/pkg/pipeline/config"
	"github.com/datuplet/datuplet/pkg/pipelineapi/queryproxy"
	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
)

const (
	sampleRows         = 20
	logTailLines       = 200
	samplesUnavailable = "samples unavailable: query service disabled"
	sampleSkipPrefix   = "sample skipped for "
	outputsPurged      = "outputs purged before the result was assembled"
	// assembleTimeout bounds an assembly, which outlives the request that
	// started it so callers joined to the flight still get the document.
	assembleTimeout = 3 * time.Minute
)

// Result is the preview result document (GET /previews/{id}).
type Result struct {
	ID          uuid.UUID     `json:"id"`
	Workspace   string        `json:"workspace"`
	Phase       string        `json:"phase"`
	Message     string        `json:"message"`
	CreatedAt   time.Time     `json:"created_at"`
	StartedAt   *time.Time    `json:"started_at"`
	CompletedAt *time.Time    `json:"completed_at"`
	ExpiresAt   time.Time     `json:"expires_at"`
	RowLimit    int           `json:"row_limit"`
	UntilStage  *string       `json:"until_stage"`
	DocName     string        `json:"doc_name"`
	Stages      []StageResult `json:"stages"`
	Outputs     []Output      `json:"outputs"`
	Warnings    []string      `json:"warnings"`
	// ResultReady is true once the document is cached; clients stop polling
	// on a terminal phase plus ResultReady.
	ResultReady bool `json:"result_ready"`
}

type StageResult struct {
	Name       string            `json:"name"`
	Phase      string            `json:"phase"`
	DurationMS *int64            `json:"duration_ms"`
	Message    string            `json:"message"`
	Components []ComponentResult `json:"components"`
}

type ComponentResult struct {
	Name           string  `json:"name"`
	Phase          string  `json:"phase"`
	ExitCode       *int32  `json:"exit_code"`
	Message        string  `json:"message"`
	LogTail        *string `json:"log_tail"`
	GatewayLogTail *string `json:"gateway_log_tail"`
}

// Output is one table a preview wrote. Bucket is logical, as in the doc.
type Output struct {
	Stage        *string      `json:"stage"`
	Component    *string      `json:"component"`
	Bucket       string       `json:"bucket"`
	Table        string       `json:"table"`
	ScratchTable string       `json:"scratch_table"`
	Rows         int64        `json:"rows"`
	AtCap        bool         `json:"at_cap"`
	Schema       []Column     `json:"schema"`
	ColumnsHint  []ColumnHint `json:"columns_hint"`
	Sample       *Sample      `json:"sample"`
}

// ColumnHint is a column in the PipelineDoc `columns:` vocabulary.
type ColumnHint struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// cachedResult is the part of a Result stored in previews.result.
type cachedResult struct {
	Stages   []StageResult `json:"stages"`
	Outputs  []Output      `json:"outputs"`
	Warnings []string      `json:"warnings"`
}

var hintTypes = map[string]bool{
	"int": true, "long": true, "float": true, "double": true, "boolean": true, "string": true, "variant": true,
}

// ColumnsHint maps Iceberg column types onto PipelineDoc column types; an
// unsupported type is hinted as string with a warning.
func ColumnsHint(cols []Column) (hint []ColumnHint, warnings []string) {
	hint = make([]ColumnHint, 0, len(cols))
	for _, c := range cols {
		t := c.Type
		if !hintTypes[t] {
			t = "string"
			warnings = append(warnings, fmt.Sprintf("column %q has type %s; hinted as string — declare a supported type or keep string", c.Name, c.Type))
		}
		hint = append(hint, ColumnHint{Name: c.Name, Type: t})
	}
	return hint, warnings
}

// Get returns a preview's result document. A terminal preview's document is
// assembled once and cached; resample retries the samples that are missing.
// ctx must carry the authenticated user (catalog and query identity).
func (s *Service) Get(ctx context.Context, projectID, id uuid.UUID, resample bool) (*Result, error) {
	p, err := store.GetPreview(ctx, s.d.DB, projectID, id)
	if err != nil {
		return nil, err
	}
	if isTerminal(p.Phase) && (p.Result == nil || resample) {
		v, err, _ := s.flight.Do(id.String(), func() (any, error) {
			actx, cancel := context.WithTimeout(context.WithoutCancel(ctx), assembleTimeout)
			defer cancel()
			return s.assemble(actx, projectID, id, resample)
		})
		if err != nil {
			return nil, err
		}
		p = v.(*store.Preview)
	}
	return render(p)
}

// assemble builds and caches the document of a terminal preview, or, with
// resample, re-samples the cached outputs that have no sample.
func (s *Service) assemble(ctx context.Context, projectID, id uuid.UUID, resample bool) (*store.Preview, error) {
	p, err := store.GetPreview(ctx, s.d.DB, projectID, id)
	if err != nil {
		return nil, err
	}
	if p.Result != nil && !resample {
		return p, nil
	}
	if p.Result == nil {
		body, err := s.build(ctx, p)
		if err != nil {
			return nil, err
		}
		raw, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("encode preview result: %w", err)
		}
		if err := store.SetPreviewResult(ctx, s.d.DB, p.ID, raw); err != nil {
			return nil, err
		}
		return store.GetPreview(ctx, s.d.DB, projectID, id)
	}
	var body cachedResult
	if err := json.Unmarshal(p.Result, &body); err != nil {
		return nil, fmt.Errorf("decode preview result: %w", err)
	}
	if !s.resample(ctx, p, &body) {
		return p, nil
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("encode preview result: %w", err)
	}
	if err := store.OverwritePreviewResult(ctx, s.d.DB, p.ID, raw); err != nil {
		return nil, err
	}
	p.Result = raw
	return p, nil
}

func (s *Service) build(ctx context.Context, p *store.Preview) (*cachedResult, error) {
	body := &cachedResult{Stages: []StageResult{}, Outputs: []Output{}, Warnings: []string{}}
	stages, err := stagesOf(p.StageStatuses)
	if err != nil {
		return nil, err
	}
	for _, st := range stages {
		r := stageResult(st)
		for i, c := range st.ComponentStatuses {
			if !failed(string(c.Phase)) || c.PodName == "" || s.d.Logs == nil {
				continue
			}
			rc := &r.Components[i]
			rc.LogTail = s.tail(ctx, p, st.Name, c, "component", &body.Warnings)
			rc.GatewayLogTail = s.tail(ctx, p, st.Name, c, "gateway", &body.Warnings)
		}
		body.Stages = append(body.Stages, r)
	}

	if p.CleanedAt != nil {
		body.Warnings = append(body.Warnings, outputsPurged)
		return body, nil
	}
	doc, err := docOf(p)
	if err != nil {
		return nil, err
	}
	rw, err := Rewrite(doc, p.ID, p.UntilStage)
	if err != nil {
		return nil, fmt.Errorf("rewrite preview doc: %w", err)
	}
	if s.d.Catalogs == nil {
		return nil, errors.New("preview outputs: no catalog configured")
	}
	cat, err := s.d.Catalogs(ctx, s.d.UserCatalogToken, p.LakekeeperProjectID, p.Warehouse)
	if err != nil {
		return nil, fmt.Errorf("open catalog: %w", err)
	}
	for _, ns := range p.ScratchNamespaces {
		tables, err := cat.ListTables(ctx, ns)
		if err != nil {
			return nil, err
		}
		sort.Strings(tables)
		for _, t := range tables {
			info, err := cat.LoadTable(ctx, ns, t)
			if err != nil {
				return nil, err
			}
			bucket := rw.Logical[ns]
			if bucket == "" {
				bucket = ns
			}
			hint, warns := ColumnsHint(info.Columns)
			body.Warnings = append(body.Warnings, warns...)
			o := Output{
				Bucket: bucket, Table: t, ScratchTable: ns + "." + t,
				Rows: info.TotalRecords, AtCap: info.TotalRecords >= int64(p.RowLimit),
				Schema: info.Columns, ColumnsHint: hint,
			}
			if o.Schema == nil {
				o.Schema = []Column{}
			}
			o.Stage, o.Component = producer(rw.Doc, ns, t)
			if s.d.Sampler != nil {
				if w := s.sample(ctx, p, &o, info.VariantColumns); w != "" {
					body.Warnings = append(body.Warnings, w)
				}
			}
			body.Outputs = append(body.Outputs, o)
		}
	}
	if s.d.Sampler == nil && len(body.Outputs) > 0 {
		body.Warnings = append(body.Warnings, samplesUnavailable)
	}
	return body, nil
}

// resample retries the missing samples of a cached document and reports
// whether it changed. Sample warnings are rebuilt from this attempt.
func (s *Service) resample(ctx context.Context, p *store.Preview, body *cachedResult) bool {
	if s.d.Sampler == nil {
		return false
	}
	changed := false
	var warnings []string
	for i := range body.Outputs {
		o := &body.Outputs[i]
		if o.Sample != nil {
			continue
		}
		var variantCols []string
		for _, c := range o.Schema {
			if c.Type == "variant" {
				variantCols = append(variantCols, c.Name)
			}
		}
		if w := s.sample(ctx, p, o, variantCols); w != "" {
			warnings = append(warnings, w)
		} else {
			changed = true
		}
	}
	if !changed {
		return false
	}
	kept := []string{}
	for _, w := range body.Warnings {
		if w != samplesUnavailable && !strings.HasPrefix(w, sampleSkipPrefix) {
			kept = append(kept, w)
		}
	}
	body.Warnings = append(kept, warnings...)
	return true
}

// sample fills o.Sample, or returns the warning explaining why it could not.
func (s *Service) sample(ctx context.Context, p *store.Preview, o *Output, variantCols []string) string {
	ns, table, _ := strings.Cut(o.ScratchTable, ".")
	smp, err := s.d.Sampler.Sample(ctx, p.LakekeeperProjectID+"/"+p.Warehouse, ns, table, variantCols, sampleRows)
	if err != nil {
		reason := err.Error()
		var qe *queryproxy.QueryError
		if errors.As(err, &qe) && qe.Kind != "" {
			reason = qe.Kind
		}
		return sampleSkipPrefix + o.ScratchTable + ": " + reason
	}
	o.Sample = smp
	return ""
}

func (s *Service) tail(ctx context.Context, p *store.Preview, stage string, c datupletv1.ComponentStatus, container string, warnings *[]string) *string {
	out, err := s.d.Logs.Tail(ctx, p.K8sNamespace, c.PodName, container, logTailLines)
	if err != nil {
		*warnings = append(*warnings, fmt.Sprintf("%s log tail unavailable for %s/%s: %v", container, stage, c.Name, err))
		return nil
	}
	if out == "" {
		return nil
	}
	return &out
}

// producer finds the doc component that writes scratch table ns.table: an
// explicit output table first, else the unique component writing bucket ns.
func producer(doc *config.Pipeline, ns, table string) (stage, component *string) {
	var bucketStage, bucketComp string
	writers := 0
	for i := range doc.Stages {
		st := &doc.Stages[i]
		for j := range st.Components {
			c := &st.Components[j]
			if c.Outputs == nil {
				continue
			}
			for _, t := range c.Outputs.Tables {
				if t.Bucket == ns && t.Name == table {
					return &st.Name, &c.Name
				}
			}
			writes := c.Outputs.DefaultBucket == ns
			for _, b := range c.Outputs.Buckets {
				writes = writes || b.Name == ns
			}
			if writes {
				writers++
				bucketStage, bucketComp = st.Name, c.Name
			}
		}
	}
	if writers == 1 {
		return &bucketStage, &bucketComp
	}
	return nil, nil
}

// render fills a Result from the row, using the cached document when there is one.
func render(p *store.Preview) (*Result, error) {
	doc, err := docOf(p)
	if err != nil {
		return nil, err
	}
	r := &Result{
		ID: p.ID, Workspace: p.Workspace, Phase: p.Phase, Message: p.Message,
		CreatedAt: p.CreatedAt, StartedAt: p.StartedAt, CompletedAt: p.CompletedAt, ExpiresAt: p.ExpiresAt,
		RowLimit: p.RowLimit, DocName: doc.Name,
		Stages: []StageResult{}, Outputs: []Output{}, Warnings: []string{},
	}
	if p.UntilStage != "" {
		r.UntilStage = &p.UntilStage
	}
	if p.Result != nil {
		var body cachedResult
		if err := json.Unmarshal(p.Result, &body); err != nil {
			return nil, fmt.Errorf("decode preview result: %w", err)
		}
		r.ResultReady = true
		if body.Stages != nil {
			r.Stages = body.Stages
		}
		if body.Outputs != nil {
			r.Outputs = body.Outputs
		}
		if body.Warnings != nil {
			r.Warnings = body.Warnings
		}
		return r, nil
	}
	stages, err := stagesOf(p.StageStatuses)
	if err != nil {
		return nil, err
	}
	for _, st := range stages {
		r.Stages = append(r.Stages, stageResult(st))
	}
	return r, nil
}

func docOf(p *store.Preview) (*config.Pipeline, error) {
	var doc config.Pipeline
	if err := json.Unmarshal(p.Doc, &doc); err != nil {
		return nil, fmt.Errorf("decode preview doc: %w", err)
	}
	return &doc, nil
}

func stagesOf(raw []byte) ([]datupletv1.StageStatus, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var stages []datupletv1.StageStatus
	if err := json.Unmarshal(raw, &stages); err != nil {
		return nil, fmt.Errorf("decode stage statuses: %w", err)
	}
	return stages, nil
}

func stageResult(st datupletv1.StageStatus) StageResult {
	r := StageResult{Name: st.Name, Phase: string(st.Phase), Message: st.Message, Components: []ComponentResult{}}
	if st.StartTime != nil && st.CompletionTime != nil {
		ms := st.CompletionTime.Sub(st.StartTime.Time).Milliseconds()
		r.DurationMS = &ms
	}
	for _, c := range st.ComponentStatuses {
		r.Components = append(r.Components, ComponentResult{
			Name: c.Name, Phase: string(c.Phase), ExitCode: c.ExitCode, Message: c.Message,
		})
	}
	return r
}

func failed(phase string) bool {
	return phase == string(datupletv1.ComponentPhaseFailedUser) || phase == string(datupletv1.ComponentPhaseFailedApplication)
}
