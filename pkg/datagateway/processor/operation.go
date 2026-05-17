// Package processor provides data processing operations for the Data Gateway.
package processor

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/datuplet/datuplet/pkg/datagateway/schema"
)

// Operation represents a single transformation operation.
type Operation interface {
	// Apply applies the operation to a record batch, returning a transformed batch.
	Apply(record arrow.Record, allocator memory.Allocator) (arrow.Record, error)

	// TransformSchema returns the schema after applying this operation.
	TransformSchema(input *schema.Schema) (*schema.Schema, error)

	// String returns a string representation of the operation.
	String() string
}

// Pipeline represents a sequence of operations to apply.
type Pipeline struct {
	Operations []Operation
}

// NewPipeline creates a new empty pipeline.
func NewPipeline() *Pipeline {
	return &Pipeline{
		Operations: make([]Operation, 0),
	}
}

// Add adds an operation to the pipeline.
func (p *Pipeline) Add(op Operation) *Pipeline {
	p.Operations = append(p.Operations, op)
	return p
}

// Apply applies all operations in sequence to a record batch.
func (p *Pipeline) Apply(record arrow.Record, allocator memory.Allocator) (arrow.Record, error) {
	if len(p.Operations) == 0 {
		record.Retain()
		return record, nil
	}

	current := record
	current.Retain()

	for _, op := range p.Operations {
		next, err := op.Apply(current, allocator)
		current.Release()
		if err != nil {
			return nil, err
		}
		current = next
	}

	return current, nil
}

// TransformSchema computes the final schema after all operations.
func (p *Pipeline) TransformSchema(input *schema.Schema) (*schema.Schema, error) {
	current := input
	for _, op := range p.Operations {
		var err error
		current, err = op.TransformSchema(current)
		if err != nil {
			return nil, err
		}
	}
	return current, nil
}
