// Package segment provides zap segment reading and writing capabilities.
// Copied from github.com/grafana/argus/pkg/segment — keep in sync until a shared module is extracted.
package segment

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/blevesearch/bleve/v2/document"
	bleve_index_api "github.com/blevesearch/bleve_index_api"
	zap "github.com/blevesearch/zapx/v17"
)

// Segment represents an immutable zap segment.
type Segment struct {
	Data    []byte // serialized zap segment bytes
	NumDocs int
	TxnID   uint64 // transaction ID, used for ordering segments
}

// SegmentBuilder creates zap segments from bleve documents.
type SegmentBuilder struct {
	zapPlugin *zap.ZapPlugin
}

// NewSegmentBuilder creates a new segment builder.
func NewSegmentBuilder() (*SegmentBuilder, error) {
	return &SegmentBuilder{zapPlugin: &zap.ZapPlugin{}}, nil
}

// BuildSegment creates a zap segment from documents.
func (b *SegmentBuilder) BuildSegment(ctx context.Context, docs []*document.Document, txnID uint64) (*Segment, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if len(docs) == 0 {
		return nil, fmt.Errorf("cannot build segment from empty document list")
	}

	for _, doc := range docs {
		doc.AddIDField()
		doc.VisitFields(func(field bleve_index_api.Field) {
			if field.Options().IsIndexed() {
				field.Analyze()
			}
		})
	}

	documents := make([]bleve_index_api.Document, len(docs))
	for i, doc := range docs {
		documents[i] = doc
	}

	seg, _, err := b.zapPlugin.New(documents)
	if err != nil {
		return nil, fmt.Errorf("create zap segment: %w", err)
	}
	defer seg.Close()

	var buf bytes.Buffer
	sb, ok := seg.(io.WriterTo)
	if !ok {
		return nil, fmt.Errorf("segment does not implement io.WriterTo")
	}
	if _, err = sb.WriteTo(&buf); err != nil {
		return nil, fmt.Errorf("serialize segment: %w", err)
	}

	return &Segment{
		Data:    buf.Bytes(),
		NumDocs: len(docs),
		TxnID:   txnID,
	}, nil
}
