package segment

import (
	"fmt"
	"os"
	"runtime"

	segment_api "github.com/blevesearch/scorch_segment_api/v2"
	zap "github.com/blevesearch/zapx/v17"
)

// SegmentReader provides read access to a single zap segment.
// It wraps a zapx segment that is memory-mapped for efficient access.
type SegmentReader struct {
	seg    segment_api.Segment
	path   string
	opened bool
}

// NewSegmentReader creates a reader from segment data.
// The data is written to a temporary file and memory-mapped when Open is called.
func NewSegmentReader(segment *Segment) (*SegmentReader, error) {
	tmpFile, err := os.CreateTemp(os.TempDir(), "segment-*.zap")
	if err != nil {
		return nil, fmt.Errorf("create temp file for segment: %w", err)
	}

	if _, err := tmpFile.Write(segment.Data); err != nil {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
		return nil, fmt.Errorf("write segment data to temp file: %w", err)
	}

	path := tmpFile.Name()
	if err := tmpFile.Close(); err != nil {
		os.Remove(path)
		return nil, fmt.Errorf("close temp file: %w", err)
	}

	r := &SegmentReader{path: path}
	// Ensure temp file is cleaned up if the reader is garbage collected without Close.
	runtime.SetFinalizer(r, func(r *SegmentReader) { r.Close() })
	return r, nil
}

// Open memory-maps the segment file and validates its footer.
func (r *SegmentReader) Open() error {
	if r.opened {
		return fmt.Errorf("segment already opened")
	}

	zapPlugin := &zap.ZapPlugin{}
	seg, err := zapPlugin.Open(r.path)
	if err != nil {
		return fmt.Errorf("open zap segment: %w", err)
	}

	r.seg = seg
	r.opened = true
	return nil
}

// Close releases the mmap and removes the temp file.
func (r *SegmentReader) Close() error {
	if !r.opened {
		return nil
	}

	var firstErr error
	if r.seg != nil {
		if err := r.seg.Close(); err != nil {
			firstErr = fmt.Errorf("close zap segment: %w", err)
		}
		r.seg = nil
	}

	if r.path != "" {
		if err := os.Remove(r.path); err != nil && !os.IsNotExist(err) && firstErr == nil {
			firstErr = fmt.Errorf("remove temp file: %w", err)
		}
		r.path = ""
	}

	r.opened = false
	return firstErr
}

// VisitStoredFields visits all stored fields for a document by local doc number.
func (r *SegmentReader) VisitStoredFields(localDocNum uint64, visitor segment_api.StoredFieldValueVisitor) error {
	if !r.opened || r.seg == nil {
		return fmt.Errorf("segment not opened")
	}
	return r.seg.VisitStoredFields(localDocNum, visitor)
}

// Segment returns the underlying zap segment.
func (r *SegmentReader) Segment() segment_api.Segment {
	return r.seg
}

// NumDocs returns the number of documents in the segment.
func (r *SegmentReader) NumDocs() int {
	if !r.opened || r.seg == nil {
		return 0
	}
	return int(r.seg.Count())
}

// DocID returns the external document ID for a local document number.
func (r *SegmentReader) DocID(localDocNum uint64) (string, error) {
	if !r.opened || r.seg == nil {
		return "", fmt.Errorf("segment not opened")
	}
	id, err := r.seg.DocID(localDocNum)
	if err != nil {
		return "", err
	}
	return string(id), nil
}

// Dictionary returns the term dictionary for a field.
func (r *SegmentReader) Dictionary(field string) (segment_api.TermDictionary, error) {
	if !r.opened || r.seg == nil {
		return nil, fmt.Errorf("segment not opened")
	}
	return r.seg.Dictionary(field)
}
