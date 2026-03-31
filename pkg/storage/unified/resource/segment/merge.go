package segment

import (
	"fmt"
	"os"

	"github.com/RoaringBitmap/roaring/v2"
	segment_api "github.com/blevesearch/scorch_segment_api/v2"
	zap "github.com/blevesearch/zapx/v17"
)

// MergeSegments merges multiple segments into one, excluding documents marked in the
// drops bitmaps (typically tombstones from Reconcile). Returns a new Segment containing
// the merged data.
func MergeSegments(readers []*SegmentReader, drops []*roaring.Bitmap) (*Segment, error) {
	if len(readers) == 0 {
		return nil, fmt.Errorf("no segments to merge")
	}

	segments := make([]segment_api.Segment, len(readers))
	for i, r := range readers {
		seg := r.Segment()
		if seg == nil {
			return nil, fmt.Errorf("segment %d not opened", i)
		}
		segments[i] = seg
	}

	tmpFile, err := os.CreateTemp("", "merge-*.zap")
	if err != nil {
		return nil, fmt.Errorf("create temp file for merge: %w", err)
	}
	outputPath := tmpFile.Name()
	_ = tmpFile.Close()
	defer os.Remove(outputPath)

	closeCh := make(chan struct{})
	defer close(closeCh)

	_, mergedDocCount, err := (&zap.ZapPlugin{}).Merge(segments, drops, outputPath, closeCh, nil)
	if err != nil {
		return nil, fmt.Errorf("zap merge: %w", err)
	}

	mergedData, err := os.ReadFile(outputPath)
	if err != nil {
		return nil, fmt.Errorf("read merged segment: %w", err)
	}

	return &Segment{
		Data:    mergedData,
		NumDocs: int(mergedDocCount),
	}, nil
}

// MergedSegmentSize returns the serialized byte size of a segment.
func MergedSegmentSize(seg *Segment) int64 {
	return int64(len(seg.Data))
}

// TotalDocCount returns the total number of live (non-dropped) documents across readers.
func TotalDocCount(readers []*SegmentReader, drops []*roaring.Bitmap) uint64 {
	var total uint64
	for i, r := range readers {
		count := uint64(r.NumDocs())
		if i < len(drops) && drops[i] != nil {
			dropped := uint64(drops[i].GetCardinality())
			if dropped < count {
				count -= dropped
			} else {
				count = 0
			}
		}
		total += count
	}
	return total
}

// EmptyMerge returns true if every document across all readers is dropped.
func EmptyMerge(readers []*SegmentReader, drops []*roaring.Bitmap) bool {
	return TotalDocCount(readers, drops) == 0
}

// MergeToBytes is a convenience wrapper that merges and returns just the raw bytes.
func MergeToBytes(readers []*SegmentReader, drops []*roaring.Bitmap) ([]byte, int, error) {
	// If every doc is dropped, return an empty segment built from zero documents.
	// ZapPlugin.Merge does not handle the all-dropped case gracefully.
	if EmptyMerge(readers, drops) {
		// Build a minimal empty-ish segment — caller should delete the manifest entry instead.
		return nil, 0, nil
	}

	seg, err := MergeSegments(readers, drops)
	if err != nil {
		return nil, 0, err
	}
	return seg.Data, seg.NumDocs, nil
}

// MergeAndOpen merges segments and returns an opened SegmentReader for the result.
// Caller is responsible for closing the returned reader.
func MergeAndOpen(readers []*SegmentReader, drops []*roaring.Bitmap) (*SegmentReader, *Segment, error) {
	merged, err := MergeSegments(readers, drops)
	if err != nil {
		return nil, nil, err
	}

	reader, err := NewSegmentReader(merged)
	if err != nil {
		return nil, nil, fmt.Errorf("create reader for merged segment: %w", err)
	}
	if err := reader.Open(); err != nil {
		return nil, nil, fmt.Errorf("open merged segment: %w", err)
	}

	return reader, merged, nil
}
