package segment

import (
	"fmt"

	"github.com/RoaringBitmap/roaring/v2"
)

// TombstoneField is the indexed field name used to mark tombstone documents.
const TombstoneField = "_tombstone"

// Reconcile builds per-segment exclusion bitmaps by identifying tombstone documents.
// Unlike argus's Reconcile, this does NOT deduplicate by document key — all versions
// are retained because the datastore needs full version history for Keys, GetLatestAndPredecessor, etc.
// Only tombstoned documents (physical deletes from rollback) are excluded.
//
// The returned slice has the same length as readers. A nil bitmap means no exclusions.
func Reconcile(readers []*SegmentReader) ([]*roaring.Bitmap, error) {
	if len(readers) == 0 {
		return nil, nil
	}

	bitmaps := make([]*roaring.Bitmap, len(readers))
	for i, reader := range readers {
		bm, err := tombstoneBitmap(reader)
		if err != nil {
			return nil, fmt.Errorf("segment %d: %w", i, err)
		}
		bitmaps[i] = bm
	}
	return bitmaps, nil
}

// tombstoneBitmap returns a roaring bitmap of document numbers that are tombstones
// within a single segment. Returns nil if the segment has no tombstones.
func tombstoneBitmap(reader *SegmentReader) (*roaring.Bitmap, error) {
	seg := reader.Segment()
	if seg == nil {
		return nil, nil
	}

	dict, err := seg.Dictionary(TombstoneField)
	if err != nil {
		return nil, fmt.Errorf("get tombstone dictionary: %w", err)
	}
	if dict == nil {
		return nil, nil
	}

	pl, err := dict.PostingsList([]byte("1"), nil, nil)
	if err != nil {
		return nil, fmt.Errorf("get tombstone postings list: %w", err)
	}
	if pl == nil || pl.Count() == 0 {
		return nil, nil
	}

	bm := roaring.New()
	iter := pl.Iterator(false, false, false, nil)
	if iter == nil {
		return nil, nil
	}
	posting, err := iter.Next()
	for err == nil && posting != nil {
		bm.Add(uint32(posting.Number()))
		posting, err = iter.Next()
	}
	if err != nil {
		return nil, fmt.Errorf("iterate tombstone postings: %w", err)
	}

	if bm.IsEmpty() {
		return nil, nil
	}
	return bm, nil
}
