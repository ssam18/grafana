package resource

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"iter"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/blevesearch/bleve/v2/analysis"
	"github.com/blevesearch/bleve/v2/document"
	"github.com/blevesearch/bleve/v2/index/scorch/mergeplan"
	"github.com/blevesearch/bleve/v2/registry"
	index "github.com/blevesearch/bleve_index_api"
	"github.com/RoaringBitmap/roaring/v2"
	segment_api "github.com/blevesearch/scorch_segment_api/v2"
	"github.com/blevesearch/vellum"
	kvpkg "github.com/grafana/grafana/pkg/storage/unified/resource/kv"
	"github.com/grafana/grafana/pkg/storage/unified/resource/segment"
	"github.com/grafana/grafana/pkg/storage/unified/sql/db"

	// Register the keyword analyzer in the bleve registry.
	_ "github.com/blevesearch/bleve/v2/analysis/analyzer/keyword"
)

const (
	segmentsSection = kvpkg.SegmentsSection
	manifestSection = kvpkg.ManifestSection
)

var _ DataStore = &segmentDataStore{}

// errTombstone is a sentinel error returned by dataKeyFromSegment when the document
// is a tombstone. Callers should skip the document rather than propagating the error.
var errTombstone = fmt.Errorf("tombstone")

// segmentDataStore is a DataStore backed by Zap segments stored in a KV store.
type segmentDataStore struct {
	kv      KV // KV store for segments + manifest
	builder *segment.SegmentBuilder

	// cache holds opened *segment.SegmentReader keyed by segment KV key.
	// Segments are immutable — cached readers never go stale until invalidated.
	// Readers have a runtime finalizer that cleans up temp files when GC'd.
	cache sync.Map // segKey string -> *segment.SegmentReader

	// compactMu holds per-group/resource mutexes to serialize compaction.
	compactMu sync.Map // "group/resource" -> *sync.Mutex

	// compactPending tracks whether a compaction is needed after the current one finishes.
	// When a Save triggers compaction but one is already running, this flag ensures
	// the running compaction loops again to pick up new segments.
	compactPending sync.Map // "group/resource" -> *atomic.Bool

	// mergePlanOpts configures the tiered merge planner.
	mergePlanOpts mergeplan.MergePlanOptions
}

// planSegment adapts manifest metadata to the mergeplan.Segment interface.
type planSegment struct {
	manifestKey string
	rv          uint64
	size        int64
}

func (s *planSegment) Id() uint64      { return s.rv }
func (s *planSegment) FullSize() int64 { return s.size }
func (s *planSegment) LiveSize() int64 { return s.size }
func (s *planSegment) HasVector() bool { return false }
func (s *planSegment) FileSize() int64 { return s.size }

var (
	keywordAnalyzerOnce sync.Once
	keywordAnalyzerVal  analysis.Analyzer
)

func getKeywordAnalyzer() analysis.Analyzer {
	keywordAnalyzerOnce.Do(func() {
		cache := registry.NewCache()
		a, err := registry.AnalyzerBuild("keyword", nil, cache)
		if err != nil {
			panic(fmt.Sprintf("failed to build keyword analyzer: %v", err))
		}
		keywordAnalyzerVal = a.(analysis.Analyzer)
	})
	return keywordAnalyzerVal
}

func newSegmentDataStore(kv KV) *segmentDataStore {
	builder, err := segment.NewSegmentBuilder()
	if err != nil {
		panic(fmt.Sprintf("failed to create segment builder: %v", err))
	}
	return &segmentDataStore{
		kv:      kv,
		builder: builder,
		mergePlanOpts: mergeplan.MergePlanOptions{
			MaxSegmentsPerTier:   10,
			MaxSegmentSize:       50 * 1024 * 1024, // 50 MB
			TierGrowth:           10.0,
			SegmentsPerMergeTask: 10,
			FloorSegmentSize:     4096, // our 1-doc segments are ~1-2 KB
			ReclaimDeletesWeight: 2.0,
		},
	}
}

// segmentDocID returns the document _id for a DataKey.
// Format: {group}/{resource}/{namespace}/{name} (or {group}/{resource}/{name} for cluster-scoped).
// Matches the prefix format used by ListRequestKey.Prefix() so sort order is consistent.
func segmentDocID(key DataKey) string {
	if key.Namespace == "" {
		return fmt.Sprintf("%s/%s/%s", key.Group, key.Resource, key.Name)
	}
	return fmt.Sprintf("%s/%s/%s/%s", key.Group, key.Resource, key.Namespace, key.Name)
}

// segmentKVKey returns the KV key for storing a segment's .zap data.
// Format: {group}/{resource}/{rv}.zap
// RVs are globally unique snowflake IDs, so this is unique per segment.
func segmentKVKey(key DataKey) string {
	return fmt.Sprintf("%s/%s/%d.zap", key.Group, key.Resource, key.ResourceVersion)
}

// manifestKVKey returns the KV key for the manifest entry.
// Format: {group}/{resource}/{rv} — one entry per segment, not per document.
// This matches the design doc's manifest layout and is compatible with compaction,
// where a merged segment replaces multiple source segments with a single manifest entry.
func manifestKVKey(key DataKey) string {
	return fmt.Sprintf("%s/%s/%d", key.Group, key.Resource, key.ResourceVersion)
}

// encodeManifestValue packs schema version and segment byte size into 12 bytes.
func encodeManifestValue(schemaVersion uint32, segmentSize int64) []byte {
	buf := make([]byte, 12)
	binary.BigEndian.PutUint32(buf[0:4], schemaVersion)
	binary.BigEndian.PutUint64(buf[4:12], uint64(segmentSize))
	return buf
}

// decodeManifestValue unpacks schema version and segment byte size.
// Returns defaults for legacy entries that only stored a 1-byte placeholder.
func decodeManifestValue(data []byte) (schemaVersion uint32, segmentSize int64) {
	if len(data) >= 12 {
		schemaVersion = binary.BigEndian.Uint32(data[0:4])
		segmentSize = int64(binary.BigEndian.Uint64(data[4:12]))
		return
	}
	// Legacy 1-byte entries: treat as schema v1 with unknown (floor) size.
	return 1, 0
}

// buildSegmentDoc builds a bleve document from a DataKey and value bytes.
func buildSegmentDoc(key DataKey, value []byte) *document.Document {
	kw := getKeywordAnalyzer()

	docID := segmentDocID(key)
	doc := document.NewDocument(docID)

	// _id — keyword, indexed + stored (added automatically by BuildSegment via AddIDField)
	// namespace — keyword, indexed, not stored
	if key.Namespace != "" {
		doc.AddField(document.NewTextFieldCustom("namespace", nil, []byte(key.Namespace), index.IndexField, kw))
	}
	// name — keyword, indexed, not stored
	doc.AddField(document.NewTextFieldCustom("name", nil, []byte(key.Name), index.IndexField, kw))
	// resource_version — numeric, indexed only (for range queries; float64 loses precision for snowflake IDs)
	doc.AddField(document.NewNumericFieldWithIndexingOptions("resource_version", nil, float64(key.ResourceVersion), index.IndexField))
	// _rv — string, stored only (exact int64 round-trip for DataKey reconstruction)
	doc.AddField(document.NewTextFieldWithIndexingOptions("_rv", nil, []byte(strconv.FormatInt(key.ResourceVersion, 10)), index.StoreField))
	// action — keyword, indexed + stored
	doc.AddField(document.NewTextFieldCustom("action", nil, []byte(string(key.Action)), index.IndexField|index.StoreField, kw))
	// folder — keyword, indexed + stored (optional)
	if key.Folder != "" {
		doc.AddField(document.NewTextFieldCustom("folder", nil, []byte(key.Folder), index.IndexField|index.StoreField, kw))
	}
	// _source — stored only (full resource bytes)
	doc.AddField(document.NewTextFieldWithIndexingOptions("_source", nil, value, index.StoreField))
	// _latest — indexed keyword. Set on every new doc; compaction corrects it so only the
	// highest-RV non-deleted version per _id retains it. Enables O(live_resources) List.
	doc.AddField(document.NewTextFieldCustom("_latest", nil, []byte("1"), index.IndexField, kw))

	return doc
}

// buildTombstoneDoc builds a minimal bleve document that marks a DataKey as tombstoned.
// It carries the same _id and _rv as the original so reads can match it, plus a _tombstone
// stored field that signals readers to skip this document. No _source is stored.
func buildTombstoneDoc(key DataKey) *document.Document {
	kw := getKeywordAnalyzer()
	docID := segmentDocID(key)
	doc := document.NewDocument(docID)

	if key.Namespace != "" {
		doc.AddField(document.NewTextFieldCustom("namespace", nil, []byte(key.Namespace), index.IndexField, kw))
	}
	doc.AddField(document.NewTextFieldCustom("name", nil, []byte(key.Name), index.IndexField, kw))
	doc.AddField(document.NewNumericFieldWithIndexingOptions("resource_version", nil, float64(key.ResourceVersion), index.IndexField))
	doc.AddField(document.NewTextFieldWithIndexingOptions("_rv", nil, []byte(strconv.FormatInt(key.ResourceVersion, 10)), index.StoreField))
	doc.AddField(document.NewTextFieldCustom("action", nil, []byte(string(key.Action)), index.IndexField|index.StoreField, kw))
	// _tombstone marker — indexed so Reconcile can use dictionary lookup, stored so readers can detect it.
	doc.AddField(document.NewTextFieldCustom("_tombstone", nil, []byte("1"), index.IndexField|index.StoreField, kw))

	return doc
}

func (s *segmentDataStore) Save(ctx context.Context, key DataKey, value io.Reader) error {
	if err := validateDataKey(key); err != nil {
		return fmt.Errorf("invalid data key: %w", err)
	}

	valueBytes, err := io.ReadAll(value)
	if err != nil {
		return fmt.Errorf("failed to read value: %w", err)
	}

	// Build a 1-doc segment.
	doc := buildSegmentDoc(key, valueBytes)
	seg, err := s.builder.BuildSegment(ctx, []*document.Document{doc}, uint64(key.ResourceVersion))
	if err != nil {
		return fmt.Errorf("failed to build segment: %w", err)
	}

	// Write .zap to KV (base64-encoded — the SQL KV's value column is longtext, not blob).
	segKey := segmentKVKey(key)
	if err := s.writeKV(ctx, segmentsSection, segKey, seg.Data); err != nil {
		return fmt.Errorf("failed to save segment: %w", err)
	}

	// Invalidate stale cache entry so reads see the new data.
	s.cache.Delete(segKey)

	// Write manifest entry with schema version and segment size.
	if err := s.writeKV(ctx, manifestSection, manifestKVKey(key), encodeManifestValue(1, int64(len(seg.Data)))); err != nil {
		return fmt.Errorf("failed to save manifest entry: %w", err)
	}

	// Compaction is triggered explicitly via CompactAll, not on every write.

	return nil
}

// writeKV writes data to the KV store.
func (s *segmentDataStore) writeKV(ctx context.Context, section, key string, data []byte) error {
	w, err := s.kv.Save(ctx, section, key)
	if err != nil {
		return err
	}
	if _, err := w.Write(data); err != nil {
		_ = w.Close()
		return err
	}
	return w.Close()
}

// openSegment returns a cached segment reader, or loads one from KV on cache miss.
// Returned readers are shared — callers must NOT close them. The runtime finalizer
// on SegmentReader handles cleanup when the reader is evicted from cache and GC'd.
func (s *segmentDataStore) openSegment(ctx context.Context, segKey string) (*segment.SegmentReader, error) {
	if cached, ok := s.cache.Load(segKey); ok {
		return cached.(*segment.SegmentReader), nil
	}

	zapReader, err := s.kv.Get(ctx, segmentsSection, segKey)
	if err != nil {
		return nil, err
	}
	zapData, err := io.ReadAll(zapReader)
	_ = zapReader.Close()
	if err != nil {
		return nil, fmt.Errorf("failed to read segment data: %w", err)
	}

	reader, err := segment.NewSegmentReader(&segment.Segment{Data: zapData})
	if err != nil {
		return nil, fmt.Errorf("failed to create segment reader: %w", err)
	}
	if err := reader.Open(); err != nil {
		return nil, fmt.Errorf("failed to open segment: %w", err)
	}

	actual, _ := s.cache.LoadOrStore(segKey, reader)
	return actual.(*segment.SegmentReader), nil
}

// collectVersions returns all DataKeys for a given _id term across multiple segments.
// Uses the _id term dictionary for O(log N) lookup per segment.
// Result set is bounded by version retention (~20 versions per resource).
func collectVersions(readers []*segment.SegmentReader, docID string) ([]DataKey, error) {
	var keys []DataKey
	for _, reader := range readers {
		dict, err := reader.Dictionary("_id")
		if err != nil {
			return nil, fmt.Errorf("failed to get _id dictionary: %w", err)
		}

		postings, err := dict.PostingsList([]byte(docID), nil, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to get postings for %s: %w", docID, err)
		}

		pIter := postings.Iterator(false, false, false, nil)
		for {
			posting, err := pIter.Next()
			if err != nil {
				return nil, fmt.Errorf("failed to iterate postings: %w", err)
			}
			if posting == nil {
				break
			}

			dk, err := dataKeyFromSegment(reader, posting.Number())
			if err == errTombstone {
				continue // skip tombstoned documents
			}
			if err != nil {
				return nil, err
			}
			keys = append(keys, dk)
		}
	}
	return keys, nil
}

// dataKeyFromSegment extracts a DataKey from a segment reader at a given doc number.
// It parses the doc ID for group/resource/namespace/name and reads stored fields
// for resource_version, action, and folder.
func dataKeyFromSegment(reader *segment.SegmentReader, docNum uint64) (DataKey, error) {
	docID, err := reader.DocID(docNum)
	if err != nil {
		return DataKey{}, fmt.Errorf("failed to get doc ID for doc %d: %w", docNum, err)
	}

	// Parse _id: {group}/{resource}/{namespace}/{name} or {group}/{resource}/{name}
	parts := strings.Split(docID, "/")
	var group, resource, namespace, name string
	switch len(parts) {
	case 4: // namespaced
		group, resource, namespace, name = parts[0], parts[1], parts[2], parts[3]
	case 3: // cluster-scoped
		group, resource, name = parts[0], parts[1], parts[2]
	default:
		return DataKey{}, fmt.Errorf("invalid doc ID format: %s", docID)
	}

	var rv int64
	var action kvpkg.DataAction
	var folder string

	var tombstone bool
	err = reader.VisitStoredFields(docNum, func(field string, typ byte, value []byte, _ []uint64) bool {
		switch field {
		case "_rv":
			rv, _ = strconv.ParseInt(string(value), 10, 64)
		case "action":
			action = kvpkg.DataAction(string(value))
		case "folder":
			folder = string(value)
		case "_tombstone":
			tombstone = true
		}
		return true
	})
	if err != nil {
		return DataKey{}, fmt.Errorf("failed to read stored fields for doc %d: %w", docNum, err)
	}

	if tombstone {
		return DataKey{}, errTombstone
	}

	return DataKey{
		Group:           group,
		Resource:        resource,
		Namespace:       namespace,
		Name:            name,
		ResourceVersion: rv,
		Action:          action,
		Folder:          folder,
	}, nil
}

func (s *segmentDataStore) Get(ctx context.Context, key DataKey) (io.ReadCloser, error) {
	// Search all segments for the (group, resource) to find the document.
	// This works both before and after compaction — a compacted multi-doc segment
	// contains the document alongside others, so we can't assume a 1:1 mapping
	// between manifest entries and documents.
	targetDocID := segmentDocID(key)
	prefix := fmt.Sprintf("%s/%s/", key.Group, key.Resource)

	for manifestKey, err := range s.kv.Keys(ctx, manifestSection, ListOptions{
		StartKey: prefix,
		EndKey:   PrefixRangeEnd(prefix),
	}) {
		if err != nil {
			return nil, err
		}

		segKey := manifestKey + ".zap"
		reader, err := s.openSegment(ctx, segKey)
		if err != nil {
			return nil, fmt.Errorf("failed to open segment %s: %w", segKey, err)
		}

		source, found, err := s.findDocInSegment(reader, targetDocID, key.ResourceVersion)
		if err != nil {
			return nil, err
		}
		if found {
			return io.NopCloser(bytes.NewReader(source)), nil
		}
	}

	return nil, ErrNotFound
}

// findDocInSegment searches a segment for a document matching the given doc ID
// and resource version. Uses the _id term dictionary for O(log N) lookup instead
// of scanning all docs.
func (s *segmentDataStore) findDocInSegment(reader *segment.SegmentReader, targetDocID string, targetRV int64) ([]byte, bool, error) {
	dict, err := reader.Dictionary("_id")
	if err != nil {
		return nil, false, fmt.Errorf("failed to get _id dictionary: %w", err)
	}

	// what's this??
	postings, err := dict.PostingsList([]byte(targetDocID), nil, nil)
	if err != nil {
		return nil, false, fmt.Errorf("failed to get postings for %s: %w", targetDocID, err)
	}

	iter := postings.Iterator(false, false, false, nil)
	for {
		posting, err := iter.Next()
		if err != nil {
			return nil, false, fmt.Errorf("failed to iterate postings: %w", err)
		}
		if posting == nil {
			break
		}

		// Read stored fields to check RV and get _source.
		var source []byte
		var rv int64
		var tombstone bool
		err = reader.VisitStoredFields(posting.Number(), func(field string, typ byte, value []byte, _ []uint64) bool {
			switch field {
			case "_rv":
				rv, _ = strconv.ParseInt(string(value), 10, 64)
			case "_source":
				source = make([]byte, len(value))
				copy(source, value)
			case "_tombstone":
				tombstone = true
			}
			return true
		})
		if err != nil {
			return nil, false, fmt.Errorf("failed to read stored fields: %w", err)
		}
		if rv == targetRV {
			if tombstone {
				return nil, false, nil
			}
			return source, true, nil
		}
	}
	return nil, false, nil
}

func (s *segmentDataStore) Delete(ctx context.Context, key DataKey) error {
	if err := validateDataKey(key); err != nil {
		return fmt.Errorf("invalid data key: %w", err)
	}

	// In the segment model, segments are immutable — we can't remove a document from one.
	// Instead, write a tombstone segment with a _tombstone marker field. Read methods check
	// for this field and skip the document. Compaction reclaims the space later.
	// This is distinct from action=deleted (a logical delete that's still readable).
	doc := buildTombstoneDoc(key)
	seg, err := s.builder.BuildSegment(ctx, []*document.Document{doc}, uint64(key.ResourceVersion))
	if err != nil {
		return fmt.Errorf("failed to build tombstone segment: %w", err)
	}

	// Write .zap — overwrites the original segment at this RV.
	segKey := segmentKVKey(key)
	if err := s.writeKV(ctx, segmentsSection, segKey, seg.Data); err != nil {
		return fmt.Errorf("failed to save tombstone segment: %w", err)
	}

	// Invalidate cached reader so reads see the tombstone, not old data.
	s.cache.Delete(segKey)

	return nil
}

// --- Merge-sort infrastructure for streaming term dictionary iteration ---

// segTermIter holds a dictionary iterator for one segment, tracking the current term.
type segTermIter struct {
	reader  *segment.SegmentReader
	dict    segment_api.TermDictionary
	iter    segment_api.DictionaryIterator
	current *index.DictEntry
}

// segTermHeap is a min-heap of segTermIter, ordered by current term.
type segTermHeap []*segTermIter

func (h segTermHeap) Len() int            { return len(h) }
func (h segTermHeap) Less(i, j int) bool   { return h[i].current.Term < h[j].current.Term }
func (h segTermHeap) Swap(i, j int)        { h[i], h[j] = h[j], h[i] }
func (h *segTermHeap) Push(x any)  { *h = append(*h, x.(*segTermIter)) }
func (h *segTermHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return item
}

func (s *segmentDataStore) Keys(ctx context.Context, key ListRequestKey, sort SortOrder) iter.Seq2[DataKey, error] {
	if err := key.Validate(); err != nil {
		return func(yield func(DataKey, error) bool) {
			yield(DataKey{}, err)
		}
	}

	return func(yield func(DataKey, error) bool) {
		// Open all segments for this group/resource.
		readers, err := s.openGroupResourceSegments(ctx, key.Group, key.Resource)
		if err != nil {
			yield(DataKey{}, err)
			return
		}
		if key.Name != "" {
			// Exact _id lookup — no merge-sort needed, small result set.
			s.keysExactName(readers, key, sort, yield)
		} else {
			// Range scan with streaming merge-sort across term dictionaries.
			s.keysMergeSorted(readers, key, sort, yield)
		}
	}
}

// openGroupResourceSegments lists manifest entries and opens all segments for a (group, resource).
func (s *segmentDataStore) openGroupResourceSegments(ctx context.Context, group, resource string) ([]*segment.SegmentReader, error) {
	prefix := fmt.Sprintf("%s/%s/", group, resource)
	var readers []*segment.SegmentReader

	for manifestKey, err := range s.kv.Keys(ctx, manifestSection, ListOptions{
		StartKey: prefix,
		EndKey:   PrefixRangeEnd(prefix),
	}) {
		if err != nil {
			return nil, err
		}

		segKey := manifestKey + ".zap"
		reader, err := s.openSegment(ctx, segKey)
		if err != nil {
			return nil, fmt.Errorf("failed to open segment %s: %w", segKey, err)
		}
		readers = append(readers, reader)
	}
	return readers, nil
}

// keysExactName handles Keys when a specific name is provided.
// Result set is small (bounded by version retention limit), so collect + sort is fine.
func (s *segmentDataStore) keysExactName(
	readers []*segment.SegmentReader,
	key ListRequestKey,
	sortOrder SortOrder,
	yield func(DataKey, error) bool,
) {
	targetDocID := segmentDocID(DataKey{
		Group: key.Group, Resource: key.Resource,
		Namespace: key.Namespace, Name: key.Name,
	})

	allKeys, err := collectVersions(readers, targetDocID)
	if err != nil {
		yield(DataKey{}, err)
		return
	}

	slices.SortFunc(allKeys, func(a, b DataKey) int {
		return strings.Compare(a.String(), b.String())
	})
	if sortOrder == SortOrderDesc {
		slices.Reverse(allKeys)
	}

	for _, dk := range allKeys {
		if !yield(dk, nil) {
			return
		}
	}
}

// keysMergeSorted handles Keys without a specific name — streaming merge-sort
// across all segments' _id term dictionaries. Memory is O(segments) for the
// heap + O(versions_per_resource) for each _id group.
func (s *segmentDataStore) keysMergeSorted(
	readers []*segment.SegmentReader,
	key ListRequestKey,
	sortOrder SortOrder,
	yield func(DataKey, error) bool,
) {
	// Build the _id prefix range for filtering.
	// _id format: {group}/{resource}/{namespace}/{name} or {group}/{resource}/{name}
	var startKey, endKey string
	if key.Namespace != "" {
		startKey = fmt.Sprintf("%s/%s/%s/", key.Group, key.Resource, key.Namespace)
	} else {
		startKey = fmt.Sprintf("%s/%s/", key.Group, key.Resource)
	}
	endKey = PrefixRangeEnd(startKey)

	// Initialize a dictionary iterator per segment and seed the heap.
	h := &segTermHeap{}
	for _, reader := range readers {
		dict, err := reader.Dictionary("_id")
		if err != nil {
			yield(DataKey{}, fmt.Errorf("failed to get _id dictionary: %w", err))
			return
		}

		iter := dict.AutomatonIterator(&vellum.AlwaysMatch{}, []byte(startKey), []byte(endKey))
		entry, err := iter.Next()
		if err != nil {
			yield(DataKey{}, fmt.Errorf("failed to advance dictionary iterator: %w", err))
			return
		}
		if entry != nil {
			heap.Push(h, &segTermIter{reader: reader, dict: dict, iter: iter, current: entry})
		}
	}
	heap.Init(h)

	// Merge-sort: pop minimum term, collect all segments with that term,
	// gather DataKeys for the _id group, sort by rv, yield.
	var group []DataKey
	for h.Len() > 0 {
		minTerm := (*h)[0].current.Term

		// Process all segments that have this term.
		for h.Len() > 0 && (*h)[0].current.Term == minTerm {
			ti := heap.Pop(h).(*segTermIter)

			postings, err := ti.dict.PostingsList([]byte(minTerm), nil, nil)
			if err != nil {
				yield(DataKey{}, fmt.Errorf("failed to get postings for %s: %w", minTerm, err))
				return
			}

			pIter := postings.Iterator(false, false, false, nil)
			for {
				posting, err := pIter.Next()
				if err != nil {
					yield(DataKey{}, fmt.Errorf("failed to iterate postings: %w", err))
					return
				}
				if posting == nil {
					break
				}

				dk, err := dataKeyFromSegment(ti.reader, posting.Number())
				if err == errTombstone {
					continue // skip tombstoned documents
				}
				if err != nil {
					yield(DataKey{}, err)
					return
				}
				group = append(group, dk)
			}

			// Advance this segment's iterator.
			next, err := ti.iter.Next()
			if err != nil {
				yield(DataKey{}, fmt.Errorf("failed to advance dictionary iterator: %w", err))
				return
			}
			if next != nil {
				ti.current = next
				heap.Push(h, ti)
			}
		}

		// Sort the _id group by rv and yield.
		slices.SortFunc(group, func(a, b DataKey) int {
			return strings.Compare(a.String(), b.String())
		})
		if sortOrder == SortOrderDesc {
			slices.Reverse(group)
		}

		for _, dk := range group {
			if !yield(dk, nil) {
				return
			}
		}
		group = group[:0]
	}
}

func (s *segmentDataStore) LastResourceVersion(ctx context.Context, key ListRequestKey) (DataKey, error) {
	if err := key.Validate(); err != nil {
		return DataKey{}, fmt.Errorf("invalid data key: %w", err)
	}
	if key.Group == "" || key.Resource == "" || key.Name == "" {
		return DataKey{}, fmt.Errorf("group, resource or name is empty")
	}

	// Iterate all versions descending, take the first one (highest RV, includes deleted).
	for dk, err := range s.Keys(ctx, key, SortOrderDesc) {
		if err != nil {
			return DataKey{}, err
		}
		return dk, nil
	}
	return DataKey{}, ErrNotFound
}

func (s *segmentDataStore) GetLatestAndPredecessor(ctx context.Context, key ListRequestKey) (DataKey, DataKey, error) {
	if err := key.Validate(); err != nil {
		return DataKey{}, DataKey{}, fmt.Errorf("invalid data key: %w", err)
	}
	if key.Group == "" || key.Resource == "" || key.Name == "" {
		return DataKey{}, DataKey{}, fmt.Errorf("group, resource or name is empty")
	}

	readers, err := s.openGroupResourceSegments(ctx, key.Group, key.Resource)
	if err != nil {
		return DataKey{}, DataKey{}, err
	}
	targetDocID := segmentDocID(DataKey{
		Group: key.Group, Resource: key.Resource,
		Namespace: key.Namespace, Name: key.Name,
	})

	allKeys, err := collectVersions(readers, targetDocID)
	if err != nil {
		return DataKey{}, DataKey{}, err
	}

	if len(allKeys) == 0 {
		return DataKey{}, DataKey{}, ErrNotFound
	}

	// Sort descending by rv — take top 2.
	slices.SortFunc(allKeys, func(a, b DataKey) int {
		return strings.Compare(b.String(), a.String()) // descending
	})

	latest := allKeys[0]
	var predecessor DataKey
	if len(allKeys) > 1 {
		predecessor = allKeys[1]
	}
	return latest, predecessor, nil
}

func (s *segmentDataStore) GetLatestResourceKey(ctx context.Context, key GetRequestKey) (DataKey, error) {
	return s.GetResourceKeyAtRevision(ctx, key, 0)
}

func (s *segmentDataStore) GetResourceKeyAtRevision(ctx context.Context, key GetRequestKey, rv int64) (DataKey, error) {
	if err := key.Validate(); err != nil {
		return DataKey{}, fmt.Errorf("invalid get request key: %w", err)
	}

	readers, err := s.openGroupResourceSegments(ctx, key.Group, key.Resource)
	if err != nil {
		return DataKey{}, err
	}
	targetDocID := segmentDocID(DataKey{
		Group: key.Group, Resource: key.Resource,
		Namespace: key.Namespace, Name: key.Name,
	})

	allKeys, err := collectVersions(readers, targetDocID)
	if err != nil {
		return DataKey{}, err
	}

	// Find highest rv that is non-deleted and within the rv cutoff.
	var best DataKey
	found := false
	for _, dk := range allKeys {
		if dk.Action == DataActionDeleted {
			continue
		}
		if rv > 0 && dk.ResourceVersion > rv {
			continue
		}
		if !found || dk.ResourceVersion > best.ResourceVersion {
			best = dk
			found = true
		}
	}

	if !found {
		return DataKey{}, ErrNotFound
	}
	return best, nil
}

func (s *segmentDataStore) ListLatestResourceKeys(ctx context.Context, key ListRequestKey) iter.Seq2[DataKey, error] {
	return s.ListResourceKeysAtRevision(ctx, ListRequestOptions{Key: key})
}

func (s *segmentDataStore) ListResourceKeysAtRevision(ctx context.Context, options ListRequestOptions) iter.Seq2[DataKey, error] {
	if err := options.Validate(); err != nil {
		return func(yield func(DataKey, error) bool) {
			yield(DataKey{}, err)
		}
	}

	rv := options.ResourceVersion
	if rv == 0 {
		rv = math.MaxInt64
	}

	// Build the Keys iteration key, applying continue cursor if present.
	listKey := options.Key
	if options.ContinueName != "" {
		listKey.Name = options.ContinueName
		if options.Key.Namespace == "" && options.ContinueNamespace != "" {
			listKey.Namespace = options.ContinueNamespace
		}
	}

	// Fast path: when listing latest (rv=MaxInt64), use _latest index.
	// Across compacted segments, only the latest non-deleted version per _id has _latest=true.
	// For uncompacted segments, multiple versions may have _latest=true — we still dedup
	// by _id, keeping the highest RV.
	if rv == math.MaxInt64 {
		return s.listLatest(ctx, listKey, options.ContinueName, options.ContinueNamespace)
	}

	// Slow path: version-at-rv — scan all versions.
	return s.listAtRevision(ctx, listKey, rv)
}

// listLatest uses the _latest postings as a filter while streaming the _id dictionary.
// After compaction, docs are sorted by _id in the segment, so we iterate in order
// without collecting into a map. For uncompacted segments with duplicate _latest entries,
// we fall back to dedup by _id.
func (s *segmentDataStore) listLatest(ctx context.Context, key ListRequestKey, continueName, continueNamespace string) iter.Seq2[DataKey, error] {
	return func(yield func(DataKey, error) bool) {
		readers, err := s.openGroupResourceSegments(ctx, key.Group, key.Resource)
		if err != nil {
			yield(DataKey{}, err)
			return
		}

		// Single-segment fast path: after compaction, there's typically one segment
		// with _latest set correctly (one per _id). Stream directly from _id dictionary
		// using _latest postings as a bitmap filter — no map, no sort.
		if len(readers) == 1 {
			s.listLatestSingleSegment(readers[0], key, continueName, continueNamespace, yield)
			return
		}

		// Multi-segment path: collect into map, dedup, sort.
		s.listLatestMultiSegment(readers, key, continueName, continueNamespace, yield)
	}
}

// listLatestSingleSegment streams results from one segment by walking the _id dictionary
// and checking each _id's postings against the _latest bitmap.
func (s *segmentDataStore) listLatestSingleSegment(
	reader *segment.SegmentReader,
	key ListRequestKey,
	continueName, continueNamespace string,
	yield func(DataKey, error) bool,
) {
	// Build a roaring bitmap of doc numbers with _latest=true for O(1) membership check.
	latestDict, err := reader.Dictionary("_latest")
	if err != nil || latestDict == nil {
		return // no _latest field — segment not compacted, return empty
	}
	latestPostings, err := latestDict.PostingsList([]byte("1"), nil, nil)
	if err != nil || latestPostings == nil {
		return
	}
	latestBitmap := roaring.New()
	lpIter := latestPostings.Iterator(false, false, false, nil)
	for {
		p, err := lpIter.Next()
		if err != nil {
			yield(DataKey{}, fmt.Errorf("build _latest bitmap: %w", err))
			return
		}
		if p == nil {
			break
		}
		latestBitmap.Add(uint32(p.Number()))
	}

	// Build _id range for namespace filtering.
	var startKey, endKey string
	if key.Namespace != "" {
		startKey = fmt.Sprintf("%s/%s/%s/", key.Group, key.Resource, key.Namespace)
	} else {
		startKey = fmt.Sprintf("%s/%s/", key.Group, key.Resource)
	}
	endKey = PrefixRangeEnd(startKey)

	// Apply continue cursor.
	if continueName != "" {
		ns := key.Namespace
		if ns == "" && continueNamespace != "" {
			ns = continueNamespace
		}
		cursorID := segmentDocID(DataKey{
			Group: key.Group, Resource: key.Resource,
			Namespace: ns, Name: continueName,
		})
		// Start just past the cursor. Since dictionary is sorted, we use cursorID+"\x00".
		if cursorID+"\x00" > startKey {
			startKey = cursorID + "\x00"
		}
	}

	// Iterate _id dictionary in sorted order.
	idDict, err := reader.Dictionary("_id")
	if err != nil || idDict == nil {
		return
	}

	iter := idDict.AutomatonIterator(&vellum.AlwaysMatch{}, []byte(startKey), []byte(endKey))
	for {
		entry, err := iter.Next()
		if err != nil {
			yield(DataKey{}, fmt.Errorf("iterate _id dictionary: %w", err))
			return
		}
		if entry == nil {
			break
		}

		// For this _id, find a posting that's in the _latest bitmap.
		idPostings, err := idDict.PostingsList([]byte(entry.Term), nil, nil)
		if err != nil {
			yield(DataKey{}, fmt.Errorf("get postings for %s: %w", entry.Term, err))
			return
		}

		pIter := idPostings.Iterator(false, false, false, nil)
		for {
			posting, err := pIter.Next()
			if err != nil {
				yield(DataKey{}, fmt.Errorf("iterate postings: %w", err))
				return
			}
			if posting == nil {
				break
			}

			if !latestBitmap.Contains(uint32(posting.Number())) {
				continue
			}

			dk, err := dataKeyFromSegment(reader, posting.Number())
			if err == errTombstone {
				continue
			}
			if err != nil {
				yield(DataKey{}, err)
				return
			}
			if dk.Action == DataActionDeleted {
				continue
			}
			if !yield(dk, nil) {
				return
			}
			break // found the latest for this _id, move to next
		}
	}
}

// listLatestMultiSegment collects _latest docs from multiple segments, deduplicates, and sorts.
func (s *segmentDataStore) listLatestMultiSegment(
	readers []*segment.SegmentReader,
	key ListRequestKey,
	continueName, continueNamespace string,
	yield func(DataKey, error) bool,
) {
	var idPrefix string
	if key.Namespace != "" {
		idPrefix = fmt.Sprintf("%s/%s/%s/", key.Group, key.Resource, key.Namespace)
	} else {
		idPrefix = fmt.Sprintf("%s/%s/", key.Group, key.Resource)
	}

	latestByID := make(map[string]DataKey)
	for _, reader := range readers {
		dict, err := reader.Dictionary("_latest")
		if err != nil || dict == nil {
			continue
		}
		postings, err := dict.PostingsList([]byte("1"), nil, nil)
		if err != nil || postings == nil {
			continue
		}

		pIter := postings.Iterator(false, false, false, nil)
		for {
			posting, err := pIter.Next()
			if err != nil {
				yield(DataKey{}, fmt.Errorf("iterate _latest postings: %w", err))
				return
			}
			if posting == nil {
				break
			}

			dk, err := dataKeyFromSegment(reader, posting.Number())
			if err == errTombstone {
				continue
			}
			if err != nil {
				yield(DataKey{}, err)
				return
			}

			docID := segmentDocID(dk)
			if !strings.HasPrefix(docID, idPrefix) {
				continue
			}
			if existing, ok := latestByID[docID]; !ok || dk.ResourceVersion > existing.ResourceVersion {
				latestByID[docID] = dk
			}
		}
	}

	ids := make([]string, 0, len(latestByID))
	for id := range latestByID {
		ids = append(ids, id)
	}
	slices.Sort(ids)

	var cursorID string
	if continueName != "" {
		ns := key.Namespace
		if ns == "" && continueNamespace != "" {
			ns = continueNamespace
		}
		cursorID = segmentDocID(DataKey{
			Group: key.Group, Resource: key.Resource,
			Namespace: ns, Name: continueName,
		})
	}

	for _, id := range ids {
		if cursorID != "" && id <= cursorID {
			continue
		}
		dk := latestByID[id]
		if dk.Action == DataActionDeleted {
			continue
		}
		if !yield(dk, nil) {
			return
		}
	}
}

// listAtRevision is the original slow path: scan all versions via Keys and dedup.
func (s *segmentDataStore) listAtRevision(ctx context.Context, listKey ListRequestKey, rv int64) iter.Seq2[DataKey, error] {
	return func(yield func(DataKey, error) bool) {
		var candidateKey *DataKey

		yieldCandidate := func() bool {
			if candidateKey.Action == DataActionDeleted {
				return true
			}
			return yield(*candidateKey, nil)
		}

		for dataKey, err := range s.Keys(ctx, listKey, SortOrderAsc) {
			if err != nil {
				yield(DataKey{}, err)
				return
			}

			if candidateKey == nil {
				if dataKey.ResourceVersion <= rv {
					candidateKey = &dataKey
				}
				continue
			}

			if !dataKey.SameResource(*candidateKey) || dataKey.ResourceVersion > rv {
				if !yieldCandidate() {
					return
				}
				if !dataKey.SameResource(*candidateKey) && dataKey.ResourceVersion <= rv {
					candidateKey = &dataKey
				} else {
					candidateKey = nil
				}
			} else {
				candidateKey = &dataKey
			}
		}
		if candidateKey != nil {
			yieldCandidate()
		}
	}
}

func (s *segmentDataStore) BatchGet(ctx context.Context, keys []DataKey) iter.Seq2[DataObj, error] {
	return func(yield func(DataObj, error) bool) {
		// Validate all keys first.
		for _, key := range keys {
			if err := validateDataKey(key); err != nil {
				yield(DataObj{}, fmt.Errorf("invalid data key %s: %w", key.String(), err))
				return
			}
		}

		// Group keys by (group, resource) so we open segments once per group/resource.
		type grKey struct{ group, resource string }
		grouped := make(map[grKey][]DataKey)
		for _, key := range keys {
			gk := grKey{key.Group, key.Resource}
			grouped[gk] = append(grouped[gk], key)
		}

		for gk, grKeys := range grouped {
			readers, err := s.openGroupResourceSegments(ctx, gk.group, gk.resource)
			if err != nil {
				yield(DataObj{}, err)
				return
			}

			for _, key := range grKeys {
				source, found := s.findDocAcrossSegments(readers, key)
				if found {
					if !yield(DataObj{
						Key:   key,
						Value: io.NopCloser(bytes.NewReader(source)),
					}, nil) {
							return
					}
				}
			}
			}
	}
}

// findDocAcrossSegments searches all readers for a document matching the key's doc ID and RV.
// Returns the _source bytes and whether the document was found.
func (s *segmentDataStore) findDocAcrossSegments(readers []*segment.SegmentReader, key DataKey) ([]byte, bool) {
	targetDocID := segmentDocID(key)
	for _, reader := range readers {
		source, found, err := s.findDocInSegment(reader, targetDocID, key.ResourceVersion)
		if err != nil {
			continue
		}
		if found {
			return source, true
		}
	}
	return nil, false
}

func (s *segmentDataStore) GetResourceStats(ctx context.Context, nsr NamespacedResource, minCount int) ([]ResourceStats, error) {
	groupResources, err := s.GetGroupResources(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get group resources: %w", err)
	}

	var stats []ResourceStats
	for _, gr := range groupResources {
		if nsr.Group != "" && gr.Group != nsr.Group {
			continue
		}
		if nsr.Resource != "" && gr.Resource != nsr.Resource {
			continue
		}

		listKey := ListRequestKey{
			Group:     gr.Group,
			Resource:  gr.Resource,
			Namespace: nsr.Namespace,
		}

		namespaceCounts := make(map[string]int64)
		namespaceVersions := make(map[string]int64)

		var currentResourceKey string
		var lastDataKey *DataKey

		processLastResource := func() {
			if lastDataKey != nil {
				if _, exists := namespaceVersions[lastDataKey.Namespace]; !exists {
					namespaceVersions[lastDataKey.Namespace] = 0
				}
				if lastDataKey.Action != DataActionDeleted {
					namespaceCounts[lastDataKey.Namespace]++
				}
				if lastDataKey.ResourceVersion > namespaceVersions[lastDataKey.Namespace] {
					namespaceVersions[lastDataKey.Namespace] = lastDataKey.ResourceVersion
				}
			}
		}

		for dataKey, err := range s.Keys(ctx, listKey, SortOrderAsc) {
			if err != nil {
				return nil, err
			}
			resourceKey := fmt.Sprintf("%s/%s/%s/%s", dataKey.Namespace, dataKey.Group, dataKey.Resource, dataKey.Name)
			if currentResourceKey != "" && resourceKey != currentResourceKey {
				processLastResource()
			}
			currentResourceKey = resourceKey
			lastDataKey = &dataKey
		}
		processLastResource()

		for ns, count := range namespaceCounts {
			if count <= int64(minCount) {
				continue
			}
			stats = append(stats, ResourceStats{
				NamespacedResource: NamespacedResource{
					Namespace: ns,
					Group:     gr.Group,
					Resource:  gr.Resource,
				},
				Count:           count,
				ResourceVersion: namespaceVersions[ns],
			})
		}

		// Reset for next group/resource.
		currentResourceKey = ""
		lastDataKey = nil
	}

	return stats, nil
}

func (s *segmentDataStore) BatchDelete(ctx context.Context, keys []DataKey) error {
	for _, key := range keys {
		if err := s.Delete(ctx, key); err != nil {
			return err
		}
	}
	return nil
}

func (s *segmentDataStore) GetGroupResources(ctx context.Context) ([]GroupResource, error) {
	// Scan manifest keys (format: {group}/{resource}/{rv}) and extract unique group/resource pairs.
	seen := make(map[string]bool)
	var results []GroupResource

	startKey := ""
	for {
		var foundKey string
		for key, err := range s.kv.Keys(ctx, manifestSection, ListOptions{
			StartKey: startKey,
			Limit:    1,
			Sort:     SortOrderAsc,
		}) {
			if err != nil {
				return nil, err
			}
			foundKey = key
			break
		}
		if foundKey == "" {
			break
		}

		// Parse {group}/{resource}/{rv}
		parts := strings.SplitN(foundKey, "/", 3)
		if len(parts) < 3 {
			startKey = foundKey + "\x00"
			continue
		}
		group, resource := parts[0], parts[1]
		grKey := group + "/" + resource
		if !seen[grKey] {
			seen[grKey] = true
			results = append(results, GroupResource{Group: group, Resource: resource})
		}

		// Jump past this entire group/resource prefix.
		startKey = PrefixRangeEnd(grKey + "/")
		if startKey == "" {
			break
		}
	}
	return results, nil
}

// grMutex returns a per-group/resource mutex for serializing compaction.
func (s *segmentDataStore) grMutex(group, resource string) *sync.Mutex {
	key := group + "/" + resource
	mu, _ := s.compactMu.LoadOrStore(key, &sync.Mutex{})
	return mu.(*sync.Mutex)
}

// CompactAll compacts all group/resources that have segments. Blocks until done.
func (s *segmentDataStore) CompactAll(ctx context.Context) error {
	grs, err := s.GetGroupResources(ctx)
	if err != nil {
		return err
	}
	for _, gr := range grs {
		if err := s.Compact(ctx, gr.Group, gr.Resource); err != nil {
			return err
		}
	}
	return nil
}

// Compact runs tiered merge planning for a group/resource and loops until
// the planner has no more work. Blocks until compaction is complete.
func (s *segmentDataStore) Compact(ctx context.Context, group, resource string) error {
	for {
		merged, err := s.compactOnce(ctx, group, resource)
		if err != nil {
			return err
		}
		if !merged {
			return nil
		}
	}
}

// maybeCompact is the background-triggered version of Compact with TryLock guard.
func (s *segmentDataStore) maybeCompact(ctx context.Context, group, resource string) error {
	mu := s.grMutex(group, resource)
	if !mu.TryLock() {
		return nil // compaction already running for this group/resource
	}
	defer mu.Unlock()

	grKey := group + "/" + resource
	pendingVal, _ := s.compactPending.LoadOrStore(grKey, &atomic.Bool{})
	pending := pendingVal.(*atomic.Bool)

	for {
		// Clear the pending flag before compacting. Any writes that arrive during
		// compaction will set it again, causing us to loop.
		pending.Store(false)

		merged, err := s.compactOnce(ctx, group, resource)
		if err != nil {
			return err
		}

		// If nothing was merged and no new writes arrived, we're done.
		if !merged && !pending.Load() {
			return nil
		}
	}
}

// compactOnce runs one round of merge planning and executes all resulting tasks.
// Returns true if any segments were merged (caller should loop).
func (s *segmentDataStore) compactOnce(ctx context.Context, group, resource string) (bool, error) {
	prefix := fmt.Sprintf("%s/%s/", group, resource)
	var segments []mergeplan.Segment
	segByKey := make(map[string]*planSegment)

	for manifestKey, err := range s.kv.Keys(ctx, manifestSection, ListOptions{
		StartKey: prefix,
		EndKey:   PrefixRangeEnd(prefix),
	}) {
		if err != nil {
			return false, fmt.Errorf("list manifest for compaction: %w", err)
		}

		parts := strings.Split(manifestKey, "/")
		if len(parts) < 3 {
			continue
		}
		rv, err := strconv.ParseUint(parts[len(parts)-1], 10, 64)
		if err != nil {
			continue
		}

		valReader, err := s.kv.Get(ctx, manifestSection, manifestKey)
		if err != nil {
			continue
		}
		valData, err := io.ReadAll(valReader)
		_ = valReader.Close()
		if err != nil {
			continue
		}
		_, segSize := decodeManifestValue(valData)
		if segSize <= 0 {
			segSize = s.mergePlanOpts.FloorSegmentSize
		}

		ps := &planSegment{manifestKey: manifestKey, rv: rv, size: segSize}
		segments = append(segments, ps)
		segByKey[manifestKey] = ps
	}

	if len(segments) <= 1 {
		return false, nil
	}

	plan, err := mergeplan.Plan(segments, &s.mergePlanOpts)
	if err != nil {
		return false, fmt.Errorf("merge plan: %w", err)
	}
	if plan == nil || len(plan.Tasks) == 0 {
		return false, nil
	}

	merged := false
	for _, task := range plan.Tasks {
		if len(task.Segments) < 2 {
			continue
		}
		if err := s.executeCompaction(ctx, task.Segments, segByKey); err != nil {
			return false, fmt.Errorf("compaction task: %w", err)
		}
		merged = true
	}

	return merged, nil
}

// executeCompaction merges the segments in a single merge task.
func (s *segmentDataStore) executeCompaction(ctx context.Context, toMerge []mergeplan.Segment, segByKey map[string]*planSegment) error {
	// Open all source segment readers.
	readers := make([]*segment.SegmentReader, 0, len(toMerge))
	manifestKeys := make([]string, 0, len(toMerge))
	segmentKeys := make([]string, 0, len(toMerge))
	var highestRV uint64

	for _, seg := range toMerge {
		ps := segByKey[seg.(*planSegment).manifestKey]
		segKey := ps.manifestKey + ".zap"

		reader, err := s.openSegment(ctx, segKey)
		if err != nil {
			return fmt.Errorf("open segment %s for compaction: %w", segKey, err)
		}
		readers = append(readers, reader)
		manifestKeys = append(manifestKeys, ps.manifestKey)
		segmentKeys = append(segmentKeys, segKey)
		if ps.rv > highestRV {
			highestRV = ps.rv
		}
	}

	// Rebuild segment with correct _latest flags.
	mergedSeg, err := segment.RebuildWithLatest(ctx, s.builder, readers, highestRV)
	if err != nil {
		return fmt.Errorf("rebuild: %w", err)
	}
	if mergedSeg == nil {
		// All docs were tombstones.
		return s.deleteCompactedSegments(ctx, manifestKeys, segmentKeys)
	}

	// Write merged segment to KV under the highest RV key.
	mergedSegKey := fmt.Sprintf("%s/%s/%d.zap", strings.Split(manifestKeys[0], "/")[0], strings.Split(manifestKeys[0], "/")[1], highestRV)
	mergedManifestKey := fmt.Sprintf("%s/%s/%d", strings.Split(manifestKeys[0], "/")[0], strings.Split(manifestKeys[0], "/")[1], highestRV)

	if err := s.writeKV(ctx, segmentsSection, mergedSegKey, mergedSeg.Data); err != nil {
		return fmt.Errorf("write merged segment: %w", err)
	}
	if err := s.writeKV(ctx, manifestSection, mergedManifestKey, encodeManifestValue(1, int64(len(mergedSeg.Data)))); err != nil {
		return fmt.Errorf("write merged manifest: %w", err)
	}

	// Invalidate cached readers for the merged key so next read picks up the new data.
	s.cache.Delete(mergedSegKey)

	// Delete old segments (excluding the one we just overwrote if highestRV was a source).
	var oldManifestKeys []string
	var oldSegmentKeys []string
	for i, mk := range manifestKeys {
		if mk == mergedManifestKey {
			continue // already overwritten
		}
		oldManifestKeys = append(oldManifestKeys, mk)
		oldSegmentKeys = append(oldSegmentKeys, segmentKeys[i])
	}

	return s.deleteCompactedSegments(ctx, oldManifestKeys, oldSegmentKeys)
}

// deleteCompactedSegments removes old manifest and segment entries after compaction.
func (s *segmentDataStore) deleteCompactedSegments(ctx context.Context, manifestKeys, segmentKeys []string) error {
	for _, mk := range manifestKeys {
		if err := s.kv.Delete(ctx, manifestSection, mk); err != nil {
			return fmt.Errorf("delete manifest %s: %w", mk, err)
		}
	}
	for _, sk := range segmentKeys {
		if err := s.kv.Delete(ctx, segmentsSection, sk); err != nil {
			return fmt.Errorf("delete segment %s: %w", sk, err)
		}
		// Don't cache.Remove here — concurrent readers may still hold the reader.
		// Old entries age out via LRU naturally.
	}
	return nil
}

func (s *segmentDataStore) ApplyBackwardsCompatibleChanges(_ context.Context, _ db.Tx, _ WriteEvent, _ DataKey) error {
	panic("segmentDataStore does not support ApplyBackwardsCompatibleChanges")
}

func (s *segmentDataStore) DeleteLegacyResourceCollection(_ context.Context, _ db.ContextExecer, _, _, _ string) error {
	panic("segmentDataStore does not support DeleteLegacyResourceCollection")
}

func (s *segmentDataStore) UpdateLegacyResourceHistoryBulk(_ context.Context, _ db.ContextExecer, _ DataKey, _, _, _ int64) error {
	panic("segmentDataStore does not support UpdateLegacyResourceHistoryBulk")
}

func (s *segmentDataStore) SyncLegacyResourceFromHistory(_ context.Context, _ db.ContextExecer, _, _, _ string) error {
	panic("segmentDataStore does not support SyncLegacyResourceFromHistory")
}
