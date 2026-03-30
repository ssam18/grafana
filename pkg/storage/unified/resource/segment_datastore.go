package resource

import (
	"bytes"
	"container/heap"
	"context"
	"fmt"
	"io"
	"iter"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/blevesearch/bleve/v2/analysis"
	"github.com/blevesearch/bleve/v2/document"
	"github.com/blevesearch/bleve/v2/registry"
	index "github.com/blevesearch/bleve_index_api"
	segment_api "github.com/blevesearch/scorch_segment_api/v2"
	"github.com/blevesearch/vellum"

	kvpkg "github.com/grafana/grafana/pkg/storage/unified/resource/kv"
	"github.com/grafana/grafana/pkg/storage/unified/resource/segment"
	"github.com/grafana/grafana/pkg/storage/unified/sql/db"

	// Register the keyword analyzer in the bleve registry.
	_ "github.com/blevesearch/bleve/v2/analysis/analyzer/keyword"
)

const (
	segmentsSection = "unified/segments"
	manifestSection = "unified/manifest"
)

var _ DataStore = &segmentDataStore{}

// errTombstone is a sentinel error returned by dataKeyFromSegment when the document
// is a tombstone. Callers should skip the document rather than propagating the error.
var errTombstone = fmt.Errorf("tombstone")

// segmentDataStore is a DataStore backed by Zap segments stored in a KV store.
type segmentDataStore struct {
	kv      KV // KV store for segments + manifest
	builder *segment.SegmentBuilder
}

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
	return &segmentDataStore{kv: kv, builder: builder}
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
	// _tombstone marker — presence alone signals this doc is a tombstone.
	doc.AddField(document.NewTextFieldWithIndexingOptions("_tombstone", nil, []byte("1"), index.StoreField))

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

	// Write .zap to KV.
	zapWriter, err := s.kv.Save(ctx, segmentsSection, segmentKVKey(key))
	if err != nil {
		return fmt.Errorf("failed to save segment: %w", err)
	}
	if _, err := zapWriter.Write(seg.Data); err != nil {
		_ = zapWriter.Close()
		return fmt.Errorf("failed to write segment data: %w", err)
	}
	if err := zapWriter.Close(); err != nil {
		return fmt.Errorf("failed to close segment writer: %w", err)
	}

	// Write manifest entry. The key carries all the metadata; value is a placeholder.
	manifestWriter, err := s.kv.Save(ctx, manifestSection, manifestKVKey(key))
	if err != nil {
		return fmt.Errorf("failed to save manifest entry: %w", err)
	}
	if _, err := manifestWriter.Write([]byte{1}); err != nil {
		_ = manifestWriter.Close()
		return fmt.Errorf("failed to write manifest entry: %w", err)
	}
	if err := manifestWriter.Close(); err != nil {
		return fmt.Errorf("failed to close manifest writer: %w", err)
	}

	return nil
}

// openSegment reads a segment from the KV store and returns an opened reader.
// Caller must close the returned reader.
func (s *segmentDataStore) openSegment(ctx context.Context, segKey string) (*segment.SegmentReader, error) {
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
	return reader, nil
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
		reader.Close()
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
	zapWriter, err := s.kv.Save(ctx, segmentsSection, segmentKVKey(key))
	if err != nil {
		return fmt.Errorf("failed to save tombstone segment: %w", err)
	}
	if _, err := zapWriter.Write(seg.Data); err != nil {
		_ = zapWriter.Close()
		return fmt.Errorf("failed to write tombstone segment data: %w", err)
	}
	if err := zapWriter.Close(); err != nil {
		return fmt.Errorf("failed to close tombstone segment writer: %w", err)
	}

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
		defer func() {
			for _, r := range readers {
				r.Close()
			}
		}()

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
			for _, r := range readers {
				r.Close()
			}
			return nil, err
		}

		segKey := manifestKey + ".zap"
		reader, err := s.openSegment(ctx, segKey)
		if err != nil {
			for _, r := range readers {
				r.Close()
			}
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
	return DataKey{}, fmt.Errorf("not implemented: LastResourceVersion")
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
	defer func() {
		for _, r := range readers {
			r.Close()
		}
	}()

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
	defer func() {
		for _, r := range readers {
			r.Close()
		}
	}()

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
	return func(yield func(DataKey, error) bool) {
		yield(DataKey{}, fmt.Errorf("not implemented: ListLatestResourceKeys"))
	}
}

func (s *segmentDataStore) ListResourceKeysAtRevision(ctx context.Context, options ListRequestOptions) iter.Seq2[DataKey, error] {
	return func(yield func(DataKey, error) bool) {
		yield(DataKey{}, fmt.Errorf("not implemented: ListResourceKeysAtRevision"))
	}
}

func (s *segmentDataStore) BatchGet(ctx context.Context, keys []DataKey) iter.Seq2[DataObj, error] {
	return func(yield func(DataObj, error) bool) {
		yield(DataObj{}, fmt.Errorf("not implemented: BatchGet"))
	}
}

func (s *segmentDataStore) GetResourceStats(ctx context.Context, nsr NamespacedResource, minCount int) ([]ResourceStats, error) {
	return nil, fmt.Errorf("not implemented: GetResourceStats")
}

func (s *segmentDataStore) BatchDelete(ctx context.Context, keys []DataKey) error {
	return fmt.Errorf("not implemented: BatchDelete")
}

func (s *segmentDataStore) GetGroupResources(ctx context.Context) ([]GroupResource, error) {
	return nil, fmt.Errorf("not implemented: GetGroupResources")
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
