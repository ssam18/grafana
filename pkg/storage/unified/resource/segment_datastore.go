package resource

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"iter"
	"sync"

	"github.com/blevesearch/bleve/v2/analysis"
	"github.com/blevesearch/bleve/v2/document"
	"github.com/blevesearch/bleve/v2/registry"
	index "github.com/blevesearch/bleve_index_api"

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
// Uses the same format as the KV datastore's DataKey.String() so that manifest keys
// are sorted the same way and prefix scanning works identically.
func manifestKVKey(key DataKey) string {
	return key.String()
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
	// resource_version — numeric, indexed + stored
	doc.AddField(document.NewNumericFieldWithIndexingOptions("resource_version", nil, float64(key.ResourceVersion), index.IndexField|index.StoreField))
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

func (s *segmentDataStore) Get(ctx context.Context, key DataKey) (io.ReadCloser, error) {
	// Check manifest entry exists.
	mReader, err := s.kv.Get(ctx, manifestSection, manifestKVKey(key))
	if err != nil {
		return nil, err // returns ErrNotFound if deleted
	}
	_ = mReader.Close()

	// Read the segment .zap data from KV.
	zapReader, err := s.kv.Get(ctx, segmentsSection, segmentKVKey(key))
	if err != nil {
		return nil, err
	}
	zapData, err := io.ReadAll(zapReader)
	_ = zapReader.Close()
	if err != nil {
		return nil, fmt.Errorf("failed to read segment data: %w", err)
	}

	// Open the segment.
	seg := &segment.Segment{
		Data:    zapData,
		NumDocs: 1,
		TxnID:   uint64(key.ResourceVersion),
	}
	reader, err := segment.NewSegmentReader(seg)
	if err != nil {
		return nil, fmt.Errorf("failed to create segment reader: %w", err)
	}
	if err := reader.Open(); err != nil {
		return nil, fmt.Errorf("failed to open segment: %w", err)
	}
	defer reader.Close()

	// Visit stored fields on doc 0 to find _source.
	var source []byte
	err = reader.VisitStoredFields(0, func(field string, typ byte, value []byte, _ []uint64) bool {
		if field == "_source" {
			source = make([]byte, len(value))
			copy(source, value)
			return false // stop visiting
		}
		return true
	})
	if err != nil {
		return nil, fmt.Errorf("failed to read stored fields: %w", err)
	}
	if source == nil {
		return nil, fmt.Errorf("segment missing _source field")
	}

	return io.NopCloser(bytes.NewReader(source)), nil
}

func (s *segmentDataStore) Delete(ctx context.Context, key DataKey) error {
	// Remove the manifest entry. Segment data becomes orphaned (janitor responsibility).
	return s.kv.Delete(ctx, manifestSection, manifestKVKey(key))
}

// --- Unimplemented methods below ---

func (s *segmentDataStore) Keys(ctx context.Context, key ListRequestKey, sort SortOrder) iter.Seq2[DataKey, error] {
	return func(yield func(DataKey, error) bool) {
		yield(DataKey{}, fmt.Errorf("not implemented: Keys"))
	}
}

func (s *segmentDataStore) LastResourceVersion(ctx context.Context, key ListRequestKey) (DataKey, error) {
	return DataKey{}, fmt.Errorf("not implemented: LastResourceVersion")
}

func (s *segmentDataStore) GetLatestAndPredecessor(ctx context.Context, key ListRequestKey) (DataKey, DataKey, error) {
	return DataKey{}, DataKey{}, fmt.Errorf("not implemented: GetLatestAndPredecessor")
}

func (s *segmentDataStore) GetLatestResourceKey(ctx context.Context, key GetRequestKey) (DataKey, error) {
	return DataKey{}, fmt.Errorf("not implemented: GetLatestResourceKey")
}

func (s *segmentDataStore) GetResourceKeyAtRevision(ctx context.Context, key GetRequestKey, rv int64) (DataKey, error) {
	return DataKey{}, fmt.Errorf("not implemented: GetResourceKeyAtRevision")
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
