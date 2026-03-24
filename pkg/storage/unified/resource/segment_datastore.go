package resource

import (
	"context"
	"fmt"
	"io"
	"iter"
)

var _ DataStore = &segmentDataStore{}

// segmentDataStore is a DataStore backed by Zap segments stored in a KV store.
type segmentDataStore struct {
	kv KV // KV store for segments + manifest
	// segments *SegmentStore  // LRU cache — added when segment infra is ready
	// builders map[schema.GroupResource]*DocumentBuilderInfo // added when indexing is wired
}

func newSegmentDataStore(kv KV) *segmentDataStore {
	return &segmentDataStore{kv: kv}
}

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

func (s *segmentDataStore) Get(ctx context.Context, key DataKey) (io.ReadCloser, error) {
	return nil, fmt.Errorf("not implemented: Get")
}

func (s *segmentDataStore) BatchGet(ctx context.Context, keys []DataKey) iter.Seq2[DataObj, error] {
	return func(yield func(DataObj, error) bool) {
		yield(DataObj{}, fmt.Errorf("not implemented: BatchGet"))
	}
}

func (s *segmentDataStore) Save(ctx context.Context, key DataKey, value io.Reader) error {
	return fmt.Errorf("not implemented: Save")
}

func (s *segmentDataStore) Delete(ctx context.Context, key DataKey) error {
	return fmt.Errorf("not implemented: Delete")
}

func (s *segmentDataStore) GetResourceStats(ctx context.Context, nsr NamespacedResource, minCount int) ([]ResourceStats, error) {
	return nil, fmt.Errorf("not implemented: GetResourceStats")
}
