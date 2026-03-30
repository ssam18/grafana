package resource

import (
	"context"
	"testing"
)

func TestIntegrationSegmentDataStore(t *testing.T) {
	implemented := []struct {
		name string
		fn   func(*testing.T, context.Context, DataStore)
	}{
		{"Save_And_Get", testDataStoreSaveAndGet},
		{"Delete", testDataStoreDelete},
		{"Keys", testDataStoreKeys},
		{"GetLatestAndPredecessor", testDataStoreGetLatestAndPredecessor},
		{"GetLatestResourceKey", testDataStoreGetLatestResourceKey},
		{"GetLatestResourceKey_Deleted", testDataStoreGetLatestResourceKeyDeleted},
		{"GetLatestResourceKey_NotFound", testDataStoreGetLatestResourceKeyNotFound},
		{"GetResourceKeyAtRevision", testDataStoreGetResourceKeyAtRevision},
		{"BatchGet", testDataStoreBatchGet},
	}

	for _, tt := range implemented {
		t.Run(tt.name, func(t *testing.T) {
			ds := setupTestSegmentDataStore(t)
			tt.fn(t, t.Context(), ds)
		})
	}

	notImplemented := []struct {
		name string
		fn   func(*testing.T, context.Context, DataStore)
	}{
		{"List", testDataStoreList},
		{"LastResourceVersion", testDataStoreLastResourceVersion},
		{"ListLatestResourceKeys", testDataStoreListLatestResourceKeys},
		{"ListLatestResourceKeys_Deleted", testDataStoreListLatestResourceKeysDeleted},
		{"ListLatestResourceKeys_Multiple", testDataStoreListLatestResourceKeysMultiple},
		{"ListResourceKeysAtRevision", testDataStoreListResourceKeysAtRevision},
		{"ListResourceKeysAtRevision_EmptyResults", testDataStoreListResourceKeysAtRevisionEmptyResults},
		{"ListResourceKeysAtRevision_ResourcesNewerThanRevision", testDataStoreListResourceKeysAtRevisionResourcesNewerThanRevision},
		{"GetResourceStats_Comprehensive", testDataStoreGetResourceStatsComprehensive},
	}

	for _, tt := range notImplemented {
		t.Run(tt.name, func(t *testing.T) {
			t.Skip("not yet implemented")
		})
	}
}
