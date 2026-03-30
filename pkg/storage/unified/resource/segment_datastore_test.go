package resource

import (
	"bytes"
	"context"
	"testing"

	kv "github.com/grafana/grafana/pkg/storage/unified/resource/kv"
	"github.com/stretchr/testify/require"
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
		{"GetResourceStats_Comprehensive", testDataStoreGetResourceStatsComprehensive},
		{"ListLatestResourceKeys", testDataStoreListLatestResourceKeys},
		{"ListLatestResourceKeys_Deleted", testDataStoreListLatestResourceKeysDeleted},
		{"ListLatestResourceKeys_Multiple", testDataStoreListLatestResourceKeysMultiple},
		// Segment-specific variant: uses unique RVs per resource (segment keys are keyed by RV).
		{"ListResourceKeysAtRevision", testSegmentDataStoreListResourceKeysAtRevision},
		{"ListResourceKeysAtRevision_EmptyResults", testDataStoreListResourceKeysAtRevisionEmptyResults},
		{"ListResourceKeysAtRevision_ResourcesNewerThanRevision", testDataStoreListResourceKeysAtRevisionResourcesNewerThanRevision},
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
	}

	for _, tt := range notImplemented {
		t.Run(tt.name, func(t *testing.T) {
			t.Skip("not yet implemented")
		})
	}
}

// testSegmentDataStoreListResourceKeysAtRevision is a segment-specific variant of
// testDataStoreListResourceKeysAtRevision. The shared test assigns the same RV to
// resource2 and resource4, which is fine for the KV datastore (keys include namespace/name)
// but collides in the segment datastore (segment keys are {group}/{resource}/{rv}).
// In production, RVs are globally unique snowflake IDs so this never happens.
// This variant uses unique RVs for every Save call while testing the same logic.
func testSegmentDataStoreListResourceKeysAtRevision(t *testing.T, ctx context.Context, ds DataStore) {
	// Generate 6 unique RVs in chronological order.
	rv1 := node.Generate().Int64()
	rv2 := node.Generate().Int64() // resource2 created
	rv2b := node.Generate().Int64() // resource4 created (unique, unlike shared test)
	rv3 := node.Generate().Int64()
	rv4 := node.Generate().Int64()
	rv5 := node.Generate().Int64()

	// Resource 1: Created at rv1, updated at rv3
	key1 := DataKey{
		Group: "apps", Resource: "resources", Namespace: "default",
		Name: "resource1", ResourceVersion: rv1,
		Action: DataActionCreated, Folder: "test-folder",
	}
	require.NoError(t, ds.Save(ctx, key1, bytes.NewReader([]byte("resource1-v1"))))

	key1Updated := key1
	key1Updated.ResourceVersion = rv3
	key1Updated.Action = DataActionUpdated
	require.NoError(t, ds.Save(ctx, key1Updated, bytes.NewReader([]byte("resource1-v2"))))

	// Resource 2: Created at rv2
	key2 := DataKey{
		Group: "apps", Resource: "resources", Namespace: "default",
		Name: "resource2", ResourceVersion: rv2,
		Action: DataActionCreated, Folder: "test-folder",
	}
	require.NoError(t, ds.Save(ctx, key2, bytes.NewReader([]byte("resource2-v1"))))

	// Resource 3: Created at rv4
	key3 := DataKey{
		Group: "apps", Resource: "resources", Namespace: "default",
		Name: "resource3", ResourceVersion: rv4,
		Action: DataActionCreated, Folder: "test-folder",
	}
	require.NoError(t, ds.Save(ctx, key3, bytes.NewReader([]byte("resource3-v1"))))

	// Resource 4: Created at rv2b, deleted at rv5
	key4 := DataKey{
		Group: "apps", Resource: "resources", Namespace: "default",
		Name: "resource4", ResourceVersion: rv2b,
		Action: DataActionCreated, Folder: "test-folder",
	}
	require.NoError(t, ds.Save(ctx, key4, bytes.NewReader([]byte("resource4-v1"))))

	key4Deleted := key4
	key4Deleted.ResourceVersion = rv5
	key4Deleted.Action = DataActionDeleted
	require.NoError(t, ds.Save(ctx, key4Deleted, bytes.NewReader([]byte("resource4-deleted"))))

	listKey := ListRequestKey{
		Group: "apps", Resource: "resources", Namespace: "default",
	}

	t.Run("list at revision rv1 - should return only resource1 initial version", func(t *testing.T) {
		var resultKeys []DataKey
		for dataKey, err := range ds.ListResourceKeysAtRevision(ctx, ListRequestOptions{Key: listKey, ResourceVersion: rv1}) {
			require.NoError(t, err)
			resultKeys = append(resultKeys, dataKey)
		}
		require.Len(t, resultKeys, 1)
		require.Equal(t, "resource1", resultKeys[0].Name)
		require.Equal(t, rv1, resultKeys[0].ResourceVersion)
		require.Equal(t, DataActionCreated, resultKeys[0].Action)
	})

	t.Run("list at revision rv2b - should return resource1, resource2 and resource4", func(t *testing.T) {
		var resultKeys []DataKey
		for dataKey, err := range ds.ListResourceKeysAtRevision(ctx, ListRequestOptions{Key: listKey, ResourceVersion: rv2b}) {
			require.NoError(t, err)
			resultKeys = append(resultKeys, dataKey)
		}
		require.Len(t, resultKeys, 3)
		names := make(map[string]int64)
		for _, result := range resultKeys {
			names[result.Name] = result.ResourceVersion
		}
		require.Equal(t, rv1, names["resource1"])
		require.Equal(t, rv2, names["resource2"])
		require.Equal(t, rv2b, names["resource4"])
	})

	t.Run("list at revision rv3 - should return resource1, resource2 and resource4", func(t *testing.T) {
		var resultKeys []DataKey
		for dataKey, err := range ds.ListResourceKeysAtRevision(ctx, ListRequestOptions{Key: listKey, ResourceVersion: rv3}) {
			require.NoError(t, err)
			resultKeys = append(resultKeys, dataKey)
		}
		require.Len(t, resultKeys, 3)
		names := make(map[string]int64)
		actions := make(map[string]kv.DataAction)
		for _, result := range resultKeys {
			names[result.Name] = result.ResourceVersion
			actions[result.Name] = result.Action
		}
		require.Equal(t, rv3, names["resource1"])
		require.Equal(t, DataActionUpdated, actions["resource1"])
		require.Equal(t, rv2, names["resource2"])
		require.Equal(t, rv2b, names["resource4"])
	})

	t.Run("list at revision rv4 - should return all resources", func(t *testing.T) {
		var resultKeys []DataKey
		for dataKey, err := range ds.ListResourceKeysAtRevision(ctx, ListRequestOptions{Key: listKey, ResourceVersion: rv4}) {
			require.NoError(t, err)
			resultKeys = append(resultKeys, dataKey)
		}
		require.Len(t, resultKeys, 4)
		names := make(map[string]int64)
		for _, result := range resultKeys {
			names[result.Name] = result.ResourceVersion
		}
		require.Equal(t, rv3, names["resource1"])
		require.Equal(t, rv2, names["resource2"])
		require.Equal(t, rv4, names["resource3"])
		require.Equal(t, rv2b, names["resource4"])
	})

	t.Run("list at revision rv5 - should exclude deleted resource4", func(t *testing.T) {
		var resultKeys []DataKey
		for dataKey, err := range ds.ListResourceKeysAtRevision(ctx, ListRequestOptions{Key: listKey, ResourceVersion: rv5}) {
			require.NoError(t, err)
			resultKeys = append(resultKeys, dataKey)
		}
		require.Len(t, resultKeys, 3)
		names := make(map[string]bool)
		for _, result := range resultKeys {
			names[result.Name] = true
		}
		require.True(t, names["resource1"])
		require.True(t, names["resource2"])
		require.True(t, names["resource3"])
		require.False(t, names["resource4"])
	})

	t.Run("list with specific resource name", func(t *testing.T) {
		specificListKey := ListRequestKey{
			Group: "apps", Resource: "resources", Namespace: "default", Name: "resource1",
		}
		var resultKeys []DataKey
		for dataKey, err := range ds.ListResourceKeysAtRevision(ctx, ListRequestOptions{Key: specificListKey, ResourceVersion: rv3}) {
			require.NoError(t, err)
			resultKeys = append(resultKeys, dataKey)
		}
		require.Len(t, resultKeys, 1)
		require.Equal(t, "resource1", resultKeys[0].Name)
		require.Equal(t, rv3, resultKeys[0].ResourceVersion)
		require.Equal(t, DataActionUpdated, resultKeys[0].Action)
	})

	t.Run("list at revision 0 should use MaxInt64", func(t *testing.T) {
		var resultKeys []DataKey
		for dataKey, err := range ds.ListResourceKeysAtRevision(ctx, ListRequestOptions{Key: listKey, ResourceVersion: 0}) {
			require.NoError(t, err)
			resultKeys = append(resultKeys, dataKey)
		}
		require.Len(t, resultKeys, 3)
		names := make(map[string]bool)
		for _, result := range resultKeys {
			names[result.Name] = true
		}
		require.True(t, names["resource1"])
		require.True(t, names["resource2"])
		require.True(t, names["resource3"])
		require.False(t, names["resource4"])
	})
}
