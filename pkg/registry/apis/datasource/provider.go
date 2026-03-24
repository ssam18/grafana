package datasource

import (
	"context"
	"encoding/json"
	"fmt"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/client-go/dynamic"
	"k8s.io/utils/ptr"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana/apps/secret/pkg/decrypt"
	"github.com/grafana/grafana/pkg/apimachinery/utils"
	"github.com/grafana/grafana/pkg/apis/datasource/v0alpha1"
	gaputils "github.com/grafana/grafana/pkg/services/apiserver/utils"
)

type datasourceProvider struct {
	gvr            schema.GroupVersionResource
	pluginID       string // the plugin type
	configProvider gaputils.RestConfigProvider
	decrypter      decrypt.DecryptService
}

func NewDatasourceProvider(
	gvr schema.GroupVersionResource,
	pluginID string,
	configProvider gaputils.RestConfigProvider,
	decrypter decrypt.DecryptService) *datasourceProvider {
	return &datasourceProvider{gvr, pluginID, configProvider, decrypter}
}

var (
	_ PluginDatasourceProvider = (*datasourceProvider)(nil)
)

func (d *datasourceProvider) client(ctx context.Context) (dynamic.ResourceInterface, error) {
	ns, ok := request.NamespaceFrom(ctx)
	if !ok {
		return nil, fmt.Errorf("missing namespace in context")
	}
	cfg, err := d.configProvider.GetRestConfig(ctx)
	if err != nil {
		return nil, err
	}
	client, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return client.Resource(d.gvr).Namespace(ns), nil
}

// CreateDataSource implements [PluginDatasourceProvider].
func (d *datasourceProvider) CreateDataSource(ctx context.Context, ds *v0alpha1.DataSource) (*v0alpha1.DataSource, error) {
	return d.doCreateOrUpdate(ctx, ds, false)
}

// UpdateDataSource implements [PluginDatasourceProvider].
func (d *datasourceProvider) UpdateDataSource(ctx context.Context, ds *v0alpha1.DataSource) (*v0alpha1.DataSource, error) {
	return d.doCreateOrUpdate(ctx, ds, true)
}

func (d *datasourceProvider) doCreateOrUpdate(ctx context.Context, ds *v0alpha1.DataSource, update bool) (*v0alpha1.DataSource, error) {
	obj, err := ds.ToUnstructured()
	if err != nil {
		return nil, err
	}
	client, err := d.client(ctx)
	if err != nil {
		return nil, err
	}
	if update {
		obj, err = client.Update(ctx, obj, v1.UpdateOptions{})
	} else {
		if obj.GetName() == "" && obj.GetGenerateName() == "" {
			obj.SetGenerateName("ds") // name prefix
		}
		obj, err = client.Create(ctx, obj, v1.CreateOptions{})
	}
	if err != nil {
		return nil, err
	}
	return v0alpha1.FromUnstructured(obj)
}

// DeleteDataSource implements [PluginDatasourceProvider].
func (d *datasourceProvider) DeleteDataSource(ctx context.Context, uid string) error {
	client, err := d.client(ctx)
	if err != nil {
		return err
	}
	return client.Delete(ctx, uid, v1.DeleteOptions{})
}

// GetDataSource implements [PluginDatasourceProvider].
func (d *datasourceProvider) GetDataSource(ctx context.Context, uid string) (*v0alpha1.DataSource, error) {
	client, err := d.client(ctx)
	if err != nil {
		return nil, err
	}
	obj, err := client.Get(ctx, uid, v1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return v0alpha1.FromUnstructured(obj)
}

// GetInstanceSettings implements [PluginDatasourceProvider].
func (d *datasourceProvider) GetInstanceSettings(ctx context.Context, uid string) (*backend.DataSourceInstanceSettings, error) {
	ns, ok := request.NamespaceFrom(ctx)
	if !ok {
		return nil, fmt.Errorf("missing namespace in context")
	}
	ds, err := d.GetDataSource(ctx, uid)
	if err != nil {
		return nil, err
	}
	meta, err := utils.MetaAccessor(ds)
	if err != nil {
		return nil, err
	}

	data, err := json.Marshal(ds.Spec.JSONData())
	if err != nil {
		return nil, fmt.Errorf("unable to marshal json data %w", err)
	}

	secure := make(map[string]string, len(ds.Secure))
	if len(ds.Secure) > 0 {
		names := make([]string, 0, len(ds.Secure))
		for _, v := range ds.Secure {
			if v.Name == "" {
				return nil, fmt.Errorf("secure value missing name")
			}
			names = append(names, v.Name)
		}
		decrypted, err := d.decrypter.Decrypt(ctx, d.gvr.Group, ns, names...)
		if err != nil {
			return nil, err
		}
		for k, v := range ds.Secure {
			found, ok := decrypted[v.Name]
			if !ok {
				return nil, fmt.Errorf("unable to decrypt value for: %s", v.Name)
			}
			if err = found.Error(); err != nil {
				return nil, err
			}
			secure[k] = string(*found.Value()) // don't use DangerouslyExpose... that only allows one access
		}
	}

	ts, _ := meta.GetUpdatedTimestamp()
	if ts == nil {
		ts = ptr.To(meta.GetCreationTimestamp().Time)
	}

	return &backend.DataSourceInstanceSettings{
		ID:                      meta.GetDeprecatedInternalID(), //nolint:staticcheck
		UID:                     uid,
		Updated:                 *ts, // creation of update time
		Name:                    ds.Spec.Title(),
		Type:                    d.pluginID,
		URL:                     ds.Spec.URL(),
		User:                    ds.Spec.User(),
		Database:                ds.Spec.Database(),
		BasicAuthEnabled:        ds.Spec.BasicAuth(),
		BasicAuthUser:           ds.Spec.BasicAuthUser(),
		JSONData:                data,
		DecryptedSecureJSONData: secure,
		APIVersion:              d.gvr.Version, // Just the version part
	}, nil
}

// ListDataSources implements [PluginDatasourceProvider].
func (d *datasourceProvider) ListDataSources(ctx context.Context) (*v0alpha1.DataSourceList, error) {
	client, err := d.client(ctx)
	if err != nil {
		return nil, err
	}
	found, err := client.List(ctx, v1.ListOptions{Limit: 2500})
	if err != nil {
		return nil, err
	}
	rsp := &v0alpha1.DataSourceList{
		ListMeta: v1.ListMeta{
			ResourceVersion: found.GetResourceVersion(),
		},
		Items: make([]v0alpha1.DataSource, 0, len(found.Items)),
	}
	for _, v := range found.Items {
		ds, err := v0alpha1.FromUnstructured(&v)
		if err != nil {
			return nil, err
		}
		rsp.Items = append(rsp.Items, *ds)
	}
	return rsp, nil
}
