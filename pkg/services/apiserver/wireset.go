package apiserver

import (
	"github.com/google/wire"

	"github.com/grafana/grafana/pkg/services/apiserver/builder"
	"github.com/grafana/grafana/pkg/services/apiserver/utils"
)

var WireSet = wire.NewSet(
	builder.ProvideBuilderMetrics,
	ProvideEventualRestConfigProvider,
	wire.Bind(new(utils.RestConfigProvider), new(*eventualRestConfigProvider)),
	wire.Bind(new(DirectRestConfigProvider), new(*eventualRestConfigProvider)),
	ProvideService,
	wire.Bind(new(Service), new(*service)),
	wire.Bind(new(builder.APIRegistrar), new(*service)),
	ProvideClientGenerator,
)
