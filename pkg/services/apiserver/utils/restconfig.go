package utils

import (
	"context"

	clientrest "k8s.io/client-go/rest"
)

type RestConfigProvider interface {
	// GetRestConfig returns a k8s client configuration that is used to provide connection info and auth for the loopback transport.
	// context is only available for tracing in this immediate function and is not to be confused with the context seen by any client verb actions that are invoked with the retrieved rest config.
	// - those client verb actions have the ability to specify their own context.
	GetRestConfig(context.Context) (*clientrest.Config, error)
}
