package dsdk

import (
	"context"
	"fmt"
	"net/http"
	"strings"
)

// DefaultBasePath is the path prefix under which NewHandler mounts the data plane endpoints.
const DefaultBasePath = "/v1"

type handlerConfig struct {
	basePath      string
	identifier    ControlPlaneIdentifier
	hasIdentifier bool
}

// ControlPlaneIdentifier returns the id of the control plane that sent a request. Prepare and start record it on the
// new data flow, so that callbacks reach that control plane. An error is answered with 401.
type ControlPlaneIdentifier func(r *http.Request) (string, error)

// WithControlPlaneIdentifier sets how NewHandler identifies the calling control plane. The default is
// DefaultControlPlaneIdentifier.
func WithControlPlaneIdentifier(identifier ControlPlaneIdentifier) HandlerOption {
	return func(c *handlerConfig) {
		c.identifier = identifier
		c.hasIdentifier = true
	}
}

// DefaultControlPlaneIdentifier returns an identifier that, if the SDK has a ControlPlaneStore holding exactly one
// registered control plane, returns that control plane's id, and "" otherwise. It fails only if the store fails.
func DefaultControlPlaneIdentifier(sdk *DataPlaneSDK) ControlPlaneIdentifier {
	return func(r *http.Request) (string, error) {
		if sdk.controlPlaneStore == nil {
			return "", nil
		}
		var controlPlanes []*ControlPlane
		err := sdk.execute(r.Context(), func(ctx context.Context) error {
			var err error
			controlPlanes, err = sdk.controlPlaneStore.FindAll(ctx)
			return err
		})
		if err != nil {
			return "", fmt.Errorf("listing control planes: %w", err)
		}
		if len(controlPlanes) == 1 {
			return controlPlanes[0].ID, nil
		}
		return "", nil
	}
}

// HandlerOption configures the handler returned by NewHandler.
type HandlerOption func(*handlerConfig)

// WithBasePath sets the path prefix of the data plane endpoints, e.g. "/api/signaling/v1". Use "" to mount them at
// the root. The prefix is also used for the Location header of 202 responses.
func WithBasePath(basePath string) HandlerOption {
	return func(c *handlerConfig) {
		c.basePath = strings.TrimSuffix(basePath, "/")
	}
}

// NewHandler returns an http.Handler that serves all data plane endpoints of the Data Plane Signaling API under the
// configured base path (DefaultBasePath unless WithBasePath is given):
//
//	POST <base>/dataflows/prepare
//	POST <base>/dataflows/start
//	POST <base>/dataflows/{id}/started
//	POST <base>/dataflows/{id}/suspend
//	POST <base>/dataflows/{id}/resume
//	POST <base>/dataflows/{id}/terminate
//	POST <base>/dataflows/{id}/completed
//	GET  <base>/dataflows/{id}/status
//
// If the SDK has a ControlPlaneStore (WithControlPlaneStore), it also serves the control plane registration
// endpoints:
//
//	PUT    <base>/controlplanes
//	DELETE <base>/controlplanes/{controlPlaneId}
//
// Prepare and start record the id of the calling control plane on new data flows; see WithControlPlaneIdentifier.
func NewHandler(sdk *DataPlaneSDK, options ...HandlerOption) http.Handler {
	config := handlerConfig{basePath: DefaultBasePath}
	for _, opt := range options {
		opt(&config)
	}
	if !config.hasIdentifier {
		config.identifier = DefaultControlPlaneIdentifier(sdk)
	}
	api := &DataPlaneApi{sdk: sdk, basePath: config.basePath, identifier: config.identifier}
	base := config.basePath + "/dataflows"

	mux := http.NewServeMux()
	mux.HandleFunc("POST "+base+"/prepare", api.Prepare)
	mux.HandleFunc("POST "+base+"/start", api.Start)
	mux.HandleFunc("POST "+base+"/{id}/started", api.Started)
	mux.HandleFunc("POST "+base+"/{id}/suspend", api.Suspend)
	mux.HandleFunc("POST "+base+"/{id}/resume", api.Resume)
	mux.HandleFunc("POST "+base+"/{id}/terminate", api.Terminate)
	mux.HandleFunc("POST "+base+"/{id}/completed", api.Completed)
	mux.HandleFunc("GET "+base+"/{id}/status", api.Status)
	if sdk.controlPlaneStore != nil {
		mux.HandleFunc("PUT "+config.basePath+"/controlplanes", api.RegisterControlPlane)
		mux.HandleFunc("DELETE "+config.basePath+"/controlplanes/{controlPlaneId}", api.DeleteControlPlane)
	}
	return mux
}
