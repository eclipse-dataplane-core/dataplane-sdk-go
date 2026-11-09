package dsdk

import (
	"net/http"
	"strings"
)

// DefaultBasePath is the path prefix under which NewHandler mounts the data plane endpoints.
const DefaultBasePath = "/v1"

type handlerConfig struct {
	basePath string
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
func NewHandler(sdk *DataPlaneSDK, options ...HandlerOption) http.Handler {
	config := handlerConfig{basePath: DefaultBasePath}
	for _, opt := range options {
		opt(&config)
	}
	api := &DataPlaneApi{sdk: sdk, basePath: config.basePath}
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
	return mux
}
