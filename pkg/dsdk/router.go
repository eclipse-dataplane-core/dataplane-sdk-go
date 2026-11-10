package dsdk

import (
	"fmt"
	"net/http"
	"strings"
)

// DefaultBasePath is the path prefix under which NewHandler mounts the data plane endpoints.
const DefaultBasePath = "/v1"

type handlerConfig struct {
	basePath      string
	authenticator Authenticator
}

// Authenticator returns the id of the control plane that sent a request, or an error, which is answered with 401.
// Prepare and start record the id on the new data flow, so that callbacks reach that control plane; the other
// endpoints answer 403 if the data flow or registration belongs to a different control plane. An empty id disables
// these checks for the request.
type Authenticator func(r *http.Request) (string, error)

// WithAuthenticator replaces how NewHandler authenticates requests. By default the Authorization header is required
// and passed to DataPlaneSDK.ExtractControlPlaneID; with WithAuthorizationDisabled no authentication takes place
// unless an authenticator is set with this option.
func WithAuthenticator(authenticator Authenticator) HandlerOption {
	return func(c *handlerConfig) {
		c.authenticator = authenticator
	}
}

// authorizationHeaderAuthenticator authenticates requests with the SDK's registered Authorizations.
func authorizationHeaderAuthenticator(sdk *DataPlaneSDK) Authenticator {
	return func(r *http.Request) (string, error) {
		header := r.Header.Get("Authorization")
		if header == "" {
			return "", fmt.Errorf("%w: missing Authorization header", ErrUnauthorized)
		}
		return sdk.ExtractControlPlaneID(r.Context(), header)
	}
}

// authenticate runs the authenticator before the handler and puts the caller id into the request context.
func (d *DataPlaneApi) authenticate(authenticator Authenticator, next http.HandlerFunc) http.HandlerFunc {
	if authenticator == nil {
		return next
	}
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := authenticator(r)
		if err != nil {
			d.writeResponse(w, http.StatusUnauthorized, &ErrorResponse{Error: fmt.Sprintf("unauthorized: %s", err)})
			return
		}
		next(w, r.WithContext(ContextWithControlPlaneID(r.Context(), id)))
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
// Every request is authenticated before its body is read: unless WithAuthorizationDisabled is set, a request without
// a valid Authorization header is answered with 401. See WithAuthenticator.
func NewHandler(sdk *DataPlaneSDK, options ...HandlerOption) http.Handler {
	config := handlerConfig{basePath: DefaultBasePath}
	if !sdk.authorizationDisabled {
		config.authenticator = authorizationHeaderAuthenticator(sdk)
	}
	for _, opt := range options {
		opt(&config)
	}
	api := &DataPlaneApi{sdk: sdk, basePath: config.basePath}
	handle := func(mux *http.ServeMux, pattern string, handler http.HandlerFunc) {
		mux.HandleFunc(pattern, api.authenticate(config.authenticator, handler))
	}
	base := config.basePath + "/dataflows"

	mux := http.NewServeMux()
	handle(mux, "POST "+base+"/prepare", api.Prepare)
	handle(mux, "POST "+base+"/start", api.Start)
	handle(mux, "POST "+base+"/{id}/started", api.Started)
	handle(mux, "POST "+base+"/{id}/suspend", api.Suspend)
	handle(mux, "POST "+base+"/{id}/resume", api.Resume)
	handle(mux, "POST "+base+"/{id}/terminate", api.Terminate)
	handle(mux, "POST "+base+"/{id}/completed", api.Completed)
	handle(mux, "GET "+base+"/{id}/status", api.Status)
	if sdk.controlPlaneStore != nil {
		handle(mux, "PUT "+config.basePath+"/controlplanes", api.RegisterControlPlane)
		handle(mux, "DELETE "+config.basePath+"/controlplanes/{controlPlaneId}", api.DeleteControlPlane)
	}
	return mux
}
