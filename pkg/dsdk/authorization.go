package dsdk

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"sort"
	"strings"
)

// Authorization implements an authorization profile, e.g. "oauth2_client_credentials". The SDK uses it in both
// directions: to authorize its own requests to a control plane, and to identify the control plane that sent a request.
type Authorization interface {
	// Type returns the authorization profile type, matched against AuthorizationEntry.Type.
	Type() string
	// AuthorizationHeader returns the Authorization header value for a request to a control plane whose registration
	// carries the given entry.
	AuthorizationHeader(ctx context.Context, entry AuthorizationEntry) (string, error)
	// ExtractCallerID returns the id of the control plane that sent a request with the given Authorization header.
	// It returns an error wrapping ErrUnauthorized if the header is not valid for this profile.
	ExtractCallerID(ctx context.Context, authorizationHeader string) (string, error)
}

// WithAuthorization registers an Authorization under its Type. Several can be registered; inbound requests are
// accepted if any of them identifies the caller.
func WithAuthorization(authorization Authorization) DataPlaneSDKOption {
	return func(sdk *DataPlaneSDK) {
		if sdk.authorizations == nil {
			sdk.authorizations = map[string]Authorization{}
		}
		sdk.authorizations[authorization.Type()] = authorization
	}
}

// WithAuthorizationDisabled turns authorization off. Inbound requests then need no Authorization header and have no
// caller id (unless a custom authenticator supplies one), outbound requests to a control plane without an
// authorization type, or with a type no Authorization is registered for, are sent without an Authorization header,
// and registrations may use any authorization type. Use it only where the network secures the signaling API, or
// for testing.
func WithAuthorizationDisabled() DataPlaneSDKOption {
	return func(sdk *DataPlaneSDK) {
		sdk.authorizationDisabled = true
	}
}

// WithInsecureEndpoints allows control plane endpoints with http:// for any host. Without it, http:// is only
// accepted for loopback hosts (localhost, 127.0.0.0/8, ::1).
func WithInsecureEndpoints() DataPlaneSDKOption {
	return func(sdk *DataPlaneSDK) {
		sdk.insecureEndpoints = true
	}
}

// AuthorizationDisabled reports whether WithAuthorizationDisabled was given.
func (dsdk *DataPlaneSDK) AuthorizationDisabled() bool {
	return dsdk.authorizationDisabled
}

// ExtractControlPlaneID returns the id of the control plane that sent a request with the given Authorization header.
// Every registered Authorization is tried; the first that accepts the header wins. Otherwise the error wraps
// ErrUnauthorized.
func (dsdk *DataPlaneSDK) ExtractControlPlaneID(ctx context.Context, authorizationHeader string) (string, error) {
	if authorizationHeader == "" {
		return "", fmt.Errorf("%w: missing Authorization header", ErrUnauthorized)
	}
	if len(dsdk.authorizations) == 0 {
		return "", fmt.Errorf("%w: no authorization is registered", ErrUnauthorized)
	}
	var failures []string
	for _, authorization := range dsdk.sortedAuthorizations() {
		id, err := authorization.ExtractCallerID(ctx, authorizationHeader)
		if err == nil && id != "" {
			return id, nil
		}
		if err == nil {
			err = errors.New("no caller id")
		}
		failures = append(failures, fmt.Sprintf("%s: %v", authorization.Type(), err))
	}
	return "", fmt.Errorf("%w: %s", ErrUnauthorized, strings.Join(failures, "; "))
}

func (dsdk *DataPlaneSDK) sortedAuthorizations() []Authorization {
	types := make([]string, 0, len(dsdk.authorizations))
	for t := range dsdk.authorizations {
		types = append(types, t)
	}
	sort.Strings(types)
	result := make([]Authorization, 0, len(types))
	for _, t := range types {
		result = append(result, dsdk.authorizations[t])
	}
	return result
}

// authorizationHeader returns the outbound Authorization header for a control plane's authorization entry.
func (dsdk *DataPlaneSDK) authorizationHeader(ctx context.Context, entry AuthorizationEntry) (string, error) {
	authorization, ok := dsdk.authorizations[entry.Type]
	switch {
	case ok:
		return authorization.AuthorizationHeader(ctx, entry)
	case dsdk.authorizationDisabled:
		return "", nil
	case entry.Type == "":
		return "", fmt.Errorf("%w: the control plane has no authorization entry", ErrAuthorizationNotSupported)
	default:
		return "", fmt.Errorf("%w: %q", ErrAuthorizationNotSupported, entry.Type)
	}
}

// checkAuthorizationType rejects an authorization type no Authorization is registered for, unless authorization is
// disabled.
func (dsdk *DataPlaneSDK) checkAuthorizationType(entry AuthorizationEntry) error {
	if dsdk.authorizationDisabled {
		return nil
	}
	if _, ok := dsdk.authorizations[entry.Type]; !ok {
		return fmt.Errorf("%w: %q", ErrAuthorizationNotSupported, entry.Type)
	}
	return nil
}

// checkEndpoint rejects a control plane endpoint that is not an absolute URL, or that uses http:// for a non-loopback
// host without WithInsecureEndpoints.
func (dsdk *DataPlaneSDK) checkEndpoint(endpoint string) error {
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" {
		return NewValidationError(fmt.Sprintf("control plane endpoint must be an absolute URL: %q", endpoint))
	}
	switch u.Scheme {
	case "https":
		return nil
	case "http":
		if dsdk.insecureEndpoints || isLoopback(u.Hostname()) {
			return nil
		}
		return NewValidationError(fmt.Sprintf("control plane endpoint must use https (or configure WithInsecureEndpoints): %q", endpoint))
	default:
		return NewValidationError(fmt.Sprintf("control plane endpoint must use https: %q", endpoint))
	}
}

func isLoopback(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// checkCaller returns ErrForbidden, and logs a warning, if the request was sent by a known control plane other than
// the flow's.
func (dsdk *DataPlaneSDK) checkCaller(callerID string, flow *DataFlow) error {
	if callerID != "" && flow.ControlPlaneID != "" && callerID != flow.ControlPlaneID {
		dsdk.Monitor.Printf("WARN: control plane %q attempted to access data flow %q owned by control plane %q\n", callerID, flow.ID, flow.ControlPlaneID)
		return fmt.Errorf("%w: data flow %s belongs to another control plane", ErrForbidden, flow.ID)
	}
	return nil
}

// checkRegistrationCaller returns ErrForbidden, and logs a warning, if a known control plane tries to register or
// delete another control plane's registration.
func (dsdk *DataPlaneSDK) checkRegistrationCaller(callerID string, action string, controlPlaneID string) error {
	if callerID != "" && callerID != controlPlaneID {
		dsdk.Monitor.Printf("WARN: control plane %q attempted to %s control plane %q\n", callerID, action, controlPlaneID)
		return fmt.Errorf("%w: control plane %s cannot %s control plane %s", ErrForbidden, callerID, action, controlPlaneID)
	}
	return nil
}
