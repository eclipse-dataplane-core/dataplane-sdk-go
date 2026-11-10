package dsdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
)

const authorizationTypeKey = "type"

// AuthorizationEntry is the authorization object of the registration messages. Type identifies an authorization
// profile (e.g. "oauth2_client_credentials"); all other properties of the profile, e.g. tokenEndpoint, clientId and
// clientSecret, are kept in Properties and serialized next to "type".
type AuthorizationEntry struct {
	Type       string
	Properties map[string]any
}

// Property returns the string property with the given name.
func (a AuthorizationEntry) Property(name string) (string, bool) {
	value, ok := a.Properties[name].(string)
	return value, ok
}

// Copy returns a copy of the entry; property values are copied shallowly.
func (a AuthorizationEntry) Copy() AuthorizationEntry {
	return AuthorizationEntry{Type: a.Type, Properties: copyMap(a.Properties)}
}

func (a AuthorizationEntry) Validate() error {
	if a.Type == "" {
		return NewValidationError("authorization.type is required")
	}
	return nil
}

func (a AuthorizationEntry) MarshalJSON() ([]byte, error) {
	out := make(map[string]any, len(a.Properties)+1)
	for k, v := range a.Properties {
		out[k] = v
	}
	out[authorizationTypeKey] = a.Type
	return json.Marshal(out)
}

func (a *AuthorizationEntry) UnmarshalJSON(data []byte) error {
	var in map[string]any
	if err := json.Unmarshal(data, &in); err != nil {
		return err
	}
	*a = AuthorizationEntry{}
	if raw, ok := in[authorizationTypeKey]; ok {
		typ, ok := raw.(string)
		if !ok {
			return fmt.Errorf("authorization.type must be a string")
		}
		a.Type = typ
		delete(in, authorizationTypeKey)
	}
	if len(in) > 0 {
		a.Properties = in
	}
	return nil
}

// ControlPlaneRegistrationMessage registers a control plane with this data plane (PUT /controlplanes).
type ControlPlaneRegistrationMessage struct {
	ControlPlaneID string             `json:"controlplaneId" validate:"required"`
	Endpoint       string             `json:"endpoint" validate:"required"`
	Authorization  AuthorizationEntry `json:"authorization"`
}

func (m *ControlPlaneRegistrationMessage) Validate() error {
	if err := validate(m); err != nil {
		return err
	}
	if err := validateEndpoint(m.Endpoint); err != nil {
		return err
	}
	return m.Authorization.Validate()
}

// DataPlaneRegistrationMessage registers this data plane with a control plane (PUT /dataplanes on the control plane).
type DataPlaneRegistrationMessage struct {
	DataPlaneID   string             `json:"dataplaneId" validate:"required"`
	Endpoint      string             `json:"endpoint" validate:"required"`
	Profiles      []string           `json:"profiles" validate:"required,min=1,dive,required"`
	Authorization AuthorizationEntry `json:"authorization"`
	Labels        []string           `json:"labels,omitempty"`
}

func (m *DataPlaneRegistrationMessage) Validate() error {
	if err := validate(m); err != nil {
		return err
	}
	if err := validateEndpoint(m.Endpoint); err != nil {
		return err
	}
	return m.Authorization.Validate()
}

func validateEndpoint(endpoint string) error {
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return NewValidationError(fmt.Sprintf("endpoint must be an absolute URL: %q", endpoint))
	}
	return nil
}

// ControlPlane is a control plane registered with this data plane.
type ControlPlane struct {
	ID            string
	Endpoint      string
	Authorization AuthorizationEntry
}

// Copy returns a deep copy of the control plane, or nil if c is nil.
func (c *ControlPlane) Copy() *ControlPlane {
	if c == nil {
		return nil
	}
	return &ControlPlane{ID: c.ID, Endpoint: c.Endpoint, Authorization: c.Authorization.Copy()}
}

// RegisterControlPlane creates or replaces the registration of a control plane. It requires a ControlPlaneStore.
func (dsdk *DataPlaneSDK) RegisterControlPlane(ctx context.Context, message ControlPlaneRegistrationMessage) error {
	if err := message.Validate(); err != nil {
		return err
	}
	if dsdk.controlPlaneStore == nil {
		return errNoControlPlaneStore
	}
	if err := dsdk.checkRegistrationCaller(ControlPlaneIDFromContext(ctx), "register", message.ControlPlaneID); err != nil {
		return err
	}
	if err := dsdk.checkEndpoint(message.Endpoint); err != nil {
		return err
	}
	if err := dsdk.checkAuthorizationType(message.Authorization); err != nil {
		return err
	}
	controlPlane := &ControlPlane{ID: message.ControlPlaneID, Endpoint: message.Endpoint, Authorization: message.Authorization.Copy()}
	return dsdk.execute(ctx, func(ctx context.Context) error {
		if err := dsdk.controlPlaneStore.Save(ctx, controlPlane); err != nil {
			return fmt.Errorf("registering control plane %s: %w", controlPlane.ID, err)
		}
		return nil
	})
}

// DeleteControlPlane removes the registration of a control plane; it returns ErrNotFound if none exists.
func (dsdk *DataPlaneSDK) DeleteControlPlane(ctx context.Context, id string) error {
	if id == "" {
		return fmt.Errorf("%w: control plane id cannot be empty", ErrInvalidInput)
	}
	if dsdk.controlPlaneStore == nil {
		return errNoControlPlaneStore
	}
	if err := dsdk.checkRegistrationCaller(ControlPlaneIDFromContext(ctx), "delete", id); err != nil {
		return err
	}
	return dsdk.execute(ctx, func(ctx context.Context) error {
		if err := dsdk.controlPlaneStore.Delete(ctx, id); err != nil {
			return fmt.Errorf("deleting control plane %s: %w", id, err)
		}
		return nil
	})
}

// RegisterDataPlane registers this data plane with the control plane (PUT {controlPlane.Endpoint}/dataplanes). The
// request is authorized with controlPlane.Authorization. The message's authorization entry, which tells the control
// plane how to authorize its requests to this data plane, must use a registered Authorization type.
func (dsdk *DataPlaneSDK) RegisterDataPlane(ctx context.Context, controlPlane ControlPlane, message DataPlaneRegistrationMessage) error {
	if err := message.Validate(); err != nil {
		return err
	}
	if err := dsdk.checkAuthorizationType(message.Authorization); err != nil {
		return err
	}
	if err := dsdk.checkEndpoint(controlPlane.Endpoint); err != nil {
		return err
	}
	return dsdk.controlPlaneClient.RegisterDataPlane(ctx, controlPlane, message)
}

// DeregisterDataPlane removes the registration of this data plane from the control plane
// (DELETE {controlPlane.Endpoint}/dataplanes/{dataPlaneID}), authorized with controlPlane.Authorization.
func (dsdk *DataPlaneSDK) DeregisterDataPlane(ctx context.Context, controlPlane ControlPlane, dataPlaneID string) error {
	if err := dsdk.checkEndpoint(controlPlane.Endpoint); err != nil {
		return err
	}
	return dsdk.controlPlaneClient.DeregisterDataPlane(ctx, controlPlane, dataPlaneID)
}

func isNotFound(err error) bool {
	return errors.Is(err, ErrNotFound)
}

var errNoControlPlaneStore = fmt.Errorf("%w: no control plane store configured", ErrInvalidInput)

// controlPlaneIDKey is the context key for the control plane id. It is an unexported type rather than a string so that
// no other package can set or read the value by accident (context keys compare by type and value).
type controlPlaneIDKey struct{}

// ContextWithControlPlaneID returns a context carrying the id of the control plane that sent the current request.
// Prepare and Start record it on new data flows, so that callbacks go to that control plane, and the other data flow
// and registration operations reject requests from a different control plane with ErrForbidden.
func ContextWithControlPlaneID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, controlPlaneIDKey{}, id)
}

// ControlPlaneIDFromContext returns the control plane id set with ContextWithControlPlaneID, or "".
func ControlPlaneIDFromContext(ctx context.Context) string {
	id, _ := ctx.Value(controlPlaneIDKey{}).(string)
	return id
}

// storeResolver resolves control plane endpoints from the registered control planes.
type storeResolver struct {
	store ControlPlaneStore
}

func (r storeResolver) ControlPlane(ctx context.Context, controlPlaneID string) (*ControlPlane, error) {
	if controlPlaneID == "" {
		return nil, fmt.Errorf("%w: the data flow has no control plane id", ErrControlPlaneNotRegistered)
	}
	controlPlane, err := r.store.FindById(ctx, controlPlaneID)
	if err != nil {
		if isNotFound(err) {
			return nil, fmt.Errorf("%w: %s", ErrControlPlaneNotRegistered, controlPlaneID)
		}
		return nil, fmt.Errorf("looking up control plane %s: %w", controlPlaneID, err)
	}
	return controlPlane, nil
}

// WithControlPlaneStore configures where registered control planes are stored. It enables RegisterControlPlane and
// DeleteControlPlane, makes NewHandler mount the control plane registration endpoints, and, unless WithControlPlane
// or WithControlPlaneResolver is also given (those take precedence), resolves callback endpoints from the registered
// control plane of each data flow.
func WithControlPlaneStore(store ControlPlaneStore) DataPlaneSDKOption {
	return func(sdk *DataPlaneSDK) {
		sdk.controlPlaneStore = store
	}
}
