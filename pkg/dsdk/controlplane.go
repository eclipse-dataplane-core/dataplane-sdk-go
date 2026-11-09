package dsdk

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Callback actions of the control plane's data flow endpoint, POST /transfers/{id}/dataflow/{action}.
const (
	ActionPrepared  = "prepared"
	ActionStarted   = "started"
	ActionCompleted = "completed"
	ActionErrored   = "errored"
	// ActionAgreement identifies a failed GET /transfers/{id}/agreement in a ControlPlaneError.
	ActionAgreement = "agreement"
	// ActionRegisterDataPlane identifies a failed PUT /dataplanes in a ControlPlaneError.
	ActionRegisterDataPlane = "register data plane"
	// ActionDeregisterDataPlane identifies a failed DELETE /dataplanes/{id} in a ControlPlaneError.
	ActionDeregisterDataPlane = "deregister data plane"
)

// ErrControlPlaneNotRegistered is returned when no control plane endpoint is known for a data flow.
var ErrControlPlaneNotRegistered = errors.New("control plane not registered")

// ControlPlaneClient calls the control plane endpoints defined by the Data Plane Signaling specification.
type ControlPlaneClient interface {
	// Notify sends the status message to {controlPlaneURL}/transfers/{message.DataFlowID}/dataflow/{action}.
	Notify(ctx context.Context, controlPlaneURL string, action string, message DataFlowStatusMessage) error
	// GetAgreement returns the raw JSON body of GET {controlPlaneURL}/transfers/{dataFlowID}/agreement.
	//
	// The body is the specification's AgreementResponse, i.e. a DSP Agreement: a JSON-LD ODRL policy with
	// @context, @id, @type, target, assigner, assignee, timestamp and permission/prohibition/obligation rule trees
	// with constraints, which may arrive in different JSON-LD forms. The SDK returns the raw document instead of a
	// Go model so that no part of the policy is dropped and the SDK does not have to own an ODRL model; callers
	// unmarshal what they need.
	//
	// TODO: return a typed Agreement model once the SDK needs to evaluate policies (DSP Agreement / ODRL).
	GetAgreement(ctx context.Context, controlPlaneURL string, dataFlowID string) (json.RawMessage, error)
	// RegisterDataPlane creates or replaces the registration of this data plane: PUT {controlPlaneURL}/dataplanes.
	RegisterDataPlane(ctx context.Context, controlPlaneURL string, message DataPlaneRegistrationMessage) error
	// DeregisterDataPlane removes the registration of this data plane: DELETE {controlPlaneURL}/dataplanes/{dataPlaneID}.
	DeregisterDataPlane(ctx context.Context, controlPlaneURL string, dataPlaneID string) error
}

// ControlPlaneResolver returns the endpoint of the control plane with the given id.
type ControlPlaneResolver interface {
	ControlPlaneURL(ctx context.Context, controlPlaneID string) (string, error)
}

// ControlPlaneError is returned when the control plane answers a request with a non-2xx status code.
type ControlPlaneError struct {
	Action     string
	StatusCode int
	Body       string
}

func (e *ControlPlaneError) Error() string {
	return fmt.Sprintf("control plane rejected %s with HTTP %d: %s", e.Action, e.StatusCode, e.Body)
}

// RetryPolicy configures how often the HTTP control plane client retries a request that failed with a 5xx status
// code or a transport error. 4xx responses are never retried. The delay before retry n (starting at 1) is
// BaseDelay * 2^(n-1).
type RetryPolicy struct {
	// Attempts is the total number of attempts; values below 1 mean a single attempt (no retries).
	Attempts  int
	BaseDelay time.Duration
}

// DefaultRetryPolicy makes 3 attempts, waiting 200ms and 400ms between them.
var DefaultRetryPolicy = RetryPolicy{Attempts: 3, BaseDelay: 200 * time.Millisecond}

// HTTPControlPlaneClient is the default ControlPlaneClient.
type HTTPControlPlaneClient struct {
	client *http.Client
	retry  RetryPolicy
	sleep  func(ctx context.Context, d time.Duration) error
}

type HTTPControlPlaneClientOption func(*HTTPControlPlaneClient)

// WithHTTPClient sets the http.Client used for requests, e.g. one whose transport adds an Authorization header.
func WithHTTPClient(client *http.Client) HTTPControlPlaneClientOption {
	return func(c *HTTPControlPlaneClient) {
		c.client = client
	}
}

func WithRetryPolicy(policy RetryPolicy) HTTPControlPlaneClientOption {
	return func(c *HTTPControlPlaneClient) {
		c.retry = policy
	}
}

func NewHTTPControlPlaneClient(options ...HTTPControlPlaneClientOption) *HTTPControlPlaneClient {
	c := &HTTPControlPlaneClient{
		client: &http.Client{Timeout: 30 * time.Second},
		retry:  DefaultRetryPolicy,
		sleep:  sleepContext,
	}
	for _, opt := range options {
		opt(c)
	}
	return c
}

func (c *HTTPControlPlaneClient) Notify(ctx context.Context, controlPlaneURL string, action string, message DataFlowStatusMessage) error {
	endpoint, err := transferURL(controlPlaneURL, message.DataFlowID, "dataflow", action)
	if err != nil {
		return err
	}
	body, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("encoding %s callback: %w", action, err)
	}
	_, err = c.do(ctx, action, func() (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set(contentType, jsonContentType)
		return req, nil
	})
	return err
}

// GetAgreement fetches the agreement of a data flow and returns the raw JSON body; see ControlPlaneClient.GetAgreement
// for why it is not decoded. A non-2xx response is returned as a *ControlPlaneError.
func (c *HTTPControlPlaneClient) GetAgreement(ctx context.Context, controlPlaneURL string, dataFlowID string) (json.RawMessage, error) {
	endpoint, err := transferURL(controlPlaneURL, dataFlowID, "agreement")
	if err != nil {
		return nil, err
	}
	body, err := c.do(ctx, ActionAgreement, func() (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Accept", jsonContentType)
		return req, nil
	})
	if err != nil {
		return nil, err
	}
	if !json.Valid(body) {
		return nil, fmt.Errorf("control plane returned an invalid agreement body for data flow %s", dataFlowID)
	}
	return body, nil
}

// RegisterDataPlane sends PUT {controlPlaneURL}/dataplanes. A non-2xx response is returned as a *ControlPlaneError.
func (c *HTTPControlPlaneClient) RegisterDataPlane(ctx context.Context, controlPlaneURL string, message DataPlaneRegistrationMessage) error {
	endpoint, err := joinURL(controlPlaneURL, "dataplanes")
	if err != nil {
		return err
	}
	body, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("encoding data plane registration: %w", err)
	}
	_, err = c.do(ctx, ActionRegisterDataPlane, func() (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set(contentType, jsonContentType)
		return req, nil
	})
	return err
}

// DeregisterDataPlane sends DELETE {controlPlaneURL}/dataplanes/{dataPlaneID}. A non-2xx response is returned as a
// *ControlPlaneError.
func (c *HTTPControlPlaneClient) DeregisterDataPlane(ctx context.Context, controlPlaneURL string, dataPlaneID string) error {
	if dataPlaneID == "" {
		return fmt.Errorf("%w: data plane id cannot be empty", ErrInvalidInput)
	}
	endpoint, err := joinURL(controlPlaneURL, "dataplanes", dataPlaneID)
	if err != nil {
		return err
	}
	_, err = c.do(ctx, ActionDeregisterDataPlane, func() (*http.Request, error) {
		return http.NewRequestWithContext(ctx, http.MethodDelete, endpoint, nil)
	})
	return err
}

// do sends the request built by newRequest, retrying on 5xx and transport errors, and returns the body of a 2xx
// response.
func (c *HTTPControlPlaneClient) do(ctx context.Context, action string, newRequest func() (*http.Request, error)) ([]byte, error) {
	attempts := max(c.retry.Attempts, 1)
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		if attempt > 1 {
			if err := c.sleep(ctx, c.retry.BaseDelay<<(attempt-2)); err != nil {
				return nil, fmt.Errorf("calling control plane (%s): %w (last error: %w)", action, err, lastErr)
			}
		}
		req, err := newRequest()
		if err != nil {
			return nil, fmt.Errorf("creating %s request: %w", action, err)
		}
		resp, err := c.client.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return nil, fmt.Errorf("calling control plane (%s): %w", action, ctx.Err())
			}
			lastErr = fmt.Errorf("calling control plane (%s): %w", action, err)
			continue
		}
		body, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		switch {
		case resp.StatusCode >= 200 && resp.StatusCode < 300:
			if readErr != nil {
				return nil, fmt.Errorf("reading %s response: %w", action, readErr)
			}
			return body, nil
		case resp.StatusCode >= 500:
			lastErr = &ControlPlaneError{Action: action, StatusCode: resp.StatusCode, Body: string(body)}
		default:
			return nil, &ControlPlaneError{Action: action, StatusCode: resp.StatusCode, Body: string(body)}
		}
	}
	return nil, lastErr
}

// transferURL returns {base}/transfers/{id}/{segments...}. The id is escaped as a single path segment, and the base
// may have a path prefix with or without a trailing slash.
func transferURL(base string, id string, segments ...string) (string, error) {
	if id == "" {
		return "", fmt.Errorf("%w: data flow id cannot be empty", ErrInvalidInput)
	}
	return joinURL(base, append([]string{"transfers", id}, segments...)...)
}

// joinURL appends the segments to the path of base, escaping each one as a single path segment. The base may have a
// path prefix with or without a trailing slash; its query and fragment are dropped.
func joinURL(base string, segments ...string) (string, error) {
	u, err := url.Parse(base)
	if err != nil {
		return "", fmt.Errorf("%w: invalid control plane URL %q: %w", ErrInvalidInput, base, err)
	}
	if u.Scheme == "" || u.Host == "" {
		return "", fmt.Errorf("%w: invalid control plane URL %q", ErrInvalidInput, base)
	}
	escaped := strings.TrimSuffix(u.EscapedPath(), "/")
	for _, s := range segments {
		escaped += "/" + url.PathEscape(s)
	}
	unescaped, err := url.PathUnescape(escaped)
	if err != nil {
		return "", err
	}
	u.Path = unescaped
	u.RawPath = escaped
	u.RawQuery = ""
	u.Fragment = ""
	return u.String(), nil
}

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// staticResolver returns the same control plane URL for every control plane id.
type staticResolver struct {
	url string
}

func (r staticResolver) ControlPlaneURL(context.Context, string) (string, error) {
	if r.url == "" {
		return "", ErrControlPlaneNotRegistered
	}
	return r.url, nil
}

// WithControlPlane configures a single control plane endpoint, used for every data flow.
func WithControlPlane(url string) DataPlaneSDKOption {
	return func(sdk *DataPlaneSDK) {
		sdk.controlPlanes = staticResolver{url: url}
	}
}

// WithControlPlaneResolver configures how the endpoint of a data flow's control plane is looked up.
func WithControlPlaneResolver(resolver ControlPlaneResolver) DataPlaneSDKOption {
	return func(sdk *DataPlaneSDK) {
		sdk.controlPlanes = resolver
	}
}

// WithControlPlaneClient replaces the HTTP client used to call the control plane.
func WithControlPlaneClient(client ControlPlaneClient) DataPlaneSDKOption {
	return func(sdk *DataPlaneSDK) {
		sdk.controlPlaneClient = client
	}
}
