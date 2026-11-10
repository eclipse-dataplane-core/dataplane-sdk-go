package dsdk

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeControlPlane records the requests it receives and answers with the configured status codes in order; the
// last code is repeated.
type fakeControlPlane struct {
	*httptest.Server
	mu       sync.Mutex
	codes    []int
	requests []recordedRequest
}

type recordedRequest struct {
	method        string
	path          string
	rawPath       string
	contentType   string
	authorization string
	body          []byte
}

func newFakeControlPlane(t *testing.T, codes ...int) *fakeControlPlane {
	t.Helper()
	if len(codes) == 0 {
		codes = []int{http.StatusOK}
	}
	cp := &fakeControlPlane{codes: codes}
	cp.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		cp.mu.Lock()
		cp.requests = append(cp.requests, recordedRequest{
			method:        r.Method,
			path:          r.URL.Path,
			rawPath:       r.URL.EscapedPath(),
			contentType:   r.Header.Get(contentType),
			authorization: r.Header.Get("Authorization"),
			body:          body,
		})
		code := cp.codes[min(len(cp.requests), len(cp.codes))-1]
		cp.mu.Unlock()
		w.WriteHeader(code)
		if code == http.StatusOK && r.Method == http.MethodGet {
			_, _ = w.Write([]byte(`{"@id": "agreement-1"}`))
		} else if code >= 400 {
			_, _ = w.Write([]byte("nope"))
		}
	}))
	t.Cleanup(cp.Close)
	return cp
}

func (cp *fakeControlPlane) received() []recordedRequest {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	return append([]recordedRequest(nil), cp.requests...)
}

func noSleep(context.Context, time.Duration) error { return nil }

func newTestClient(policy RetryPolicy) *HTTPControlPlaneClient {
	client := NewHTTPControlPlaneClient(WithRetryPolicy(policy))
	client.sleep = noSleep
	return client
}

// newNotifySDK returns an SDK backed by a fake store and wired to the fake control plane.
func newNotifySDK(t *testing.T, cp *fakeControlPlane, options ...DataPlaneSDKOption) (*DataPlaneSDK, *fakeStore) {
	t.Helper()
	base := []DataPlaneSDKOption{WithControlPlaneClient(newTestClient(DefaultRetryPolicy))}
	if cp != nil {
		base = append(base, WithControlPlane(ControlPlane{ID: testCaller, Endpoint: cp.URL + "/api/control", Authorization: testEntry()}))
	}
	return newTestSDK(t, append(base, options...)...)
}

func decodeCallback(t *testing.T, request recordedRequest) map[string]any {
	t.Helper()
	var body map[string]any
	require.NoError(t, json.Unmarshal(request.body, &body))
	return body
}

func assertStatusMessageSchema(t *testing.T, schemas map[string]*jsonschema.Schema, request recordedRequest) {
	t.Helper()
	instance, err := jsonschema.UnmarshalJSON(bytes.NewReader(request.body))
	require.NoError(t, err)
	assert.NoError(t, schemas["DataFlowStatusMessage"].Validate(instance), string(request.body))
}

// --- Notify* ---

func Test_Notify_SendsCallbacks(t *testing.T) {
	schemas := loadSchemas(t)
	address := newDataAddress()
	addressProcessor := func(state DataFlowState) DataFlowProcessor {
		return func(context.Context, *DataFlow, *DataPlaneSDK, *ProcessorOptions) (*DataFlowStatusMessage, error) {
			return &DataFlowStatusMessage{State: state, DataAddress: address}, nil
		}
	}

	tests := map[string]struct {
		flow        *DataFlow
		notify      func(sdk *DataPlaneSDK) error
		action      string
		state       DataFlowState
		wantAddress bool
		wantError   string
	}{
		"prepared, push consumer provides its address": {
			flow: &DataFlow{ID: "flow", Consumer: true, Profile: pushProfile, State: Preparing},
			notify: func(sdk *DataPlaneSDK) error {
				_, err := sdk.NotifyPrepared(context.Background(), "flow", addressProcessor(Prepared))
				return err
			},
			action: ActionPrepared, state: Prepared, wantAddress: true,
		},
		"prepared, pull consumer sends no address": {
			flow: &DataFlow{ID: "flow", Consumer: true, Profile: pullProfile, State: Preparing},
			notify: func(sdk *DataPlaneSDK) error {
				_, err := sdk.NotifyPrepared(context.Background(), "flow", addressProcessor(Prepared))
				return err
			},
			action: ActionPrepared, state: Prepared,
		},
		"prepared without processor": {
			flow: &DataFlow{ID: "flow", Consumer: true, Profile: pullProfile, State: Preparing},
			notify: func(sdk *DataPlaneSDK) error {
				_, err := sdk.NotifyPrepared(context.Background(), "flow", nil)
				return err
			},
			action: ActionPrepared, state: Prepared,
		},
		"started, pull provider provides its address": {
			flow: &DataFlow{ID: "flow", Profile: pullProfile, State: Starting},
			notify: func(sdk *DataPlaneSDK) error {
				_, err := sdk.NotifyStarted(context.Background(), "flow", addressProcessor(Started))
				return err
			},
			action: ActionStarted, state: Started, wantAddress: true,
		},
		"started, push provider sends no address": {
			flow: &DataFlow{ID: "flow", Profile: pushProfile, State: Starting, DestinationDataAddress: newDataAddress()},
			notify: func(sdk *DataPlaneSDK) error {
				_, err := sdk.NotifyStarted(context.Background(), "flow", addressProcessor(Started))
				return err
			},
			action: ActionStarted, state: Started,
		},
		"completed": {
			flow:   &DataFlow{ID: "flow", Profile: pullProfile, State: Started, SourceDataAddress: newDataAddress()},
			notify: func(sdk *DataPlaneSDK) error { return sdk.NotifyCompleted(context.Background(), "flow") },
			action: ActionCompleted, state: Completed,
		},
		"errored": {
			flow: &DataFlow{ID: "flow", Profile: pullProfile, State: Started, SourceDataAddress: newDataAddress()},
			notify: func(sdk *DataPlaneSDK) error {
				return sdk.NotifyErrored(context.Background(), "flow", errors.New("disk full"))
			},
			action: ActionErrored, state: Terminated, wantError: "disk full",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			cp := newFakeControlPlane(t)
			sdk, store := newNotifySDK(t, cp)
			store.put(tc.flow)

			require.NoError(t, tc.notify(sdk))

			requests := cp.received()
			require.Len(t, requests, 1)
			request := requests[0]
			assert.Equal(t, http.MethodPost, request.method)
			assert.Equal(t, "/api/control/transfers/flow/dataflow/"+tc.action, request.path)
			assert.Equal(t, jsonContentType, request.contentType)
			assertStatusMessageSchema(t, schemas, request)

			body := decodeCallback(t, request)
			assert.Equal(t, "flow", body["dataFlowId"])
			assert.Equal(t, tc.state.String(), body["state"])
			assert.NotEmpty(t, body["messageId"])
			if tc.wantAddress {
				require.Contains(t, body, "dataAddress")
				assert.Equal(t, address.Endpoint, body["dataAddress"].(map[string]any)["endpoint"])
			} else {
				assert.NotContains(t, body, "dataAddress")
			}
			if tc.wantError != "" {
				assert.Equal(t, tc.wantError, body["error"])
			} else {
				assert.NotContains(t, body, "error")
			}

			flow := store.get(t, "flow")
			assert.Equal(t, tc.state, flow.State)
			if tc.wantError != "" {
				assert.Equal(t, tc.wantError, flow.ErrorDetail)
			}
		})
	}
}

func Test_NotifyPrepared_ReturnsMessage(t *testing.T) {
	cp := newFakeControlPlane(t)
	sdk, store := newNotifySDK(t, cp)
	store.put(&DataFlow{ID: "flow", Consumer: true, Profile: pushProfile, State: Preparing})
	address := newDataAddress()

	message, err := sdk.NotifyPrepared(context.Background(), "flow", func(_ context.Context, _ *DataFlow, _ *DataPlaneSDK, options *ProcessorOptions) (*DataFlowStatusMessage, error) {
		assert.False(t, options.Duplicate)
		return &DataFlowStatusMessage{State: Prepared, DataAddress: address}, nil
	})
	require.NoError(t, err)
	assert.Equal(t, Prepared, message.State)
	assert.Equal(t, "flow", message.DataFlowID)
	assert.NotEmpty(t, message.MessageID)
	assert.Equal(t, address, message.DataAddress)
	assert.Equal(t, address, store.get(t, "flow").DestinationDataAddress)
}

func Test_Notify_PersistsOnlyAfterSuccess(t *testing.T) {
	for _, code := range []int{http.StatusBadRequest, http.StatusNotFound, http.StatusInternalServerError} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			cp := newFakeControlPlane(t, code)
			sdk, store := newNotifySDK(t, cp)
			store.put(&DataFlow{ID: "flow", Profile: pullProfile, State: Starting})

			_, err := sdk.NotifyStarted(context.Background(), "flow", nil)
			var cpErr *ControlPlaneError
			require.ErrorAs(t, err, &cpErr)
			assert.Equal(t, ActionStarted, cpErr.Action)
			assert.Equal(t, code, cpErr.StatusCode)
			assert.Equal(t, "nope", cpErr.Body)

			flow := store.get(t, "flow")
			assert.Equal(t, Starting, flow.State, "the stored flow must not change when the callback fails")
		})
	}
}

func Test_Notify_TransportError(t *testing.T) {
	cp := newFakeControlPlane(t)
	sdk, store := newNotifySDK(t, cp)
	cp.Close() // nothing listens any more
	store.put(&DataFlow{ID: "flow", Profile: pullProfile, State: Started})

	err := sdk.NotifyCompleted(context.Background(), "flow")
	require.Error(t, err)
	var cpErr *ControlPlaneError
	assert.False(t, errors.As(err, &cpErr), "a transport error is not a ControlPlaneError")
	assert.Equal(t, Started, store.get(t, "flow").State)
}

func Test_Notify_ProcessorErrors(t *testing.T) {
	tests := map[string]DataFlowProcessor{
		"wrong state": stateProcessor(Started),
		"error": func(context.Context, *DataFlow, *DataPlaneSDK, *ProcessorOptions) (*DataFlowStatusMessage, error) {
			return nil, errors.New("boom")
		},
		"nil response": func(context.Context, *DataFlow, *DataPlaneSDK, *ProcessorOptions) (*DataFlowStatusMessage, error) {
			return nil, nil
		},
	}
	for name, processor := range tests {
		t.Run(name, func(t *testing.T) {
			cp := newFakeControlPlane(t)
			sdk, store := newNotifySDK(t, cp)
			store.put(&DataFlow{ID: "flow", Consumer: true, Profile: pullProfile, State: Preparing})

			_, err := sdk.NotifyPrepared(context.Background(), "flow", processor)
			assert.Error(t, err)
			assert.Empty(t, cp.received())
			assert.Equal(t, Preparing, store.get(t, "flow").State)
		})
	}
}

func Test_Notify_InvalidTransitions(t *testing.T) {
	tests := map[string]struct {
		state  DataFlowState
		notify func(sdk *DataPlaneSDK) error
	}{
		"started on terminated": {Terminated, func(sdk *DataPlaneSDK) error {
			_, err := sdk.NotifyStarted(context.Background(), "flow", stateProcessor(Started))
			return err
		}},
		"prepared on completed": {Completed, func(sdk *DataPlaneSDK) error {
			_, err := sdk.NotifyPrepared(context.Background(), "flow", nil)
			return err
		}},
		"started on preparing": {Preparing, func(sdk *DataPlaneSDK) error {
			_, err := sdk.NotifyStarted(context.Background(), "flow", nil)
			return err
		}},
		"completed on prepared": {Prepared, func(sdk *DataPlaneSDK) error { return sdk.NotifyCompleted(context.Background(), "flow") }},
		"errored on completed": {Completed, func(sdk *DataPlaneSDK) error {
			return sdk.NotifyErrored(context.Background(), "flow", errors.New("late"))
		}},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			cp := newFakeControlPlane(t)
			sdk, store := newNotifySDK(t, cp)
			store.put(&DataFlow{ID: "flow", Profile: pullProfile, State: tc.state})

			assert.ErrorIs(t, tc.notify(sdk), ErrInvalidTransition)
			assert.Empty(t, cp.received())
			assert.Equal(t, tc.state, store.get(t, "flow").State)
		})
	}
}

func Test_Notify_Duplicates(t *testing.T) {
	tests := map[string]struct {
		state  DataFlowState
		notify func(sdk *DataPlaneSDK) error
	}{
		"prepared": {Prepared, func(sdk *DataPlaneSDK) error {
			_, err := sdk.NotifyPrepared(context.Background(), "flow", func(_ context.Context, _ *DataFlow, _ *DataPlaneSDK, options *ProcessorOptions) (*DataFlowStatusMessage, error) {
				assert.True(t, options.Duplicate)
				return &DataFlowStatusMessage{State: Prepared}, nil
			})
			return err
		}},
		"started": {Started, func(sdk *DataPlaneSDK) error {
			_, err := sdk.NotifyStarted(context.Background(), "flow", nil)
			return err
		}},
		"completed": {Completed, func(sdk *DataPlaneSDK) error { return sdk.NotifyCompleted(context.Background(), "flow") }},
		"errored": {Terminated, func(sdk *DataPlaneSDK) error {
			return sdk.NotifyErrored(context.Background(), "flow", errors.New("again"))
		}},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			cp := newFakeControlPlane(t)
			sdk, store := newNotifySDK(t, cp)
			store.put(&DataFlow{ID: "flow", Consumer: true, Profile: pullProfile, State: tc.state, StateCount: 7})

			require.NoError(t, tc.notify(sdk))
			require.Len(t, cp.received(), 1, "a duplicate notification is sent again")
			assert.Equal(t, tc.state.String(), decodeCallback(t, cp.received()[0])["state"])
			flow := store.get(t, "flow")
			assert.Equal(t, tc.state, flow.State)
			assert.Equal(t, uint(7), flow.StateCount)
		})
	}
}

func Test_Notify_UnknownFlow(t *testing.T) {
	cp := newFakeControlPlane(t)
	sdk, _ := newNotifySDK(t, cp)
	assert.ErrorIs(t, sdk.NotifyCompleted(context.Background(), "unknown"), ErrNotFound)
	assert.Empty(t, cp.received())
}

func Test_Notify_NoControlPlane(t *testing.T) {
	sdk, store := newNotifySDK(t, nil)
	store.put(&DataFlow{ID: "flow", Profile: pullProfile, State: Started})
	assert.ErrorIs(t, sdk.NotifyCompleted(context.Background(), "flow"), ErrControlPlaneNotRegistered)
	assert.Equal(t, Started, store.get(t, "flow").State)
}

func Test_WithControlPlane_ValidatesEndpoint(t *testing.T) {
	for _, endpoint := range []string{"", "/relative", "ftp://cp.example.com", "http://cp.example.com"} {
		_, err := NewDataPlaneSDK(WithStore(&fakeStore{}), WithTransactionContext(&mockTrxContext{}),
			WithControlPlane(ControlPlane{Endpoint: endpoint, Authorization: testEntry()}))
		assert.ErrorIs(t, err, ErrValidation, endpoint)
	}
	for _, endpoint := range []string{"https://cp.example.com", "http://localhost:8080", "http://127.0.0.1:9", "http://[::1]:9"} {
		_, err := NewDataPlaneSDK(WithStore(&fakeStore{}), WithTransactionContext(&mockTrxContext{}),
			WithControlPlane(ControlPlane{Endpoint: endpoint, Authorization: testEntry()}))
		assert.NoError(t, err, endpoint)
	}
	_, err := NewDataPlaneSDK(WithStore(&fakeStore{}), WithTransactionContext(&mockTrxContext{}), WithInsecureEndpoints(),
		WithControlPlane(ControlPlane{Endpoint: "http://cp.example.com", Authorization: testEntry()}))
	assert.NoError(t, err)
}

type recordingResolver struct {
	ids []string
	url string
}

func (r *recordingResolver) ControlPlane(_ context.Context, id string) (*ControlPlane, error) {
	r.ids = append(r.ids, id)
	return &ControlPlane{ID: id, Endpoint: r.url, Authorization: testEntry()}, nil
}

func Test_Notify_UsesResolver(t *testing.T) {
	cp := newFakeControlPlane(t)
	resolver := &recordingResolver{url: cp.URL}
	sdk, store := newNotifySDK(t, nil, WithControlPlaneResolver(resolver))
	store.put(&DataFlow{ID: "flow", Profile: pullProfile, State: Started, ControlPlaneID: "cp-1"})

	require.NoError(t, sdk.NotifyCompleted(context.Background(), "flow"))
	assert.Equal(t, []string{"cp-1"}, resolver.ids)
	assert.Equal(t, "/transfers/flow/dataflow/completed", cp.received()[0].path)
}

// --- local suspend / resume ---

func Test_SuspendAndResumeLocally(t *testing.T) {
	cp := newFakeControlPlane(t)
	sdk, store := newNotifySDK(t, cp)
	store.put(&DataFlow{ID: "flow", Profile: pullProfile, State: Started})

	require.NoError(t, sdk.SuspendLocally(context.Background(), "flow", "maintenance"))
	flow := store.get(t, "flow")
	assert.Equal(t, Suspended, flow.State)
	assert.Equal(t, SuspendedByLocal, flow.SuspendedBy)
	assert.Equal(t, "maintenance", flow.ErrorDetail)
	require.NoError(t, sdk.SuspendLocally(context.Background(), "flow", "maintenance"), "duplicate")

	// the control plane cannot resume a locally suspended flow
	_, err := sdk.Resume(context.Background(), "flow", DataFlowResumeMessage{MessageID: "m"})
	assert.ErrorIs(t, err, ErrInvalidTransition)

	require.NoError(t, sdk.ResumeLocally(context.Background(), "flow", stateProcessor(Started)))
	flow = store.get(t, "flow")
	assert.Equal(t, Started, flow.State)
	assert.Equal(t, SuspendedByNone, flow.SuspendedBy)
	assert.Empty(t, cp.received(), "local suspend and resume do not call the control plane")
}

func Test_SuspendLocally_Errors(t *testing.T) {
	sdk, store := newNotifySDK(t, nil)
	store.put(&DataFlow{ID: "prepared", State: Prepared})
	store.put(&DataFlow{ID: "remote", State: Suspended, SuspendedBy: SuspendedByRemote})

	assert.ErrorIs(t, sdk.SuspendLocally(context.Background(), "prepared", ""), ErrInvalidTransition)
	assert.ErrorIs(t, sdk.SuspendLocally(context.Background(), "remote", ""), ErrInvalidTransition)
	assert.ErrorIs(t, sdk.SuspendLocally(context.Background(), "unknown", ""), ErrNotFound)
}

func Test_ResumeLocally_Errors(t *testing.T) {
	sdk, store := newNotifySDK(t, nil)
	store.put(&DataFlow{ID: "remote", State: Suspended, SuspendedBy: SuspendedByRemote})
	store.put(&DataFlow{ID: "local", Profile: pullProfile, State: Suspended, SuspendedBy: SuspendedByLocal})

	assert.ErrorIs(t, sdk.ResumeLocally(context.Background(), "remote", nil), ErrInvalidTransition)
	assert.Error(t, sdk.ResumeLocally(context.Background(), "local", stateProcessor(Suspended)))
	assert.Equal(t, Suspended, store.get(t, "local").State)
	assert.ErrorIs(t, sdk.ResumeLocally(context.Background(), "unknown", nil), ErrNotFound)
}

// --- HTTP client ---

func Test_HTTPClient_Retries(t *testing.T) {
	tests := map[string]struct {
		codes      []int
		policy     RetryPolicy
		wantErr    bool
		wantStatus int
		requests   int
	}{
		"success":                      {[]int{200}, DefaultRetryPolicy, false, 0, 1},
		"5xx twice then success":       {[]int{503, 500, 200}, DefaultRetryPolicy, false, 0, 3},
		"5xx exhausts attempts":        {[]int{502}, DefaultRetryPolicy, true, 502, 3},
		"4xx is not retried":           {[]int{409}, DefaultRetryPolicy, true, 409, 1},
		"5xx then 4xx stops":           {[]int{500, 400}, DefaultRetryPolicy, true, 400, 2},
		"retries disabled":             {[]int{500, 200}, RetryPolicy{Attempts: 1}, true, 500, 1},
		"zero attempts means one":      {[]int{500, 200}, RetryPolicy{}, true, 500, 1},
		"more attempts than responses": {[]int{500, 500, 500, 500, 204}, RetryPolicy{Attempts: 5, BaseDelay: time.Millisecond}, false, 0, 5},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			cp := newFakeControlPlane(t, tc.codes...)
			var delays []time.Duration
			client := NewHTTPControlPlaneClient(WithRetryPolicy(tc.policy))
			client.sleep = func(_ context.Context, d time.Duration) error {
				delays = append(delays, d)
				return nil
			}

			err := client.Notify(context.Background(), ControlPlane{Endpoint: cp.URL}, ActionStarted, DataFlowStatusMessage{MessageID: "m", DataFlowID: "flow", State: Started})
			if tc.wantErr {
				var cpErr *ControlPlaneError
				require.ErrorAs(t, err, &cpErr)
				assert.Equal(t, tc.wantStatus, cpErr.StatusCode)
			} else {
				assert.NoError(t, err)
			}
			assert.Len(t, cp.received(), tc.requests)
			assert.Len(t, delays, tc.requests-1)
			for i, d := range delays {
				assert.Equal(t, tc.policy.BaseDelay<<i, d, "exponential backoff")
			}
		})
	}
}

func Test_HTTPClient_RetriesTransportErrors(t *testing.T) {
	var calls int
	client := NewHTTPControlPlaneClient(WithHTTPClient(&http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		return nil, errors.New("connection refused")
	})}))
	client.sleep = noSleep

	err := client.Notify(context.Background(), ControlPlane{Endpoint: "http://cp.example"}, ActionStarted, DataFlowStatusMessage{DataFlowID: "flow"})
	assert.ErrorContains(t, err, "connection refused")
	assert.Equal(t, DefaultRetryPolicy.Attempts, calls)
}

func Test_HTTPClient_ContextCancellationStopsRetries(t *testing.T) {
	cp := newFakeControlPlane(t, http.StatusServiceUnavailable)
	client := NewHTTPControlPlaneClient(WithRetryPolicy(RetryPolicy{Attempts: 10, BaseDelay: time.Hour}))
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		// cancel once the first attempt failed and the client waits for the backoff
		for len(cp.received()) == 0 {
			time.Sleep(time.Millisecond)
		}
		cancel()
	}()

	start := time.Now()
	err := client.Notify(ctx, ControlPlane{Endpoint: cp.URL}, ActionStarted, DataFlowStatusMessage{DataFlowID: "flow"})
	assert.ErrorIs(t, err, context.Canceled)
	assert.Less(t, time.Since(start), 10*time.Second)
	assert.Len(t, cp.received(), 1)
}

func Test_HTTPClient_UsesCustomClient(t *testing.T) {
	var authorization string
	cp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authorization = r.Header.Get("Authorization")
	}))
	defer cp.Close()
	httpClient := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		r.Header.Set("Authorization", "Bearer token")
		return http.DefaultTransport.RoundTrip(r)
	})}

	client := NewHTTPControlPlaneClient(WithHTTPClient(httpClient))
	require.NoError(t, client.Notify(context.Background(), ControlPlane{Endpoint: cp.URL}, ActionCompleted, DataFlowStatusMessage{DataFlowID: "flow"}))
	assert.Equal(t, "Bearer token", authorization)
}

func Test_HTTPClient_Agreement(t *testing.T) {
	cp := newFakeControlPlane(t)
	client := newTestClient(DefaultRetryPolicy)

	agreement, err := client.GetAgreement(context.Background(), ControlPlane{Endpoint: cp.URL + "/api/"}, "flow-1")
	require.NoError(t, err)
	assert.JSONEq(t, `{"@id": "agreement-1"}`, string(agreement))
	assert.Equal(t, http.MethodGet, cp.received()[0].method)
	assert.Equal(t, "/api/transfers/flow-1/agreement", cp.received()[0].path)

	failing := newFakeControlPlane(t, http.StatusNotFound)
	_, err = client.GetAgreement(context.Background(), ControlPlane{Endpoint: failing.URL}, "flow-1")
	var cpErr *ControlPlaneError
	require.ErrorAs(t, err, &cpErr)
	assert.Equal(t, ActionAgreement, cpErr.Action)
	assert.Equal(t, http.StatusNotFound, cpErr.StatusCode)
}

func Test_HTTPClient_EscapesFlowIDOnTheWire(t *testing.T) {
	cp := newFakeControlPlane(t)
	client := newTestClient(DefaultRetryPolicy)
	require.NoError(t, client.Notify(context.Background(), ControlPlane{Endpoint: cp.URL + "/api"}, ActionStarted, DataFlowStatusMessage{DataFlowID: "../../admin?x=1#f"}))
	request := cp.received()[0]
	assert.Equal(t, "/api/transfers/..%2F..%2Fadmin%3Fx=1%23f/dataflow/started", request.rawPath)
	assert.Equal(t, "/api/transfers/../../admin?x=1#f/dataflow/started", request.path)
}

func Test_TransferURL(t *testing.T) {
	tests := []struct {
		base, id string
		segments []string
		want     string
	}{
		{"http://cp.example", "flow-1", []string{"dataflow", "started"}, "http://cp.example/transfers/flow-1/dataflow/started"},
		{"http://cp.example/", "flow-1", []string{"dataflow", "started"}, "http://cp.example/transfers/flow-1/dataflow/started"},
		{"http://cp.example/api", "flow-1", []string{"dataflow", "started"}, "http://cp.example/api/transfers/flow-1/dataflow/started"},
		{"http://cp.example/api/", "flow-1", []string{"dataflow", "started"}, "http://cp.example/api/transfers/flow-1/dataflow/started"},
		{"https://cp.example:8443/a%20b/", "flow-1", []string{"agreement"}, "https://cp.example:8443/a%20b/transfers/flow-1/agreement"},
		{"http://cp.example/api", "../../admin?x=1#f", []string{"dataflow", "started"}, "http://cp.example/api/transfers/..%2F..%2Fadmin%3Fx=1%23f/dataflow/started"},
		{"http://cp.example/api?debug=1#frag", "flow 1", []string{"dataflow", "errored"}, "http://cp.example/api/transfers/flow%201/dataflow/errored"},
	}
	for _, tc := range tests {
		t.Run(tc.base+" "+tc.id, func(t *testing.T) {
			got, err := transferURL(tc.base, tc.id, tc.segments...)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}

	for _, base := range []string{"not a url", "cp.example/api", "://", ""} {
		_, err := transferURL(base, "flow-1", "dataflow", "started")
		assert.ErrorIs(t, err, ErrInvalidInput, base)
	}
	_, err := transferURL("http://cp.example", "", "dataflow", "started")
	assert.ErrorIs(t, err, ErrInvalidInput)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
