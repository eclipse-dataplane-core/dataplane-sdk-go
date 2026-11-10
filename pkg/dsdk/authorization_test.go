package dsdk

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// otherAuthorization accepts "Other <id>" headers.
type otherAuthorization struct{}

func (otherAuthorization) Type() string { return "other" }

func (otherAuthorization) AuthorizationHeader(context.Context, AuthorizationEntry) (string, error) {
	return "Other dp", nil
}

func (otherAuthorization) ExtractCallerID(_ context.Context, header string) (string, error) {
	if len(header) > 6 && header[:6] == "Other " {
		return header[6:], nil
	}
	return "", fmt.Errorf("%w: not an other header", ErrUnauthorized)
}

// allRoutes lists every route NewHandler mounts when a control plane store is configured.
func allRoutes(flowID string) []struct{ method, path string } {
	return []struct{ method, path string }{
		{http.MethodPost, "/v1/dataflows/prepare"},
		{http.MethodPost, "/v1/dataflows/start"},
		{http.MethodPost, "/v1/dataflows/" + flowID + "/started"},
		{http.MethodPost, "/v1/dataflows/" + flowID + "/suspend"},
		{http.MethodPost, "/v1/dataflows/" + flowID + "/resume"},
		{http.MethodPost, "/v1/dataflows/" + flowID + "/terminate"},
		{http.MethodPost, "/v1/dataflows/" + flowID + "/completed"},
		{http.MethodGet, "/v1/dataflows/" + flowID + "/status"},
		{http.MethodPut, "/v1/controlplanes"},
		{http.MethodDelete, "/v1/controlplanes/cp-1"},
	}
}

// --- inbound ---

func Test_Router_RequiresAuthorization(t *testing.T) {
	sdk, store := newTestSDK(t, WithControlPlaneStore(newFakeControlPlaneStore()))
	handler := NewHandler(sdk)
	store.put(&DataFlow{ID: "flow", Profile: pullProfile, State: Started})

	for _, route := range allRoutes("flow") {
		for name, header := range map[string]string{"missing": "", "invalid": "Bogus x", "empty id": "Test "} {
			t.Run(route.method+" "+route.path+" "+name, func(t *testing.T) {
				// a malformed body: authentication must fail first
				rr := serveAs(t, handler, header, route.method, route.path, "{")
				assert.Equal(t, http.StatusUnauthorized, rr.Code)
				var errorResponse ErrorResponse
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &errorResponse))
				assert.NotEmpty(t, errorResponse.Error)
			})
		}
	}
	assert.Equal(t, Started, store.get(t, "flow").State)
}

func Test_Router_NoAuthorizationRegistered(t *testing.T) {
	store := &fakeStore{flows: map[string]*DataFlow{}}
	sdk, err := NewDataPlaneSDK(WithStore(store), WithTransactionContext(&mockTrxContext{}))
	require.NoError(t, err)
	rr := serve(t, NewHandler(sdk), http.MethodPost, "/v1/dataflows/start", createStartMessage(pullProfile, nil))
	assert.Equal(t, http.StatusUnauthorized, rr.Code)
}

func Test_Router_RecordsCaller(t *testing.T) {
	sdk, store := newTestSDK(t)
	handler := NewHandler(sdk)

	start := createStartMessage(pullProfile, nil)
	require.Equal(t, http.StatusOK, serveAs(t, handler, "Test cp-1", http.MethodPost, "/v1/dataflows/start", start).Code)
	assert.Equal(t, "cp-1", store.get(t, start.DataFlowID).ControlPlaneID)

	prepare := createPrepareMessage(pullProfile)
	require.Equal(t, http.StatusOK, serveAs(t, handler, "Test cp-2", http.MethodPost, "/v1/dataflows/prepare", prepare).Code)
	assert.Equal(t, "cp-2", store.get(t, prepare.DataFlowID).ControlPlaneID)
}

func Test_Router_ForbidsOtherControlPlanes(t *testing.T) {
	tests := []struct {
		method, path string
		body         any
		state        DataFlowState
		consumer     bool
	}{
		{http.MethodPost, "/v1/dataflows/flow/started", DataFlowStartedNotificationMessage{MessageID: "m"}, Prepared, true},
		{http.MethodPost, "/v1/dataflows/flow/suspend", DataFlowSuspendMessage{MessageID: "m"}, Started, false},
		{http.MethodPost, "/v1/dataflows/flow/resume", DataFlowResumeMessage{MessageID: "m"}, Suspended, false},
		{http.MethodPost, "/v1/dataflows/flow/terminate", DataFlowTerminateMessage{MessageID: "m"}, Started, false},
		{http.MethodPost, "/v1/dataflows/flow/completed", nil, Started, false},
		{http.MethodGet, "/v1/dataflows/flow/status", nil, Started, false},
	}
	for _, tc := range tests {
		t.Run(tc.path, func(t *testing.T) {
			sdk, store := newTestSDK(t)
			handler := NewHandler(sdk)
			flow := &DataFlow{ID: "flow", Consumer: tc.consumer, Profile: pushProfile, State: tc.state, ControlPlaneID: "owner"}
			if tc.state == Suspended {
				flow.SuspendedBy = SuspendedByRemote
			}
			store.put(flow)

			rr := serveAs(t, handler, "Test intruder", tc.method, tc.path, tc.body)
			assert.Equal(t, http.StatusForbidden, rr.Code, rr.Body.String())
			assert.Equal(t, tc.state, store.get(t, "flow").State)

			rr = serveAs(t, handler, "Test owner", tc.method, tc.path, tc.body)
			assert.Less(t, rr.Code, 300, rr.Body.String())
		})
	}

	t.Run("flows without a control plane are not checked", func(t *testing.T) {
		sdk, store := newTestSDK(t)
		store.put(&DataFlow{ID: "flow", Profile: pullProfile, State: Started})
		assert.Equal(t, http.StatusOK, serveAs(t, NewHandler(sdk), "Test anyone", http.MethodGet, "/v1/dataflows/flow/status", nil).Code)
	})

	t.Run("duplicate prepare from another control plane", func(t *testing.T) {
		sdk, store := newTestSDK(t)
		prepare := createPrepareMessage(pullProfile)
		store.put(&DataFlow{ID: prepare.DataFlowID, Consumer: true, Profile: pullProfile, State: Prepared, ControlPlaneID: "owner"})
		assert.Equal(t, http.StatusForbidden, serveAs(t, NewHandler(sdk), "Test intruder", http.MethodPost, "/v1/dataflows/prepare", prepare).Code)
		assert.Equal(t, http.StatusOK, serveAs(t, NewHandler(sdk), "Test owner", http.MethodPost, "/v1/dataflows/prepare", prepare).Code)
	})
}

func Test_Router_RegistrationRequiresMatchingCaller(t *testing.T) {
	controlPlanes := newFakeControlPlaneStore()
	sdk, _ := newTestSDK(t, WithControlPlaneStore(controlPlanes))
	handler := NewHandler(sdk)

	assert.Equal(t, http.StatusForbidden, serveAs(t, handler, "Test cp-2", http.MethodPut, "/v1/controlplanes", newRegistrationMessage("cp-1")).Code)
	_, err := controlPlanes.FindById(context.Background(), "cp-1")
	assert.ErrorIs(t, err, ErrNotFound)

	require.Equal(t, http.StatusOK, serveAs(t, handler, "Test cp-1", http.MethodPut, "/v1/controlplanes", newRegistrationMessage("cp-1")).Code)
	assert.Equal(t, http.StatusForbidden, serveAs(t, handler, "Test cp-2", http.MethodDelete, "/v1/controlplanes/cp-1", nil).Code)
	assert.Equal(t, http.StatusNoContent, serveAs(t, handler, "Test cp-1", http.MethodDelete, "/v1/controlplanes/cp-1", nil).Code)
}

func Test_Router_AuthorizationDisabled(t *testing.T) {
	controlPlanes := newFakeControlPlaneStore()
	sdk, store := newTestSDK(t, WithAuthorizationDisabled(), WithControlPlaneStore(controlPlanes))
	assert.True(t, sdk.AuthorizationDisabled())
	handler := NewHandler(sdk)

	start := createStartMessage(pullProfile, nil)
	require.Equal(t, http.StatusOK, serveAs(t, handler, "", http.MethodPost, "/v1/dataflows/start", start).Code)
	assert.Empty(t, store.get(t, start.DataFlowID).ControlPlaneID)

	// a header is ignored, so no caller checks apply
	store.put(&DataFlow{ID: "flow", Profile: pullProfile, State: Started, ControlPlaneID: "owner"})
	assert.Equal(t, http.StatusOK, serveAs(t, handler, "Test intruder", http.MethodGet, "/v1/dataflows/flow/status", nil).Code)
	assert.Equal(t, http.StatusOK, serveAs(t, handler, "", http.MethodPut, "/v1/controlplanes", newRegistrationMessage("cp-1")).Code)

	// with authorization disabled, any authorization type can be registered
	unknown := newRegistrationMessage("cp-2")
	unknown.Authorization = AuthorizationEntry{Type: "unknown"}
	assert.Equal(t, http.StatusOK, serveAs(t, handler, "", http.MethodPut, "/v1/controlplanes", unknown).Code)
}

func Test_Router_CustomAuthenticator(t *testing.T) {
	authenticator := WithAuthenticator(func(r *http.Request) (string, error) {
		id := r.Header.Get("X-Control-Plane")
		if id == "" {
			return "", errors.New("missing header")
		}
		return id, nil
	})
	for name, options := range map[string][]DataPlaneSDKOption{"enabled": nil, "disabled": {WithAuthorizationDisabled()}} {
		t.Run(name, func(t *testing.T) {
			sdk, store := newTestSDK(t, options...)
			handler := NewHandler(sdk, authenticator)

			msg := createStartMessage(pullProfile, nil)
			rr := serve(t, handler, http.MethodPost, "/v1/dataflows/start", msg)
			assert.Equal(t, http.StatusUnauthorized, rr.Code, "the default scheme is replaced")
			assert.Contains(t, rr.Body.String(), "missing header")

			data, err := json.Marshal(msg)
			require.NoError(t, err)
			req, err := http.NewRequest(http.MethodPost, "/v1/dataflows/start", bytes.NewReader(data))
			require.NoError(t, err)
			req.Header.Set("X-Control-Plane", "cp-7")
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, req)
			assert.Equal(t, http.StatusOK, recorder.Code)
			assert.Equal(t, "cp-7", store.get(t, msg.DataFlowID).ControlPlaneID)
		})
	}
}

func Test_ExtractControlPlaneID(t *testing.T) {
	sdk, _ := newTestSDK(t, WithAuthorization(otherAuthorization{}))

	id, err := sdk.ExtractControlPlaneID(context.Background(), "Test cp-1")
	require.NoError(t, err)
	assert.Equal(t, "cp-1", id)
	id, err = sdk.ExtractControlPlaneID(context.Background(), "Other cp-2")
	require.NoError(t, err)
	assert.Equal(t, "cp-2", id)

	_, err = sdk.ExtractControlPlaneID(context.Background(), "Bearer x")
	assert.ErrorIs(t, err, ErrUnauthorized)
	assert.ErrorContains(t, err, "test:")
	assert.ErrorContains(t, err, "other:")
	_, err = sdk.ExtractControlPlaneID(context.Background(), "")
	assert.ErrorIs(t, err, ErrUnauthorized)

	none, err := NewDataPlaneSDK(WithStore(&fakeStore{}), WithTransactionContext(&mockTrxContext{}))
	require.NoError(t, err)
	_, err = none.ExtractControlPlaneID(context.Background(), "Test cp-1")
	assert.ErrorIs(t, err, ErrUnauthorized)
}

// --- registration ---

func Test_RegisterControlPlane_AuthorizationAndEndpoint(t *testing.T) {
	sdk, _ := newTestSDK(t, WithControlPlaneStore(newFakeControlPlaneStore()))
	ctx := context.Background()

	unsupported := newRegistrationMessage("cp-1")
	unsupported.Authorization = AuthorizationEntry{Type: "oauth2_client_credentials"}
	assert.ErrorIs(t, sdk.RegisterControlPlane(ctx, unsupported), ErrAuthorizationNotSupported)

	plain := newRegistrationMessage("cp-1")
	plain.Endpoint = "http://cp.example.com"
	assert.ErrorIs(t, sdk.RegisterControlPlane(ctx, plain), ErrValidation)

	loopback := newRegistrationMessage("cp-1")
	loopback.Endpoint = "http://localhost:8080/signaling"
	assert.NoError(t, sdk.RegisterControlPlane(ctx, loopback))

	insecure, _ := newTestSDK(t, WithControlPlaneStore(newFakeControlPlaneStore()), WithInsecureEndpoints())
	assert.NoError(t, insecure.RegisterControlPlane(ctx, plain))

	// the API answers 400 for an unsupported type
	rr := serveAs(t, NewHandler(sdk), "Test cp-1", http.MethodPut, "/v1/controlplanes", unsupported)
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "authorization not supported")
}

func Test_RegisterDataPlane_Authorization(t *testing.T) {
	cp := newFakeControlPlane(t)
	sdk, _ := newNotifySDK(t, nil)
	target := ControlPlane{ID: "cp-1", Endpoint: cp.URL, Authorization: testEntry()}

	unsupported := newDataPlaneRegistration()
	unsupported.Authorization = AuthorizationEntry{Type: "oauth2_client_credentials"}
	assert.ErrorIs(t, sdk.RegisterDataPlane(context.Background(), target, unsupported), ErrAuthorizationNotSupported)
	assert.Empty(t, cp.received())

	require.NoError(t, sdk.RegisterDataPlane(context.Background(), target, newDataPlaneRegistration()))
	require.NoError(t, sdk.DeregisterDataPlane(context.Background(), target, "dataplane-1"))
	for _, request := range cp.received() {
		assert.Equal(t, "Test "+testPrincipal, request.authorization)
	}

	insecureTarget := ControlPlane{Endpoint: "http://cp.example.com", Authorization: testEntry()}
	assert.ErrorIs(t, sdk.RegisterDataPlane(context.Background(), insecureTarget, newDataPlaneRegistration()), ErrValidation)
	assert.ErrorIs(t, sdk.DeregisterDataPlane(context.Background(), insecureTarget, "dataplane-1"), ErrValidation)
}

// --- outbound ---

func Test_Notify_SendsAuthorizationHeader(t *testing.T) {
	cp := newFakeControlPlane(t)
	sdk, store := newNotifySDK(t, cp)
	store.put(&DataFlow{ID: "flow", Profile: pullProfile, State: Started})

	require.NoError(t, sdk.NotifyCompleted(context.Background(), "flow"))
	assert.Equal(t, "Test "+testPrincipal, cp.received()[0].authorization)
}

func Test_Notify_WithoutOutboundAuthorization(t *testing.T) {
	cases := map[string]AuthorizationEntry{
		"no type":          {},
		"unsupported type": {Type: "oauth2_client_credentials"},
	}
	for name, entry := range cases {
		t.Run(name+" fails", func(t *testing.T) {
			cp := newFakeControlPlane(t)
			sdk, store := newTestSDK(t, WithControlPlaneClient(newTestClient(DefaultRetryPolicy)),
				WithControlPlane(ControlPlane{Endpoint: cp.URL, Authorization: entry}))
			store.put(&DataFlow{ID: "flow", Profile: pullProfile, State: Started})

			assert.ErrorIs(t, sdk.NotifyCompleted(context.Background(), "flow"), ErrAuthorizationNotSupported)
			assert.Empty(t, cp.received())
			assert.Equal(t, Started, store.get(t, "flow").State)
		})
		t.Run(name+" is sent without a header when disabled", func(t *testing.T) {
			cp := newFakeControlPlane(t)
			sdk, store := newTestSDK(t, WithAuthorizationDisabled(), WithControlPlaneClient(newTestClient(DefaultRetryPolicy)),
				WithControlPlane(ControlPlane{Endpoint: cp.URL, Authorization: entry}))
			store.put(&DataFlow{ID: "flow", Profile: pullProfile, State: Started})

			require.NoError(t, sdk.NotifyCompleted(context.Background(), "flow"))
			require.Len(t, cp.received(), 1)
			assert.Empty(t, cp.received()[0].authorization)
		})
	}
}

func Test_Notify_AuthorizationHeaderError(t *testing.T) {
	cp := newFakeControlPlane(t)
	sdk, store := newTestSDK(t, WithControlPlaneClient(newTestClient(DefaultRetryPolicy)),
		WithControlPlane(ControlPlane{Endpoint: cp.URL, Authorization: AuthorizationEntry{Type: testAuthorizationType}}))
	store.put(&DataFlow{ID: "flow", Profile: pullProfile, State: Started})

	assert.ErrorContains(t, sdk.NotifyCompleted(context.Background(), "flow"), "no principal")
	assert.Empty(t, cp.received())
	assert.Equal(t, Started, store.get(t, "flow").State)
}

func Test_HTTPClient_CustomAuthorizationHeaderIsKept(t *testing.T) {
	cp := newFakeControlPlane(t)
	client := NewHTTPControlPlaneClient(WithAuthorizationHeader(func(context.Context, AuthorizationEntry) (string, error) {
		return "Custom", nil
	}))
	sdk, store := newTestSDK(t, WithControlPlaneClient(client), WithControlPlane(ControlPlane{Endpoint: cp.URL, Authorization: testEntry()}))
	store.put(&DataFlow{ID: "flow", Profile: pullProfile, State: Started})

	require.NoError(t, sdk.NotifyCompleted(context.Background(), "flow"))
	assert.Equal(t, "Custom", cp.received()[0].authorization)
}

// capturingMonitor records the messages logged through the SDK's Monitor.
type capturingMonitor struct {
	mu       sync.Mutex
	messages []string
}

func (m *capturingMonitor) Println(v ...any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = append(m.messages, fmt.Sprintln(v...))
}

func (m *capturingMonitor) Printf(format string, v ...any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = append(m.messages, fmt.Sprintf(format, v...))
}

func (m *capturingMonitor) warnings() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var warnings []string
	for _, message := range m.messages {
		if strings.HasPrefix(message, "WARN: ") {
			warnings = append(warnings, strings.TrimSpace(message))
		}
	}
	return warnings
}

func Test_ForbiddenAccess_LogsWarning(t *testing.T) {
	flowRoutes := []struct {
		method, path string
		body         any
	}{
		{http.MethodPost, "/v1/dataflows/flow/started", DataFlowStartedNotificationMessage{MessageID: "m"}},
		{http.MethodPost, "/v1/dataflows/flow/suspend", DataFlowSuspendMessage{MessageID: "m"}},
		{http.MethodPost, "/v1/dataflows/flow/resume", DataFlowResumeMessage{MessageID: "m"}},
		{http.MethodPost, "/v1/dataflows/flow/terminate", DataFlowTerminateMessage{MessageID: "m"}},
		{http.MethodPost, "/v1/dataflows/flow/completed", nil},
		{http.MethodGet, "/v1/dataflows/flow/status", nil},
	}
	for _, route := range flowRoutes {
		t.Run(route.path, func(t *testing.T) {
			monitor := &capturingMonitor{}
			sdk, store := newTestSDK(t, WithMonitor(monitor))
			store.put(&DataFlow{ID: "flow", Consumer: true, Profile: pushProfile, State: Started, ControlPlaneID: "owner"})

			require.Equal(t, http.StatusForbidden, serveAs(t, NewHandler(sdk), "Test intruder", route.method, route.path, route.body).Code)
			assert.Equal(t, []string{`WARN: control plane "intruder" attempted to access data flow "flow" owned by control plane "owner"`}, monitor.warnings())
		})
	}

	t.Run("prepare", func(t *testing.T) {
		monitor := &capturingMonitor{}
		sdk, store := newTestSDK(t, WithMonitor(monitor))
		prepare := createPrepareMessage(pullProfile)
		store.put(&DataFlow{ID: prepare.DataFlowID, Consumer: true, Profile: pullProfile, State: Prepared, ControlPlaneID: "owner"})

		require.Equal(t, http.StatusForbidden, serveAs(t, NewHandler(sdk), "Test intruder", http.MethodPost, "/v1/dataflows/prepare", prepare).Code)
		assert.Equal(t, []string{fmt.Sprintf(`WARN: control plane "intruder" attempted to access data flow %q owned by control plane "owner"`, prepare.DataFlowID)}, monitor.warnings())
	})

	t.Run("registration", func(t *testing.T) {
		monitor := &capturingMonitor{}
		sdk, _ := newTestSDK(t, WithMonitor(monitor), WithControlPlaneStore(newFakeControlPlaneStore()))
		handler := NewHandler(sdk)

		require.Equal(t, http.StatusForbidden, serveAs(t, handler, "Test intruder", http.MethodPut, "/v1/controlplanes", newRegistrationMessage("cp-1")).Code)
		require.Equal(t, http.StatusForbidden, serveAs(t, handler, "Test intruder", http.MethodDelete, "/v1/controlplanes/cp-1", nil).Code)
		assert.Equal(t, []string{
			`WARN: control plane "intruder" attempted to register control plane "cp-1"`,
			`WARN: control plane "intruder" attempted to delete control plane "cp-1"`,
		}, monitor.warnings())
	})

	t.Run("allowed access logs nothing", func(t *testing.T) {
		monitor := &capturingMonitor{}
		sdk, store := newTestSDK(t, WithMonitor(monitor), WithControlPlaneStore(newFakeControlPlaneStore()))
		store.put(&DataFlow{ID: "flow", Profile: pullProfile, State: Started, ControlPlaneID: "owner"})
		handler := NewHandler(sdk)

		require.Equal(t, http.StatusOK, serveAs(t, handler, "Test owner", http.MethodGet, "/v1/dataflows/flow/status", nil).Code)
		require.Equal(t, http.StatusOK, serveAs(t, handler, "Test cp-1", http.MethodPut, "/v1/controlplanes", newRegistrationMessage("cp-1")).Code)
		assert.Empty(t, monitor.warnings())
	})
}
