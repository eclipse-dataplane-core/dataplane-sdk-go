package dsdk

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"sync"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeControlPlaneStore is a minimal ControlPlaneStore that stores copies.
type fakeControlPlaneStore struct {
	mu            sync.Mutex
	controlPlanes map[string]*ControlPlane
	err           error
}

func newFakeControlPlaneStore(controlPlanes ...*ControlPlane) *fakeControlPlaneStore {
	s := &fakeControlPlaneStore{controlPlanes: map[string]*ControlPlane{}}
	for _, cp := range controlPlanes {
		s.controlPlanes[cp.ID] = cp.Copy()
	}
	return s
}

func (s *fakeControlPlaneStore) Save(_ context.Context, cp *ControlPlane) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	s.controlPlanes[cp.ID] = cp.Copy()
	return nil
}

func (s *fakeControlPlaneStore) FindById(_ context.Context, id string) (*ControlPlane, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return nil, s.err
	}
	cp, ok := s.controlPlanes[id]
	if !ok {
		return nil, ErrNotFound
	}
	return cp.Copy(), nil
}

func (s *fakeControlPlaneStore) FindAll(context.Context) ([]*ControlPlane, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return nil, s.err
	}
	var all []*ControlPlane
	for _, cp := range s.controlPlanes {
		all = append(all, cp.Copy())
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })
	return all, nil
}

func (s *fakeControlPlaneStore) Delete(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.controlPlanes[id]; !ok {
		return ErrNotFound
	}
	delete(s.controlPlanes, id)
	return nil
}

func newRegistrationMessage(id string) ControlPlaneRegistrationMessage {
	return ControlPlaneRegistrationMessage{
		ControlPlaneID: id,
		Endpoint:       "https://cp.example.com/signaling",
		Authorization: AuthorizationEntry{
			Type: testAuthorizationType,
			Properties: map[string]any{
				"principal":     testPrincipal,
				"tokenEndpoint": "https://auth.example.com/oauth2/token",
				"clientId":      "123456789",
				"clientSecret":  "s3cr3t",
			},
		},
	}
}

func newDataPlaneRegistration() DataPlaneRegistrationMessage {
	return DataPlaneRegistrationMessage{
		DataPlaneID:   "dataplane-1",
		Endpoint:      "https://dp.example.com/v1",
		Profiles:      []string{pullProfile, pushProfile},
		Authorization: AuthorizationEntry{Type: testAuthorizationType, Properties: map[string]any{"clientId": "dp"}},
		Labels:        []string{"gold"},
	}
}

// --- messages ---

func Test_AuthorizationEntry_JSON(t *testing.T) {
	entry := newRegistrationMessage("cp").Authorization
	data, err := json.Marshal(entry)
	require.NoError(t, err)
	assert.JSONEq(t, `{"type": "test", "principal": "test-dp", "tokenEndpoint": "https://auth.example.com/oauth2/token", "clientId": "123456789", "clientSecret": "s3cr3t"}`, string(data))

	var decoded AuthorizationEntry
	require.NoError(t, json.Unmarshal(data, &decoded))
	assert.Equal(t, entry, decoded)
	clientID, ok := decoded.Property("clientId")
	assert.True(t, ok)
	assert.Equal(t, "123456789", clientID)
	_, ok = decoded.Property("missing")
	assert.False(t, ok)

	// non-string properties are preserved
	require.NoError(t, json.Unmarshal([]byte(`{"type": "x", "scopes": ["a", "b"], "ttl": 30}`), &decoded))
	assert.Equal(t, AuthorizationEntry{Type: "x", Properties: map[string]any{"scopes": []any{"a", "b"}, "ttl": float64(30)}}, decoded)
	_, ok = decoded.Property("ttl")
	assert.False(t, ok, "Property only returns strings")

	// type only
	data, err = json.Marshal(AuthorizationEntry{Type: "none"})
	require.NoError(t, err)
	assert.JSONEq(t, `{"type": "none"}`, string(data))

	assert.Error(t, json.Unmarshal([]byte(`{"type": 42}`), &decoded))
	assert.Error(t, json.Unmarshal([]byte(`"oauth2"`), &decoded))
}

func Test_RegistrationMessages_JSON(t *testing.T) {
	message := newRegistrationMessage("cp-1")
	data, err := json.Marshal(message)
	require.NoError(t, err)
	var raw map[string]any
	require.NoError(t, json.Unmarshal(data, &raw))
	assert.Equal(t, "cp-1", raw["controlplaneId"])
	assert.Equal(t, testAuthorizationType, raw["authorization"].(map[string]any)["type"])

	var decoded ControlPlaneRegistrationMessage
	require.NoError(t, json.Unmarshal(data, &decoded))
	assert.Equal(t, message, decoded)

	dataPlane := newDataPlaneRegistration()
	data, err = json.Marshal(dataPlane)
	require.NoError(t, err)
	var decodedDataPlane DataPlaneRegistrationMessage
	require.NoError(t, json.Unmarshal(data, &decodedDataPlane))
	assert.Equal(t, dataPlane, decodedDataPlane)

	// spec example
	example := `{"controlplaneId": "bcf2d204-03bc-4354-8e92-b15b68d3c358", "endpoint": "https://example.com/signaling", "authorization": {"type": "..."}}`
	require.NoError(t, json.Unmarshal([]byte(example), &decoded))
	assert.NoError(t, decoded.Validate())
}

func Test_RegistrationMessages_ConformToSchemas(t *testing.T) {
	schemas := loadSchemas(t)
	assertConforms(t, schemas, "ControlPlaneRegistrationMessage", newRegistrationMessage("cp-1"))
	assertConforms(t, schemas, "DataPlaneRegistrationMessage", newDataPlaneRegistration())
	withoutLabels := newDataPlaneRegistration()
	withoutLabels.Labels = nil
	assertConforms(t, schemas, "DataPlaneRegistrationMessage", withoutLabels)
	assertConforms(t, schemas, "AuthorizationEntry", newRegistrationMessage("cp-1").Authorization)
	assertConforms(t, schemas, "AuthorizationEntry", AuthorizationEntry{Type: "none"})
}

func Test_ControlPlaneRegistrationMessage_Validate(t *testing.T) {
	tests := map[string]func(m *ControlPlaneRegistrationMessage){
		"missing id":              func(m *ControlPlaneRegistrationMessage) { m.ControlPlaneID = "" },
		"missing endpoint":        func(m *ControlPlaneRegistrationMessage) { m.Endpoint = "" },
		"relative endpoint":       func(m *ControlPlaneRegistrationMessage) { m.Endpoint = "/signaling" },
		"endpoint without scheme": func(m *ControlPlaneRegistrationMessage) { m.Endpoint = "cp.example.com" },
		"missing authorization":   func(m *ControlPlaneRegistrationMessage) { m.Authorization = AuthorizationEntry{} },
		"authorization without type": func(m *ControlPlaneRegistrationMessage) {
			m.Authorization = AuthorizationEntry{Properties: map[string]any{"clientId": "x"}}
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			message := newRegistrationMessage("cp-1")
			mutate(&message)
			assert.ErrorIs(t, message.Validate(), ErrValidation)
		})
	}
	message := newRegistrationMessage("cp-1")
	assert.NoError(t, message.Validate())
}

func Test_DataPlaneRegistrationMessage_Validate(t *testing.T) {
	tests := map[string]func(m *DataPlaneRegistrationMessage){
		"missing id":            func(m *DataPlaneRegistrationMessage) { m.DataPlaneID = "" },
		"missing endpoint":      func(m *DataPlaneRegistrationMessage) { m.Endpoint = "" },
		"relative endpoint":     func(m *DataPlaneRegistrationMessage) { m.Endpoint = "v1" },
		"no profiles":           func(m *DataPlaneRegistrationMessage) { m.Profiles = nil },
		"empty profile":         func(m *DataPlaneRegistrationMessage) { m.Profiles = []string{""} },
		"missing authorization": func(m *DataPlaneRegistrationMessage) { m.Authorization = AuthorizationEntry{} },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			message := newDataPlaneRegistration()
			mutate(&message)
			assert.ErrorIs(t, message.Validate(), ErrValidation)
		})
	}
	message := newDataPlaneRegistration()
	assert.NoError(t, message.Validate())
}

// --- SDK ---

func Test_RegisterAndDeleteControlPlane(t *testing.T) {
	store := newFakeControlPlaneStore()
	sdk, _ := newTestSDK(t, WithControlPlaneStore(store))

	require.NoError(t, sdk.RegisterControlPlane(context.Background(), newRegistrationMessage("cp-1")))
	controlPlane, err := store.FindById(context.Background(), "cp-1")
	require.NoError(t, err)
	assert.Equal(t, "https://cp.example.com/signaling", controlPlane.Endpoint)
	assert.Equal(t, testAuthorizationType, controlPlane.Authorization.Type)

	// replace semantics
	update := newRegistrationMessage("cp-1")
	update.Endpoint = "https://cp2.example.com"
	update.Authorization = AuthorizationEntry{Type: testAuthorizationType}
	require.NoError(t, sdk.RegisterControlPlane(context.Background(), update))
	controlPlane, _ = store.FindById(context.Background(), "cp-1")
	assert.Equal(t, &ControlPlane{ID: "cp-1", Endpoint: "https://cp2.example.com", Authorization: AuthorizationEntry{Type: testAuthorizationType}}, controlPlane)

	require.NoError(t, sdk.DeleteControlPlane(context.Background(), "cp-1"))
	assert.ErrorIs(t, sdk.DeleteControlPlane(context.Background(), "cp-1"), ErrNotFound)
	assert.ErrorIs(t, sdk.DeleteControlPlane(context.Background(), ""), ErrInvalidInput)

	invalid := newRegistrationMessage("cp-2")
	invalid.Endpoint = "nope"
	assert.ErrorIs(t, sdk.RegisterControlPlane(context.Background(), invalid), ErrValidation)
	_, err = store.FindById(context.Background(), "cp-2")
	assert.ErrorIs(t, err, ErrNotFound)
}

func Test_RegisterControlPlane_WithoutStore(t *testing.T) {
	sdk, _ := newTestSDK(t)
	assert.ErrorIs(t, sdk.RegisterControlPlane(context.Background(), newRegistrationMessage("cp-1")), ErrInvalidInput)
	assert.ErrorIs(t, sdk.DeleteControlPlane(context.Background(), "cp-1"), ErrInvalidInput)
}

func Test_PrepareAndStart_RecordControlPlaneID(t *testing.T) {
	sdk, store := newTestSDK(t)
	ctx := ContextWithControlPlaneID(context.Background(), "cp-1")
	assert.Equal(t, "cp-1", ControlPlaneIDFromContext(ctx))
	assert.Equal(t, "", ControlPlaneIDFromContext(context.Background()))

	prepare := createPrepareMessage(pullProfile)
	_, err := sdk.Prepare(ctx, prepare)
	require.NoError(t, err)
	assert.Equal(t, "cp-1", store.get(t, prepare.DataFlowID).ControlPlaneID)

	start := createStartMessage(pullProfile, nil)
	_, err = sdk.Start(ctx, start)
	require.NoError(t, err)
	assert.Equal(t, "cp-1", store.get(t, start.DataFlowID).ControlPlaneID)

	without := createStartMessage(pullProfile, nil)
	_, err = sdk.Start(context.Background(), without)
	require.NoError(t, err)
	assert.Empty(t, store.get(t, without.DataFlowID).ControlPlaneID)
}

func Test_StoreResolver(t *testing.T) {
	cp := newFakeControlPlane(t)
	controlPlanes := newFakeControlPlaneStore(&ControlPlane{ID: "cp-1", Endpoint: cp.URL + "/registered", Authorization: testEntry()})
	sdk, store := newNotifySDK(t, nil, WithControlPlaneStore(controlPlanes))

	store.put(&DataFlow{ID: "known", Profile: pullProfile, State: Started, ControlPlaneID: "cp-1"})
	require.NoError(t, sdk.NotifyCompleted(context.Background(), "known"))
	require.Len(t, cp.received(), 1)
	assert.Equal(t, "/registered/transfers/known/dataflow/completed", cp.received()[0].path)

	store.put(&DataFlow{ID: "unknown", Profile: pullProfile, State: Started, ControlPlaneID: "cp-2"})
	assert.ErrorIs(t, sdk.NotifyCompleted(context.Background(), "unknown"), ErrControlPlaneNotRegistered)

	store.put(&DataFlow{ID: "none", Profile: pullProfile, State: Started})
	assert.ErrorIs(t, sdk.NotifyCompleted(context.Background(), "none"), ErrControlPlaneNotRegistered)

	controlPlanes.err = errors.New("db down")
	err := sdk.NotifyCompleted(context.Background(), "known")
	assert.ErrorContains(t, err, "db down")
	assert.NotErrorIs(t, err, ErrControlPlaneNotRegistered)
	assert.Len(t, cp.received(), 1)
}

func Test_StaticControlPlaneTakesPrecedenceOverStore(t *testing.T) {
	static := newFakeControlPlane(t)
	registered := newFakeControlPlane(t)
	controlPlanes := newFakeControlPlaneStore(&ControlPlane{ID: "cp-1", Endpoint: registered.URL, Authorization: testEntry()})
	// the static URL is given before the store; order does not matter
	sdk, store := newNotifySDK(t, static, WithControlPlaneStore(controlPlanes))
	store.put(&DataFlow{ID: "flow", Profile: pullProfile, State: Started, ControlPlaneID: "cp-1"})

	require.NoError(t, sdk.NotifyCompleted(context.Background(), "flow"))
	assert.Len(t, static.received(), 1)
	assert.Empty(t, registered.received())
}

// --- HTTP ---

func Test_Router_ControlPlaneRegistration(t *testing.T) {
	controlPlanes := newFakeControlPlaneStore()
	sdk, _ := newTestSDK(t, WithControlPlaneStore(controlPlanes))
	handler := NewHandler(sdk)

	rr := serveAs(t, handler, "Test cp-1", http.MethodPut, "/v1/controlplanes", newRegistrationMessage("cp-1"))
	assert.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Empty(t, rr.Body.String())
	_, err := controlPlanes.FindById(context.Background(), "cp-1")
	assert.NoError(t, err)

	// update
	update := newRegistrationMessage("cp-1")
	update.Endpoint = "https://other.example.com"
	assert.Equal(t, http.StatusOK, serveAs(t, handler, "Test cp-1", http.MethodPut, "/v1/controlplanes", update).Code)
	controlPlane, _ := controlPlanes.FindById(context.Background(), "cp-1")
	assert.Equal(t, "https://other.example.com", controlPlane.Endpoint)

	for name, body := range map[string]any{
		"malformed":             "{",
		"missing authorization": `{"controlplaneId": "cp-2", "endpoint": "https://cp.example.com"}`,
		"invalid endpoint":      `{"controlplaneId": "cp-2", "endpoint": "nope", "authorization": {"type": "x"}}`,
	} {
		t.Run(name, func(t *testing.T) {
			rr := serveAs(t, handler, "Test cp-1", http.MethodPut, "/v1/controlplanes", body)
			assert.Equal(t, http.StatusBadRequest, rr.Code)
			var errorResponse ErrorResponse
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &errorResponse))
			assert.NotEmpty(t, errorResponse.Error)
		})
	}

	rr = serveAs(t, handler, "Test cp-1", http.MethodDelete, "/v1/controlplanes/cp-1", nil)
	assert.Equal(t, http.StatusNoContent, rr.Code)
	assert.Empty(t, rr.Body.String())
	rr = serveAs(t, handler, "Test cp-1", http.MethodDelete, "/v1/controlplanes/cp-1", nil)
	assert.Equal(t, http.StatusNotFound, rr.Code)

	assert.Equal(t, http.StatusMethodNotAllowed, serveAs(t, handler, "Test cp-1", http.MethodGet, "/v1/controlplanes", nil).Code)
	assert.Equal(t, http.StatusMethodNotAllowed, serveAs(t, handler, "Test cp-1", http.MethodPost, "/v1/controlplanes", newRegistrationMessage("x")).Code)
}

func Test_Router_ControlPlaneRegistration_NotMountedWithoutStore(t *testing.T) {
	sdk, _ := newTestSDK(t)
	handler := NewHandler(sdk)
	assert.Equal(t, http.StatusNotFound, serve(t, handler, http.MethodPut, "/v1/controlplanes", newRegistrationMessage("cp-1")).Code)
	assert.Equal(t, http.StatusNotFound, serve(t, handler, http.MethodDelete, "/v1/controlplanes/cp-1", nil).Code)
}

func Test_Router_ControlPlaneRegistration_BasePath(t *testing.T) {
	sdk, _ := newTestSDK(t, WithControlPlaneStore(newFakeControlPlaneStore()))
	handler := NewHandler(sdk, WithBasePath("/api/signaling"))
	assert.Equal(t, http.StatusOK, serveAs(t, handler, "Test cp-1", http.MethodPut, "/api/signaling/controlplanes", newRegistrationMessage("cp-1")).Code)
	assert.Equal(t, http.StatusNoContent, serveAs(t, handler, "Test cp-1", http.MethodDelete, "/api/signaling/controlplanes/cp-1", nil).Code)
}

// --- client ---

func Test_HTTPClient_RegisterDataPlane(t *testing.T) {
	schemas := loadSchemas(t)
	cp := newFakeControlPlane(t)
	sdk, _ := newNotifySDK(t, nil)

	require.NoError(t, sdk.RegisterDataPlane(context.Background(), ControlPlane{Endpoint: cp.URL + "/api/", Authorization: testEntry()}, newDataPlaneRegistration()))
	request := cp.received()[0]
	assert.Equal(t, http.MethodPut, request.method)
	assert.Equal(t, "/api/dataplanes", request.path)
	assert.Equal(t, jsonContentType, request.contentType)
	instance, err := jsonschema.UnmarshalJSON(bytes.NewReader(request.body))
	require.NoError(t, err)
	assert.NoError(t, schemas["DataPlaneRegistrationMessage"].Validate(instance), string(request.body))

	invalid := newDataPlaneRegistration()
	invalid.Profiles = nil
	assert.ErrorIs(t, sdk.RegisterDataPlane(context.Background(), ControlPlane{Endpoint: cp.URL, Authorization: testEntry()}, invalid), ErrValidation)
	assert.Len(t, cp.received(), 1, "an invalid registration is not sent")
}

func Test_HTTPClient_DeregisterDataPlane(t *testing.T) {
	cp := newFakeControlPlane(t, http.StatusNoContent)
	sdk, _ := newNotifySDK(t, nil)

	require.NoError(t, sdk.DeregisterDataPlane(context.Background(), ControlPlane{Endpoint: cp.URL + "/api", Authorization: testEntry()}, "data plane/1?x"))
	request := cp.received()[0]
	assert.Equal(t, http.MethodDelete, request.method)
	assert.Equal(t, "/api/dataplanes/data%20plane%2F1%3Fx", request.rawPath)

	assert.ErrorIs(t, sdk.DeregisterDataPlane(context.Background(), ControlPlane{Endpoint: cp.URL, Authorization: testEntry()}, ""), ErrInvalidInput)
}

func Test_HTTPClient_RegistrationRetries(t *testing.T) {
	tests := map[string]struct {
		codes    []int
		status   int
		requests int
	}{
		"5xx then success": {[]int{503, 200}, 0, 2},
		"5xx exhausted":    {[]int{500}, 500, 3},
		"4xx not retried":  {[]int{409}, 409, 1},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			for _, call := range []struct {
				action string
				run    func(client *HTTPControlPlaneClient, url string) error
			}{
				{ActionRegisterDataPlane, func(client *HTTPControlPlaneClient, url string) error {
					return client.RegisterDataPlane(context.Background(), ControlPlane{Endpoint: url}, newDataPlaneRegistration())
				}},
				{ActionDeregisterDataPlane, func(client *HTTPControlPlaneClient, url string) error {
					return client.DeregisterDataPlane(context.Background(), ControlPlane{Endpoint: url}, "dataplane-1")
				}},
			} {
				cp := newFakeControlPlane(t, tc.codes...)
				err := call.run(newTestClient(DefaultRetryPolicy), cp.URL)
				if tc.status == 0 {
					assert.NoError(t, err)
				} else {
					var cpErr *ControlPlaneError
					require.ErrorAs(t, err, &cpErr)
					assert.Equal(t, tc.status, cpErr.StatusCode)
					assert.Equal(t, call.action, cpErr.Action)
				}
				assert.Len(t, cp.received(), tc.requests, call.action)
			}
		})
	}
}

// contextDroppingTrx runs the callback with a fresh context, like a TransactionContext that does not propagate the
// caller's context values.
type contextDroppingTrx struct{}

func (contextDroppingTrx) Execute(_ context.Context, fn func(ctx context.Context) error) error {
	return fn(context.Background())
}

func Test_PrepareAndStart_RecordControlPlaneID_WhenTrxDropsContext(t *testing.T) {
	sdk, store := newTestSDK(t, WithTransactionContext(contextDroppingTrx{}))
	ctx := ContextWithControlPlaneID(context.Background(), "cp-1")

	prepare := createPrepareMessage(pullProfile)
	_, err := sdk.Prepare(ctx, prepare)
	require.NoError(t, err)
	assert.Equal(t, "cp-1", store.get(t, prepare.DataFlowID).ControlPlaneID)

	start := createStartMessage(pullProfile, nil)
	_, err = sdk.Start(ctx, start)
	require.NoError(t, err)
	assert.Equal(t, "cp-1", store.get(t, start.DataFlowID).ControlPlaneID)
}
