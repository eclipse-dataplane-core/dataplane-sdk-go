package tests

// End-to-end scenarios: a consumer and a provider data plane, each an SDK instance served by NewHandler, and a fake
// control plane, all over HTTP with an authorization profile. The scenarios follow the sequences of the Data Plane
// Signaling specification for provider-push and consumer-pull, each with synchronous and asynchronous transitions.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/eclipse-dataplane-core/dataplane-sdk-go/pkg/dsdk"
	"github.com/eclipse-dataplane-core/dataplane-sdk-go/pkg/memory"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	e2ePushProfile     = "https://w3id.org/dspace-sig/profile/http-push"
	e2ePullProfile     = "https://w3id.org/dspace-sig/profile/http-pull"
	e2eControlPlaneID  = "e2e-control-plane"
	e2eProviderID      = "e2e-provider"
	e2eConsumerID      = "e2e-consumer"
	e2eAsyncKey        = "async"
	e2eAuthType        = "e2e"
	e2eAuthPrefix      = "E2E "
	e2ePrincipal       = "principal"
	e2eConsumerAddress = "https://consumer.example.com/ingest"
	e2eProviderAddress = "https://provider.example.com/data"
)

// e2eAuthorization authenticates "E2E <id>" headers; outbound it sends "E2E <principal>".
type e2eAuthorization struct{}

func (e2eAuthorization) Type() string { return e2eAuthType }

func (e2eAuthorization) AuthorizationHeader(_ context.Context, entry dsdk.AuthorizationEntry) (string, error) {
	principal, ok := entry.Property(e2ePrincipal)
	if !ok {
		return "", fmt.Errorf("%w: no principal", dsdk.ErrInvalidInput)
	}
	return e2eAuthPrefix + principal, nil
}

func (e2eAuthorization) ExtractCallerID(_ context.Context, header string) (string, error) {
	id, ok := strings.CutPrefix(header, e2eAuthPrefix)
	if !ok || id == "" {
		return "", dsdk.ErrUnauthorized
	}
	return id, nil
}

func e2eEntry(principal string) dsdk.AuthorizationEntry {
	return dsdk.AuthorizationEntry{Type: e2eAuthType, Properties: map[string]any{e2ePrincipal: principal}}
}

func e2eAddress(endpoint string) *dsdk.DataAddress {
	return &dsdk.DataAddress{
		EndpointType:       "https://w3id.org/idsa/v4.1/HTTP",
		Endpoint:           endpoint,
		EndpointProperties: []dsdk.EndpointProperty{{Name: "authorization", Value: "token"}, {Name: "authType", Value: "bearer"}},
	}
}

// --- fake control plane ---

type e2eCallback struct {
	caller  string
	action  string
	message dsdk.DataFlowStatusMessage
}

type e2eControlPlane struct {
	server     *httptest.Server
	mu         sync.Mutex
	dataPlanes map[string]dsdk.DataPlaneRegistrationMessage
	callbacks  []e2eCallback
}

func newE2EControlPlane(t *testing.T) *e2eControlPlane {
	cp := &e2eControlPlane{dataPlanes: map[string]dsdk.DataPlaneRegistrationMessage{}}
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /dataplanes", func(w http.ResponseWriter, r *http.Request) {
		var registration dsdk.DataPlaneRegistrationMessage
		if err := json.NewDecoder(r.Body).Decode(&registration); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if r.Header.Get("Authorization") != e2eAuthPrefix+registration.DataPlaneID {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		cp.mu.Lock()
		cp.dataPlanes[registration.DataPlaneID] = registration
		cp.mu.Unlock()
	})
	mux.HandleFunc("POST /transfers/{id}/dataflow/{action}", func(w http.ResponseWriter, r *http.Request) {
		caller, _ := strings.CutPrefix(r.Header.Get("Authorization"), e2eAuthPrefix)
		cp.mu.Lock()
		_, registered := cp.dataPlanes[caller]
		cp.mu.Unlock()
		if !registered {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		var message dsdk.DataFlowStatusMessage
		if err := json.NewDecoder(r.Body).Decode(&message); err != nil || message.DataFlowID != r.PathValue("id") {
			http.Error(w, "bad callback", http.StatusBadRequest)
			return
		}
		cp.mu.Lock()
		cp.callbacks = append(cp.callbacks, e2eCallback{caller: caller, action: r.PathValue("action"), message: message})
		cp.mu.Unlock()
	})
	cp.server = httptest.NewServer(mux)
	t.Cleanup(cp.server.Close)
	return cp
}

func (cp *e2eControlPlane) received() []e2eCallback {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	return append([]e2eCallback(nil), cp.callbacks...)
}

// controlPlane is how a data plane addresses this control plane.
func (cp *e2eControlPlane) controlPlane(dataPlaneID string) dsdk.ControlPlane {
	return dsdk.ControlPlane{ID: e2eControlPlaneID, Endpoint: cp.server.URL, Authorization: e2eEntry(dataPlaneID)}
}

// call sends a signaling request to the data plane, authenticated as the control plane.
func (cp *e2eControlPlane) call(t *testing.T, dp *e2eDataPlane, method string, path string, body any) (int, http.Header, []byte) {
	t.Helper()
	var reader io.Reader = http.NoBody
	if body != nil {
		data, err := json.Marshal(body)
		require.NoError(t, err)
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequest(method, dp.server.URL+dsdk.DefaultBasePath+path, reader)
	require.NoError(t, err)
	req.Header.Set("Authorization", e2eAuthPrefix+e2eControlPlaneID)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, resp.Header, data
}

// signal sends a signaling request and decodes the DataFlowStatusMessage of the expected status code.
func (cp *e2eControlPlane) signal(t *testing.T, dp *e2eDataPlane, path string, body any, wantCode int) dsdk.DataFlowStatusMessage {
	t.Helper()
	code, _, data := cp.call(t, dp, http.MethodPost, path, body)
	require.Equal(t, wantCode, code, string(data))
	var status dsdk.DataFlowStatusMessage
	require.NoError(t, json.Unmarshal(data, &status), string(data))
	return status
}

// expectOK sends a signaling request without a response body and requires 200.
func (cp *e2eControlPlane) expectOK(t *testing.T, dp *e2eDataPlane, path string, body any) {
	t.Helper()
	code, _, data := cp.call(t, dp, http.MethodPost, path, body)
	require.Equal(t, http.StatusOK, code, string(data))
}

// status returns the data flow state reported by GET /status.
func (cp *e2eControlPlane) status(t *testing.T, dp *e2eDataPlane, id string) dsdk.DataFlowState {
	t.Helper()
	code, _, data := cp.call(t, dp, http.MethodGet, "/dataflows/"+id+"/status", nil)
	require.Equal(t, http.StatusOK, code, string(data))
	var status dsdk.DataFlowStatusResponseMessage
	require.NoError(t, json.Unmarshal(data, &status))
	assert.Equal(t, id, status.DataFlowID)
	return status.State
}

// --- data planes ---

// e2eStores are the stores of one data plane.
type e2eStores struct {
	flows         dsdk.DataplaneStore
	trx           dsdk.TransactionContext
	controlPlanes dsdk.ControlPlaneStore
}

func memoryStores(*testing.T) e2eStores {
	return e2eStores{flows: memory.NewInMemoryStore(), trx: memory.InMemoryTrxContext{}, controlPlanes: memory.NewInMemoryControlPlaneStore()}
}

type e2eDataPlane struct {
	id     string
	sdk    *dsdk.DataPlaneSDK
	server *httptest.Server
}

func e2eIsAsync(flow *dsdk.DataFlow) bool {
	async, _ := flow.Metadata[e2eAsyncKey].(bool)
	return async
}

// newE2EDataPlane creates a data plane. A flow whose metadata has "async": true is prepared or started
// asynchronously; otherwise the consumer provides its push address on prepare and the provider its pull address on
// start.
func newE2EDataPlane(t *testing.T, id string, stores e2eStores) *e2eDataPlane {
	t.Helper()
	sdk, err := dsdk.NewDataPlaneSDK(
		dsdk.WithStore(stores.flows),
		dsdk.WithTransactionContext(stores.trx),
		dsdk.WithControlPlaneStore(stores.controlPlanes),
		dsdk.WithAuthorization(e2eAuthorization{}),
		dsdk.WithPrepareProcessor(func(_ context.Context, flow *dsdk.DataFlow, _ *dsdk.DataPlaneSDK, _ *dsdk.ProcessorOptions) (*dsdk.DataFlowStatusMessage, error) {
			if e2eIsAsync(flow) {
				return &dsdk.DataFlowStatusMessage{State: dsdk.Preparing}, nil
			}
			return &dsdk.DataFlowStatusMessage{State: dsdk.Prepared, DataAddress: e2eAddress(e2eConsumerAddress)}, nil
		}),
		dsdk.WithStartProcessor(func(_ context.Context, flow *dsdk.DataFlow, _ *dsdk.DataPlaneSDK, _ *dsdk.ProcessorOptions) (*dsdk.DataFlowStatusMessage, error) {
			if flow.Consumer {
				return &dsdk.DataFlowStatusMessage{State: dsdk.Started}, nil
			}
			if e2eIsAsync(flow) {
				return &dsdk.DataFlowStatusMessage{State: dsdk.Starting}, nil
			}
			return &dsdk.DataFlowStatusMessage{State: dsdk.Started, DataAddress: e2eAddress(e2eProviderAddress)}, nil
		}),
	)
	require.NoError(t, err)
	server := httptest.NewServer(dsdk.NewHandler(sdk))
	t.Cleanup(server.Close)
	return &e2eDataPlane{id: id, sdk: sdk, server: server}
}

// connect registers the data plane with the control plane and the control plane with the data plane, both over HTTP.
func connect(t *testing.T, cp *e2eControlPlane, dp *e2eDataPlane, profile string) {
	t.Helper()
	require.NoError(t, dp.sdk.RegisterDataPlane(context.Background(), cp.controlPlane(dp.id), dsdk.DataPlaneRegistrationMessage{
		DataPlaneID:   dp.id,
		Endpoint:      dp.server.URL + dsdk.DefaultBasePath,
		Profiles:      []string{profile},
		Authorization: e2eEntry(e2eControlPlaneID),
	}))
	code, _, data := cp.call(t, dp, http.MethodPut, "/controlplanes", dsdk.ControlPlaneRegistrationMessage{
		ControlPlaneID: e2eControlPlaneID,
		Endpoint:       cp.server.URL,
		Authorization:  e2eEntry(dp.id),
	})
	require.Equal(t, http.StatusOK, code, string(data))
}

// --- scenarios ---

type e2eScenario struct {
	profile string
	async   bool
}

func (s e2eScenario) push() bool { return strings.HasSuffix(s.profile, "-push") }

func (s e2eScenario) name() string {
	mode := "sync"
	if s.async {
		mode = "async"
	}
	return map[bool]string{true: "provider-push", false: "consumer-pull"}[s.push()] + "/" + mode
}

func (s e2eScenario) baseMessage(id string, participant, counterParty string) dsdk.DataFlowBaseMessage {
	message := dsdk.DataFlowBaseMessage{
		MessageID:        uuid.NewString(),
		ParticipantID:    participant,
		CounterPartyID:   counterParty,
		DataspaceContext: "e2e",
		DataFlowID:       id,
		AgreementID:      "agreement-1",
		DatasetID:        "dataset-1",
		Profile:          s.profile,
		Claims:           map[string]any{"membership": "active"},
	}
	if s.async {
		message.Metadata = map[string]any{e2eAsyncKey: true}
	}
	return message
}

// e2eTransfer holds a running transfer: the flow ids on the consumer and the provider data plane differ, as they are
// the transfer process ids of two control planes.
type e2eTransfer struct {
	cp                     *e2eControlPlane
	consumer, provider     *e2eDataPlane
	consumerID, providerID string
}

// startTransfer runs registration, prepare, start and started and asserts every step.
func startTransfer(t *testing.T, s e2eScenario, consumerStores, providerStores e2eStores) *e2eTransfer {
	t.Helper()
	cp := newE2EControlPlane(t)
	consumer := newE2EDataPlane(t, e2eConsumerID, consumerStores)
	provider := newE2EDataPlane(t, e2eProviderID, providerStores)
	connect(t, cp, consumer, s.profile)
	connect(t, cp, provider, s.profile)
	ctx := context.Background()
	tr := &e2eTransfer{cp: cp, consumer: consumer, provider: provider, consumerID: uuid.NewString(), providerID: uuid.NewString()}

	// prepare the consumer; a push consumer provides the address the provider pushes to
	prepare := dsdk.DataFlowPrepareMessage{DataFlowBaseMessage: s.baseMessage(tr.consumerID, "consumer", "provider")}
	var consumerAddress *dsdk.DataAddress
	if s.async {
		code, header, data := cp.call(t, consumer, http.MethodPost, "/dataflows/prepare", prepare)
		require.Equal(t, http.StatusAccepted, code, string(data))
		assert.Equal(t, dsdk.DefaultBasePath+"/dataflows/"+tr.consumerID+"/status", header.Get("Location"))
		assert.Equal(t, dsdk.Preparing, cp.status(t, consumer, tr.consumerID))

		_, err := consumer.sdk.NotifyPrepared(ctx, tr.consumerID, func(context.Context, *dsdk.DataFlow, *dsdk.DataPlaneSDK, *dsdk.ProcessorOptions) (*dsdk.DataFlowStatusMessage, error) {
			return &dsdk.DataFlowStatusMessage{State: dsdk.Prepared, DataAddress: e2eAddress(e2eConsumerAddress)}, nil
		})
		require.NoError(t, err)
		callback := tr.lastCallback(t, dsdk.ActionPrepared, tr.consumerID)
		assert.Equal(t, e2eConsumerID, callback.caller)
		assert.Equal(t, dsdk.Prepared, callback.message.State)
		consumerAddress = callback.message.DataAddress
	} else {
		status := cp.signal(t, consumer, "/dataflows/prepare", prepare, http.StatusOK)
		assert.Equal(t, dsdk.Prepared, status.State)
		consumerAddress = status.DataAddress
	}
	assert.Equal(t, dsdk.Prepared, cp.status(t, consumer, tr.consumerID))
	if s.push() {
		require.NotNil(t, consumerAddress)
		assert.Equal(t, e2eConsumerAddress, consumerAddress.Endpoint)
	} else {
		assert.Nil(t, consumerAddress, "a pull consumer provides no address")
	}

	// start the provider; push carries the consumer's address, pull returns the provider's address
	start := dsdk.DataFlowStartMessage{DataFlowBaseMessage: s.baseMessage(tr.providerID, "provider", "consumer")}
	if s.push() {
		start.DataAddress = consumerAddress
	}
	var providerAddress *dsdk.DataAddress
	if s.async {
		code, header, data := cp.call(t, provider, http.MethodPost, "/dataflows/start", start)
		require.Equal(t, http.StatusAccepted, code, string(data))
		assert.Equal(t, dsdk.DefaultBasePath+"/dataflows/"+tr.providerID+"/status", header.Get("Location"))
		assert.Equal(t, dsdk.Starting, cp.status(t, provider, tr.providerID))

		_, err := provider.sdk.NotifyStarted(ctx, tr.providerID, func(context.Context, *dsdk.DataFlow, *dsdk.DataPlaneSDK, *dsdk.ProcessorOptions) (*dsdk.DataFlowStatusMessage, error) {
			return &dsdk.DataFlowStatusMessage{State: dsdk.Started, DataAddress: e2eAddress(e2eProviderAddress)}, nil
		})
		require.NoError(t, err)
		callback := tr.lastCallback(t, dsdk.ActionStarted, tr.providerID)
		assert.Equal(t, e2eProviderID, callback.caller)
		assert.Equal(t, dsdk.Started, callback.message.State)
		providerAddress = callback.message.DataAddress
	} else {
		status := cp.signal(t, provider, "/dataflows/start", start, http.StatusOK)
		assert.Equal(t, dsdk.Started, status.State)
		providerAddress = status.DataAddress
	}
	assert.Equal(t, dsdk.Started, cp.status(t, provider, tr.providerID))
	if s.push() {
		assert.Nil(t, providerAddress, "a push provider provides no address")
	} else {
		require.NotNil(t, providerAddress)
		assert.Equal(t, e2eProviderAddress, providerAddress.Endpoint)
	}

	// tell the consumer the transfer has started; pull carries the provider's address
	started := dsdk.DataFlowStartedNotificationMessage{MessageID: uuid.NewString()}
	if !s.push() {
		started.DataAddress = providerAddress
	}
	status := cp.signal(t, consumer, "/dataflows/"+tr.consumerID+"/started", started, http.StatusOK)
	assert.Equal(t, dsdk.Started, status.State)
	assert.Equal(t, dsdk.Started, cp.status(t, consumer, tr.consumerID))
	return tr
}

func (tr *e2eTransfer) lastCallback(t *testing.T, action string, id string) e2eCallback {
	t.Helper()
	callbacks := tr.cp.received()
	for i := len(callbacks) - 1; i >= 0; i-- {
		if callbacks[i].action == action && callbacks[i].message.DataFlowID == id {
			assert.NotEmpty(t, callbacks[i].message.MessageID)
			return callbacks[i]
		}
	}
	require.Failf(t, "callback not received", "%s for %s in %+v", action, id, callbacks)
	return e2eCallback{}
}

// suspendAndResume suspends and resumes the transfer on both data planes. On resume, the party that receives an
// address gets it again (push provider), and the party that provides one returns it (pull provider, push consumer).
func (tr *e2eTransfer) suspendAndResume(t *testing.T, s e2eScenario) {
	t.Helper()
	for _, dp := range []struct {
		plane *e2eDataPlane
		id    string
	}{{tr.consumer, tr.consumerID}, {tr.provider, tr.providerID}} {
		tr.cp.expectOK(t, dp.plane, "/dataflows/"+dp.id+"/suspend", dsdk.DataFlowSuspendMessage{MessageID: uuid.NewString(), Reason: "maintenance"})
		assert.Equal(t, dsdk.Suspended, tr.cp.status(t, dp.plane, dp.id))
	}

	resume := dsdk.DataFlowResumeMessage{MessageID: uuid.NewString()}
	if s.push() {
		resume.DataAddress = e2eAddress(e2eConsumerAddress + "/v2")
	}
	providerStatus := tr.cp.signal(t, tr.provider, "/dataflows/"+tr.providerID+"/resume", resume, http.StatusOK)
	assert.Equal(t, dsdk.Started, providerStatus.State)
	consumerStatus := tr.cp.signal(t, tr.consumer, "/dataflows/"+tr.consumerID+"/resume", dsdk.DataFlowResumeMessage{MessageID: uuid.NewString()}, http.StatusOK)
	assert.Equal(t, dsdk.Started, consumerStatus.State)

	if s.push() {
		assert.Nil(t, providerStatus.DataAddress)
		require.NotNil(t, consumerStatus.DataAddress)
		assert.Equal(t, e2eConsumerAddress, consumerStatus.DataAddress.Endpoint)
	} else {
		require.NotNil(t, providerStatus.DataAddress)
		assert.Equal(t, e2eProviderAddress, providerStatus.DataAddress.Endpoint)
		assert.Nil(t, consumerStatus.DataAddress)
	}
	assert.Equal(t, dsdk.Started, tr.cp.status(t, tr.provider, tr.providerID))
	assert.Equal(t, dsdk.Started, tr.cp.status(t, tr.consumer, tr.consumerID))
}

// complete finishes the transfer: the sending data plane (push provider, pull consumer) calls NotifyCompleted, and
// the control plane forwards /completed to the other data plane.
func (tr *e2eTransfer) complete(t *testing.T, s e2eScenario) {
	t.Helper()
	notifier, notifierID, other, otherID := tr.provider, tr.providerID, tr.consumer, tr.consumerID
	if !s.push() {
		notifier, notifierID, other, otherID = tr.consumer, tr.consumerID, tr.provider, tr.providerID
	}
	require.NoError(t, notifier.sdk.NotifyCompleted(context.Background(), notifierID))
	callback := tr.lastCallback(t, dsdk.ActionCompleted, notifierID)
	assert.Equal(t, notifier.id, callback.caller)
	assert.Equal(t, dsdk.Completed, callback.message.State)
	assert.Nil(t, callback.message.DataAddress)

	tr.cp.expectOK(t, other, "/dataflows/"+otherID+"/completed", nil)
	assert.Equal(t, dsdk.Completed, tr.cp.status(t, notifier, notifierID))
	assert.Equal(t, dsdk.Completed, tr.cp.status(t, other, otherID))

	// COMPLETED is final
	code, _, _ := tr.cp.call(t, other, http.MethodPost, "/dataflows/"+otherID+"/terminate", dsdk.DataFlowTerminateMessage{MessageID: uuid.NewString()})
	assert.Equal(t, http.StatusBadRequest, code)
}

// terminate terminates the transfer on both data planes.
func (tr *e2eTransfer) terminate(t *testing.T) {
	t.Helper()
	tr.cp.expectOK(t, tr.provider, "/dataflows/"+tr.providerID+"/terminate", dsdk.DataFlowTerminateMessage{MessageID: uuid.NewString(), Reason: "policy violation"})
	tr.cp.expectOK(t, tr.consumer, "/dataflows/"+tr.consumerID+"/terminate", dsdk.DataFlowTerminateMessage{MessageID: uuid.NewString(), Reason: "policy violation"})
	assert.Equal(t, dsdk.Terminated, tr.cp.status(t, tr.provider, tr.providerID))
	assert.Equal(t, dsdk.Terminated, tr.cp.status(t, tr.consumer, tr.consumerID))
}

func e2eScenarios() []e2eScenario {
	return []e2eScenario{
		{profile: e2ePushProfile, async: false},
		{profile: e2ePushProfile, async: true},
		{profile: e2ePullProfile, async: false},
		{profile: e2ePullProfile, async: true},
	}
}

func Test_E2E_Complete(t *testing.T) {
	for _, s := range e2eScenarios() {
		t.Run(s.name(), func(t *testing.T) {
			tr := startTransfer(t, s, memoryStores(t), memoryStores(t))
			tr.suspendAndResume(t, s)
			tr.complete(t, s)
		})
	}
}

func Test_E2E_Terminate(t *testing.T) {
	for _, s := range e2eScenarios() {
		t.Run(s.name(), func(t *testing.T) {
			tr := startTransfer(t, s, memoryStores(t), memoryStores(t))
			tr.terminate(t)
			// no data plane sent a callback for the termination
			for _, callback := range tr.cp.received() {
				assert.NotEqual(t, dsdk.ActionErrored, callback.action)
			}
		})
	}
}

func Test_E2E_NotifyErrored(t *testing.T) {
	s := e2eScenario{profile: e2ePullProfile}
	tr := startTransfer(t, s, memoryStores(t), memoryStores(t))

	require.NoError(t, tr.provider.sdk.NotifyErrored(context.Background(), tr.providerID, fmt.Errorf("disk failure")))
	callback := tr.lastCallback(t, dsdk.ActionErrored, tr.providerID)
	assert.Equal(t, dsdk.Terminated, callback.message.State)
	assert.Equal(t, "disk failure", callback.message.Error)
	assert.Equal(t, dsdk.Terminated, tr.cp.status(t, tr.provider, tr.providerID))

	tr.cp.expectOK(t, tr.consumer, "/dataflows/"+tr.consumerID+"/terminate", dsdk.DataFlowTerminateMessage{MessageID: uuid.NewString(), Reason: "provider failed"})
	assert.Equal(t, dsdk.Terminated, tr.cp.status(t, tr.consumer, tr.consumerID))
}

func Test_E2E_OtherControlPlaneIsForbidden(t *testing.T) {
	tr := startTransfer(t, e2eScenario{profile: e2ePushProfile}, memoryStores(t), memoryStores(t))

	req, err := http.NewRequest(http.MethodGet, tr.provider.server.URL+dsdk.DefaultBasePath+"/dataflows/"+tr.providerID+"/status", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", e2eAuthPrefix+"another-control-plane")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
}
