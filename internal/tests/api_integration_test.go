//go:build postgres

package tests

import (
	"bytes"
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/eclipse-dataplane-core/dataplane-sdk-go/pkg/dsdk"
	"github.com/eclipse-dataplane-core/dataplane-sdk-go/pkg/postgres"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	pullProfile = "https://w3id.org/dspace-sig/profile/http-pull"
	pushProfile = "https://w3id.org/dspace-sig/profile/s3-push"
	// asyncKey in the message metadata makes the test processors report PREPARING/STARTING
	asyncKey = "async"
)

var ctx = context.Background()

// database SQL database connection, used for test setup and assertions
var database *sql.DB

var handler http.Handler

func TestMain(m *testing.M) {
	db, container := postgres.SetupDatabase(&testing.T{}, ctx)
	database = db

	sdk, err := newSdk(db)
	if err != nil {
		panic(err)
	}
	handler = dsdk.NewHandler(sdk)
	code := m.Run()
	_ = db.Close()
	_ = container.Terminate(ctx)
	os.Exit(code)
}

// --- prepare ---

func Test_Prepare_Pull(t *testing.T) {
	msg := newPrepareMessage(pullProfile)
	rr := post(t, "/v1/dataflows/prepare", msg)

	assert.Equal(t, http.StatusOK, rr.Code)
	status := decodeStatus(t, rr)
	assert.Equal(t, dsdk.Prepared, status.State)
	assert.Equal(t, msg.DataFlowID, status.DataFlowID)
	assert.NotEmpty(t, status.MessageID)
	assert.Nil(t, status.DataAddress)
	assert.Equal(t, dsdk.Prepared, findFlow(t, msg.DataFlowID).State)
}

func Test_Prepare_Push_ReturnsDataAddress(t *testing.T) {
	msg := newPrepareMessage(pushProfile)
	rr := post(t, "/v1/dataflows/prepare", msg)

	assert.Equal(t, http.StatusOK, rr.Code)
	status := decodeStatus(t, rr)
	require.NotNil(t, status.DataAddress)
	assert.Equal(t, "https://consumer.example.com/push", status.DataAddress.Endpoint)
}

func Test_Prepare_Async(t *testing.T) {
	msg := newPrepareMessage(pullProfile)
	msg.Metadata = map[string]any{asyncKey: true}
	rr := post(t, "/v1/dataflows/prepare", msg)

	assert.Equal(t, http.StatusAccepted, rr.Code)
	assert.Equal(t, "/v1/dataflows/"+msg.DataFlowID+"/status", rr.Header().Get("Location"))
	assert.Equal(t, dsdk.Preparing, decodeStatus(t, rr).State)
}

func Test_Prepare_InvalidPayload(t *testing.T) {
	pm := newPrepareMessage(pullProfile)
	pm.CounterPartyID = "" // should raise a validation error
	rr := post(t, "/v1/dataflows/prepare", pm)
	assert.Equal(t, http.StatusBadRequest, rr.Code)

	// the handler must stop after the validation error and write exactly one response body
	decoder := json.NewDecoder(rr.Body)
	var responseMessage dsdk.ErrorResponse
	assert.NoError(t, decoder.Decode(&responseMessage))
	assert.NotEmpty(t, responseMessage.Error)
	assert.False(t, decoder.More())
}

func Test_Prepare_MalformedJSON(t *testing.T) {
	req, err := http.NewRequest(http.MethodPost, "/v1/dataflows/prepare", strings.NewReader("{"))
	require.NoError(t, err)
	req.Header.Set("Authorization", testAuthorization)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func Test_Prepare_WrongState(t *testing.T) {
	msg := newPrepareMessage(pullProfile)
	createFlow(t, msg.DataFlowID, pullProfile, dsdk.Started, true)

	rr := post(t, "/v1/dataflows/prepare", msg)
	assert.Equal(t, http.StatusConflict, rr.Code)
}

// --- start ---

func Test_Start_Pull_ReturnsDataAddress(t *testing.T) {
	msg := newStartMessage(pullProfile, nil)
	rr := post(t, "/v1/dataflows/start", msg)

	assert.Equal(t, http.StatusOK, rr.Code)
	status := decodeStatus(t, rr)
	assert.Equal(t, dsdk.Started, status.State)
	assert.Equal(t, msg.DataFlowID, status.DataFlowID)
	require.NotNil(t, status.DataAddress)
	token, ok := status.DataAddress.Property("authorization")
	assert.True(t, ok)
	assert.Equal(t, "token-"+msg.DataFlowID, token)

	flow := findFlow(t, msg.DataFlowID)
	assert.Equal(t, dsdk.Started, flow.State)
	assert.False(t, flow.Consumer)
	assert.Equal(t, msg.Claims, flow.Claims)
}

func Test_Start_Push(t *testing.T) {
	msg := newStartMessage(pushProfile, newDataAddress("https://consumer.example.com/push"))
	rr := post(t, "/v1/dataflows/start", msg)

	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Nil(t, decodeStatus(t, rr).DataAddress)
	assert.Equal(t, "https://consumer.example.com/push", findFlow(t, msg.DataFlowID).DestinationDataAddress.Endpoint)
}

func Test_Start_Async(t *testing.T) {
	msg := newStartMessage(pullProfile, nil)
	msg.Metadata = map[string]any{asyncKey: true}
	rr := post(t, "/v1/dataflows/start", msg)

	assert.Equal(t, http.StatusAccepted, rr.Code)
	assert.Equal(t, "/v1/dataflows/"+msg.DataFlowID+"/status", rr.Header().Get("Location"))
	assert.Equal(t, dsdk.Starting, decodeStatus(t, rr).State)
}

func Test_Start_PullWithDataAddress(t *testing.T) {
	msg := newStartMessage(pullProfile, newDataAddress("https://example.com"))
	rr := post(t, "/v1/dataflows/start", msg)
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assertNoFlow(t, msg.DataFlowID)
}

func Test_Start_PushWithoutDataAddress(t *testing.T) {
	msg := newStartMessage(pushProfile, nil)
	rr := post(t, "/v1/dataflows/start", msg)
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assertNoFlow(t, msg.DataFlowID)
}

func Test_Start_Duplicate(t *testing.T) {
	msg := newStartMessage(pullProfile, nil)
	assert.Equal(t, http.StatusOK, post(t, "/v1/dataflows/start", msg).Code)

	rr := post(t, "/v1/dataflows/start", msg)
	assert.Equal(t, http.StatusConflict, rr.Code)
}

func Test_Start_InvalidPayload(t *testing.T) {
	sm := newStartMessage(pullProfile, nil)
	sm.CounterPartyID = "" // should raise a validation error
	rr := post(t, "/v1/dataflows/start", sm)
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	var errorResponse dsdk.ErrorResponse
	assert.NoError(t, json.NewDecoder(rr.Body).Decode(&errorResponse))
	assert.NotEmpty(t, errorResponse.Error)
}

// --- started ---

func Test_Started_Pull(t *testing.T) {
	id := uuid.NewString()
	createFlow(t, id, pullProfile, dsdk.Prepared, true)

	rr := post(t, "/v1/dataflows/"+id+"/started", dsdk.DataFlowStartedNotificationMessage{
		MessageID:   uuid.NewString(),
		DataAddress: newDataAddress("https://provider.example.com/data"),
	})

	assert.Equal(t, http.StatusOK, rr.Code)
	status := decodeStatus(t, rr)
	assert.Equal(t, dsdk.Started, status.State)
	assert.Equal(t, id, status.DataFlowID)

	flow := findFlow(t, id)
	assert.Equal(t, dsdk.Started, flow.State)
	assert.Equal(t, "https://provider.example.com/data", flow.SourceDataAddress.Endpoint)
}

func Test_Started_Push(t *testing.T) {
	id := uuid.NewString()
	createFlow(t, id, pushProfile, dsdk.Prepared, true)

	rr := post(t, "/v1/dataflows/"+id+"/started", dsdk.DataFlowStartedNotificationMessage{MessageID: uuid.NewString()})
	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, dsdk.Started, decodeStatus(t, rr).State)
}

func Test_Started_Duplicate(t *testing.T) {
	id := uuid.NewString()
	createFlow(t, id, pushProfile, dsdk.Started, true)

	rr := post(t, "/v1/dataflows/"+id+"/started", dsdk.DataFlowStartedNotificationMessage{MessageID: uuid.NewString()})
	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, dsdk.Started, decodeStatus(t, rr).State)
}

func Test_Started_DataAddressRules(t *testing.T) {
	pull := uuid.NewString()
	createFlow(t, pull, pullProfile, dsdk.Prepared, true)
	rr := post(t, "/v1/dataflows/"+pull+"/started", dsdk.DataFlowStartedNotificationMessage{MessageID: uuid.NewString()})
	assert.Equal(t, http.StatusBadRequest, rr.Code)

	push := uuid.NewString()
	createFlow(t, push, pushProfile, dsdk.Prepared, true)
	rr = post(t, "/v1/dataflows/"+push+"/started", dsdk.DataFlowStartedNotificationMessage{
		MessageID:   uuid.NewString(),
		DataAddress: newDataAddress("https://example.com"),
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func Test_Started_MissingMessageID(t *testing.T) {
	id := uuid.NewString()
	createFlow(t, id, pushProfile, dsdk.Prepared, true)
	rr := post(t, "/v1/dataflows/"+id+"/started", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func Test_Started_WrongState(t *testing.T) {
	id := uuid.NewString()
	createFlow(t, id, pushProfile, dsdk.Terminated, true)
	rr := post(t, "/v1/dataflows/"+id+"/started", dsdk.DataFlowStartedNotificationMessage{MessageID: uuid.NewString()})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func Test_Started_WhenNotFound(t *testing.T) {
	rr := post(t, "/v1/dataflows/"+uuid.NewString()+"/started", dsdk.DataFlowStartedNotificationMessage{MessageID: uuid.NewString()})
	assert.Equal(t, http.StatusNotFound, rr.Code)
}

// --- suspend / resume ---

func Test_SuspendAndResume(t *testing.T) {
	msg := newStartMessage(pullProfile, nil)
	require.Equal(t, http.StatusOK, post(t, "/v1/dataflows/start", msg).Code)
	id := msg.DataFlowID

	rr := post(t, "/v1/dataflows/"+id+"/suspend", dsdk.DataFlowSuspendMessage{MessageID: uuid.NewString(), Reason: "maintenance"})
	assert.Equal(t, http.StatusOK, rr.Code)
	flow := findFlow(t, id)
	assert.Equal(t, dsdk.Suspended, flow.State)
	assert.Equal(t, dsdk.SuspendedByRemote, flow.SuspendedBy)
	assert.Equal(t, "maintenance", flow.ErrorDetail)

	rr = post(t, "/v1/dataflows/"+id+"/resume", dsdk.DataFlowResumeMessage{MessageID: uuid.NewString()})
	assert.Equal(t, http.StatusOK, rr.Code)
	status := decodeStatus(t, rr)
	assert.Equal(t, dsdk.Started, status.State)
	assert.Equal(t, id, status.DataFlowID)
	require.NotNil(t, status.DataAddress, "a pull provider returns its data address on resume")
	assert.Equal(t, "https://provider.example.com/data", status.DataAddress.Endpoint)

	flow = findFlow(t, id)
	assert.Equal(t, dsdk.Started, flow.State)
	assert.Equal(t, dsdk.SuspendedByNone, flow.SuspendedBy)
}

func Test_Suspend_RequiresBody(t *testing.T) {
	id := uuid.NewString()
	createFlow(t, id, pullProfile, dsdk.Started, false)

	assert.Equal(t, http.StatusBadRequest, postRaw(t, "/v1/dataflows/"+id+"/suspend", "").Code)
	assert.Equal(t, http.StatusBadRequest, post(t, "/v1/dataflows/"+id+"/suspend", map[string]any{"reason": "x"}).Code)
	assert.Equal(t, dsdk.Started, findFlow(t, id).State)
}

func Test_Suspend_WhenNotExists(t *testing.T) {
	rr := post(t, "/v1/dataflows/"+uuid.NewString()+"/suspend", dsdk.DataFlowSuspendMessage{MessageID: uuid.NewString()})
	assert.Equal(t, http.StatusNotFound, rr.Code)
}

func Test_Suspend_WhenNotStarted(t *testing.T) {
	id := uuid.NewString()
	createFlow(t, id, pullProfile, dsdk.Prepared, true)
	rr := post(t, "/v1/dataflows/"+id+"/suspend", dsdk.DataFlowSuspendMessage{MessageID: uuid.NewString()})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func Test_Resume_RequiresBody(t *testing.T) {
	id := uuid.NewString()
	createFlow(t, id, pullProfile, dsdk.Suspended, false)
	assert.Equal(t, http.StatusBadRequest, postRaw(t, "/v1/dataflows/"+id+"/resume", "").Code)
	assert.Equal(t, http.StatusBadRequest, post(t, "/v1/dataflows/"+id+"/resume", map[string]any{}).Code)
}

func Test_Resume_WhenNotSuspended(t *testing.T) {
	id := uuid.NewString()
	createFlow(t, id, pullProfile, dsdk.Started, false)
	rr := post(t, "/v1/dataflows/"+id+"/resume", dsdk.DataFlowResumeMessage{MessageID: uuid.NewString()})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func Test_Resume_SuspendedLocally(t *testing.T) {
	id := uuid.NewString()
	flow, err := newFlowBuilder(id, pullProfile).State(dsdk.Suspended).SuspendedBy(dsdk.SuspendedByLocal).Build()
	require.NoError(t, err)
	require.NoError(t, postgres.NewStore(database).Create(ctx, flow))

	rr := post(t, "/v1/dataflows/"+id+"/resume", dsdk.DataFlowResumeMessage{MessageID: uuid.NewString()})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Equal(t, dsdk.Suspended, findFlow(t, id).State)
}

func Test_Resume_WhenNotFound(t *testing.T) {
	rr := post(t, "/v1/dataflows/"+uuid.NewString()+"/resume", dsdk.DataFlowResumeMessage{MessageID: uuid.NewString()})
	assert.Equal(t, http.StatusNotFound, rr.Code)
}

// --- terminate ---

func Test_Terminate_Success(t *testing.T) {
	id := uuid.NewString()
	createFlow(t, id, pullProfile, dsdk.Started, false)

	rr := post(t, "/v1/dataflows/"+id+"/terminate", dsdk.DataFlowTerminateMessage{MessageID: uuid.NewString(), Reason: "violation"})
	assert.Equal(t, http.StatusOK, rr.Code)
	flow := findFlow(t, id)
	assert.Equal(t, dsdk.Terminated, flow.State)
	assert.Equal(t, "violation", flow.ErrorDetail)
}

func Test_Terminate_RequiresBody(t *testing.T) {
	id := uuid.NewString()
	createFlow(t, id, pullProfile, dsdk.Started, false)
	assert.Equal(t, http.StatusBadRequest, postRaw(t, "/v1/dataflows/"+id+"/terminate", "").Code)
	assert.Equal(t, http.StatusBadRequest, post(t, "/v1/dataflows/"+id+"/terminate", map[string]any{"reason": "x"}).Code)
	assert.Equal(t, dsdk.Started, findFlow(t, id).State)
}

func Test_Terminate_WhenCompleted(t *testing.T) {
	id := uuid.NewString()
	createFlow(t, id, pullProfile, dsdk.Completed, false)
	rr := post(t, "/v1/dataflows/"+id+"/terminate", dsdk.DataFlowTerminateMessage{MessageID: uuid.NewString()})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Equal(t, dsdk.Completed, findFlow(t, id).State)
}

func Test_Terminate_WhenNotFound(t *testing.T) {
	rr := post(t, "/v1/dataflows/"+uuid.NewString()+"/terminate", dsdk.DataFlowTerminateMessage{MessageID: uuid.NewString()})
	assert.Equal(t, http.StatusNotFound, rr.Code)
}

// --- completed ---

func Test_Complete(t *testing.T) {
	id := uuid.NewString()
	createFlow(t, id, pullProfile, dsdk.Started, false)

	rr := postRaw(t, "/v1/dataflows/"+id+"/completed", "")
	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, dsdk.Completed, findFlow(t, id).State)
}

func Test_Complete_NotFound(t *testing.T) {
	rr := postRaw(t, "/v1/dataflows/"+uuid.NewString()+"/completed", "")
	assert.Equal(t, http.StatusNotFound, rr.Code)
}

func Test_Complete_WrongState(t *testing.T) {
	id := uuid.NewString()
	createFlow(t, id, pullProfile, dsdk.Prepared, true)
	rr := postRaw(t, "/v1/dataflows/"+id+"/completed", "")
	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

// --- status ---

func Test_GetStatus(t *testing.T) {
	id := uuid.NewString()
	createFlow(t, id, pullProfile, dsdk.Started, false)

	rr := get(t, "/v1/dataflows/"+id+"/status")
	assert.Equal(t, http.StatusOK, rr.Code)
	var status dsdk.DataFlowStatusResponseMessage
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&status))
	assert.Equal(t, id, status.DataFlowID)
	assert.Equal(t, dsdk.Started, status.State)
}

func Test_GetStatus_WireFormat(t *testing.T) {
	id := uuid.NewString()
	createFlow(t, id, pullProfile, dsdk.Initialized, false)

	rr := get(t, "/v1/dataflows/"+id+"/status")
	var raw map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &raw))
	assert.Equal(t, map[string]any{"dataFlowId": id, "state": "INITIALIZED"}, raw, "only the DataFlowStatusResponseMessage fields")
}

func Test_GetStatus_NotFound(t *testing.T) {
	rr := get(t, "/v1/dataflows/"+uuid.NewString()+"/status")
	assert.Equal(t, http.StatusNotFound, rr.Code)
}

// --- routing ---

func Test_Router_MethodNotAllowed(t *testing.T) {
	rr := get(t, "/v1/dataflows/prepare")
	assert.Equal(t, http.StatusMethodNotAllowed, rr.Code)
}

func Test_Router_UnknownPath(t *testing.T) {
	rr := postRaw(t, "/dataflows/prepare", "{}")
	assert.Equal(t, http.StatusNotFound, rr.Code)
}

func Test_Router_CustomBasePath(t *testing.T) {
	sdk, err := newSdk(database)
	require.NoError(t, err)
	custom := dsdk.NewHandler(sdk, dsdk.WithBasePath("/api/signaling/"))

	msg := newStartMessage(pullProfile, nil)
	msg.Metadata = map[string]any{asyncKey: true}
	payload, err := json.Marshal(msg)
	require.NoError(t, err)
	req, err := http.NewRequest(http.MethodPost, "/api/signaling/dataflows/start", bytes.NewBuffer(payload))
	require.NoError(t, err)
	req.Header.Set("Authorization", testAuthorization)
	rr := httptest.NewRecorder()
	custom.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusAccepted, rr.Code)
	assert.Equal(t, "/api/signaling/dataflows/"+msg.DataFlowID+"/status", rr.Header().Get("Location"))

	req, err = http.NewRequest(http.MethodGet, "/v1/dataflows/"+msg.DataFlowID+"/status", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", testAuthorization)
	rr = httptest.NewRecorder()
	custom.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusNotFound, rr.Code)
}

// --- authorization ---

func Test_Authorization_Required(t *testing.T) {
	msg := newStartMessage(pullProfile, nil)
	payload, err := json.Marshal(msg)
	require.NoError(t, err)
	for _, header := range []string{"", "Bearer nope"} {
		req, err := http.NewRequest(http.MethodPost, "/v1/dataflows/start", bytes.NewBuffer(payload))
		require.NoError(t, err)
		if header != "" {
			req.Header.Set("Authorization", header)
		}
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		assert.Equal(t, http.StatusUnauthorized, rr.Code)
	}
	assertNoFlow(t, msg.DataFlowID)
}

func Test_Authorization_RecordsAndChecksCaller(t *testing.T) {
	msg := newStartMessage(pullProfile, nil)
	require.Equal(t, http.StatusOK, post(t, "/v1/dataflows/start", msg).Code)
	assert.Equal(t, "tests-control-plane", findFlow(t, msg.DataFlowID).ControlPlaneID)

	req, err := http.NewRequest(http.MethodGet, "/v1/dataflows/"+msg.DataFlowID+"/status", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Test another-control-plane")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusForbidden, rr.Code)
}

// --- helpers ---

func post(t *testing.T, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	payload, err := json.Marshal(body)
	require.NoError(t, err)
	return postRaw(t, path, string(payload))
}

func postRaw(t *testing.T, path string, body string) *httptest.ResponseRecorder {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, path, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Authorization", testAuthorization)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	return rr
}

func get(t *testing.T, path string) *httptest.ResponseRecorder {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, path, nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", testAuthorization)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	return rr
}

func decodeStatus(t *testing.T, rr *httptest.ResponseRecorder) dsdk.DataFlowStatusMessage {
	t.Helper()
	assert.Equal(t, "application/json", rr.Header().Get("Content-Type"))
	var status dsdk.DataFlowStatusMessage
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&status), rr.Body.String())
	return status
}

func findFlow(t *testing.T, id string) *dsdk.DataFlow {
	t.Helper()
	flow, err := postgres.NewStore(database).FindById(ctx, id)
	require.NoError(t, err)
	return flow
}

func assertNoFlow(t *testing.T, id string) {
	t.Helper()
	_, err := postgres.NewStore(database).FindById(ctx, id)
	assert.ErrorIs(t, err, dsdk.ErrNotFound)
}

func createFlow(t *testing.T, id string, profile string, state dsdk.DataFlowState, consumer bool) {
	t.Helper()
	flow, err := newFlowBuilder(id, profile).State(state).Consumer(consumer).Build()
	require.NoError(t, err)
	if state == dsdk.Suspended {
		flow.SuspendedBy = dsdk.SuspendedByRemote
	}
	if !consumer && profile == pullProfile {
		flow.SourceDataAddress = newDataAddress("https://provider.example.com/data")
	}
	require.NoError(t, postgres.NewStore(database).Create(ctx, flow))
}

func newFlowBuilder(id string, profile string) *dsdk.DataFlowBuilder {
	return dsdk.NewDataFlowBuilder().
		ID(id).
		ParticipantID("participant-id").
		DataspaceContext("dscontext").
		CounterpartyID("counterparty-id").
		AgreementID("agreement-id").
		DatasetID("dataset-id").
		Profile(profile).
		Claims(map[string]any{"membership": "active"})
}

func newBaseMessage(profile string) dsdk.DataFlowBaseMessage {
	return dsdk.DataFlowBaseMessage{
		MessageID:        uuid.NewString(),
		ParticipantID:    uuid.NewString(),
		CounterPartyID:   uuid.NewString(),
		DataspaceContext: uuid.NewString(),
		DataFlowID:       uuid.NewString(),
		AgreementID:      uuid.NewString(),
		DatasetID:        uuid.NewString(),
		Profile:          profile,
		Claims:           map[string]any{"membership": "active"},
	}
}

func newPrepareMessage(profile string) dsdk.DataFlowPrepareMessage {
	return dsdk.DataFlowPrepareMessage{DataFlowBaseMessage: newBaseMessage(profile)}
}

func newStartMessage(profile string, address *dsdk.DataAddress) dsdk.DataFlowStartMessage {
	return dsdk.DataFlowStartMessage{DataFlowBaseMessage: newBaseMessage(profile), DataAddress: address}
}

func newDataAddress(endpoint string) *dsdk.DataAddress {
	return &dsdk.DataAddress{
		EndpointType:       "https://w3id.org/idsa/v4.1/HTTP",
		Endpoint:           endpoint,
		EndpointProperties: []dsdk.EndpointProperty{{Name: "authType", Value: "bearer"}},
	}
}

func isAsync(flow *dsdk.DataFlow) bool {
	async, _ := flow.Metadata[asyncKey].(bool)
	return async
}

// testAuthorization is the Authorization header the tests authenticate with; see headerAuthorization.
const testAuthorization = "Test tests-control-plane"

// headerAuthorization accepts "Test <id>" headers. It is a stand-in for a real authorization profile.
type headerAuthorization struct{}

func (headerAuthorization) Type() string { return "test" }

func (headerAuthorization) AuthorizationHeader(context.Context, dsdk.AuthorizationEntry) (string, error) {
	return "Test tests-data-plane", nil
}

func (headerAuthorization) ExtractCallerID(_ context.Context, header string) (string, error) {
	id, ok := strings.CutPrefix(header, "Test ")
	if !ok || id == "" {
		return "", dsdk.ErrUnauthorized
	}
	return id, nil
}

func newSdk(db *sql.DB) (*dsdk.DataPlaneSDK, error) {
	return dsdk.NewDataPlaneSDK(
		dsdk.WithAuthorization(headerAuthorization{}),
		dsdk.WithStore(postgres.NewStore(db)),
		dsdk.WithTransactionContext(postgres.NewDBTransactionContext(db)),
		dsdk.WithPrepareProcessor(func(_ context.Context, flow *dsdk.DataFlow, _ *dsdk.DataPlaneSDK, _ *dsdk.ProcessorOptions) (*dsdk.DataFlowStatusMessage, error) {
			if isAsync(flow) {
				return &dsdk.DataFlowStatusMessage{State: dsdk.Preparing}, nil
			}
			if flow.IsPush() {
				return &dsdk.DataFlowStatusMessage{State: dsdk.Prepared, DataAddress: newDataAddress("https://consumer.example.com/push")}, nil
			}
			return &dsdk.DataFlowStatusMessage{State: dsdk.Prepared}, nil
		}),
		dsdk.WithStartProcessor(func(_ context.Context, flow *dsdk.DataFlow, _ *dsdk.DataPlaneSDK, _ *dsdk.ProcessorOptions) (*dsdk.DataFlowStatusMessage, error) {
			if isAsync(flow) {
				return &dsdk.DataFlowStatusMessage{State: dsdk.Starting}, nil
			}
			if flow.IsPull() {
				address, err := dsdk.NewDataAddressBuilder().
					EndpointType("https://w3id.org/idsa/v4.1/HTTP").
					Endpoint("https://provider.example.com/data").
					Property("authorization", "token-"+flow.ID).
					Build()
				if err != nil {
					return nil, err
				}
				return &dsdk.DataFlowStatusMessage{State: dsdk.Started, DataAddress: address}, nil
			}
			return &dsdk.DataFlowStatusMessage{State: dsdk.Started}, nil
		}),
	)
}
