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

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/metaform/dataplane-sdk-go/pkg/dsdk"
	"github.com/metaform/dataplane-sdk-go/pkg/postgres"
	"github.com/stretchr/testify/assert"
)

// TestMain ensures containers are cleaned up and allows global setup if desired.
var ctx = context.Background()

// database SQL database connection, used for test setup and assertionss
var database *sql.DB

// newServerWithSdk instantiates a new HTTP server using the DataPlane SDK and registers its callbacks with endpoints
func newServerWithSdk(t *testing.T, sdk *dsdk.DataPlaneSDK) http.Handler {
	t.Helper()
	sdkApi := dsdk.NewDataPlaneApi(sdk)
	r := chi.NewRouter()

	r.Post("/dataflows/start", sdkApi.Start)
	r.Post("/dataflows/{id}/started", func(writer http.ResponseWriter, request *http.Request) {
		id := chi.URLParam(request, "id")
		sdkApi.StartById(writer, request, id)
	})
	r.Post("/dataflows/prepare", sdkApi.Prepare)
	r.Post("/dataflows/{id}/terminate", func(writer http.ResponseWriter, request *http.Request) {
		id := chi.URLParam(request, "id")
		sdkApi.Terminate(id, writer, request)
	})
	r.Post("/dataflows/{id}/suspend", func(writer http.ResponseWriter, request *http.Request) {
		id := chi.URLParam(request, "id")
		sdkApi.Suspend(id, writer, request)
	})
	r.Get("/dataflows/{id}/status", func(writer http.ResponseWriter, request *http.Request) {
		id := chi.URLParam(request, "id")
		sdkApi.Status(id, writer, request)
	})
	return r
}

var handler http.Handler

func TestMain(m *testing.M) {
	db, container := postgres.SetupDatabase(&testing.T{}, ctx)
	database = db
	t := &testing.T{}

	sdk, err := newSdk(db)
	assert.NoError(t, err)
	handler = newServerWithSdk(t, sdk)
	code := m.Run()
	_ = db.Close()
	_ = container.Terminate(ctx)
	os.Exit(code)
}

// E2E tests
func Test_Start_NotYetExists(t *testing.T) {

	payload, err := serialize(newStartMessage())
	assert.NoError(t, err)

	req, err := http.NewRequest(http.MethodPost, "/dataflows/start", bytes.NewBuffer(payload))
	assert.NoError(t, err)

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var responseMessage dsdk.DataFlowResponseMessage
	err = json.NewDecoder(rr.Body).Decode(&responseMessage)
	assert.NoError(t, err)
	assert.Equal(t, responseMessage.State, dsdk.Started)
}

func Test_Start_InvalidPayload(t *testing.T) {
	sm := newStartMessage()
	sm.CounterPartyID = "" // should raise a validation error
	payload, err := serialize(sm)
	assert.NoError(t, err)
	req, err := http.NewRequest(http.MethodPost, "/dataflows/start", bytes.NewBuffer(payload))
	assert.NoError(t, err)

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.NotNil(t, rr.Body.String())
}

func Test_StartByID_WhenNotFound(t *testing.T) {
	id := uuid.New().String()

	requestBody, err := serialize(newStartByIdMessage())
	assert.NoError(t, err)

	req, err := http.NewRequest(http.MethodPost, "/dataflows/"+id+"/started", bytes.NewBuffer(requestBody))
	assert.NoError(t, err)

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusNotFound, rr.Code)

}

func Test_StartByID_WhenStartedOrStarting(t *testing.T) {

	states := []dsdk.DataFlowState{
		dsdk.Started,
		dsdk.Starting,
	}

	for _, state := range states {
		id := uuid.New().String()
		store := postgres.NewStore(database)
		flow, err := newFlowBuilder().ID(id).State(state).Consumer(true).Build()
		assert.NoError(t, err)
		assert.NoError(t, store.Create(ctx, flow))

		requestBody, err := serialize(newStartByIdMessage())
		assert.NoError(t, err)

		req, err := http.NewRequest(http.MethodPost, "/dataflows/"+id+"/started", bytes.NewBuffer(requestBody))
		assert.NoError(t, err)

		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code)
		found, err := store.FindById(ctx, id)
		assert.NoError(t, err)
		assert.Equal(t, dsdk.Started, found.State)
	}

}

func Test_StartByID_WhenPrepared(t *testing.T) {

	tests := []struct {
		isConsumer       bool
		expectedHttpCode int
		expectedState    dsdk.DataFlowState
	}{
		{
			isConsumer:       true,
			expectedHttpCode: http.StatusOK,
			expectedState:    dsdk.Started,
		},
		{
			isConsumer:       false,
			expectedHttpCode: http.StatusBadRequest,
			expectedState:    dsdk.Prepared,
		},
	}

	for _, test := range tests {
		id := uuid.New().String()
		store := postgres.NewStore(database)
		flow, err := newFlowBuilder().ID(id).State(dsdk.Prepared).Consumer(test.isConsumer).Build()
		assert.NoError(t, err)
		assert.NoError(t, store.Create(ctx, flow))

		requestBody, err := serialize(newStartByIdMessage())
		assert.NoError(t, err)

		req, err := http.NewRequest(http.MethodPost, "/dataflows/"+id+"/started", bytes.NewBuffer(requestBody))
		assert.NoError(t, err)

		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)

		assert.Equal(t, test.expectedHttpCode, rr.Code)
		found, err := store.FindById(ctx, id)
		assert.NoError(t, err)
		assert.Equal(t, test.expectedState, found.State)
	}

}

func Test_StartByID_MissingSourceAddress(t *testing.T) {
	id := uuid.New().String()
	store := postgres.NewStore(database)
	flow, err := newFlowBuilder().ID(id).State(dsdk.Prepared).Consumer(true).Build()
	assert.NoError(t, err)
	assert.NoError(t, store.Create(ctx, flow))
	requestBody, err := serialize(dsdk.DataFlowStartedNotificationMessage{})
	assert.NoError(t, err)

	req, err := http.NewRequest(http.MethodPost, "/dataflows/"+flow.ID+"/started", bytes.NewBuffer(requestBody))
	assert.NoError(t, err)

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
}

func Test_Prepare(t *testing.T) {
	payload, err := serialize(newPrepareMessage())
	assert.NoError(t, err)

	req, err := http.NewRequest(http.MethodPost, "/dataflows/prepare", bytes.NewBuffer(payload))
	assert.NoError(t, err)

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var responseMessage dsdk.DataFlowResponseMessage
	err = json.NewDecoder(rr.Body).Decode(&responseMessage)
	assert.NoError(t, err)
	assert.Equal(t, responseMessage.State, dsdk.Prepared)
}

func Test_Prepare_WrongState(t *testing.T) {

	s := postgres.NewStore(database)
	flow, err := newFlowBuilder().State(dsdk.Started).Build()
	assert.NoError(t, err)
	assert.NoError(t, s.Create(ctx, flow))

	message := newPrepareMessage()
	message.ProcessID = flow.ID
	payload, err := serialize(message)
	assert.NoError(t, err)

	req, err := http.NewRequest(http.MethodPost, "/dataflows/prepare", bytes.NewBuffer(payload))
	assert.NoError(t, err)

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusConflict, rr.Code)
}

func Test_Suspend_Success(t *testing.T) {

	id := uuid.New().String()
	flow, err := newFlowBuilder().ID(id).State(dsdk.Started).Build()
	assert.NoError(t, err)
	store := postgres.NewStore(database)
	err = store.Create(ctx, flow)
	assert.NoError(t, err)

	req, err := http.NewRequest(http.MethodPost, "/dataflows/"+flow.ID+"/suspend", strings.NewReader(""))
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	byId, err := store.FindById(ctx, id)
	assert.NoError(t, err)
	assert.Equal(t, dsdk.Suspended, byId.State)
}

func Test_Suspend_WithReason(t *testing.T) {

	id := uuid.New().String()
	flow, err := newFlowBuilder().ID(id).State(dsdk.Started).Build()
	assert.NoError(t, err)
	store := postgres.NewStore(database)
	err = store.Create(ctx, flow)
	assert.NoError(t, err)

	req, err := http.NewRequest(http.MethodPost, "/dataflows/"+flow.ID+"/suspend", strings.NewReader(`{"reason": "test reason"}`))
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	byId, err := store.FindById(ctx, id)
	assert.NoError(t, err)
	assert.Equal(t, dsdk.Suspended, byId.State)
	assert.Equal(t, "test reason", byId.ErrorDetail)
}

func Test_Suspend_WhenNotExists(t *testing.T) {
	id := uuid.New().String()
	flow, err := newFlowBuilder().ID(id).State(dsdk.Started).Build()
	assert.NoError(t, err)
	//missing: storing the flow

	req, err := http.NewRequest(http.MethodPost, "/dataflows/"+flow.ID+"/suspend", strings.NewReader(""))
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusNotFound, rr.Code)
}

func Test_Suspend_WhenNotStarted(t *testing.T) {
	id := uuid.New().String()
	flow, err := newFlowBuilder().ID(id).State(dsdk.Completed).Build() // completed flows cannot transition to suspended
	assert.NoError(t, err)
	err = postgres.NewStore(database).Create(ctx, flow)
	assert.NoError(t, err)

	req, err := http.NewRequest(http.MethodPost, "/dataflows/"+flow.ID+"/suspend", strings.NewReader(""))
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func Test_Terminate_Success(t *testing.T) {
	id := uuid.New().String()
	flow, err := newFlowBuilder().ID(id).State(dsdk.Started).Build()
	assert.NoError(t, err)
	store := postgres.NewStore(database)
	err = store.Create(ctx, flow)
	assert.NoError(t, err)

	req, err := http.NewRequest(http.MethodPost, "/dataflows/"+flow.ID+"/terminate", strings.NewReader(""))
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	byId, err := store.FindById(ctx, id)
	assert.NoError(t, err)
	assert.Equal(t, dsdk.Terminated, byId.State)

}

func Test_Terminate_WithReason(t *testing.T) {
	id := uuid.New().String()
	flow, err := newFlowBuilder().ID(id).State(dsdk.Started).Build()
	assert.NoError(t, err)
	store := postgres.NewStore(database)
	err = store.Create(ctx, flow)
	assert.NoError(t, err)

	req, err := http.NewRequest(http.MethodPost, "/dataflows/"+flow.ID+"/terminate", strings.NewReader(`{"reason": "test reason"}`))
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	byId, err := store.FindById(ctx, id)
	assert.NoError(t, err)
	assert.Equal(t, dsdk.Terminated, byId.State)
	assert.Equal(t, "test reason", byId.ErrorDetail)
}

func Test_Terminate_WhenNotFound(t *testing.T) {
	id := uuid.New().String()
	flow, err := newFlowBuilder().ID(id).State(dsdk.Started).Build()
	assert.NoError(t, err)

	req, err := http.NewRequest(http.MethodPost, "/dataflows/"+flow.ID+"/terminate", strings.NewReader(""))
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusNotFound, rr.Code)
}

func Test_GetStatus(t *testing.T) {
	id := uuid.New().String()
	flow, err := newFlowBuilder().ID(id).State(dsdk.Started).Build()
	assert.NoError(t, err)
	store := postgres.NewStore(database)
	err = store.Create(ctx, flow)
	assert.NoError(t, err)

	req, err := http.NewRequest(http.MethodGet, "/dataflows/"+flow.ID+"/status", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	var responseMessage dsdk.DataFlowStatusResponseMessage
	err = json.NewDecoder(rr.Body).Decode(&responseMessage)
	assert.NoError(t, err)
	assert.Equal(t, responseMessage.State, dsdk.Started)
}

func Test_GetStatus_NotFound(t *testing.T) {

	req, err := http.NewRequest(http.MethodGet, "/dataflows/not-exist/status", nil)
	assert.NoError(t, err)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusNotFound, rr.Code)
}

func newFlowBuilder() *dsdk.DataFlowBuilder {
	bldr := &dsdk.DataFlowBuilder{}
	return bldr.ID("test-id").
		ParticipantID(uuid.New().String()).
		DataspaceContext(uuid.New().String()).
		CounterpartyID(uuid.New().String()).
		CallbackAddress(newCallback()).
		TransferType(newTransferType())
}

func serialize(obj any) ([]byte, error) {
	marshal, err := json.Marshal(obj)
	return marshal, err
}

func newStartMessage() dsdk.DataFlowStartMessage {
	return dsdk.DataFlowStartMessage{
		DataFlowBaseMessage: dsdk.DataFlowBaseMessage{
			MessageID:        uuid.New().String(),
			ParticipantID:    uuid.New().String(),
			CounterPartyID:   uuid.New().String(),
			DataspaceContext: uuid.New().String(),
			ProcessID:        uuid.New().String(),
			AgreementID:      uuid.New().String(),
			DatasetID:        uuid.New().String(),
			CallbackAddress:  newCallback(),
			TransferType:     newTransferType(),
			DataAddress: dsdk.DataAddress{
				Properties: map[string]any{
					"foo": "bar",
				},
			},
		},
	}
}

func newStartByIdMessage() dsdk.DataFlowStartedNotificationMessage {
	return dsdk.DataFlowStartedNotificationMessage{
		DataAddress: &dsdk.DataAddress{
			Properties: map[string]any{
				"foo": "bar",
			},
		},
	}
}

func newTransferType() dsdk.TransferType {
	return dsdk.TransferType{
		DestinationType: "com.test.http",
		FlowType:        "pull",
	}
}

func newPrepareMessage() dsdk.DataFlowPrepareMessage {
	return dsdk.DataFlowPrepareMessage{
		DataFlowBaseMessage: dsdk.DataFlowBaseMessage{
			MessageID:        uuid.New().String(),
			ParticipantID:    uuid.New().String(),
			CounterPartyID:   uuid.New().String(),
			DataspaceContext: uuid.New().String(),
			ProcessID:        uuid.New().String(),
			AgreementID:      uuid.New().String(),
			DatasetID:        uuid.New().String(),
			CallbackAddress:  newCallback(),
			TransferType:     newTransferType(),
			DataAddress:      dsdk.DataAddress{},
		},
	}
}

func newCallback() dsdk.CallbackURL {
	return dsdk.CallbackURL{Scheme: "http", Host: "test.com", Path: "/callback"}
}

func newSdk(db *sql.DB) (*dsdk.DataPlaneSDK, error) {
	sdk, err := dsdk.NewDataPlaneSDK(
		dsdk.WithStore(postgres.NewStore(db)),
		dsdk.WithTransactionContext(postgres.NewDBTransactionContext(db)),
	)
	return sdk, err
}
