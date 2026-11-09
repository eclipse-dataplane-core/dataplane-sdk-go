package dsdk

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	signalingSchemaBase = "https://w3id.org/dspace-sig/v1.0/"
	dspDataAddressURL   = "https://w3id.org/dspace/2025/1/transfer/data-address-schema.json"
	pullProfile         = "https://w3id.org/dspace-sig/profile/http-pull"
	pushProfile         = "https://w3id.org/dspace-sig/profile/s3-push"
)

// dataFlowSchemas are the schemas of the data plane endpoints. The registration and agreement schemas in testdata
// reference further DSP schemas and are not compiled until the SDK implements those endpoints.
var dataFlowSchemas = []string{
	"DataFlowBaseMessage",
	"DataFlowPrepareMessage",
	"DataFlowStartMessage",
	"DataFlowStartedNotificationMessage",
	"DataFlowSuspendMessage",
	"DataFlowResumeMessage",
	"DataFlowTerminateMessage",
	"DataFlowStatusMessage",
	"DataFlowStatusResponseMessage",
}

// loadSchemas compiles the published Data Plane Signaling data flow schemas (copied from eclipse-dataplane-signaling
// at a0752cd) and the DSP DataAddress schema they reference, without network access.
func loadSchemas(t *testing.T) map[string]*jsonschema.Schema {
	t.Helper()
	compiler := jsonschema.NewCompiler()
	files, err := filepath.Glob("testdata/schemas/*.json")
	require.NoError(t, err)
	require.NotEmpty(t, files)

	for _, file := range files {
		data, err := os.ReadFile(file)
		require.NoError(t, err)
		doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(data))
		require.NoError(t, err)

		base := filepath.Base(file)
		url := signalingSchemaBase + base
		if base == "dsp-data-address-schema.json" {
			url = dspDataAddressURL
		}
		require.NoError(t, compiler.AddResource(url, doc))
	}

	schemas := make(map[string]*jsonschema.Schema)
	for _, name := range dataFlowSchemas {
		schema, err := compiler.Compile(signalingSchemaBase + name + ".schema.json")
		require.NoError(t, err, name)
		schemas[name] = schema
	}
	return schemas
}

func assertConforms(t *testing.T, schemas map[string]*jsonschema.Schema, schemaName string, message any) {
	t.Helper()
	schema, ok := schemas[schemaName]
	require.True(t, ok, "schema %s not found", schemaName)
	data, err := json.Marshal(message)
	require.NoError(t, err)
	instance, err := jsonschema.UnmarshalJSON(bytes.NewReader(data))
	require.NoError(t, err)
	assert.NoError(t, schema.Validate(instance), "%s does not conform: %s", schemaName, data)
}

func Test_Messages_ConformToSchemas(t *testing.T) {
	schemas := loadSchemas(t)
	assert.Len(t, schemas, len(dataFlowSchemas))

	address := newDataAddress()
	base := newBaseMessage()
	base.Labels = []string{"gold", "blue"}
	base.Metadata = map[string]any{"region": "westeurope"}

	tests := []struct {
		schema  string
		message any
	}{
		{"DataFlowPrepareMessage", DataFlowPrepareMessage{DataFlowBaseMessage: base}},
		{"DataFlowPrepareMessage", DataFlowPrepareMessage{DataFlowBaseMessage: newBaseMessage()}},
		{"DataFlowStartMessage", DataFlowStartMessage{DataFlowBaseMessage: base, DataAddress: address}},
		{"DataFlowStartMessage", DataFlowStartMessage{DataFlowBaseMessage: newBaseMessage()}},
		{"DataFlowStartedNotificationMessage", DataFlowStartedNotificationMessage{MessageID: uuid.NewString(), DataAddress: address}},
		{"DataFlowStartedNotificationMessage", DataFlowStartedNotificationMessage{MessageID: uuid.NewString()}},
		{"DataFlowSuspendMessage", DataFlowSuspendMessage{MessageID: uuid.NewString(), Reason: "maintenance"}},
		{"DataFlowSuspendMessage", DataFlowSuspendMessage{MessageID: uuid.NewString()}},
		{"DataFlowResumeMessage", DataFlowResumeMessage{MessageID: uuid.NewString(), DataAddress: address}},
		{"DataFlowResumeMessage", DataFlowResumeMessage{MessageID: uuid.NewString()}},
		{"DataFlowTerminateMessage", DataFlowTerminateMessage{MessageID: uuid.NewString(), Reason: "error"}},
		{"DataFlowStatusMessage", DataFlowStatusMessage{MessageID: uuid.NewString(), DataFlowID: "flow-1", State: Started, DataAddress: address}},
		{"DataFlowStatusMessage", DataFlowStatusMessage{MessageID: uuid.NewString(), DataFlowID: "flow-1", State: Terminated, Error: "failed"}},
		{"DataFlowStatusResponseMessage", DataFlowStatusResponseMessage{DataFlowID: "flow-1", State: Preparing}},
	}
	for _, tc := range tests {
		t.Run(tc.schema, func(t *testing.T) {
			assertConforms(t, schemas, tc.schema, tc.message)
		})
	}

	t.Run("every state", func(t *testing.T) {
		for state := range stateNames {
			assertConforms(t, schemas, "DataFlowStatusMessage", DataFlowStatusMessage{MessageID: "m", DataFlowID: "f", State: state})
			assertConforms(t, schemas, "DataFlowStatusResponseMessage", DataFlowStatusResponseMessage{DataFlowID: "f", State: state})
		}
	})
}

func Test_Messages_SchemaRejectsMissingRequired(t *testing.T) {
	schemas := loadSchemas(t)
	for _, name := range []string{"DataFlowPrepareMessage", "DataFlowStartMessage", "DataFlowSuspendMessage", "DataFlowStatusMessage"} {
		instance, err := jsonschema.UnmarshalJSON(strings.NewReader(`{"reason": "x"}`))
		require.NoError(t, err)
		assert.Error(t, schemas[name].Validate(instance), name)
	}

	// the DSP DataAddress schema is applied: @type is required there, which DataAddress.MarshalJSON always adds
	instance, err := jsonschema.UnmarshalJSON(strings.NewReader(`{"messageId": "m", "dataAddress": {"endpointType": "t"}}`))
	require.NoError(t, err)
	assert.Error(t, schemas["DataFlowStartedNotificationMessage"].Validate(instance))
}

// Test_Messages_DecodeSpecExamples decodes the non-normative examples of the specification.
func Test_Messages_DecodeSpecExamples(t *testing.T) {
	start := `{
	  "messageId": "b1d5f9e2-3c4b-4f7a-9c3e-2f1e5d6c7b8a",
	  "participantId": "provider-participant-id",
	  "counterPartyId": "consumer-participant-id",
	  "dataspaceContext": "test-dataspace-context",
	  "dataFlowId": "test-transfer-process-id",
	  "agreementId": "test-agreement-id",
	  "datasetId": "asset-id",
	  "profile": "https://w3id.org/dspace-sig/profile/http-pull",
	  "claims": {"membership": "active", "sub": "subject"},
	  "dataAddress": {
	    "endpointType": "https://w3id.org/idsa/v4.1/HTTP",
	    "endpoint": "http://dataplane.provider.com/api/public",
	    "endpointProperties": [
	      {"name": "authorization", "value": "<AUTH_TOKEN>"},
	      {"name": "authType", "value": "bearer"}
	    ]
	  },
	  "labels": ["gold", "blue"],
	  "metadata": {"bucketName": "sourceBucket", "region": "westeurope"}
	}`
	var message DataFlowStartMessage
	require.NoError(t, json.Unmarshal([]byte(start), &message))
	assert.NoError(t, message.Validate())
	assert.Equal(t, "test-transfer-process-id", message.DataFlowID)
	assert.Equal(t, "https://w3id.org/dspace-sig/profile/http-pull", message.Profile)
	assert.Equal(t, "active", message.Claims["membership"])
	assert.Equal(t, []string{"gold", "blue"}, message.Labels)
	assert.Equal(t, "westeurope", message.Metadata["region"])
	token, ok := message.DataAddress.Property("authorization")
	assert.True(t, ok)
	assert.Equal(t, "<AUTH_TOKEN>", token)

	status := `{"messageId": "m", "dataFlowId": "dataFlow-123", "state": "PREPARED", "error": ""}`
	var statusMessage DataFlowStatusMessage
	require.NoError(t, json.Unmarshal([]byte(status), &statusMessage))
	assert.Equal(t, Prepared, statusMessage.State)
}

func Test_DataFlowState_JSON(t *testing.T) {
	data, err := json.Marshal(Initialized)
	require.NoError(t, err)
	assert.JSONEq(t, `"INITIALIZED"`, string(data))

	var state DataFlowState
	require.NoError(t, json.Unmarshal([]byte(`"SUSPENDED"`), &state))
	assert.Equal(t, Suspended, state)

	assert.Error(t, json.Unmarshal([]byte(`"UNINITIALIZED"`), &state))
	assert.Error(t, json.Unmarshal([]byte(`300`), &state))
	_, err = json.Marshal(DataFlowState(42))
	assert.Error(t, err)
}

func Test_Message_MissingProperties(t *testing.T) {
	msg := DataFlowPrepareMessage{DataFlowBaseMessage: DataFlowBaseMessage{MessageID: "test-id"}}
	assert.ErrorIs(t, msg.Validate(), ErrValidation)
}

func Test_Message_Success(t *testing.T) {
	msg := DataFlowPrepareMessage{DataFlowBaseMessage: newBaseMessage()}
	assert.NoError(t, msg.Validate())
}

func Test_Message_RequiredFields(t *testing.T) {
	tests := map[string]func(m *DataFlowBaseMessage){
		"messageId":        func(m *DataFlowBaseMessage) { m.MessageID = "" },
		"participantId":    func(m *DataFlowBaseMessage) { m.ParticipantID = "" },
		"counterPartyId":   func(m *DataFlowBaseMessage) { m.CounterPartyID = "" },
		"dataspaceContext": func(m *DataFlowBaseMessage) { m.DataspaceContext = "" },
		"dataFlowId":       func(m *DataFlowBaseMessage) { m.DataFlowID = "" },
		"agreementId":      func(m *DataFlowBaseMessage) { m.AgreementID = "" },
		"datasetId":        func(m *DataFlowBaseMessage) { m.DatasetID = "" },
		"profile":          func(m *DataFlowBaseMessage) { m.Profile = "" },
		"claims":           func(m *DataFlowBaseMessage) { m.Claims = nil },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			base := newBaseMessage()
			mutate(&base)
			msg := DataFlowStartMessage{DataFlowBaseMessage: base}
			assert.ErrorIs(t, msg.Validate(), ErrValidation)
		})
	}
}

func Test_Message_EmptyClaims(t *testing.T) {
	base := newBaseMessage()
	base.Claims = map[string]any{}
	msg := DataFlowPrepareMessage{DataFlowBaseMessage: base}
	assert.NoError(t, msg.Validate())
}

func Test_StartMessage_Success(t *testing.T) {
	startMsg := DataFlowStartMessage{DataFlowBaseMessage: newBaseMessage(), DataAddress: newDataAddress()}
	assert.NoError(t, startMsg.Validate())
}

func Test_StartMessage_WithoutDataAddress(t *testing.T) {
	startMsg := DataFlowStartMessage{DataFlowBaseMessage: newBaseMessage()}
	assert.NoError(t, startMsg.Validate())
}

func Test_StartMessage_InvalidDataAddress(t *testing.T) {
	startMsg := DataFlowStartMessage{DataFlowBaseMessage: newBaseMessage(), DataAddress: &DataAddress{Endpoint: "https://example.com"}}
	assert.ErrorIs(t, startMsg.Validate(), ErrValidation)
}

func Test_SignalMessages_RequireMessageID(t *testing.T) {
	assert.ErrorIs(t, (&DataFlowStartedNotificationMessage{}).Validate(), ErrValidation)
	assert.ErrorIs(t, (&DataFlowSuspendMessage{Reason: "x"}).Validate(), ErrValidation)
	assert.ErrorIs(t, (&DataFlowResumeMessage{}).Validate(), ErrValidation)
	assert.ErrorIs(t, (&DataFlowTerminateMessage{Reason: "x"}).Validate(), ErrValidation)

	assert.NoError(t, (&DataFlowStartedNotificationMessage{MessageID: "m"}).Validate())
	assert.NoError(t, (&DataFlowSuspendMessage{MessageID: "m"}).Validate())
	assert.NoError(t, (&DataFlowResumeMessage{MessageID: "m"}).Validate())
	assert.NoError(t, (&DataFlowTerminateMessage{MessageID: "m"}).Validate())
}

func newBaseMessage() DataFlowBaseMessage {
	return DataFlowBaseMessage{
		MessageID:        uuid.New().String(),
		ParticipantID:    uuid.New().String(),
		CounterPartyID:   uuid.New().String(),
		DataspaceContext: uuid.New().String(),
		DataFlowID:       uuid.New().String(),
		AgreementID:      uuid.New().String(),
		DatasetID:        uuid.New().String(),
		Profile:          pullProfile,
		Claims:           map[string]any{"membership": "active"},
	}
}

func newDataAddress() *DataAddress {
	return &DataAddress{
		EndpointType: "https://w3id.org/idsa/v4.1/HTTP",
		Endpoint:     "https://example.com/api/data",
		EndpointProperties: []EndpointProperty{
			{Name: "authorization", Value: "token"},
			{Name: "authType", Value: "bearer"},
		},
	}
}
