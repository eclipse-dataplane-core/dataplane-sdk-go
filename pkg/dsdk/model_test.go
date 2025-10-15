package dsdk

import (
	"encoding/json"
	"net/url"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_dataFlowStartSerialize(t *testing.T) {
	callbackURL, _ := url.Parse("https://example.com/callback")
	build, _ := NewDataAddressBuilder().Property("foo", "bar").Build()
	original := DataFlowStartMessage{
		DataFlowBaseMessage: DataFlowBaseMessage{
			MessageID:        uuid.New().String(),
			ParticipantID:    "participant123",
			CounterPartyID:   uuid.New().String(),
			DataspaceContext: uuid.New().String(),
			ProcessID:        uuid.New().String(),
			AgreementID:      "agreement456",
			DatasetID:        uuid.New().String(),
			CallbackAddress:  CallbackURL(*callbackURL),
			TransferType: TransferType{
				DestinationType: "PULL",
				FlowType:        FlowType("PULL"),
			},
			DataAddress: *build,
		},
	}

	jsonData, err := json.Marshal(original)
	assert.NoError(t, err)

	var decoded DataFlowStartMessage
	err = json.Unmarshal(jsonData, &decoded)
	assert.NoError(t, err)

	assert.Equal(t, original.ParticipantID, decoded.ParticipantID, "ParticipantID should be equal")
	assert.Equal(t, original.AgreementID, decoded.AgreementID, "AgreementID should be equal")
	assert.Equal(t, original.CallbackAddress, decoded.CallbackAddress, "CallbackAddress should be equal")
	assert.Equal(t, original.TransferType, decoded.TransferType, "TransferType should be equal")
	assert.Equal(t, original.DataAddress, decoded.DataAddress, "DataAddress should be equal")
	assert.Equal(t, original, original)
}

func TestDataFlowBuilder_Build(t *testing.T) {
	validURL, err := url.Parse("http://example.com/callback")
	if err != nil {
		t.Fatal("failed to parse test URL:", err)
	}

	tests := map[string]struct {
		builder func() *DataFlowBuilder
		wantErr bool
	}{
		"success with all fields": {
			builder: func() *DataFlowBuilder {
				return NewDataFlowBuilder().
					ID("test-id").
					UpdatedAt(int64(time.Now().Unix())).
					CreatedAt(int64(time.Now().Unix())).
					ParticipantID("part-123").
					DataspaceContext("ctx-123").
					CounterpartyID("counter-123").
					State(Started).
					StateTimestamp(time.Now().Unix()).
					SourceDataAddress(DataAddress{Properties: map[string]any{"source": "test"}}).
					DestinationDataAddress(DataAddress{Properties: map[string]any{"dest": "test"}}).
					CallbackAddress(CallbackURL(*validURL)).
					TransferType(TransferType{
						DestinationType: "test-dest",
						FlowType:        Pull,
					}).
					RuntimeID("runtime-123")
			},
			wantErr: false,
		},
		"fails with empty builder": {
			builder: func() *DataFlowBuilder {
				return NewDataFlowBuilder()
			},
			wantErr: true,
		},
		"fails without ID": {
			builder: func() *DataFlowBuilder {
				b := createValidBuilder(validURL)
				return b.ID("")
			},
			wantErr: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			flow, err := tc.builder().Build()

			if tc.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// Verify non-zero/non-empty values for required fields
			if flow.ID == "" {
				t.Error("ID is empty")
			}
			if flow.UpdatedAt == 0 {
				t.Error("UpdatedAt is zero")
			}
			if flow.CreatedAt == 0 {
				t.Error("CreatedAt is zero")
			}
			if flow.ParticipantID == "" {
				t.Error("ParticipantID is empty")
			}
			if flow.DataspaceContext == "" {
				t.Error("DataspaceContext is empty")
			}
			if flow.CounterPartyID == "" {
				t.Error("CounterPartyID is empty")
			}
			if flow.StateTimestamp == 0 {
				t.Error("StateTimestamp is zero")
			}
			if flow.CallbackAddress.URL() == nil {
				t.Error("CallbackAddress is nil")
			}
			if flow.TransferType.DestinationType == "" {
				t.Error("TransferType destination type is empty")
			}
			if flow.TransferType.FlowType == "" {
				t.Error("TransferType flow type is empty")
			}
		})
	}
}

func TestDataAddressBuilder_EndpointProperty(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		typeVal string
		value   any
		want    any
	}{
		{
			name:    "sets valid string property",
			key:     "endpoint",
			typeVal: "string",
			value:   "https://api.example.com/v1/data",
			want:    []interface{}{map[string]interface{}{"key": "endpoint", "type": "string", "value": "https://api.example.com/v1/data"}},
		},
		{
			name:    "sets integer property",
			key:     "port",
			typeVal: "int",
			value:   8080,
			want:    []interface{}{map[string]interface{}{"key": "port", "type": "int", "value": 8080}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := NewDataAddressBuilder()
			result := builder.EndpointProperty(tt.key, tt.typeVal, tt.value)

			if result != builder {
				t.Error("EndpointProperty should return the builder for method chaining")
			}

			// Build and verify the property was set
			dataAddress, err := builder.Build()
			if err != nil {
				t.Fatalf("Build() failed: %v", err)
			}

			got, exists := dataAddress.Properties[EndpointProperties]
			require.True(t, exists, "EndpointProperties not found in built DataAddress")

			// Compare the entire slice using testify
			assert.Equal(t, tt.want, got, "EndpointProperty slice should match expected")

		})
	}
}

// Helper function to create a valid builder for testing
func createValidBuilder(validURL *url.URL) *DataFlowBuilder {
	return NewDataFlowBuilder().
		ID("test-id").
		UpdatedAt(int64(time.Now().Unix())).
		CreatedAt(int64(time.Now().Unix())).
		ParticipantID("part-123").
		DataspaceContext("ctx-123").
		CounterpartyID("counter-123").
		State(Started).
		StateTimestamp(time.Now().Unix()).
		SourceDataAddress(DataAddress{Properties: map[string]any{"source": "test"}}).
		DestinationDataAddress(DataAddress{Properties: map[string]any{"dest": "test"}}).
		CallbackAddress(CallbackURL(*validURL)).
		TransferType(TransferType{
			DestinationType: "test-dest",
			FlowType:        Pull,
		}).
		RuntimeID("runtime-123")
}
