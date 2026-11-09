package dsdk

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_dataFlowStartSerialize(t *testing.T) {
	address, err := NewDataAddressBuilder().EndpointType("https://w3id.org/idsa/v4.1/HTTP").Property("foo", "bar").Build()
	require.NoError(t, err)
	original := DataFlowStartMessage{
		DataFlowBaseMessage: DataFlowBaseMessage{
			MessageID:        uuid.New().String(),
			ParticipantID:    "participant123",
			CounterPartyID:   uuid.New().String(),
			DataspaceContext: uuid.New().String(),
			DataFlowID:       uuid.New().String(),
			AgreementID:      "agreement456",
			DatasetID:        uuid.New().String(),
			Profile:          pushProfile,
			Claims:           map[string]any{"sub": "subject"},
			Labels:           []string{"gold"},
			Metadata:         map[string]any{"bucketName": "destinationBucket"},
		},
		DataAddress: address,
	}

	jsonData, err := json.Marshal(original)
	require.NoError(t, err)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(jsonData, &raw))
	for _, key := range []string{"messageId", "participantId", "counterPartyId", "dataspaceContext", "dataFlowId", "agreementId", "datasetId", "profile", "claims", "labels", "metadata", "dataAddress"} {
		assert.Contains(t, raw, key)
	}

	var decoded DataFlowStartMessage
	require.NoError(t, json.Unmarshal(jsonData, &decoded))
	assert.Equal(t, original, decoded)
}

func Test_DataAddress_JSON(t *testing.T) {
	address := newDataAddress()
	data, err := json.Marshal(address)
	require.NoError(t, err)
	assert.JSONEq(t, `{
		"@type": "DataAddress",
		"endpointType": "https://w3id.org/idsa/v4.1/HTTP",
		"endpoint": "https://example.com/api/data",
		"endpointProperties": [
			{"@type": "EndpointProperty", "name": "authorization", "value": "token"},
			{"@type": "EndpointProperty", "name": "authType", "value": "bearer"}
		]
	}`, string(data))

	var decoded DataAddress
	require.NoError(t, json.Unmarshal(data, &decoded))
	assert.Equal(t, *address, decoded)

	// @type is optional on input
	require.NoError(t, json.Unmarshal([]byte(`{"endpointType": "t", "endpointProperties": [{"name": "a", "value": "b"}]}`), &decoded))
	assert.Equal(t, DataAddress{EndpointType: "t", EndpointProperties: []EndpointProperty{{Name: "a", Value: "b"}}}, decoded)

	assert.Error(t, json.Unmarshal([]byte(`{"@type": "Other", "endpointType": "t"}`), &decoded))
}

func TestDataAddressBuilder(t *testing.T) {
	address, err := NewDataAddressBuilder().
		EndpointType("https://w3id.org/idsa/v4.1/HTTP").
		Endpoint("https://example.com").
		Property("authorization", "token").
		Property("authType", "bearer").
		Build()
	require.NoError(t, err)
	assert.Equal(t, "https://w3id.org/idsa/v4.1/HTTP", address.EndpointType)
	assert.Equal(t, "https://example.com", address.Endpoint)
	assert.Equal(t, []EndpointProperty{{Name: "authorization", Value: "token"}, {Name: "authType", Value: "bearer"}}, address.EndpointProperties)

	value, ok := address.Property("authType")
	assert.True(t, ok)
	assert.Equal(t, "bearer", value)
	_, ok = address.Property("missing")
	assert.False(t, ok)

	_, err = NewDataAddressBuilder().Endpoint("https://example.com").Build()
	assert.ErrorIs(t, err, ErrValidation)

	_, err = NewDataAddressBuilder().EndpointType("t").Property("", "value").Build()
	assert.ErrorIs(t, err, ErrValidation)
}

func TestDataFlow_PushPull(t *testing.T) {
	tests := []struct {
		profile    string
		push, pull bool
	}{
		{"https://w3id.org/dspace-sig/profile/s3-push", true, false},
		{"https://w3id.org/dspace-sig/profile/http-pull", false, true},
		{"HTTP-PUSH", true, false},
		{"FileSystem-PULL", false, true},
		{"Push", true, false},
		{"http-pull-something", false, false},
		{"", false, false},
	}
	for _, tc := range tests {
		t.Run(tc.profile, func(t *testing.T) {
			flow := DataFlow{Profile: tc.profile}
			assert.Equal(t, tc.push, flow.IsPush())
			assert.Equal(t, tc.pull, flow.IsPull())
		})
	}
}

func TestDataFlow_Copy(t *testing.T) {
	flow := &DataFlow{
		ID:                "id",
		Claims:            map[string]any{"a": "b"},
		Labels:            []string{"gold"},
		Metadata:          map[string]any{"c": "d"},
		SourceDataAddress: newDataAddress(),
	}
	c := flow.Copy()
	c.Claims["a"] = "changed"
	c.Labels[0] = "changed"
	c.Metadata["c"] = "changed"
	c.SourceDataAddress.EndpointProperties[0].Value = "changed"

	assert.Equal(t, "b", flow.Claims["a"])
	assert.Equal(t, "gold", flow.Labels[0])
	assert.Equal(t, "d", flow.Metadata["c"])
	assert.Equal(t, "token", flow.SourceDataAddress.EndpointProperties[0].Value)
	assert.Nil(t, c.DestinationDataAddress)
}

func TestDataFlowBuilder_Build(t *testing.T) {
	tests := map[string]struct {
		builder func() *DataFlowBuilder
		wantErr bool
	}{
		"success with all fields": {
			builder: createValidBuilder,
			wantErr: false,
		},
		"fails with empty builder": {
			builder: NewDataFlowBuilder,
			wantErr: true,
		},
		"fails without ID": {
			builder: func() *DataFlowBuilder {
				return createValidBuilder().ID("")
			},
			wantErr: true,
		},
		"fails without profile": {
			builder: func() *DataFlowBuilder {
				return createValidBuilder().Profile("")
			},
			wantErr: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			flow, err := tc.builder().Build()

			if tc.wantErr {
				assert.ErrorIs(t, err, ErrValidation)
				return
			}
			require.NoError(t, err)

			assert.NotEmpty(t, flow.ID)
			assert.NotZero(t, flow.UpdatedAt)
			assert.NotZero(t, flow.CreatedAt)
			assert.NotEmpty(t, flow.ParticipantID)
			assert.NotEmpty(t, flow.DataspaceContext)
			assert.NotEmpty(t, flow.CounterPartyID)
			assert.NotZero(t, flow.StateTimestamp)
			assert.Equal(t, pullProfile, flow.Profile)
			assert.Equal(t, map[string]any{"sub": "subject"}, flow.Claims)
			assert.Equal(t, []string{"gold"}, flow.Labels)
			assert.Equal(t, map[string]any{"region": "westeurope"}, flow.Metadata)
			assert.Equal(t, "cp-1", flow.ControlPlaneID)
			assert.Equal(t, SuspendedByNone, flow.SuspendedBy)
		})
	}
}

func createValidBuilder() *DataFlowBuilder {
	return NewDataFlowBuilder().
		ID("test-id").
		UpdatedAt(time.Now().Unix()).
		CreatedAt(time.Now().Unix()).
		ParticipantID("part-123").
		DataspaceContext("ctx-123").
		CounterpartyID("counter-123").
		Profile(pullProfile).
		Claims(map[string]any{"sub": "subject"}).
		Labels([]string{"gold"}).
		Metadata(map[string]any{"region": "westeurope"}).
		ControlPlaneID("cp-1").
		State(Started).
		StateTimestamp(time.Now().Unix()).
		SourceDataAddress(newDataAddress()).
		RuntimeID("runtime-123")
}
