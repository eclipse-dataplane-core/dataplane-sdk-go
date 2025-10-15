package dsdk

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func Test_Message_MissingProperties(t *testing.T) {

	msg := DataFlowBaseMessage{
		MessageID: "test-id",
	}
	err := msg.Validate()
	assert.ErrorIs(t, err, ErrValidation)
}

func Test_Message_Success(t *testing.T) {
	msg := newBaseMessage()
	err := msg.Validate()
	assert.NoError(t, err)
}

func Test_Message_InvalidCallbackAddress(t *testing.T) {
	payload := newBaseMessage()
	payload.CallbackAddress = CallbackURL{}
	err := payload.Validate()

	assert.ErrorIs(t, err, ErrValidation)
}

func Test_Message_InvalidTransferType(t *testing.T) {
	msg := newBaseMessage()
	msg.TransferType = TransferType{}
	err := msg.Validate()
	assert.ErrorIs(t, err, ErrValidation)
}

func Test_StartMessage_Success(t *testing.T) {
	msg := newBaseMessage()
	startMsg := DataFlowStartMessage{DataFlowBaseMessage: msg}
	err := startMsg.Validate()
	assert.NoError(t, err)
}

func Test_StartMessage_MissingProperties(t *testing.T) {
	startMsg := DataFlowStartMessage{DataFlowBaseMessage: DataFlowBaseMessage{
		MessageID: "test-id",
	}}
	err := startMsg.Validate()
	assert.ErrorIs(t, err, ErrValidation)
}

func Test_StartMessage_MissingSourceDataAddress(t *testing.T) {
	startMsg := DataFlowStartMessage{DataFlowBaseMessage: newBaseMessage()}

	assert.NoError(t, startMsg.Validate())
}

func Test_PrepareMessage_Success(t *testing.T) {
	msg := newBaseMessage()
	startMsg := DataFlowPrepareMessage{DataFlowBaseMessage: msg}
	err := startMsg.Validate()
	assert.NoError(t, err)
}

func Test_PrepareMessage_MissingProperties(t *testing.T) {
	startMsg := DataFlowPrepareMessage{DataFlowBaseMessage: DataFlowBaseMessage{
		MessageID: "test-id",
	}}
	err := startMsg.Validate()
	assert.ErrorIs(t, err, ErrValidation)
}

func Test_TransitionMessage_Success(t *testing.T) {
	msg := DataFlowTransitionMessage{}
	assert.NoError(t, msg.Validate())

	msg2 := DataFlowTransitionMessage{Reason: "test-reason"}
	assert.NoError(t, msg2.Validate())
}

func newBaseMessage() DataFlowBaseMessage {
	return DataFlowBaseMessage{
		MessageID:        uuid.New().String(),
		ParticipantID:    uuid.New().String(),
		CounterPartyID:   uuid.New().String(),
		DataspaceContext: uuid.New().String(),
		ProcessID:        uuid.New().String(),
		AgreementID:      uuid.New().String(),
		DatasetID:        uuid.New().String(),
		CallbackAddress:  CallbackURL{Scheme: "http", Host: "test.com", Path: "/callback"},
		TransferType: TransferType{
			DestinationType: "test-type",
			FlowType:        "pull",
		},
		DataAddress: &DataAddress{},
	}
}
