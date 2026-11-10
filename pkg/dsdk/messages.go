package dsdk

import (
	"github.com/go-playground/validator/v10"
)

var v = validator.New()

// DataFlowBaseMessage contains the properties shared by the DataFlowPrepareMessage and the DataFlowStartMessage.
type DataFlowBaseMessage struct {
	MessageID        string         `json:"messageId" validate:"required"`
	ParticipantID    string         `json:"participantId" validate:"required"`
	CounterPartyID   string         `json:"counterPartyId" validate:"required"`
	DataspaceContext string         `json:"dataspaceContext" validate:"required"`
	DataFlowID       string         `json:"dataFlowId" validate:"required"`
	AgreementID      string         `json:"agreementId" validate:"required"`
	DatasetID        string         `json:"datasetId" validate:"required"`
	Profile          string         `json:"profile" validate:"required"`
	Claims           map[string]any `json:"claims" validate:"required"`
	Labels           []string       `json:"labels,omitempty"`
	Metadata         map[string]any `json:"metadata,omitempty"`
}

// DataFlowPrepareMessage is sent by the consumer control plane to prepare a data flow.
type DataFlowPrepareMessage struct {
	DataFlowBaseMessage
}

func (d *DataFlowPrepareMessage) Validate() error {
	return validate(d)
}

// DataFlowStartMessage is sent by the provider control plane to start a data flow. For provider-push transfers it
// carries the DataAddress the data must be pushed to; it must be omitted for consumer-pull transfers.
type DataFlowStartMessage struct {
	DataFlowBaseMessage
	DataAddress *DataAddress `json:"dataAddress,omitempty"`
}

func (d *DataFlowStartMessage) Validate() error {
	return validate(d)
}

// DataFlowStartedNotificationMessage notifies the consumer data plane that a transfer has started. For consumer-pull
// transfers it carries the DataAddress to obtain the data from, which the specification makes optional; it must be
// omitted for provider-push transfers.
type DataFlowStartedNotificationMessage struct {
	MessageID   string       `json:"messageId" validate:"required"`
	DataAddress *DataAddress `json:"dataAddress,omitempty"`
}

func (d *DataFlowStartedNotificationMessage) Validate() error {
	return validate(d)
}

// DataFlowSuspendMessage signals the data plane to suspend a data flow.
type DataFlowSuspendMessage struct {
	MessageID string `json:"messageId" validate:"required"`
	Reason    string `json:"reason,omitempty"`
}

func (d *DataFlowSuspendMessage) Validate() error {
	return validate(d)
}

// DataFlowResumeMessage signals the data plane to resume a suspended data flow.
type DataFlowResumeMessage struct {
	MessageID   string       `json:"messageId" validate:"required"`
	DataAddress *DataAddress `json:"dataAddress,omitempty"`
}

func (d *DataFlowResumeMessage) Validate() error {
	return validate(d)
}

// DataFlowTerminateMessage signals the data plane to terminate a data flow.
type DataFlowTerminateMessage struct {
	MessageID string `json:"messageId" validate:"required"`
	Reason    string `json:"reason,omitempty"`
}

func (d *DataFlowTerminateMessage) Validate() error {
	return validate(d)
}

// DataFlowStatusMessage is returned by the data plane in response to prepare, start, started, resume and status
// requests.
type DataFlowStatusMessage struct {
	MessageID   string        `json:"messageId"`
	DataFlowID  string        `json:"dataFlowId"`
	State       DataFlowState `json:"state"`
	DataAddress *DataAddress  `json:"dataAddress,omitempty"`
	Error       string        `json:"error,omitempty"`
}

// DataFlowStatusResponseMessage is returned by the data plane in response to a status request.
type DataFlowStatusResponseMessage struct {
	DataFlowID string        `json:"dataFlowId"`
	State      DataFlowState `json:"state"`
}

// ErrorResponse is the body the data plane returns with HTTP 4xx and 5xx responses.
type ErrorResponse struct {
	Error string `json:"error"`
}

func validate(s any) error {
	if err := v.Struct(s); err != nil {
		return WrapValidationError(err)
	}
	return nil
}
