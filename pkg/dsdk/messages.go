package dsdk

import (
	"github.com/go-playground/validator/v10"
)

var v = validator.New()

type DataFlowBaseMessage struct {
	MessageID        string       `json:"messageID" validate:"required"`
	ParticipantID    string       `json:"participantID" validate:"required"`
	CounterPartyID   string       `json:"counterPartyID" validate:"required"`
	DataspaceContext string       `json:"dataspaceContext" validate:"required"`
	ProcessID        string       `json:"processID" validate:"required"`
	AgreementID      string       `json:"agreementID" validate:"required"`
	DatasetID        string       `json:"datasetID"`
	CallbackAddress  CallbackURL  `json:"callbackAddress" validate:"required,callback-url"`
	TransferType     TransferType `json:"transferType" validate:"required"`
	DataAddress      *DataAddress `json:"dataAddress"`
}

func (d *DataFlowBaseMessage) Validate() error {
	err := v.RegisterValidation("callback-url", func(fl validator.FieldLevel) bool {
		u, ok := fl.Field().Interface().(CallbackURL)
		return ok && !u.IsEmpty()
	})
	if err != nil {
		return err
	}

	if err := v.Struct(d); err != nil {
		return WrapValidationError(err)
	}
	return nil
}

type DataFlowStartMessage struct {
	DataFlowBaseMessage
}

func (d *DataFlowStartMessage) Validate() error {
	err := d.DataFlowBaseMessage.Validate() // call base validator
	if err != nil {
		return WrapValidationError(err)
	}
	err = v.Struct(d)
	if err != nil {
		return WrapValidationError(err)
	}
	return nil
}

type DataFlowStartedNotificationMessage struct {
	DataAddress *DataAddress `json:"dataAddress,omitempty"`
}

func (d *DataFlowStartedNotificationMessage) Validate() error {
	err := v.Struct(d)
	if err != nil {
		return WrapValidationError(err)
	}
	return nil
}

type DataFlowPrepareMessage struct {
	DataFlowBaseMessage
}

type DataFlowTransitionMessage struct {
	Reason string `json:"reason"`
}

func (d *DataFlowTransitionMessage) Validate() error {
	return nil // no special behaviour yet
}

type DataFlowResponseMessage struct {
	DataplaneID string        `json:"dataplaneID"`
	DataAddress *DataAddress  `json:"dataAddress,omitempty"`
	State       DataFlowState `json:"state"`
	Error       string        `json:"error"`
}

type DataFlowStatusResponseMessage struct {
	State      DataFlowState `json:"state"`
	DataFlowID string        `json:"dataFlowID"`
}
