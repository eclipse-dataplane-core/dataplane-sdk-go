package dsdk

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

// NotifyPrepared completes an asynchronous prepare: it transitions the flow to PREPARED and sends the prepared
// callback to the control plane. The optional processor runs first, e.g. to provide the DataAddress of a push
// consumer, and must report PREPARED. The flow is persisted only if the control plane accepts the callback.
func (dsdk *DataPlaneSDK) NotifyPrepared(ctx context.Context, id string, processor DataFlowProcessor) (*DataFlowStatusMessage, error) {
	return dsdk.notifyTransition(ctx, id, ActionPrepared, Prepared, processor, (*DataFlow).TransitionToPrepared)
}

// NotifyStarted completes an asynchronous start: it transitions the flow to STARTED and sends the started callback to
// the control plane. The optional processor runs first, e.g. to provide the DataAddress of a pull provider, and must
// report STARTED. The flow is persisted only if the control plane accepts the callback.
func (dsdk *DataPlaneSDK) NotifyStarted(ctx context.Context, id string, processor DataFlowProcessor) (*DataFlowStatusMessage, error) {
	return dsdk.notifyTransition(ctx, id, ActionStarted, Started, processor, (*DataFlow).TransitionToStarted)
}

// NotifyCompleted transitions a STARTED flow to COMPLETED and sends the completed callback to the control plane.
// The flow is persisted only if the control plane accepts the callback.
func (dsdk *DataPlaneSDK) NotifyCompleted(ctx context.Context, id string) error {
	return dsdk.notify(ctx, id, ActionCompleted, func(flow *DataFlow) (*DataFlowStatusMessage, error) {
		if err := flow.TransitionToCompleted(); err != nil {
			return nil, err
		}
		return &DataFlowStatusMessage{}, nil
	})
}

// NotifyErrored terminates the flow because of cause and sends the errored callback, carrying cause as the error
// description, to the control plane. The flow is persisted only if the control plane accepts the callback.
func (dsdk *DataPlaneSDK) NotifyErrored(ctx context.Context, id string, cause error) error {
	if cause == nil {
		return fmt.Errorf("%w: cause cannot be nil", ErrInvalidInput)
	}
	return dsdk.notify(ctx, id, ActionErrored, func(flow *DataFlow) (*DataFlowStatusMessage, error) {
		if err := flow.TransitionToTerminated(cause.Error()); err != nil {
			return nil, err
		}
		return &DataFlowStatusMessage{Error: cause.Error()}, nil
	})
}

// SuspendLocally suspends a STARTED flow on the data plane's own initiative. The specification has no suspended
// callback, so the control plane is not called. Only ResumeLocally can resume the flow.
func (dsdk *DataPlaneSDK) SuspendLocally(ctx context.Context, id string, reason string) error {
	return dsdk.execute(ctx, func(ctx context.Context) error {
		flow, err := dsdk.Store.FindById(ctx, id)
		if err != nil {
			return fmt.Errorf("suspending data flow %s: %w", id, err)
		}
		if flow.State == Suspended && flow.SuspendedBy == SuspendedByLocal {
			return nil
		}
		if flow.State != Started {
			return fmt.Errorf("%w: cannot transition from %v to SUSPENDED", ErrInvalidTransition, flow.State)
		}
		if err := flow.TransitionToSuspended(reason, SuspendedByLocal); err != nil {
			return err
		}
		if err := dsdk.Store.Save(ctx, flow); err != nil {
			return fmt.Errorf("suspending data flow %s: %w", flow.ID, err)
		}
		return nil
	})
}

// ResumeLocally resumes a flow suspended with SuspendLocally. The optional processor runs first and must report
// STARTED. The control plane is not called.
func (dsdk *DataPlaneSDK) ResumeLocally(ctx context.Context, id string, processor DataFlowProcessor) error {
	return dsdk.execute(ctx, func(ctx context.Context) error {
		flow, err := dsdk.Store.FindById(ctx, id)
		if err != nil {
			return fmt.Errorf("resuming data flow %s: %w", id, err)
		}
		if flow.State != Suspended || flow.SuspendedBy != SuspendedByLocal {
			return fmt.Errorf("%w: data flow %s is not suspended locally (state %s, suspended by %q)", ErrInvalidTransition, flow.ID, flow.State, flow.SuspendedBy)
		}
		if processor != nil {
			response, err := dsdk.process(ctx, processor, flow, &ProcessorOptions{})
			if err != nil {
				return err
			}
			if response.State != Started {
				return fmt.Errorf("resume processor returned an invalid state %s", response.State)
			}
		}
		if err := flow.TransitionToResumed(SuspendedByLocal); err != nil {
			return err
		}
		if err := dsdk.Store.Save(ctx, flow); err != nil {
			return fmt.Errorf("resuming data flow %s: %w", flow.ID, err)
		}
		return nil
	})
}

// notifyTransition implements NotifyPrepared and NotifyStarted.
func (dsdk *DataPlaneSDK) notifyTransition(ctx context.Context,
	id string,
	action string,
	target DataFlowState,
	processor DataFlowProcessor,
	transition func(*DataFlow) error) (*DataFlowStatusMessage, error) {

	var message *DataFlowStatusMessage
	err := dsdk.notify(ctx, id, action, func(flow *DataFlow) (*DataFlowStatusMessage, error) {
		if flow.isFinal() {
			return nil, fmt.Errorf("%w: data flow %s is in final state %s", ErrInvalidTransition, flow.ID, flow.State)
		}
		if processor != nil {
			response, err := dsdk.process(ctx, processor, flow, &ProcessorOptions{Duplicate: flow.State == target})
			if err != nil {
				return nil, err
			}
			if response.State != target {
				return nil, fmt.Errorf("%s processor returned an invalid state %s", action, response.State)
			}
		}
		if err := transition(flow); err != nil {
			return nil, err
		}
		msg := &DataFlowStatusMessage{}
		if providesAddress(flow) {
			msg.DataAddress = providedAddress(flow).Copy()
		}
		message = msg
		return msg, nil
	})
	if err != nil {
		return nil, err
	}
	return message, nil
}

// notify loads the flow, lets apply transition it and build the callback message, sends the callback and persists
// the flow only if the control plane accepted it, all within one transaction.
func (dsdk *DataPlaneSDK) notify(ctx context.Context, id string, action string, apply func(*DataFlow) (*DataFlowStatusMessage, error)) error {
	if id == "" {
		return fmt.Errorf("%w: data flow id cannot be empty", ErrInvalidInput)
	}
	return dsdk.execute(ctx, func(ctx context.Context) error {
		flow, err := dsdk.Store.FindById(ctx, id)
		if err != nil {
			return fmt.Errorf("notifying %s for data flow %s: %w", action, id, err)
		}
		if dsdk.controlPlanes == nil {
			return fmt.Errorf("notifying %s for data flow %s: %w", action, id, ErrControlPlaneNotRegistered)
		}
		endpoint, err := dsdk.controlPlanes.ControlPlaneURL(ctx, flow.ControlPlaneID)
		if err != nil {
			return fmt.Errorf("notifying %s for data flow %s: %w", action, id, err)
		}

		message, err := apply(flow)
		if err != nil {
			return err
		}
		message.MessageID = uuid.NewString()
		message.DataFlowID = flow.ID
		message.State = flow.State

		// a rejected or failed callback leaves the stored flow unchanged; *ControlPlaneError is reachable via errors.As
		if err := dsdk.controlPlaneClient.Notify(ctx, endpoint, action, *message); err != nil {
			return fmt.Errorf("notifying %s for data flow %s: %w", action, id, err)
		}
		if err := dsdk.Store.Save(ctx, flow); err != nil {
			return fmt.Errorf("saving data flow %s after %s callback: %w", flow.ID, action, err)
		}
		return nil
	})
}
