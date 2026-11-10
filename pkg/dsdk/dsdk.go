package dsdk

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/google/uuid"
)

// DataFlowProcessor is an extension point for prepare (consumer), start (provider) and resume. Implementations may
// modify the data flow instance, which is persisted by the SDK, and return the resulting state, an optional DataAddress
// and an optional error description. The SDK sets the messageId and dataFlowId of the returned message. If the message
// is a duplicate, implementations must support idempotent behavior.
type DataFlowProcessor func(context context.Context, flow *DataFlow, sdk *DataPlaneSDK, options *ProcessorOptions) (*DataFlowStatusMessage, error)

// ProcessorOptions are passed to a DataFlowProcessor.
type ProcessorOptions struct {
	// Duplicate is true if the flow already reached the requested state and the message is processed again.
	Duplicate bool
	// DataAddress is the DataAddress received with the message, if any.
	DataAddress *DataAddress
}

// DataFlowHandler is an extension point for started, suspend, terminate and complete. It runs before the transition is
// persisted; an error aborts the operation and leaves the data flow unchanged.
type DataFlowHandler func(context.Context, *DataFlow) error

// LogMonitor receives the SDK's log messages. The default writes to the standard logger.
type LogMonitor interface {
	Println(v ...any)
	Printf(format string, v ...any)
}

var errNilResponse = errors.New("processor returned no response")

// DataPlaneSDK implements the data flow state machine of the Data Plane Signaling protocol. Create it with
// NewDataPlaneSDK and serve it with NewHandler.
type DataPlaneSDK struct {
	Store      DataplaneStore
	TrxContext TransactionContext
	Monitor    LogMonitor

	onPrepare   DataFlowProcessor
	onStart     DataFlowProcessor
	onStarted   DataFlowHandler
	onResume    DataFlowProcessor
	onTerminate DataFlowHandler
	onSuspend   DataFlowHandler
	onComplete  DataFlowHandler

	controlPlanes      ControlPlaneResolver
	controlPlaneClient ControlPlaneClient
	controlPlaneStore  ControlPlaneStore
	staticControlPlane *ControlPlane

	authorizations        map[string]Authorization
	authorizationDisabled bool
	insecureEndpoints     bool
}

// Prepare is called on the consumer to prepare for receiving data.
// It invokes the onPrepare processor and persists the created flow. For push transfers the response carries the
// DataAddress the provider pushes data to.
func (dsdk *DataPlaneSDK) Prepare(ctx context.Context, message DataFlowPrepareMessage) (*DataFlowStatusMessage, error) {
	if err := message.Validate(); err != nil {
		return nil, err
	}
	id := message.DataFlowID
	// read before execute: a TransactionContext may run the callback with a context that lacks the caller's values
	controlPlaneID := ControlPlaneIDFromContext(ctx)
	var response *DataFlowStatusMessage
	err := dsdk.execute(ctx, func(ctx context.Context) error {
		flow, err := dsdk.Store.FindById(ctx, id)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return fmt.Errorf("performing de-duplication for %s: %w", id, err)
		}

		if flow != nil {
			if err := dsdk.checkCaller(controlPlaneID, flow); err != nil {
				return err
			}
		}
		switch {
		case flow != nil && (flow.State == Preparing || flow.State == Prepared):
			// duplicate message, pass to processor to generate a data address if needed
			response, err = dsdk.process(ctx, dsdk.onPrepare, flow, &ProcessorOptions{Duplicate: true})
			if err != nil {
				return err
			}
			if err := dsdk.prepareState(response, flow); err != nil {
				return err
			}
			if err := dsdk.Store.Save(ctx, flow); err != nil {
				return fmt.Errorf("saving data flow %s: %w", flow.ID, err)
			}
			return nil
		case flow != nil:
			return fmt.Errorf("%w: data flow %s is not in PREPARING or PREPARED state but in %s", ErrConflict, flow.ID, flow.State)
		}

		flow, err = newFlow(controlPlaneID, message.DataFlowBaseMessage, true)
		if err != nil {
			return err
		}

		response, err = dsdk.process(ctx, dsdk.onPrepare, flow, &ProcessorOptions{})
		if err != nil {
			return err
		}
		if err := dsdk.prepareState(response, flow); err != nil {
			return err
		}
		if err := dsdk.Store.Create(ctx, flow); err != nil {
			return fmt.Errorf("creating data flow %s: %w", flow.ID, err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return response, nil
}

// Start is called on the provider and starts a data flow based on the given start message.
// It invokes the onStart processor and persists the created flow. For pull transfers the response carries the
// DataAddress the consumer obtains the data from. A start message for an existing data flow is rejected with ErrConflict.
func (dsdk *DataPlaneSDK) Start(ctx context.Context, message DataFlowStartMessage) (*DataFlowStatusMessage, error) {
	if err := message.Validate(); err != nil {
		return nil, err
	}
	id := message.DataFlowID
	controlPlaneID := ControlPlaneIDFromContext(ctx)
	var response *DataFlowStatusMessage
	err := dsdk.execute(ctx, func(ctx context.Context) error {
		existing, err := dsdk.Store.FindById(ctx, id)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return fmt.Errorf("performing de-duplication for %s: %w", id, err)
		}
		if existing != nil {
			return fmt.Errorf("%w: data flow %s already exists", ErrConflict, id)
		}

		flow, err := newFlow(controlPlaneID, message.DataFlowBaseMessage, false)
		if err != nil {
			return err
		}
		switch {
		case flow.IsPull() && message.DataAddress != nil:
			return fmt.Errorf("%w: a start message for a pull transfer must not contain a data address", ErrInvalidInput)
		case flow.IsPush() && message.DataAddress == nil:
			return fmt.Errorf("%w: a start message for a push transfer must contain a data address", ErrInvalidInput)
		}
		dsdk.setReceivedAddress(flow, message.DataAddress)

		response, err = dsdk.process(ctx, dsdk.onStart, flow, &ProcessorOptions{DataAddress: message.DataAddress})
		if err != nil {
			return err
		}
		if err := dsdk.startState(response, flow); err != nil {
			return err
		}
		if err := dsdk.Store.Create(ctx, flow); err != nil {
			return fmt.Errorf("creating data flow %s: %w", flow.ID, err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return response, nil
}

// Started is called on the consumer when the provider has started the transfer. For pull transfers the message must
// carry the DataAddress to obtain the data from; for push transfers it must not carry one. It records the received
// DataAddress on the flow (SourceDataAddress), invokes the started handler (WithStartedProcessor), transitions the
// flow to STARTED and persists it. A duplicate message for a STARTED flow is acknowledged without invoking the handler.
func (dsdk *DataPlaneSDK) Started(ctx context.Context, id string, message DataFlowStartedNotificationMessage) (*DataFlowStatusMessage, error) {
	if err := message.Validate(); err != nil {
		return nil, err
	}
	var response *DataFlowStatusMessage
	callerID := ControlPlaneIDFromContext(ctx)
	err := dsdk.execute(ctx, func(ctx context.Context) error {
		flow, err := dsdk.Store.FindById(ctx, id)
		if err != nil {
			return fmt.Errorf("starting data flow %s: %w", id, err)
		}
		if err := dsdk.checkCaller(callerID, flow); err != nil {
			return err
		}
		if !flow.Consumer {
			return fmt.Errorf("%w: started is only valid for consumer data flows", ErrInvalidInput)
		}
		switch {
		case flow.IsPull() && message.DataAddress == nil:
			return fmt.Errorf("%w: a started message for a pull transfer must contain a data address", ErrInvalidInput)
		case flow.IsPush() && message.DataAddress != nil:
			return fmt.Errorf("%w: a started message for a push transfer must not contain a data address", ErrInvalidInput)
		}

		// the consumer already handed out its DataAddress (if any) in the prepare response, so the response never
		// carries one
		response = &DataFlowStatusMessage{MessageID: uuid.NewString(), DataFlowID: flow.ID, State: Started}
		switch flow.State {
		case Started:
			return nil // duplicate message, skip processing
		case Prepared, Starting:
		default:
			return fmt.Errorf("%w: data flow %s cannot be started from state %s", ErrInvalidTransition, flow.ID, flow.State)
		}

		dsdk.setReceivedAddress(flow, message.DataAddress)
		if err := dsdk.onStarted(ctx, flow); err != nil {
			return fmt.Errorf("starting data flow %s: %w", flow.ID, err)
		}
		if err := flow.TransitionToStarted(); err != nil {
			return err
		}
		if err := dsdk.Store.Save(ctx, flow); err != nil {
			return fmt.Errorf("saving data flow %s: %w", flow.ID, err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return response, nil
}

// Suspend suspends a STARTED data flow on request of the control plane.
func (dsdk *DataPlaneSDK) Suspend(ctx context.Context, id string, message DataFlowSuspendMessage) error {
	if err := message.Validate(); err != nil {
		return err
	}
	callerID := ControlPlaneIDFromContext(ctx)
	return dsdk.execute(ctx, func(ctx context.Context) error {
		flow, err := dsdk.Store.FindById(ctx, id)
		if err != nil {
			return fmt.Errorf("suspending data flow %s: %w", id, err)
		}
		if err := dsdk.checkCaller(callerID, flow); err != nil {
			return err
		}

		if flow.State == Suspended {
			return nil // duplicate message, skip processing
		}
		if flow.State != Started {
			return fmt.Errorf("%w: cannot transition from %v to SUSPENDED", ErrInvalidTransition, flow.State)
		}

		if err := dsdk.onSuspend(ctx, flow); err != nil {
			return fmt.Errorf("suspending data flow %s: %w", flow.ID, err)
		}
		if err := flow.TransitionToSuspended(message.Reason, SuspendedByRemote); err != nil {
			return err
		}
		if err := dsdk.Store.Save(ctx, flow); err != nil {
			return fmt.Errorf("suspending data flow %s: %w", flow.ID, err)
		}
		return nil
	})
}

// Resume resumes a data flow that was suspended by the control plane. The message may carry a new DataAddress of the
// counterparty. The response carries the DataAddress this data plane provides to the counterparty, if any.
func (dsdk *DataPlaneSDK) Resume(ctx context.Context, id string, message DataFlowResumeMessage) (*DataFlowStatusMessage, error) {
	if err := message.Validate(); err != nil {
		return nil, err
	}
	var response *DataFlowStatusMessage
	callerID := ControlPlaneIDFromContext(ctx)
	err := dsdk.execute(ctx, func(ctx context.Context) error {
		flow, err := dsdk.Store.FindById(ctx, id)
		if err != nil {
			return fmt.Errorf("resuming data flow %s: %w", id, err)
		}
		if err := dsdk.checkCaller(callerID, flow); err != nil {
			return err
		}

		if flow.State != Suspended {
			return fmt.Errorf("%w: data flow %s cannot be resumed from state %s", ErrInvalidTransition, flow.ID, flow.State)
		}
		if flow.SuspendedBy != SuspendedByRemote {
			return fmt.Errorf("%w: data flow %s was suspended by the %s party and can only be resumed by it", ErrInvalidTransition, flow.ID, flow.SuspendedBy)
		}

		if message.DataAddress != nil {
			dsdk.setReceivedAddress(flow, message.DataAddress)
		}
		response, err = dsdk.process(ctx, dsdk.onResume, flow, &ProcessorOptions{DataAddress: message.DataAddress})
		if err != nil {
			return err
		}
		if response.State != Started {
			return fmt.Errorf("onResume returned an invalid state %s", response.State)
		}
		if err := flow.TransitionToResumed(SuspendedByRemote); err != nil {
			return err
		}
		if err := dsdk.Store.Save(ctx, flow); err != nil {
			return fmt.Errorf("resuming data flow %s: %w", flow.ID, err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return response, nil
}

// Terminate terminates a data flow. COMPLETED flows cannot be terminated.
func (dsdk *DataPlaneSDK) Terminate(ctx context.Context, id string, message DataFlowTerminateMessage) error {
	if err := message.Validate(); err != nil {
		return err
	}
	callerID := ControlPlaneIDFromContext(ctx)
	return dsdk.execute(ctx, func(ctx context.Context) error {
		flow, err := dsdk.Store.FindById(ctx, id)
		if err != nil {
			return fmt.Errorf("terminating data flow %s: %w", id, err)
		}
		if err := dsdk.checkCaller(callerID, flow); err != nil {
			return err
		}

		if flow.State == Terminated {
			return nil // duplicate message, skip processing
		}
		if flow.State == Completed {
			return fmt.Errorf("%w: data flow %s is already COMPLETED", ErrInvalidTransition, flow.ID)
		}

		if err := dsdk.onTerminate(ctx, flow); err != nil {
			return fmt.Errorf("terminating data flow %s: %w", flow.ID, err)
		}
		if err := flow.TransitionToTerminated(message.Reason); err != nil {
			return err
		}
		if err := dsdk.Store.Save(ctx, flow); err != nil {
			return fmt.Errorf("terminating data flow %s: %w", flow.ID, err)
		}
		return nil
	})
}

// Status returns the data flow with the given id.
func (dsdk *DataPlaneSDK) Status(ctx context.Context, id string) (*DataFlow, error) {
	var flow *DataFlow
	callerID := ControlPlaneIDFromContext(ctx)
	err := dsdk.execute(ctx, func(ctx context.Context) error {
		found, err := dsdk.Store.FindById(ctx, id)
		if err != nil {
			return err
		}
		if err := dsdk.checkCaller(callerID, found); err != nil {
			return err
		}
		flow = found
		return nil
	})
	return flow, err
}

// Complete marks a STARTED data flow as COMPLETED and invokes the onComplete processor.
func (dsdk *DataPlaneSDK) Complete(ctx context.Context, id string) error {
	if id == "" {
		return fmt.Errorf("%w: data flow id cannot be empty", ErrInvalidInput)
	}

	callerID := ControlPlaneIDFromContext(ctx)
	return dsdk.execute(ctx, func(ctx context.Context) error {
		flow, err := dsdk.Store.FindById(ctx, id)
		if err != nil {
			return fmt.Errorf("completing data flow %s: %w", id, err)
		}
		if err := dsdk.checkCaller(callerID, flow); err != nil {
			return err
		}

		if flow.State == Completed { // de-duplication
			return nil
		}

		if err := flow.TransitionToCompleted(); err != nil {
			return err
		}
		// only invoked if the transition was successful
		if err := dsdk.onComplete(ctx, flow); err != nil {
			return fmt.Errorf("completing data flow %s: %w", flow.ID, err)
		}
		if err := dsdk.Store.Save(ctx, flow); err != nil {
			return fmt.Errorf("completing data flow %s: %w", flow.ID, err)
		}
		return nil
	})
}

// newFlow creates a data flow from the message for the given control plane.
func newFlow(controlPlaneID string, message DataFlowBaseMessage, consumer bool) (*DataFlow, error) {
	flow, err := NewDataFlowBuilder().ID(message.DataFlowID).
		Consumer(consumer).
		State(Initialized).
		AgreementID(message.AgreementID).
		DatasetID(message.DatasetID).
		ParticipantID(message.ParticipantID).
		CounterpartyID(message.CounterPartyID).
		DataspaceContext(message.DataspaceContext).
		Profile(message.Profile).
		Claims(message.Claims).
		Labels(message.Labels).
		Metadata(message.Metadata).
		ControlPlaneID(controlPlaneID).
		Build()
	if err != nil {
		return nil, fmt.Errorf("creating data flow: %w", err)
	}
	return flow, nil
}

// process invokes the processor, fills in the message and data flow ids, and keeps the DataAddress only where the
// specification allows this data plane to send one: consumer-push and provider-pull. The returned address is
// recorded on the flow.
func (dsdk *DataPlaneSDK) process(ctx context.Context, processor DataFlowProcessor, flow *DataFlow, options *ProcessorOptions) (*DataFlowStatusMessage, error) {
	response, err := processor(ctx, flow, dsdk, options)
	if err != nil {
		return nil, fmt.Errorf("processing data flow %s: %w", flow.ID, err)
	}
	if response == nil {
		return nil, fmt.Errorf("processing data flow %s: %w", flow.ID, errNilResponse)
	}
	response.MessageID = uuid.NewString()
	response.DataFlowID = flow.ID
	if providesAddress(flow) {
		if response.DataAddress != nil {
			setProvidedAddress(flow, response.DataAddress)
		} else {
			response.DataAddress = providedAddress(flow).Copy()
		}
	} else {
		response.DataAddress = nil
	}
	return response, nil
}

// providesAddress is true for flows where this data plane hands a DataAddress to the counterparty: the consumer
// receives pushed data, and the provider serves pulled data.
func providesAddress(flow *DataFlow) bool {
	return (flow.Consumer && flow.IsPush()) || (!flow.Consumer && flow.IsPull())
}

func providedAddress(flow *DataFlow) *DataAddress {
	if flow.Consumer {
		return flow.DestinationDataAddress
	}
	return flow.SourceDataAddress
}

func setProvidedAddress(flow *DataFlow, address *DataAddress) {
	if flow.Consumer {
		flow.DestinationDataAddress = address.Copy()
	} else {
		flow.SourceDataAddress = address.Copy()
	}
}

// setReceivedAddress records a DataAddress received from the counterparty: where the provider pushes to, or where the
// consumer pulls from.
func (dsdk *DataPlaneSDK) setReceivedAddress(flow *DataFlow, address *DataAddress) {
	if address == nil {
		return
	}
	if flow.Consumer {
		flow.SourceDataAddress = address.Copy()
	} else {
		flow.DestinationDataAddress = address.Copy()
	}
}

func (dsdk *DataPlaneSDK) prepareState(response *DataFlowStatusMessage, flow *DataFlow) error {
	switch response.State {
	case Prepared:
		return flow.TransitionToPrepared()
	case Preparing:
		return flow.TransitionToPreparing()
	default:
		return fmt.Errorf("onPrepare returned an invalid state %s", response.State)
	}
}

func (dsdk *DataPlaneSDK) startState(response *DataFlowStatusMessage, flow *DataFlow) error {
	switch response.State {
	case Started:
		return flow.TransitionToStarted()
	case Starting:
		return flow.TransitionToStarting()
	default:
		return fmt.Errorf("onStart returned an invalid state %s", response.State)
	}
}

func (dsdk *DataPlaneSDK) execute(ctx context.Context, callback func(ctx2 context.Context) error) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return dsdk.TrxContext.Execute(ctx, callback)
	}
}

// DataPlaneSDKOption configures a DataPlaneSDK instance
type DataPlaneSDKOption func(*DataPlaneSDK)

// WithStore sets the data flow store (required).
func WithStore(store DataplaneStore) DataPlaneSDKOption {
	return func(sdk *DataPlaneSDK) {
		sdk.Store = store
	}
}

// WithTransactionContext sets the transaction context every SDK operation runs in (required).
func WithTransactionContext(trxContext TransactionContext) DataPlaneSDKOption {
	return func(sdk *DataPlaneSDK) {
		sdk.TrxContext = trxContext
	}
}

// WithMonitor sets where the SDK logs to.
func WithMonitor(monitor LogMonitor) DataPlaneSDKOption {
	return func(sdk *DataPlaneSDK) {
		sdk.Monitor = monitor
	}
}

// WithPrepareProcessor sets the processor invoked for /prepare on the consumer. It reports PREPARED, or PREPARING to
// complete asynchronously with NotifyPrepared. The default reports PREPARED.
func WithPrepareProcessor(processor DataFlowProcessor) DataPlaneSDKOption {
	return func(sdk *DataPlaneSDK) {
		sdk.onPrepare = processor
	}
}

// WithStartProcessor sets the processor invoked for /start on the provider. It reports STARTED, or STARTING to
// complete asynchronously with NotifyStarted; for pull transfers it provides the DataAddress the consumer pulls from.
// The default reports STARTED.
func WithStartProcessor(processor DataFlowProcessor) DataPlaneSDKOption {
	return func(sdk *DataPlaneSDK) {
		sdk.onStart = processor
	}
}

// WithStartedProcessor sets the handler invoked for /started on the consumer, e.g. to start pulling data or to accept
// pushed data. For pull transfers the received DataAddress is on the flow (SourceDataAddress) when it runs. An error
// aborts the operation and leaves the flow unchanged; otherwise the flow transitions to STARTED.
func WithStartedProcessor(handler DataFlowHandler) DataPlaneSDKOption {
	return func(sdk *DataPlaneSDK) {
		sdk.onStarted = handler
	}
}

// WithResumeProcessor sets the processor invoked for /resume; it must report STARTED. The default reports STARTED.
func WithResumeProcessor(processor DataFlowProcessor) DataPlaneSDKOption {
	return func(sdk *DataPlaneSDK) {
		sdk.onResume = processor
	}
}

// WithTerminateProcessor sets the handler invoked for /terminate.
func WithTerminateProcessor(handler DataFlowHandler) DataPlaneSDKOption {
	return func(sdk *DataPlaneSDK) {
		sdk.onTerminate = handler
	}
}

// WithSuspendProcessor sets the handler invoked for /suspend.
func WithSuspendProcessor(handler DataFlowHandler) DataPlaneSDKOption {
	return func(sdk *DataPlaneSDK) {
		sdk.onSuspend = handler
	}
}

// WithCompleteProcessor sets the handler invoked for /completed.
func WithCompleteProcessor(handler DataFlowHandler) DataPlaneSDKOption {
	return func(sdk *DataPlaneSDK) {
		sdk.onComplete = handler
	}
}

// NewDataPlaneSDK creates an SDK instance. WithStore and WithTransactionContext are required; the processors default
// to reporting the target state without further action.
func NewDataPlaneSDK(options ...DataPlaneSDKOption) (*DataPlaneSDK, error) {
	sdk := &DataPlaneSDK{}

	// Apply all options
	for _, opt := range options {
		opt(sdk)
	}

	// Validate required fields
	if sdk.Store == nil {
		return nil, errors.New("store is required")
	}
	if sdk.TrxContext == nil {
		return nil, errors.New("transaction context is required")
	}

	// Set defaults for optional fields
	if sdk.Monitor == nil {
		sdk.Monitor = defaultLogMonitor{}
	}
	if sdk.onPrepare == nil {
		sdk.onPrepare = stateProcessor(Prepared)
	}
	if sdk.onStart == nil {
		sdk.onStart = stateProcessor(Started)
	}
	if sdk.onStarted == nil {
		sdk.onStarted = noopHandler
	}
	if sdk.onResume == nil {
		sdk.onResume = stateProcessor(Started)
	}
	if sdk.onTerminate == nil {
		sdk.onTerminate = noopHandler
	}
	if sdk.onSuspend == nil {
		sdk.onSuspend = noopHandler
	}
	if sdk.onComplete == nil {
		sdk.onComplete = noopHandler
	}
	if sdk.controlPlaneClient == nil {
		sdk.controlPlaneClient = NewHTTPControlPlaneClient()
	}
	// the HTTP client authorizes its requests with the SDK's registered Authorizations unless configured otherwise
	if client, ok := sdk.controlPlaneClient.(*HTTPControlPlaneClient); ok && client.authorize == nil {
		client.authorize = sdk.authorizationHeader
	}
	if sdk.staticControlPlane != nil {
		if err := sdk.checkEndpoint(sdk.staticControlPlane.Endpoint); err != nil {
			return nil, err
		}
	}
	if sdk.controlPlanes == nil && sdk.controlPlaneStore != nil {
		sdk.controlPlanes = storeResolver{store: sdk.controlPlaneStore}
	}
	return sdk, nil
}

// stateProcessor returns a processor that only reports the given state.
func stateProcessor(state DataFlowState) DataFlowProcessor {
	return func(context.Context, *DataFlow, *DataPlaneSDK, *ProcessorOptions) (*DataFlowStatusMessage, error) {
		return &DataFlowStatusMessage{State: state}, nil
	}
}

func noopHandler(context.Context, *DataFlow) error {
	return nil
}

type defaultLogMonitor struct {
}

func (d defaultLogMonitor) Println(v ...any) {
	log.Println(v...)
}

func (d defaultLogMonitor) Printf(format string, v ...any) {
	log.Printf(format, v...)
}
